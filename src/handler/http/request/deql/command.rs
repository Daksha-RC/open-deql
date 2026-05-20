//! DeQL Command Execute endpoint — POST /{org_id}/deql/{aggregate}/{commandname}
//!
//! Implements REQ-CMD-001–010 and all acceptance criteria from spec.md

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes;
use infra::db::ORM_CLIENT;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tracing;

use crate::common::meta::ingestion;

/// Query parameters for async mode check.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct AsyncModeQuery {
    pub mode: Option<String>,
}

/// Execute a DeQL command against an aggregate.
///
/// **Endpoint:** `POST /{org_id}/deql/{aggregate}/{commandname}`
///
/// **Request body:** Flat JSON object with command parameters.
/// **Response:** 200 with `{events: [...]}` on success; 400/404/422 on error.
#[utoipa::path(
    post,
    path = "/{org_id}/deql/{aggregate}/{commandname}",
    context_path = "/api",
    tag = "DeQL",
    operation_id = "ExecuteDeqlCommand",
    summary = "Execute a DeQL command",
    description = "Execute a command against an aggregate. Command parameters are passed as flat JSON properties in the request body.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("aggregate" = String, Path, description = "Aggregate name"),
        ("commandname" = String, Path, description = "Command name"),
        ("mode" = Option<String>, Query, description = "Query mode; 'async' returns 501"),
    ),
    request_body(content = Object, description = "Flat JSON object with command parameters"),
    responses(
        (status = StatusCode::OK, description = "Command executed successfully"),
        (status = StatusCode::BAD_REQUEST, description = "Invalid payload or request"),
        (status = StatusCode::NOT_FOUND, description = "Command or aggregate not found"),
        (status = StatusCode::UNPROCESSABLE_ENTITY, description = "Business logic rejection (guard failed)"),
        (status = StatusCode::NOT_IMPLEMENTED, description = "Async mode not yet supported"),
    )
)]
#[cfg(feature = "deql")]
pub async fn execute(
    Path((org_id, aggregate, commandname)): Path<(String, String, String)>,
    Query(mode_query): Query<AsyncModeQuery>,
    Json(body): Json<Map<String, Value>>,
) -> Response {
    use o2_deql::RehydrateServiceImpl;

    use super::super::dereg::get_deql_state;
    use crate::deql::RehydrateService;

    // Get the global DeQL state from handler module
    let deql_state = get_deql_state().await;

    // Step a: Check for async mode (not yet supported)
    if mode_query.mode.as_deref() == Some("async") {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({
                "error": "Async mode (mode=async) is not yet implemented"
            })),
        )
            .into_response();
    }

    tracing::info!(
        org_id = %org_id,
        aggregate = %aggregate,
        command = %commandname,
        "DeQL command execution requested"
    );

    // Step b: Get org's DeReg registry from global OrgDeRegMap singleton
    let org_dereg = deql_state.org_map.get_or_init(&org_id).await;

    // Step c: Get DeReg read lock (do NOT hold during I/O)
    let dereg = org_dereg.read().await;

    // Step d: Check if registry is empty and needs rehydration
    // The registry is empty on first access and must be populated from the audit log
    let registry_empty = dereg.aggregate_count() == 0;
    drop(dereg); // Release read lock before I/O

    if registry_empty {
        tracing::debug!(org_id = %org_id, "Registry empty, triggering rehydration from audit log");

        // Rehydrate the registry from the persisted audit log
        let db = Arc::new(
            ORM_CLIENT
                .get_or_init(infra::db::connect_to_orm)
                .await
                .clone(),
        );
        let service: Arc<dyn RehydrateService> = Arc::new(RehydrateServiceImpl::new(
            db,
            Arc::new(deql_state.org_map.clone_for_service()),
            Arc::new(deql_state.lock_map.clone_for_service()),
            deql_state.rehydrate_state_map.clone(),
        ));

        match service.rehydrate_org(&org_id, None, None).await {
            Ok(result) => {
                tracing::info!(
                    org_id = %org_id,
                    rows_processed = result.rows_processed,
                    elapsed_ms = result.elapsed_ms,
                    "Registry rehydrated successfully"
                );
            }
            Err(crate::deql::RehydrateError::InProgress) => {
                // Rehydration already running, wait a moment and let command proceed with partial
                // state
                tracing::warn!(org_id = %org_id, "Rehydration already in progress, command proceeding with partial state");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            Err(e) => {
                tracing::error!(org_id = %org_id, error = ?e, "Rehydration failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": "Failed to load registry",
                        "details": format!("Registry rehydration failed: {:?}", e)
                    })),
                )
                    .into_response();
            }
        }
    }

    // Now that registry is (hopefully) populated, look up the command
    let dereg = org_dereg.read().await;

    // Step e: Look up aggregate (just to verify it exists) - case-insensitive
    let _agg = match dereg.get_aggregate_ci(&aggregate) {
        Some(a) => a,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "error": "Aggregate not found",
                    "details": format!("Aggregate '{}' does not exist", aggregate)
                })),
            )
                .into_response();
        }
    };

    // Step f: Look up command in registry - case-insensitive
    let cmd_def = match dereg.get_command_ci(&commandname) {
        Some(c) => c,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "error": "Command not found",
                    "details": format!(
                        "Command '{}' not found in registry",
                        commandname
                    )
                })),
            )
                .into_response();
        }
    };

    // Step f: Validate command payload
    let validation_errors = crate::deql::validate_command_payload(cmd_def, &body);
    if !validation_errors.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Command payload validation failed",
                "details": validation_errors
            })),
        )
            .into_response();
    }

    // Step g: Build Execute AST
    let execute_ast = match build_execute_ast(&commandname, &body) {
        Ok(ast) => ast,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Failed to build command AST",
                    "details": e
                })),
            )
                .into_response();
        }
    };

    // Step h: Execute the command (release dereg lock BEFORE async I/O)
    let org_id_str = org_id.clone();
    let aggregate_str = aggregate.clone();

    // Clone the dereg for the execution (still holding the read lock here)
    let org_dereg_cloned = org_dereg.clone();
    drop(dereg); // CRITICAL: Release the read lock before async I/O

    // Now execute with the cloned org_dereg (read lock not held during execution)
    let execution_result =
        crate::deql::execute_command(&execute_ast, org_dereg_cloned, &org_id_str).await;

    // Step i: Format response (and persist events)
    match execution_result {
        Ok(crate::deql::ExecutionResult::Success(exec_success)) => {
            tracing::info!(
                org_id = %org_id_str,
                aggregate = %aggregate_str,
                command = %commandname,
                event_count = exec_success.events.len(),
                "DeQL command executed successfully"
            );

            // Step j: Event ingestion — persist emitted events to OpenObserve logs
            // Build response with field masking: SENSITIVE → null in response, VOLATILE → kept in
            // response
            let mut response_events = Vec::new();
            let mut ingest_events = Vec::new();

            for event in exec_success.events {
                // Build response version (null out SENSITIVE fields)
                let mut response_fields = event.fields.clone();
                for sensitive_field in &event.sensitive_fields {
                    if let Some(pos) = response_fields
                        .iter()
                        .position(|(k, _)| k == sensitive_field)
                    {
                        response_fields[pos].1 = serde_json::Value::Null;
                    }
                }

                let response_event = json!({
                    "event_type": event.event_type,
                    "stream_id": event.stream_id,
                    "fields": response_fields.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<serde_json::Map<String, serde_json::Value>>(),
                    "volatile_fields": event.volatile_fields,
                    "sensitive_fields": event.sensitive_fields,
                });
                response_events.push(response_event);

                // Build ingest version (null out VOLATILE fields, keep SENSITIVE)
                let mut ingest_fields = event.fields.clone();
                for volatile_field in &event.volatile_fields {
                    if let Some(pos) = ingest_fields.iter().position(|(k, _)| k == volatile_field) {
                        ingest_fields[pos].1 = serde_json::Value::Null;
                    }
                }

                // Create log record with metadata
                let mut log_record = serde_json::Map::new();

                // Mandatory metadata fields for DeQL events
                log_record.insert(
                    "deql_org_id".to_string(),
                    serde_json::Value::String(org_id_str.clone()),
                );
                log_record.insert(
                    "deql_aggregate".to_string(),
                    serde_json::Value::String(aggregate_str.clone()),
                );
                log_record.insert(
                    "deql_command".to_string(),
                    serde_json::Value::String(commandname.clone()),
                );
                log_record.insert(
                    "deql_event_type".to_string(),
                    serde_json::Value::String(event.event_type.clone()),
                );
                log_record.insert(
                    "deql_stream_id".to_string(),
                    serde_json::Value::String(event.stream_id.clone()),
                );

                // Timestamp (microseconds) for event ordering
                let now_micros = config::utils::time::now_micros();
                log_record.insert(
                    "_timestamp".to_string(),
                    serde_json::Value::Number(now_micros.into()),
                );

                // Add all business fields (with VOLATILE nulled, SENSITIVE preserved)
                for (field_name, field_value) in ingest_fields {
                    log_record.insert(field_name, field_value);
                }

                ingest_events.push(serde_json::Value::Object(log_record));
            }

            // Persist events to OpenObserve logs
            if !ingest_events.is_empty() {
                let stream_name = format!(
                    "deql_{}_{}",
                    aggregate_str.to_lowercase(),
                    if ingest_events.len() == 1 {
                        ingest_events[0]
                            .get("deql_event_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("event")
                            .to_lowercase()
                    } else {
                        // Multiple event types in one response - use a generic suffix
                        "events".to_string()
                    }
                );

                let bytes = bytes::Bytes::from(
                    serde_json::to_string(&ingest_events).unwrap_or_else(|_| "[]".to_string()),
                );
                let req = ingestion::IngestionRequest::Usage(bytes);

                // Use a static thread_id of 0 for DeQL ingestion
                match crate::service::logs::ingest::ingest(
                    0,
                    &org_id_str,
                    &stream_name,
                    req,
                    ingestion::IngestUser::SystemJob(ingestion::SystemJobType::DeqlRuntime),
                    None,
                    false,
                )
                .await
                {
                    Ok(resp) if resp.code == 200 => {
                        tracing::info!(
                            org_id = %org_id_str,
                            stream = %stream_name,
                            event_count = ingest_events.len(),
                            "DeQL events persisted to logs"
                        );
                    }
                    Ok(resp) => {
                        tracing::warn!(
                            org_id = %org_id_str,
                            stream = %stream_name,
                            code = resp.code,
                            error = %resp.error.unwrap_or_default(),
                            "Failed to persist DeQL events to logs"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            org_id = %org_id_str,
                            stream = %stream_name,
                            error = %e,
                            "Error ingesting DeQL events"
                        );
                    }
                }
            }

            (
                StatusCode::OK,
                Json(json!({
                    "events": response_events,
                    "meta": {
                        "org_id": org_id_str,
                        "aggregate": aggregate_str,
                        "command": commandname,
                        "count": response_events.len()
                    }
                })),
            )
                .into_response()
        }
        Ok(crate::deql::ExecutionResult::Rejected(rejection)) => {
            tracing::warn!(
                org_id = %org_id_str,
                aggregate = %aggregate_str,
                command = %commandname,
                decision = %rejection.decision_name,
                guard = %rejection.guard_expression,
                "DeQL command rejected by guard"
            );

            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "Command rejected",
                    "details": "Guard condition evaluated to false",
                    "decision": rejection.decision_name,
                    "guard": rejection.guard_expression,
                    "meta": {
                        "org_id": org_id_str,
                        "aggregate": aggregate_str,
                        "command": commandname
                    }
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(
                org_id = %org_id_str,
                aggregate = %aggregate_str,
                command = %commandname,
                error = %e,
                "DeQL command execution failed"
            );

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Command execution failed",
                    "details": e.to_string(),
                    "meta": {
                        "org_id": org_id_str,
                        "aggregate": aggregate_str,
                        "command": commandname
                    }
                })),
            )
                .into_response()
        }
    }
}

/// Stub implementation when DeQL feature is not enabled.
#[cfg(not(feature = "deql"))]
pub async fn execute(
    Path((org_id, aggregate, commandname)): Path<(String, String, String)>,
    _mode_query: Query<AsyncModeQuery>,
    _body: Json<Map<String, Value>>,
) -> Response {
    tracing::warn!(
        org_id = %org_id,
        aggregate = %aggregate,
        command = %commandname,
        "DeQL support is not compiled (enable 'deql' feature)"
    );

    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "DeQL support is not compiled",
            "details": "Enable the 'deql' feature flag when building openobserve"
        })),
    )
        .into_response()
}

/// Build an Execute AST from command name and parameters.
#[cfg(feature = "deql")]
fn build_execute_ast(
    commandname: &str,
    payload: &Map<String, Value>,
) -> Result<crate::deql::Execute, String> {
    use crate::deql::{Assignment, Execute, Span, Spanned};

    let mut assignments = Vec::new();
    for (key, value) in payload {
        let value_str = match value {
            Value::String(s) => format!("'{s}'"),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
            _ => {
                return Err(format!(
                    "Unsupported value type for field '{}': complex types not allowed",
                    key
                ));
            }
        };

        assignments.push(Assignment {
            field: Spanned {
                node: key.clone(),
                span: Span { start: 0, end: 0 },
            },
            value: Spanned {
                node: value_str,
                span: Span { start: 0, end: 0 },
            },
        });
    }

    Ok(Execute {
        command: Spanned {
            node: commandname.to_string(),
            span: Span { start: 0, end: 0 },
        },
        assignments,
    })
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::post,
    };
    use tower::ServiceExt;

    use super::execute;

    #[tokio::test]
    async fn command_execute_returns_400_for_missing_command() {
        let app = Router::new().route("/{org_id}/deql/{aggregate}/command", post(execute));

        let req = Request::builder()
            .method("POST")
            .uri("/o1/deql/bank_account/command")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn command_execute_returns_202_for_async_mode() {
        let app = Router::new().route("/{org_id}/deql/{aggregate}/command", post(execute));

        let req = Request::builder()
            .method("POST")
            .uri("/o1/deql/bank_account/command")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"mode":"async","command":"Deposit"}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn command_execute_returns_200_for_sync_mode() {
        let app = Router::new().route("/{org_id}/deql/{aggregate}/command", post(execute));

        let req = Request::builder()
            .method("POST")
            .uri("/o1/deql/bank_account/command")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"mode":"sync","command":"Deposit","payload":{"amount":100}}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
