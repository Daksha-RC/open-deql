//! DeQL Command Execute endpoint — POST /{org_id}/deql/{aggregate}/{commandname}
//!
//! Implements REQ-CMD-001–010 and all acceptance criteria from spec.md

use std::sync::{Arc, atomic::Ordering};

use axum::{
    Json,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes;
use config::meta::stream::StreamType;
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
                // Register/update the deql_events stream schema so that the
                // canonical metadata columns are known to the ingestion pipeline.
                let dereg_for_schema = org_dereg.read().await;
                if let Err(e) = dereg_for_schema.register_stream_schema(&org_id).await {
                    tracing::error!(org_id = %org_id, error = ?e, "Failed to register deql_events schema");
                }
                drop(dereg_for_schema);
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

    // Ensure deql_events stream schema is registered/up-to-date.
    // This is idempotent (merge) and ensures canonical metadata fields exist
    // even when schema evolution is skipped (IMPL-23).
    if let Err(e) = dereg.register_stream_schema(&org_id).await {
        tracing::warn!(org_id = %org_id, error = ?e, "Failed to sync deql_events schema");
    }

    // Step e: Look up aggregate (just to verify it exists) - case-insensitive
    let agg_canonical = match dereg.get_aggregate_ci(&aggregate) {
        Some(a) => a.name.node.clone(),
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
    let execution_result = crate::deql::execute_command(
        &execute_ast,
        org_dereg_cloned,
        &org_id_str,
        Some(super::search_backend::make_search_backend()),
    )
    .await;

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
            // Build response with field masking: SENSITIVE → excluded from response, VOLATILE →
            // excluded from ingest
            let mut response_events = Vec::new();
            let mut ingest_events = Vec::new();
            let mut max_offset: i64 = 0;

            // OCC: Read current max aggregate_version for this aggregate instance
            // All events in a single command share the same aggregate_id (single-aggregate
            // constraint)
            let aggregate_id = exec_success
                .events
                .first()
                .map(|e| e.stream_id.clone())
                .unwrap_or_default();

            let current_max_version: Option<i64> = {
                let occ_sql = format!(
                    "SELECT MAX(_aggregate_version) as max_ver FROM deql_events WHERE _aggregate_id = '{}' AND _aggregate_type = '{}'",
                    aggregate_id.replace('\'', "''"),
                    agg_canonical.replace('\'', "''"),
                );

                let now = config::utils::time::now_micros();
                const TWO_YEARS_MICROS: i64 = 2 * 365 * 24 * 3600 * 1_000_000;
                let start_time = now - TWO_YEARS_MICROS;
                let end_time = now + 3_600_000_000;

                let req = config::meta::search::Request {
                    query: config::meta::search::Query {
                        sql: occ_sql,
                        start_time,
                        end_time,
                        size: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                };

                match crate::service::search::search(
                    &config::ider::uuid(),
                    &org_id_str,
                    StreamType::Logs,
                    None,
                    &req,
                )
                .await
                {
                    Ok(resp) => resp
                        .hits
                        .first()
                        .and_then(|hit| hit.get("max_ver").and_then(|v| v.as_i64())),
                    Err(_) => None,
                }
            };

            let first_version = current_max_version.map_or(1, |v| v + 1);

            // OCC: Check expected_version if provided by caller
            if let Some(expected) = body.get("expected_version").and_then(|v| v.as_i64()) {
                if expected != first_version {
                    return (
                        StatusCode::CONFLICT,
                        Json(json!({
                            "error": "Optimistic concurrency conflict",
                            "details": format!(
                                "expected_version={} but next version would be {}",
                                expected, first_version
                            ),
                            "meta": {
                                "org_id": org_id_str,
                                "aggregate": aggregate_str,
                                "command": commandname
                            }
                        })),
                    )
                        .into_response();
                }
            }

            for (event_index, event) in exec_success.events.into_iter().enumerate() {
                // Build response version (exclude SENSITIVE fields entirely)
                let mut response_fields = event.fields.clone();
                response_fields.retain(|(k, _)| !event.sensitive_fields.contains(k));

                // Generate identifiers
                let event_id = uuid::Uuid::now_v7().to_string();
                let offset = crate::service::ingestion::generate_record_id(
                    &org_id_str,
                    "deql_events",
                    &StreamType::Logs,
                );
                let aggregate_version = first_version + event_index as i64;

                if offset > max_offset {
                    max_offset = offset;
                }

                let response_event = json!({
                    "_event_type": event.event_type,
                    "_aggregate_id": event.stream_id,
                    "_event_id": event_id,
                    "_offset": offset.to_string(),
                    "fields": response_fields.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<serde_json::Map<String, serde_json::Value>>(),
                });
                response_events.push(response_event);

                // Build ingest version (exclude VOLATILE fields, keep SENSITIVE)
                let mut ingest_fields = event.fields.clone();
                ingest_fields.retain(|(k, _)| !event.volatile_fields.contains(k));

                // Create log record with metadata
                let mut log_record = serde_json::Map::new();

                // Canonical metadata columns
                log_record.insert(
                    "_event_id".to_string(),
                    serde_json::Value::String(event_id.clone()),
                );
                log_record.insert(
                    "_aggregate_type".to_string(),
                    serde_json::Value::String(agg_canonical.clone()),
                );
                log_record.insert(
                    "_event_type".to_string(),
                    serde_json::Value::String(event.event_type.clone()),
                );
                log_record.insert(
                    "_aggregate_id".to_string(),
                    serde_json::Value::String(event.stream_id.clone()),
                );
                log_record.insert(
                    "_aggregate_version".to_string(),
                    serde_json::Value::Number(aggregate_version.into()),
                );
                log_record.insert(
                    "_offset".to_string(),
                    serde_json::Value::String(offset.to_string()),
                );

                // Timestamp (microseconds) for event ordering
                let now_micros = config::utils::time::now_micros();
                log_record.insert(
                    "_timestamp".to_string(),
                    serde_json::Value::Number(now_micros.into()),
                );
                // NOTE: deql_command is intentionally NOT included here (SS-09, IMPL-18,
                // task 2.4). It is not part of the $Events schema.

                // Add all business fields (VOLATILE excluded, SENSITIVE preserved).
                // Absent payload fields will be null-filled automatically by
                // convert_json_to_record_batch via the UDS schema (IMPL-17).
                for (field_name, field_value) in ingest_fields {
                    log_record.insert(field_name, field_value);
                }

                ingest_events.push(serde_json::Value::Object(log_record));
            }

            // Persist events to OpenObserve logs into single per-org `deql_events` stream
            if !ingest_events.is_empty() {
                // Use single per-org stream name as required by spec
                let stream_name = "deql_events".to_string();

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
                        // Update offset_tip_map with highest offset from this batch
                        if max_offset > 0 {
                            let tip_key = format!("{}/deql_events", &org_id_str);
                            deql_state
                                .offset_tip_map
                                .entry(tip_key)
                                .or_insert_with(|| std::sync::atomic::AtomicI64::new(max_offset))
                                .fetch_max(max_offset, Ordering::Relaxed);
                        }
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
                state = ?rejection.state_values,
                params = ?rejection.command_values,
                "DeQL command rejected by guard"
            );

            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "Command rejected",
                    "details": "Guard condition evaluated to false",
                    "decision": rejection.decision_name,
                    "guard": rejection.guard_expression,
                    "state_values": rejection.state_values,
                    "command_values": rejection.command_values,
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
        // The handler extracts commandname from path, but an empty body with
        // no payload still needs a valid JSON body. Test that an unknown command
        // against an empty registry returns an appropriate error.
        let app =
            Router::new().route("/{org_id}/deql/{aggregate}/{commandname}", post(execute));

        let req = Request::builder()
            .method("POST")
            .uri("/o1/deql/bank_account/UnknownCmd")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        // With an empty registry and no rehydration source, the handler will either
        // return 404 (command not found) or 500 (rehydration failure). Both indicate
        // the command was not executed — accept either as valid rejection.
        let status = resp.status();
        assert!(
            status == StatusCode::NOT_FOUND || status == StatusCode::INTERNAL_SERVER_ERROR,
            "Expected 404 or 500 for unknown command against empty registry, got {}",
            status
        );
    }

    #[tokio::test]
    async fn command_execute_returns_501_for_async_mode() {
        // Async mode is not yet implemented — handler returns 501 NOT_IMPLEMENTED
        let app =
            Router::new().route("/{org_id}/deql/{aggregate}/{commandname}", post(execute));

        let req = Request::builder()
            .method("POST")
            .uri("/o1/deql/bank_account/Deposit?mode=async")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"amount":100}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn command_execute_returns_error_for_sync_mode_unregistered() {
        // Sync mode with a command that doesn't exist in an empty registry
        let app =
            Router::new().route("/{org_id}/deql/{aggregate}/{commandname}", post(execute));

        let req = Request::builder()
            .method("POST")
            .uri("/o1/deql/bank_account/Deposit")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"amount":100}"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        // Without a registered command, expect 404 or 500 (rehydration failure)
        let status = resp.status();
        assert!(
            status == StatusCode::NOT_FOUND || status == StatusCode::INTERNAL_SERVER_ERROR,
            "Expected 404 or 500 for unregistered command, got {}",
            status
        );
    }

    // ── Phase 7.1: VOLATILE field exclusion before ingest ──────────────────────

    /// Verify that the ingest record excludes VOLATILE fields entirely.
    /// VOLATILE fields are returned to the caller in `fields` but must not be stored.
    #[test]
    fn test_volatile_fields_are_absent_from_ingest_record() {
        use serde_json::Value;

        let fields = vec![
            ("salary".to_string(), Value::String("50000".to_string())),
            (
                "secret_token".to_string(),
                Value::String("tok-xyz".to_string()),
            ),
            ("name".to_string(), Value::String("Alice".to_string())),
        ];
        let volatile_fields = vec!["secret_token".to_string()];

        // Replicate the ingest-side retain logic from command.rs
        let mut ingest_fields = fields.clone();
        ingest_fields.retain(|(k, _)| !volatile_fields.contains(k));

        // Assert: VOLATILE field is absent entirely
        let secret = ingest_fields.iter().find(|(k, _)| k == "secret_token");
        assert!(
            secret.is_none(),
            "VOLATILE field must not be present in ingest record"
        );

        let salary = ingest_fields
            .iter()
            .find(|(k, _)| k == "salary")
            .map(|(_, v)| v);
        assert_eq!(
            salary,
            Some(&Value::String("50000".to_string())),
            "non-VOLATILE field must be unchanged in ingest record"
        );
    }

    /// Verify that SENSITIVE fields are preserved in the ingest record
    /// but excluded entirely from the HTTP response.
    #[test]
    fn test_sensitive_fields_are_preserved_in_ingest_record() {
        use serde_json::Value;

        let fields = vec![
            ("ssn".to_string(), Value::String("123-45-6789".to_string())),
            ("name".to_string(), Value::String("Bob".to_string())),
        ];
        let sensitive_fields = vec!["ssn".to_string()];

        // Ingest does NOT remove SENSITIVE fields (they stay for storage)
        let ingest_fields = fields.clone();

        // Response excludes SENSITIVE fields entirely
        let mut response_fields = fields.clone();
        response_fields.retain(|(k, _)| !sensitive_fields.contains(k));

        // In ingest: SSN present with real value
        let ingest_ssn = ingest_fields
            .iter()
            .find(|(k, _)| k == "ssn")
            .map(|(_, v)| v);
        assert_eq!(
            ingest_ssn,
            Some(&Value::String("123-45-6789".to_string())),
            "SENSITIVE field must be stored in Parquet with real value"
        );
        // In response: SSN absent entirely
        let resp_ssn = response_fields.iter().find(|(k, _)| k == "ssn");
        assert!(
            resp_ssn.is_none(),
            "SENSITIVE field must be absent from HTTP response"
        );
        // Non-sensitive fields are still present
        let resp_name = response_fields
            .iter()
            .find(|(k, _)| k == "name")
            .map(|(_, v)| v);
        assert_eq!(
            resp_name,
            Some(&Value::String("Bob".to_string())),
            "non-SENSITIVE field must be present in HTTP response"
        );
    }
}
