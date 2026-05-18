//! HTTP request handlers for DeReg endpoints (Phase 2).
//!
//! Wraps `o2_deql` crate functions and exposes them as Axum handlers.
//! All endpoints are behind `#[cfg(feature = "deql")]`.

pub mod rehydrate;

#[cfg(feature = "deql")]
pub use rehydrate::trigger_rehydrate;

use std::sync::Arc;

use axum::{
    Json,
    body::{Body, to_bytes},
    extract::{Path, Query},
    http::{Request, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use infra::db::{ORM_CLIENT, connect_to_orm};
use o2_deql::{
    allocator, meta_json,
    metrics::collect_metrics,
    org_registry::OrgDeRegMap,
    parser::{error::Severity, parser::parse},
    replay::{ReplayRefreshParams, replay_refresh, replay_validate, validate_definitions},
    worker_registry::{OrgLockMap, WorkerRegistry},
    OrgRehydrateStateMap,
};
use sea_orm::TransactionTrait;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::sync::OnceCell;

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024; // 10 MiB

/// Global DeQL state shared across all handlers.
pub struct DeqlState {
    pub org_map: OrgDeRegMap,
    pub worker_registry: WorkerRegistry,
    pub lock_map: OrgLockMap,
    /// Per-org rehydrate watermarks and last results
    pub rehydrate_state_map: OrgRehydrateStateMap,
}

impl DeqlState {
    fn new() -> Self {
        Self {
            org_map: OrgDeRegMap::new(),
            worker_registry: WorkerRegistry::new(),
            lock_map: OrgLockMap::new(),
            rehydrate_state_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

static DEQL_STATE: OnceCell<Arc<DeqlState>> = OnceCell::const_new();

/// Get (or initialize) the global DeQL state.
pub async fn get_deql_state() -> &'static Arc<DeqlState> {
    DEQL_STATE
        .get_or_init(|| async { Arc::new(DeqlState::new()) })
        .await
}

#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    pub scope: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ReplayRefreshQuery {
    pub id: Option<i64>,
    pub offset: Option<i64>,
}

/// Register DeQL definitions
#[utoipa::path(
    post,
    path = "/{org_id}/dereg/definitions",
    context_path = "/api",
    tag = "DeReg",
    operation_id = "DeqlDefinitions",
    summary = "Register DeQL definitions",
    description = "Registers DeQL language definitions (text/plain). Returns results with created IDs and status for each statement.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
    ),
    request_body(content = String, description = "DeQL definitions", content_type = "text/plain", example = json!("CREATE OR REPLACE AGGREGATE Employee;")),
    responses(
        (status = StatusCode::CREATED, description = "Definitions processed"),
        (status = StatusCode::BAD_REQUEST, description = "Parse error"),
        (status = StatusCode::PAYLOAD_TOO_LARGE, description = "Payload too large"),
        (status = StatusCode::UNSUPPORTED_MEDIA_TYPE, description = "Unsupported Media Type"),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Server Error"),
    ),
    extensions(
        ("x-o2-mcp" = json!({"description": "Registers DeQL definitions", "category": "deql"}))
    )
)]
pub async fn definitions(Path(org_id): Path<String>, req: Request<Body>) -> Response {
    let state = get_deql_state().await;

    // Check replay-refresh lock [REQ-064]
    if state.lock_map.is_locked(&org_id).await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "replay-refresh in progress, try again later"})),
        )
            .into_response();
    }

    // Content-Type check: accept text/*, treat missing as text/plain
    if let Some(ct_val) = req.headers().get(CONTENT_TYPE) {
        if let Ok(ct_str) = ct_val.to_str() {
            if !ct_str.starts_with("text/") {
                return (
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    Json(json!({"error": "Unsupported Media Type, expected text/plain"})),
                )
                    .into_response();
            }
        }
    }

    // Read body and enforce size limit
    let bytes = match to_bytes(req.into_body(), MAX_BODY_BYTES + 1).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("body read error: {e}") })),
            )
                .into_response();
        }
    };

    if bytes.len() > MAX_BODY_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"error": "payload too large"})),
        )
            .into_response();
    }

    let statement = match std::str::from_utf8(&bytes) {
        Ok(s) => s.to_string(),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid UTF-8 in body"})),
            )
                .into_response();
        }
    };

    let db = ORM_CLIENT.get_or_init(connect_to_orm).await;

    // Parse the statement
    let (parsed, diagnostics) = parse(&statement);
    let has_parse_errors = diagnostics
        .iter()
        .any(|diagnostic| matches!(diagnostic.severity, Severity::Error));

    if parsed.statements.is_empty() || has_parse_errors {
        // Parse failure — persist a single error row.
        let txn = match db.begin().await {
            Ok(t) => t,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("db error: {e}") })),
                )
                    .into_response();
            }
        };

        let id = match allocator::allocate_next_id_txn(&txn).await {
            Ok(id) => id,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("id allocator error: {e}") })),
                )
                    .into_response();
            }
        };

        let diag_strings: Vec<String> = diagnostics
            .iter()
            .map(|diagnostic| diagnostic.display(&statement))
            .collect();
        let error_message = if diag_strings.is_empty() {
            "empty script".to_string()
        } else {
            diag_strings.join("\n")
        };
        let response_message = if diag_strings.is_empty() {
            "empty script".to_string()
        } else {
            diag_strings[0].clone()
        };
        let meta = if diagnostics.is_empty() {
            meta_json::build_error_meta(&error_message)
        } else {
            meta_json::build_parse_error_meta(&statement, &diagnostics)
        };

        use o2_deql::store::dereg_meta_store;
        use sea_orm::{ActiveModelTrait, Set};
        let model = dereg_meta_store::ActiveModel {
            id: Set(id),
            org_id: Set(org_id.clone()),
            stream_id: Set("ERROR:PARSE".to_string()),
            event_type: Set("ParseError".to_string()),
            concept_type: Set("PARSE_ERROR".to_string()),
            concept_key: Set(0),
            occurred_at: Set(chrono::Utc::now().into()),
            status: Set("parse_error".to_string()),
            error_message: Set(Some(error_message)),
            statement: Set(statement.clone()),
            meta: Set(meta),
        };
        if let Err(e) = model.insert(&txn).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("persist error: {e}") })),
            )
                .into_response();
        }
        if let Err(e) = txn.commit().await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("commit error: {e}") })),
            )
                .into_response();
        }

        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "status": "parse_error",
                "id": id,
                "message": response_message,
                "diagnostics": diag_strings,
            })),
        )
            .into_response();
    }

    // Validate every statement against a temporary copy before any DB write.
    let dereg_lock = state.org_map.get_or_init(&org_id).await;
    let base_dereg = { dereg_lock.read().await.clone() };
    let mut temp_dereg = base_dereg;

    #[derive(Debug)]
    struct PendingStatement {
        id: i64,
        statement_text: String,
        registration: o2_deql::RegistrationResult,
        stream_id: String,
        meta: Value,
    }

    let mut pending = Vec::with_capacity(parsed.statements.len());
    let mut statement_cursor = 0usize;
    let statement_count = parsed.statements.len();

    for (index, spanned_stmt) in parsed.statements.iter().enumerate() {
        let stmt = &spanned_stmt.node;
        let id = match allocator::allocate_next_id(db).await {
            Ok(id) => id,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("id allocator error: {e}") })),
                )
                    .into_response();
            }
        };

        let statement_text = statement_text_for_slice(
            &statement,
            statement_cursor,
            spanned_stmt.span.end,
            index + 1 == statement_count,
        )
        .to_string();
        statement_cursor = spanned_stmt.span.end;

        let registration = match temp_dereg.register_statement(stmt) {
            Ok(reg) => reg,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "status": "error",
                        "error": {
                            "code": "VALIDATION_FAILED",
                            "message": e.to_string(),
                            "statement_index": index,
                            "statement_text": statement_text,
                        }
                    })),
                )
                    .into_response();
            }
        };

        let stream_id = format!(
            "{}:{}",
            format!("{:?}", registration.concept_type).to_lowercase(),
            registration.concept_name.as_str()
        );
        let meta = meta_json::build_meta(stmt);

        pending.push(PendingStatement {
            id,
            statement_text,
            registration,
            stream_id,
            meta,
        });
    }

    let txn = match db.begin().await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("db error: {e}") })),
            )
                .into_response();
        }
    };

    let mut results = Vec::with_capacity(pending.len());

    for prepared in pending {
        let id = prepared.id;

        let concept_key =
            match allocator::allocate_concept_key_txn(&txn, &org_id, &prepared.stream_id).await {
                Ok(k) => k,
                Err(e) => {
                    let _ = txn.rollback().await;
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("concept_key error: {e}") })),
                    )
                        .into_response();
                }
            };

        use o2_deql::store::dereg_meta_store;
        use sea_orm::{ActiveModelTrait, Set};
        let model = dereg_meta_store::ActiveModel {
            id: Set(id),
            org_id: Set(org_id.clone()),
            stream_id: Set(prepared.stream_id.clone()),
            event_type: Set(prepared.registration.event_type.to_string()),
            concept_type: Set(format!("{:?}", prepared.registration.concept_type).to_uppercase()),
            concept_key: Set(concept_key),
            occurred_at: Set(chrono::Utc::now().into()),
            status: Set("ok".to_string()),
            error_message: Set(None),
            statement: Set(prepared.statement_text.clone()),
            meta: Set(prepared.meta.clone()),
        };
        if let Err(e) = model.insert(&txn).await {
            let _ = txn.rollback().await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("persist error: {e}") })),
            )
                .into_response();
        }

        results.push(json!({
            "id": id,
            "status": "ok",
            "event_type": prepared.registration.event_type,
            "concept_type": format!("{:?}", prepared.registration.concept_type),
            "concept_name": prepared.registration.concept_name,
        }));
    }

    if let Err(e) = txn.commit().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("commit error: {e}") })),
        )
            .into_response();
    }

    {
        let mut live = dereg_lock.write().await;
        *live = temp_dereg;
    }

    (StatusCode::CREATED, Json(json!({ "results": results }))).into_response()
}

fn statement_text_for_slice(source: &str, start: usize, end: usize, is_last: bool) -> &str {
    if is_last {
        &source[start..]
    } else {
        &source[start..end]
    }
}
// end of definitions()

// ── GET /{org_id}/dereg/metrics ───────────────────────────────────────────
// [T2.4.1] Status API — report org_tip_id, latest_id, last_applied_id, lag, etc.

#[utoipa::path(
    get,
    path = "/{org_id}/dereg/metrics",
    context_path = "/api",
    tag = "DeReg",
    operation_id = "DeqlMetrics",
    summary = "Get DeQL metrics",
    description = "Get metrics for DeQL projections; use query param `scope=all` to fetch all org metrics.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("scope" = Option<String>, Query, description = "Metrics scope; 'all' returns metrics for all orgs"),
    ),
    responses(
        (status = StatusCode::OK, description = "OK"),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Server Error"),
    ),
    extensions(
        ("x-o2-mcp" = json!({"module":"DeQL","operation":"metrics"}))
    )
)]
pub async fn metrics(Path(org_id): Path<String>, Query(query): Query<MetricsQuery>) -> Response {
    let state = get_deql_state().await;
    let db = ORM_CLIENT.get_or_init(connect_to_orm).await;

    if query.scope.as_deref() == Some("all") {
        // Return metrics for all known orgs
        let org_ids = state.org_map.org_ids().await;
        let mut all_metrics = Vec::new();
        for oid in &org_ids {
            match collect_metrics(db, oid, Some(&state.worker_registry)).await {
                Ok(m) => all_metrics.push(m),
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("metrics error for {oid}: {e}")})),
                    )
                        .into_response();
                }
            }
        }
        return (StatusCode::OK, Json(json!({ "orgs": all_metrics }))).into_response();
    }

    match collect_metrics(db, &org_id, Some(&state.worker_registry)).await {
        Ok(m) => (StatusCode::OK, Json(json!(m))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("metrics error: {e}")})),
        )
            .into_response(),
    }
}

// ── POST /{org_id}/dereg/admin/replay ─────────────────────────────────────
// [T2.3.3] Validation-only replay — read-only, no mutations.

#[utoipa::path(
    post,
    path = "/{org_id}/dereg/admin/replay",
    context_path = "/api",
    tag = "DeReg",
    operation_id = "DeqlReplayValidate",
    summary = "Run DeQL replay validation",
    description = "Perform validation-only replay for the organization; does not mutate production state.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
    ),
    responses(
        (status = StatusCode::OK, description = "Validation successful"),
        (status = StatusCode::SERVICE_UNAVAILABLE, description = "Replay-refresh in progress"),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Server Error"),
    ),
    extensions(
        ("x-o2-mcp" = json!({"module":"DeQL","operation":"replay_validate"}))
    )
)]
pub async fn replay(Path(org_id): Path<String>) -> Response {
    let state = get_deql_state().await;

    // Check replay-refresh lock [REQ-064]
    if state.lock_map.is_locked(&org_id).await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "replay-refresh in progress, try again later"})),
        )
            .into_response();
    }

    let db = ORM_CLIENT.get_or_init(connect_to_orm).await;

    match replay_validate(db, &org_id).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("replay error: {e}")})),
        )
            .into_response(),
    }
}

// ── POST /{org_id}/dereg/admin/replay-refresh ────────────────────────────
// [T2.3.5] Full projection rebuild with lock, worker stop/start.

#[utoipa::path(
    post,
    path = "/{org_id}/dereg/admin/replay-refresh",
    context_path = "/api",
    tag = "DeReg",
    operation_id = "DeqlReplayRefresh",
    summary = "Replay refresh (full projection rebuild)",
    description = "Performs a full projection rebuild for an organization. Either `id` or `offset` may be specified, but not both.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("id" = Option<i64>, Query, description = "Optional replay id to start from"),
        ("offset" = Option<i64>, Query, description = "Optional offset to start from"),
    ),
    responses(
        (status = StatusCode::OK, description = "Replay refresh initiated"),
        (status = StatusCode::BAD_REQUEST, description = "Invalid parameters"),
        (status = StatusCode::SERVICE_UNAVAILABLE, description = "Replay-refresh in progress"),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Server Error"),
    )
)]
pub async fn replay_refresh_handler(
    Path(org_id): Path<String>,
    Query(query): Query<ReplayRefreshQuery>,
) -> Response {
    let state = get_deql_state().await;

    // Mutual exclusion: id and offset cannot both be set [REQ-061c]
    if query.id.is_some() && query.offset.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "id and offset are mutually exclusive"})),
        )
            .into_response();
    }

    // Acquire org-level write lock [REQ-063-1]
    let _lock_guard = state.lock_map.acquire_write(&org_id).await;

    // Stop projection worker [REQ-063-2]
    state.worker_registry.stop_org(&org_id).await;

    let db = ORM_CLIENT.get_or_init(connect_to_orm).await;

    let params = ReplayRefreshParams {
        id: query.id,
        offset: query.offset,
    };

    match replay_refresh(db, &org_id, &params).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("replay-refresh error: {e}")})),
        )
            .into_response(),
    }
    // Lock guard drops here, unblocking other endpoints [REQ-063-9]
}

// ── POST /{org_id}/dereg/admin/validate ──────────────────────────────────
// [T2.3.4] Validation report only — structured results and error counts.

#[utoipa::path(
    post,
    path = "/{org_id}/dereg/admin/validate",
    context_path = "/api",
    tag = "DeReg",
    operation_id = "DeqlValidateDefinitions",
    summary = "Validate DeQL definitions",
    description = "Run validation-only report on DeQL definitions.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
    ),
    responses(
        (status = StatusCode::OK, description = "Validation report"),
        (status = StatusCode::SERVICE_UNAVAILABLE, description = "Replay-refresh in progress"),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Server Error"),
    )
)]
pub async fn validate(Path(org_id): Path<String>) -> Response {
    let state = get_deql_state().await;

    // Check replay-refresh lock [REQ-064]
    if state.lock_map.is_locked(&org_id).await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "replay-refresh in progress, try again later"})),
        )
            .into_response();
    }

    let db = ORM_CLIENT.get_or_init(connect_to_orm).await;

    match validate_definitions(db, &org_id).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("validation error: {e}")})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse, statement_text_for_slice};

    #[test]
    fn statement_slices_keep_comments_and_whitespace() {
        let source = "-- leading comment\nCREATE AGGREGATE A;\n/* next statement */\nCREATE EVENT B (id UUID);\n";
        let (parsed, diagnostics) = parse(source);
        assert!(diagnostics.is_empty(), "diagnostics: {:?}", diagnostics);

        let mut cursor = 0usize;
        let mut slices = Vec::new();
        for (index, stmt) in parsed.statements.iter().enumerate() {
            let text = statement_text_for_slice(
                source,
                cursor,
                stmt.span.end,
                index + 1 == parsed.statements.len(),
            );
            cursor = stmt.span.end;
            slices.push(text.to_string());
        }

        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0], "-- leading comment\nCREATE AGGREGATE A;");
        assert_eq!(
            slices[1],
            "\n/* next statement */\nCREATE EVENT B (id UUID);\n"
        );
    }
}
