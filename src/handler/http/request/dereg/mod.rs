//! HTTP request handlers for DeReg endpoints (Phase 2).
//!
//! Wraps `o2_deql` crate functions and exposes them as Axum handlers.
//! All endpoints are behind `#[cfg(feature = "deql")]`.

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
    parser::parser::parse,
    replay::{ReplayRefreshParams, replay_refresh, replay_validate, validate_definitions},
    worker_registry::{OrgLockMap, WorkerRegistry},
};
use sea_orm::TransactionTrait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::OnceCell;

const MAX_BODY_BYTES: usize = 10 * 1024 * 1024; // 10 MiB

/// Global DeQL state shared across all handlers.
pub struct DeqlState {
    pub org_map: OrgDeRegMap,
    pub worker_registry: WorkerRegistry,
    pub lock_map: OrgLockMap,
}

impl DeqlState {
    fn new() -> Self {
        Self {
            org_map: OrgDeRegMap::new(),
            worker_registry: WorkerRegistry::new(),
            lock_map: OrgLockMap::new(),
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

    if parsed.statements.is_empty() {
        // Parse failure — persist failed row
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

        let diag_strings: Vec<String> = diagnostics.iter().map(|d| format!("{d:?}")).collect();
        let meta = meta_json::build_error_meta(&diag_strings.join("; "));

        use o2_deql::store::dereg_meta_store;
        use sea_orm::{ActiveModelTrait, Set};
        let model = dereg_meta_store::ActiveModel {
            id: Set(id),
            org_id: Set(org_id.clone()),
            stream_id: Set("unknown".to_string()),
            event_type: Set("ParseFailed".to_string()),
            concept_type: Set("UNKNOWN".to_string()),
            concept_key: Set(0),
            occurred_at: Set(chrono::Utc::now().into()),
            status: Set("failed".to_string()),
            error_message: Set(Some(diag_strings.join("; "))),
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
                "diagnostics": diag_strings,
            })),
        )
            .into_response();
    }

    // Process each statement
    let dereg_lock = state.org_map.get_or_init(&org_id).await;
    let mut dereg = dereg_lock.write().await;
    let mut results = Vec::new();

    for spanned_stmt in &parsed.statements {
        let stmt = &spanned_stmt.node;

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

        match dereg.register_statement(stmt) {
            Ok(reg) => {
                let stream_id = format!(
                    "{}:{}",
                    format!("{:?}", reg.concept_type).to_lowercase(),
                    reg.concept_name
                );
                let concept_key =
                    match allocator::allocate_concept_key_txn(&txn, &org_id, &stream_id).await {
                        Ok(k) => k,
                        Err(e) => {
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": format!("concept_key error: {e}") })),
                            )
                                .into_response();
                        }
                    };
                let meta = meta_json::build_meta(stmt);

                use o2_deql::store::dereg_meta_store;
                use sea_orm::{ActiveModelTrait, Set};
                let model = dereg_meta_store::ActiveModel {
                    id: Set(id),
                    org_id: Set(org_id.clone()),
                    stream_id: Set(stream_id.clone()),
                    event_type: Set(reg.event_type.to_string()),
                    concept_type: Set(format!("{:?}", reg.concept_type).to_uppercase()),
                    concept_key: Set(concept_key),
                    occurred_at: Set(chrono::Utc::now().into()),
                    status: Set("ok".to_string()),
                    error_message: Set(None),
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

                results.push(json!({
                    "id": id,
                    "status": "ok",
                    "event_type": reg.event_type,
                    "concept_type": format!("{:?}", reg.concept_type),
                    "concept_name": reg.concept_name,
                }));
            }
            Err(e) => {
                let meta = meta_json::build_error_meta(&e.to_string());

                use o2_deql::store::dereg_meta_store;
                use sea_orm::{ActiveModelTrait, Set};
                let model = dereg_meta_store::ActiveModel {
                    id: Set(id),
                    org_id: Set(org_id.clone()),
                    stream_id: Set("unknown".to_string()),
                    event_type: Set("RegistrationFailed".to_string()),
                    concept_type: Set("UNKNOWN".to_string()),
                    concept_key: Set(0),
                    occurred_at: Set(chrono::Utc::now().into()),
                    status: Set("failed".to_string()),
                    error_message: Set(Some(e.to_string())),
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

                results.push(json!({
                    "id": id,
                    "status": "failed",
                    "error": e.to_string(),
                }));
            }
        }
    }

    let all_ok = results.iter().all(|r| r["status"] == "ok");
    let status_code = if all_ok {
        StatusCode::CREATED
    } else {
        StatusCode::MULTI_STATUS
    };

    (status_code, Json(json!({ "results": results }))).into_response()
}
// end of definitions()

// ── GET /{org_id}/dereg/metrics ───────────────────────────────────────────
// [T2.4.1] Status API — report org_tip_id, latest_id, last_applied_id, lag, etc.

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
    // Handler tests require full DB setup; covered by o2-deql integration tests.
}
