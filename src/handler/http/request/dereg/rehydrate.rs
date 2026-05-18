//! Rehydrate endpoint handler for DeReg.
//!
//! Provides HTTP endpoint to trigger rehydration of in-memory DeQL state from audit logs.
//! Available only when `deql` feature is enabled.

#![cfg_attr(not(feature = "deql"), allow(dead_code))]

use std::sync::Arc;

use axum::{
    Json,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use infra::db::{ORM_CLIENT, connect_to_orm};
use o2_deql::RehydrateServiceImpl;
use serde_json::json;

use super::get_deql_state;

/// Trigger a rehydration of the in-memory DeQL registry for the specified organization.
///
/// # Description
/// This endpoint triggers an asynchronous rehydration job that reconstructs the in-memory `DeReg`
/// state from the canonical audit log (`dereg_meta_store`). The job runs in the background; the
/// endpoint returns immediately with HTTP 202 Accepted.
///
/// ## Behavior
/// - If rehydration is already in progress for the org, returns HTTP 409 Conflict.
/// - Returns HTTP 202 Accepted with rehydration job started in background.
///
/// ## Polling Status
/// Use `GET /{org}/deql/info` to check rehydration status. The response will include:
/// - `rehydrate_revision`: Last successful sequence id (watermark)
/// - `last_rehydrate_result`: Status, timing, rows processed, and any error message
#[utoipa::path(
    post,
    path = "/{org_id}/dereg/rehydrate",
    context_path = "/api",
    tag = "DeReg",
    operation_id = "RehydrateDeReg",
    summary = "Trigger rehydration",
    description = "Trigger asynchronous rehydration of in-memory DeReg state from audit logs. Returns 202 Accepted immediately; poll /deql/info to check status.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
    ),
    responses(
        (status = StatusCode::ACCEPTED, description = "Rehydration job started in background"),
        (status = StatusCode::CONFLICT, description = "Rehydration already in progress"),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Server Error"),
    ),
    extensions(
        ("x-o2-mcp" = json!({"description": "Triggers rehydration of DeReg state from audit logs", "category": "deql"}))
    )
)]
#[cfg(feature = "deql")]
pub async fn trigger_rehydrate(Path(org_id): Path<String>) -> Response {
    let state = get_deql_state().await;

    // Check if already in progress (early guard)
    if state.lock_map.is_locked(&org_id).await {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "Rehydration already in progress for this organization"})),
        )
            .into_response();
    }

    // Get database connection (returns Arc<DatabaseConnection>)
    let db = Arc::new(ORM_CLIENT.get_or_init(connect_to_orm).await.clone());

    // Generate trace ID for correlation
    let trace_id = format!(
        "rehydrate-{}-{}",
        org_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );

    // Clone necessary data for the background task
    let org_id_clone = org_id.clone();
    let trace_id_clone = trace_id.clone();
    let org_map = Arc::new(state.org_map.clone_for_service());
    let lock_map = Arc::new(state.lock_map.clone_for_service());
    let rehydrate_state_map = state.rehydrate_state_map.clone();

    // Spawn background job to run rehydrate
    tokio::spawn(async move {
        let service: Arc<dyn o2_deql::RehydrateService> = Arc::new(RehydrateServiceImpl::new(
            db,
            org_map,
            lock_map,
            rehydrate_state_map,
        ));

        match service
            .rehydrate_org(&org_id_clone, None, Some(&trace_id_clone))
            .await
        {
            Ok(result) => {
                tracing::info!(
                    org_id = %org_id_clone,
                    trace_id = %result.rehydrate_id,
                    rows_processed = result.rows_processed,
                    elapsed_ms = result.elapsed_ms,
                    status = %result.status,
                    "Rehydrate completed"
                );
            }
            Err(e) => {
                tracing::error!(
                    org_id = %org_id_clone,
                    trace_id = %trace_id_clone,
                    error = %e,
                    "Rehydrate failed"
                );
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(json!({
            "status": "accepted",
            "message": "Rehydration job started in background",
            "org_id": org_id,
            "trace_id": trace_id
        })),
    )
        .into_response()
}
