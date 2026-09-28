//! Metrics endpoint logic — `GET /{org}/dereg/metrics`
//!
//! [REQ-057e] [REQ-058] [REQ-059] [REQ-060] [REQ-068-072]

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter};
use serde::Serialize;

use crate::projection_worker::{get_org_tip_id, get_watermark};
use crate::store::dereg_meta_store;
use crate::worker_registry::WorkerRegistry;

/// Per-org metrics response. [REQ-068a-k]
#[derive(Debug, Clone, Serialize)]
pub struct OrgMetrics {
    pub org_id: String,
    pub org_tip_id: i64,
    pub latest_id: i64,
    pub last_applied_id: i64,
    pub projection_lag: i64,
    pub projection_worker_last_run_at: Option<String>,
    pub projection_worker_last_error: Option<String>,
    pub last_replay_status: Option<String>,
    pub failed_row_count: u64,
    pub active_background_workers: u32,
}

/// Collect metrics for a single org. [REQ-071] Lightweight — no table scans.
pub async fn collect_metrics(
    db: &DatabaseConnection,
    org_id: &str,
    worker_registry: Option<&WorkerRegistry>,
) -> Result<OrgMetrics, sea_orm::DbErr> {
    let org_tip_id = get_org_tip_id(db, org_id).await?;
    let last_applied_id = get_watermark(db, org_id).await?;
    let projection_lag = org_tip_id - last_applied_id;

    // COUNT failed rows [REQ-068h]
    let failed_row_count = dereg_meta_store::Entity::find()
        .filter(dereg_meta_store::Column::OrgId.eq(org_id))
        .filter(dereg_meta_store::Column::Status.eq("failed"))
        .count(db)
        .await? as u64;

    let active_background_workers = match worker_registry {
        Some(wr) => wr.active_count_for_org(org_id).await as u32,
        None => 0,
    };

    Ok(OrgMetrics {
        org_id: org_id.to_string(),
        org_tip_id,
        latest_id: org_tip_id,
        last_applied_id,
        projection_lag,
        projection_worker_last_run_at: None,
        projection_worker_last_error: None,
        last_replay_status: None,
        failed_row_count,
        active_background_workers,
    })
}
