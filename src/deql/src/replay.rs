//! Replay and replay-refresh logic.
//!
//! - `replay_validate` — validation-only replay (read-only, no mutations) [REQ-044-053]
//! - `replay_refresh` — full projection rebuild with lock [REQ-061-067]

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};

use crate::dereg::DeReg;
use crate::parser::parser::parse;
use crate::projection_worker::{apply_full_rebuild, compute_effective_rows_full, get_org_tip_id};
use crate::store::dereg_meta_store;

/// Replay validation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    pub status: String,
    pub replayed: usize,
    pub errors: Vec<String>,
}

/// Replay-refresh result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRefreshResult {
    pub status: String,
    pub org_tip_id: i64,
    pub replayed_until_id: i64,
    pub applied_offset: i64,
    pub watermark: i64,
}

/// Parameters for replay-refresh endpoint.
#[derive(Debug, Clone, Default)]
pub struct ReplayRefreshParams {
    /// Replay up to this specific row id.
    pub id: Option<i64>,
    /// Replay up to `org_tip_id - offset`.
    pub offset: Option<i64>,
}

/// Validation-only replay. Does NOT mutate projections or watermark.
/// [REQ-044] [REQ-046] [REQ-050] [REQ-051] [REQ-052]
pub async fn replay_validate(
    db: &DatabaseConnection,
    org_id: &str,
) -> Result<ReplayResult, sea_orm::DbErr> {
    // Read all rows for org [REQ-044]
    let rows = dereg_meta_store::Entity::find()
        .filter(dereg_meta_store::Column::OrgId.eq(org_id))
        .order_by_asc(dereg_meta_store::Column::Id)
        .all(db)
        .await?;

    if rows.is_empty() {
        return Ok(ReplayResult {
            status: "ok".to_string(),
            replayed: 0,
            errors: vec![],
        });
    }

    // Compute effective rows using full tombstone-aware logic [REQ-047] [REQ-053]
    let effective = compute_effective_rows_full(&rows);
    let mut errors = Vec::new();

    // Validate each effective row by re-parsing its statement [REQ-049]
    let mut dereg = DeReg::new();
    for eff in &effective {
        if eff.is_tombstone {
            continue; // Tombstones are valid by definition
        }

        // Try to parse the statement
        let (parsed, diagnostics) = parse(&eff.statement);

        if parsed.statements.is_empty() && !diagnostics.is_empty() {
            errors.push(format!(
                "{}: parse error: {:?}",
                eff.stream_id, diagnostics
            ));
            continue;
        }

        // Try to register in temporary DeReg (validates cross-refs)
        for spanned_stmt in &parsed.statements {
            if let Err(e) = dereg.register_statement(&spanned_stmt.node) {
                errors.push(format!("{}: {}", eff.stream_id, e));
            }
        }
    }

    Ok(ReplayResult {
        status: if errors.is_empty() {
            "ok".to_string()
        } else {
            "validation_errors".to_string()
        },
        replayed: effective.len(),
        errors,
    })
}

/// Execute replay-refresh: full projection rebuild.
/// [REQ-061] [REQ-063]
///
/// Caller is responsible for acquiring org lock and stopping workers before calling this.
pub async fn replay_refresh(
    db: &DatabaseConnection,
    org_id: &str,
    params: &ReplayRefreshParams,
) -> Result<ReplayRefreshResult, sea_orm::DbErr> {
    // Resolve target id [REQ-061a] [REQ-061b] [REQ-061d] [REQ-061e]
    let org_tip = get_org_tip_id(db, org_id).await?;

    let until_id = match (params.id, params.offset) {
        (Some(id), None) => id.min(org_tip),           // [REQ-061a]
        (None, Some(offset)) => (org_tip - offset).max(0), // [REQ-061b]
        (None, None) => org_tip,                       // [REQ-061d]
        (Some(_), Some(_)) => {
            // [REQ-061c] mutually exclusive — shouldn't reach here
            org_tip
        }
    };

    // Perform full rebuild [REQ-063-4 through REQ-063-7]
    let replayed_until_id = apply_full_rebuild(db, org_id, until_id).await?;

    Ok(ReplayRefreshResult {
        status: "ok".to_string(),
        org_tip_id: org_tip,
        replayed_until_id,
        applied_offset: org_tip - replayed_until_id,
        watermark: replayed_until_id,
    })
}

/// Validate endpoint — validates all rows but returns structured report.
/// Similar to replay_validate but returns error counts and details.
/// [REQ-057c]
pub async fn validate_definitions(
    db: &DatabaseConnection,
    org_id: &str,
) -> Result<ReplayResult, sea_orm::DbErr> {
    replay_validate(db, org_id).await
}
