//! Projection worker — applies `dereg_meta_store` rows to projection tables.
//!
//! [REQ-039] through [REQ-043]
//!
//! The worker reads rows from `dereg_meta_store` where `id > last_applied_id`,
//! determines effective state per `stream_id`, and upserts/soft-deletes projection rows.

use std::collections::HashMap;

use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, Set, TransactionTrait,
};
use serde_json::Value as Json;

use crate::store::{
    dereg_meta_store, projection_watermark,
    projections::{
        meta_aggregates, meta_commands, meta_concepts, meta_decisions, meta_events,
        meta_inspections, meta_templates, meta_templates_instances,
    },
};

/// A resolved effective row per stream_id — the latest successful state.
#[derive(Debug, Clone)]
pub struct EffectiveRow {
    pub id: i64,
    pub org_id: String,
    pub stream_id: String,
    pub event_type: String,
    pub concept_type: String,
    pub concept_key: i64,
    pub status: String,
    pub statement: String,
    pub meta: Json,
    pub is_tombstone: bool,
}

/// Run one tick of the projection worker for a given org.
///
/// Returns the number of rows processed, or 0 if there's nothing new.
/// [REQ-039] [REQ-040] [REQ-041] [REQ-042]
pub async fn run_projection_tick(
    db: &DatabaseConnection,
    org_id: &str,
) -> Result<usize, sea_orm::DbErr> {
    let watermark = get_watermark(db, org_id).await?;

    // Fetch new rows since watermark [REQ-039]
    let rows = dereg_meta_store::Entity::find()
        .filter(dereg_meta_store::Column::OrgId.eq(org_id))
        .filter(dereg_meta_store::Column::Id.gt(watermark))
        .order_by_asc(dereg_meta_store::Column::Id)
        .all(db)
        .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    let max_id = rows.iter().map(|r| r.id).max().unwrap();
    let count = rows.len();

    // Compute effective rows [REQ-040] [REQ-047] [REQ-053]
    let effective = compute_effective_rows(&rows);

    // Apply in a transaction [REQ-042]
    let txn = db.begin().await?;
    for eff in &effective {
        apply_effective_row(&txn, eff).await?;
    }

    // Persist watermark [REQ-042]
    upsert_watermark(&txn, org_id, max_id).await?;
    txn.commit().await?;

    Ok(count)
}

/// Apply projection logic from row 0 up to `until_id` for an org.
/// Used by replay-refresh. Wipes projections first, then applies fresh.
/// [REQ-063-4] [REQ-063-5] [REQ-063-6] [REQ-063-7]
pub async fn apply_full_rebuild(
    db: &DatabaseConnection,
    org_id: &str,
    until_id: i64,
) -> Result<i64, sea_orm::DbErr> {
    // Read all rows up to until_id [REQ-063-4]
    let rows = dereg_meta_store::Entity::find()
        .filter(dereg_meta_store::Column::OrgId.eq(org_id))
        .filter(dereg_meta_store::Column::Id.lte(until_id))
        .order_by_asc(dereg_meta_store::Column::Id)
        .all(db)
        .await?;

    if rows.is_empty() {
        // Reset watermark to 0
        let txn = db.begin().await?;
        upsert_watermark(&txn, org_id, 0).await?;
        txn.commit().await?;
        return Ok(0);
    }

    let max_applied = rows.iter().map(|r| r.id).max().unwrap();

    // Compute effective state using ALL rows (tombstone-aware)
    let effective = compute_effective_rows_full(&rows);

    let txn = db.begin().await?;

    // Clear existing projections for this org [REQ-063-6]
    clear_projections_for_org(&txn, org_id).await?;

    // Apply effective rows
    for eff in &effective {
        apply_effective_row(&txn, eff).await?;
    }

    // Persist watermark [REQ-063-7]
    upsert_watermark(&txn, org_id, max_applied).await?;
    txn.commit().await?;

    Ok(max_applied)
}

/// Compute effective rows from a batch (incremental mode).
/// For each stream_id, selects the row with MAX(concept_key) WHERE status='ok'.
/// [REQ-040]
pub fn compute_effective_rows(rows: &[dereg_meta_store::Model]) -> Vec<EffectiveRow> {
    let mut by_stream: HashMap<&str, &dereg_meta_store::Model> = HashMap::new();

    for row in rows {
        if row.status != "ok" {
            continue; // [REQ-040] failed rows excluded
        }
        let entry = by_stream.entry(row.stream_id.as_str()).or_insert(row);
        if row.concept_key > entry.concept_key {
            *entry = row;
        }
    }

    by_stream
        .into_values()
        .map(|row| {
            let is_tombstone = row
                .meta
                .get("tombstone")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            EffectiveRow {
                id: row.id,
                org_id: row.org_id.clone(),
                stream_id: row.stream_id.clone(),
                event_type: row.event_type.clone(),
                concept_type: row.concept_type.clone(),
                concept_key: row.concept_key,
                status: row.status.clone(),
                statement: row.statement.clone(),
                meta: row.meta.clone(),
                is_tombstone,
            }
        })
        .collect()
}

/// Compute effective rows from ALL rows (full rebuild mode).
/// Applies tombstone rule [REQ-053]: for each stream_id, find latest tombstone,
/// ignore rows with concept_key < tombstone. Then max concept_key from remaining ok rows.
pub fn compute_effective_rows_full(rows: &[dereg_meta_store::Model]) -> Vec<EffectiveRow> {
    // Group by stream_id
    let mut by_stream: HashMap<&str, Vec<&dereg_meta_store::Model>> = HashMap::new();
    for row in rows {
        by_stream
            .entry(row.stream_id.as_str())
            .or_default()
            .push(row);
    }

    let mut effective = Vec::new();

    for (_stream_id, stream_rows) in &by_stream {
        // Find latest tombstone concept_key
        let latest_tombstone_key = stream_rows
            .iter()
            .filter(|r| {
                r.status == "ok"
                    && r.meta
                        .get("tombstone")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
            })
            .map(|r| r.concept_key)
            .max();

        // Find effective row: max concept_key where status='ok' and not shadowed
        let effective_row = stream_rows
            .iter()
            .filter(|r| r.status == "ok")
            .filter(|r| {
                if let Some(tomb_key) = latest_tombstone_key {
                    r.concept_key >= tomb_key
                } else {
                    true
                }
            })
            .max_by_key(|r| r.concept_key);

        if let Some(row) = effective_row {
            let is_tombstone = row
                .meta
                .get("tombstone")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            effective.push(EffectiveRow {
                id: row.id,
                org_id: row.org_id.clone(),
                stream_id: row.stream_id.clone(),
                event_type: row.event_type.clone(),
                concept_type: row.concept_type.clone(),
                concept_key: row.concept_key,
                status: row.status.clone(),
                statement: row.statement.clone(),
                meta: row.meta.clone(),
                is_tombstone,
            });
        }
    }

    effective
}

/// Apply a single effective row to the appropriate projection table.
/// [REQ-041a] [REQ-041b]
async fn apply_effective_row<C: ConnectionTrait>(
    conn: &C,
    eff: &EffectiveRow,
) -> Result<(), sea_orm::DbErr> {
    let concept_type = eff.concept_type.as_str();
    let name = extract_name_from_stream_id(&eff.stream_id);

    // Always upsert meta_concepts [REQ-030]
    upsert_meta_concepts(conn, eff, &name).await?;

    // Upsert concept-specific projection table
    match concept_type {
        "AGGREGATE" => upsert_meta_aggregates(conn, eff, &name).await?,
        "COMMAND" => upsert_meta_commands(conn, eff, &name).await?,
        "EVENT" => upsert_meta_events(conn, eff, &name).await?,
        "DECISION" => upsert_meta_decisions(conn, eff, &name).await?,
        "TEMPLATE" => upsert_meta_templates(conn, eff, &name).await?,
        "INSPECTION" => upsert_meta_inspections(conn, eff, &name).await?,
        _ => {} // Unknown concept types (e.g. EVENTSTORE) — skip projection
    }

    Ok(())
}

/// Extract concept name from stream_id (format: "type:Name")
fn extract_name_from_stream_id(stream_id: &str) -> String {
    stream_id
        .split_once(':')
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| stream_id.to_string())
}

// --- Projection upsert functions ---

async fn upsert_meta_concepts<C: ConnectionTrait>(
    conn: &C,
    eff: &EffectiveRow,
    name: &str,
) -> Result<(), sea_orm::DbErr> {
    let kind = eff.concept_type.clone();
    let existing =
        meta_concepts::Entity::find_by_id((eff.org_id.clone(), kind.clone(), name.to_string()))
            .one(conn)
            .await?;

    match existing {
        Some(model) => {
            let mut am = model.into_active_model();
            am.json_source = Set(eff.meta.clone());
            am.last_applied_id = Set(eff.id);
            am.is_dropped = Set(eff.is_tombstone);
            am.update(conn).await?;
        }
        None => {
            let am = meta_concepts::ActiveModel {
                org_id: Set(eff.org_id.clone()),
                kind: Set(kind),
                name: Set(name.to_string()),
                json_source: Set(eff.meta.clone()),
                last_applied_id: Set(eff.id),
                is_dropped: Set(eff.is_tombstone),
            };
            am.insert(conn).await?;
        }
    }
    Ok(())
}

async fn upsert_meta_aggregates<C: ConnectionTrait>(
    conn: &C,
    eff: &EffectiveRow,
    name: &str,
) -> Result<(), sea_orm::DbErr> {
    let fields_json = eff
        .meta
        .get("fields")
        .cloned()
        .unwrap_or(serde_json::json!([]));

    let existing = meta_aggregates::Entity::find_by_id((eff.org_id.clone(), name.to_string()))
        .one(conn)
        .await?;

    match existing {
        Some(model) => {
            let mut am = model.into_active_model();
            am.fields_json = Set(fields_json);
            am.last_applied_id = Set(eff.id);
            am.is_dropped = Set(eff.is_tombstone);
            am.update(conn).await?;
        }
        None => {
            let am = meta_aggregates::ActiveModel {
                org_id: Set(eff.org_id.clone()),
                name: Set(name.to_string()),
                fields_json: Set(fields_json),
                last_applied_id: Set(eff.id),
                is_dropped: Set(eff.is_tombstone),
            };
            am.insert(conn).await?;
        }
    }
    Ok(())
}

async fn upsert_meta_commands<C: ConnectionTrait>(
    conn: &C,
    eff: &EffectiveRow,
    name: &str,
) -> Result<(), sea_orm::DbErr> {
    let attributes_json = eff
        .meta
        .get("fields")
        .cloned()
        .unwrap_or(serde_json::json!([]));
    let aggregate = eff
        .meta
        .get("aggregate")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let existing = meta_commands::Entity::find_by_id((eff.org_id.clone(), name.to_string()))
        .one(conn)
        .await?;

    match existing {
        Some(model) => {
            let mut am = model.into_active_model();
            am.aggregate = Set(aggregate);
            am.attributes_json = Set(attributes_json);
            am.full_sql = Set(eff.statement.clone());
            am.last_applied_id = Set(eff.id);
            am.is_dropped = Set(eff.is_tombstone);
            am.update(conn).await?;
        }
        None => {
            let am = meta_commands::ActiveModel {
                org_id: Set(eff.org_id.clone()),
                name: Set(name.to_string()),
                aggregate: Set(aggregate),
                attributes_json: Set(attributes_json),
                full_sql: Set(eff.statement.clone()),
                last_applied_id: Set(eff.id),
                is_dropped: Set(eff.is_tombstone),
            };
            am.insert(conn).await?;
        }
    }
    Ok(())
}

async fn upsert_meta_events<C: ConnectionTrait>(
    conn: &C,
    eff: &EffectiveRow,
    name: &str,
) -> Result<(), sea_orm::DbErr> {
    let attributes_json = eff
        .meta
        .get("fields")
        .cloned()
        .unwrap_or(serde_json::json!([]));
    let aggregate = eff
        .meta
        .get("aggregate")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let existing = meta_events::Entity::find_by_id((eff.org_id.clone(), name.to_string()))
        .one(conn)
        .await?;

    match existing {
        Some(model) => {
            let mut am = model.into_active_model();
            am.aggregate = Set(aggregate);
            am.attributes_json = Set(attributes_json);
            am.full_sql = Set(eff.statement.clone());
            am.last_applied_id = Set(eff.id);
            am.is_dropped = Set(eff.is_tombstone);
            am.update(conn).await?;
        }
        None => {
            let am = meta_events::ActiveModel {
                org_id: Set(eff.org_id.clone()),
                name: Set(name.to_string()),
                aggregate: Set(aggregate),
                attributes_json: Set(attributes_json),
                full_sql: Set(eff.statement.clone()),
                last_applied_id: Set(eff.id),
                is_dropped: Set(eff.is_tombstone),
            };
            am.insert(conn).await?;
        }
    }
    Ok(())
}

async fn upsert_meta_decisions<C: ConnectionTrait>(
    conn: &C,
    eff: &EffectiveRow,
    name: &str,
) -> Result<(), sea_orm::DbErr> {
    let aggregate = eff
        .meta
        .get("aggregate")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let command = eff
        .meta
        .get("command")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let emits_json = eff
        .meta
        .get("emits")
        .cloned()
        .unwrap_or(serde_json::json!([]));
    let has_guard = eff
        .meta
        .get("has_guard")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let guard_sql = eff
        .meta
        .get("guard_sql")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let state_sql = eff
        .meta
        .get("state_sql")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let existing = meta_decisions::Entity::find_by_id((eff.org_id.clone(), name.to_string()))
        .one(conn)
        .await?;

    match existing {
        Some(model) => {
            let mut am = model.into_active_model();
            am.aggregate = Set(aggregate);
            am.command = Set(command);
            am.emits_json = Set(emits_json);
            am.has_guard = Set(has_guard);
            am.guard_sql = Set(guard_sql);
            am.state_sql = Set(state_sql);
            am.full_sql = Set(eff.statement.clone());
            am.last_applied_id = Set(eff.id);
            am.is_dropped = Set(eff.is_tombstone);
            am.update(conn).await?;
        }
        None => {
            let am = meta_decisions::ActiveModel {
                org_id: Set(eff.org_id.clone()),
                name: Set(name.to_string()),
                aggregate: Set(aggregate),
                command: Set(command),
                emits_json: Set(emits_json),
                has_guard: Set(has_guard),
                guard_sql: Set(guard_sql),
                state_sql: Set(state_sql),
                full_sql: Set(eff.statement.clone()),
                last_applied_id: Set(eff.id),
                is_dropped: Set(eff.is_tombstone),
            };
            am.insert(conn).await?;
        }
    }
    Ok(())
}

async fn upsert_meta_templates<C: ConnectionTrait>(
    conn: &C,
    eff: &EffectiveRow,
    name: &str,
) -> Result<(), sea_orm::DbErr> {
    let parameters_json = eff
        .meta
        .get("parameters")
        .cloned()
        .unwrap_or(serde_json::json!([]));

    let existing = meta_templates::Entity::find_by_id((eff.org_id.clone(), name.to_string()))
        .one(conn)
        .await?;

    match existing {
        Some(model) => {
            let mut am = model.into_active_model();
            am.parameters_json = Set(parameters_json);
            am.full_sql = Set(eff.statement.clone());
            am.last_applied_id = Set(eff.id);
            am.is_dropped = Set(eff.is_tombstone);
            am.update(conn).await?;
        }
        None => {
            let am = meta_templates::ActiveModel {
                org_id: Set(eff.org_id.clone()),
                name: Set(name.to_string()),
                parameters_json: Set(parameters_json),
                full_sql: Set(eff.statement.clone()),
                last_applied_id: Set(eff.id),
                is_dropped: Set(eff.is_tombstone),
            };
            am.insert(conn).await?;
        }
    }
    Ok(())
}

async fn upsert_meta_inspections<C: ConnectionTrait>(
    conn: &C,
    eff: &EffectiveRow,
    name: &str,
) -> Result<(), sea_orm::DbErr> {
    let decision_name = eff
        .meta
        .get("decision_name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let input_output_json = eff
        .meta
        .get("input_output")
        .cloned()
        .unwrap_or(serde_json::json!({}));

    let existing = meta_inspections::Entity::find_by_id((eff.org_id.clone(), name.to_string()))
        .one(conn)
        .await?;

    match existing {
        Some(model) => {
            let mut am = model.into_active_model();
            am.decision_name = Set(decision_name);
            am.input_output_json = Set(input_output_json);
            am.full_sql = Set(eff.statement.clone());
            am.last_applied_id = Set(eff.id);
            am.is_dropped = Set(eff.is_tombstone);
            am.update(conn).await?;
        }
        None => {
            let am = meta_inspections::ActiveModel {
                org_id: Set(eff.org_id.clone()),
                name: Set(name.to_string()),
                decision_name: Set(decision_name),
                input_output_json: Set(input_output_json),
                full_sql: Set(eff.statement.clone()),
                last_applied_id: Set(eff.id),
                is_dropped: Set(eff.is_tombstone),
            };
            am.insert(conn).await?;
        }
    }
    Ok(())
}

// --- Watermark helpers ---

pub async fn get_watermark(db: &DatabaseConnection, org_id: &str) -> Result<i64, sea_orm::DbErr> {
    let wm = projection_watermark::Entity::find_by_id(org_id.to_string())
        .one(db)
        .await?;
    Ok(wm.map(|w| w.last_applied_id).unwrap_or(0))
}

async fn upsert_watermark<C: ConnectionTrait>(
    conn: &C,
    org_id: &str,
    last_applied_id: i64,
) -> Result<(), sea_orm::DbErr> {
    let existing = projection_watermark::Entity::find_by_id(org_id.to_string())
        .one(conn)
        .await?;

    match existing {
        Some(model) => {
            let mut am = model.into_active_model();
            am.last_applied_id = Set(last_applied_id);
            am.updated_at = Set(chrono::Utc::now().into());
            am.update(conn).await?;
        }
        None => {
            let am = projection_watermark::ActiveModel {
                org_id: Set(org_id.to_string()),
                last_applied_id: Set(last_applied_id),
                updated_at: Set(chrono::Utc::now().into()),
            };
            am.insert(conn).await?;
        }
    }
    Ok(())
}

/// Clear all projection rows for an org (used during replay-refresh rebuild).
async fn clear_projections_for_org<C: ConnectionTrait>(
    conn: &C,
    org_id: &str,
) -> Result<(), sea_orm::DbErr> {
    meta_concepts::Entity::delete_many()
        .filter(meta_concepts::Column::OrgId.eq(org_id))
        .exec(conn)
        .await?;
    meta_aggregates::Entity::delete_many()
        .filter(meta_aggregates::Column::OrgId.eq(org_id))
        .exec(conn)
        .await?;
    meta_commands::Entity::delete_many()
        .filter(meta_commands::Column::OrgId.eq(org_id))
        .exec(conn)
        .await?;
    meta_events::Entity::delete_many()
        .filter(meta_events::Column::OrgId.eq(org_id))
        .exec(conn)
        .await?;
    meta_decisions::Entity::delete_many()
        .filter(meta_decisions::Column::OrgId.eq(org_id))
        .exec(conn)
        .await?;
    meta_inspections::Entity::delete_many()
        .filter(meta_inspections::Column::OrgId.eq(org_id))
        .exec(conn)
        .await?;
    meta_templates::Entity::delete_many()
        .filter(meta_templates::Column::OrgId.eq(org_id))
        .exec(conn)
        .await?;
    meta_templates_instances::Entity::delete_many()
        .filter(meta_templates_instances::Column::OrgId.eq(org_id))
        .exec(conn)
        .await?;
    Ok(())
}

/// Get the org_tip_id (MAX(id) for the org in dereg_meta_store).
pub async fn get_org_tip_id(db: &DatabaseConnection, org_id: &str) -> Result<i64, sea_orm::DbErr> {
    use sea_orm::{FromQueryResult, Statement};

    #[derive(Debug, FromQueryResult)]
    struct MaxId {
        max_id: Option<i64>,
    }

    let result = MaxId::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT MAX(id) as max_id FROM dereg_meta_store WHERE org_id = $1",
        [org_id.into()],
    ))
    .one(db)
    .await?;

    Ok(result.and_then(|r| r.max_id).unwrap_or(0))
}
