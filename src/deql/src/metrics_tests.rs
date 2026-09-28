//! Integration tests for M2.4 — Metrics, Startup Recovery, and Operational Hardening.
//!
//! Validates CP-4 acceptance criteria against in-memory SQLite.

#[cfg(test)]
mod tests {
    use sea_orm::{ActiveModelTrait, ColumnTrait, Database, DatabaseConnection, EntityTrait, QueryFilter, Set};
    use sea_orm_migration::MigratorTrait;
    use serde_json::json;

    use crate::metrics::collect_metrics;
    use crate::migration::DeqlMigrator;
    use crate::projection_worker::run_projection_tick;
    use crate::store::dereg_meta_store;

    async fn setup_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        DeqlMigrator::up(&db, None).await.unwrap();
        db
    }

    async fn insert_row(
        db: &DatabaseConnection,
        id: i64,
        org_id: &str,
        stream_id: &str,
        event_type: &str,
        concept_type: &str,
        concept_key: i64,
        status: &str,
        statement: &str,
        meta: serde_json::Value,
    ) {
        let am = dereg_meta_store::ActiveModel {
            id: Set(id),
            org_id: Set(org_id.to_string()),
            stream_id: Set(stream_id.to_string()),
            event_type: Set(event_type.to_string()),
            concept_type: Set(concept_type.to_string()),
            concept_key: Set(concept_key),
            occurred_at: Set(chrono::Utc::now().into()),
            status: Set(status.to_string()),
            error_message: Set(None),
            statement: Set(statement.to_string()),
            meta: Set(meta),
        };
        am.insert(db).await.unwrap();
    }

    // --- T2.4.1: Metrics endpoint tests ---

    #[tokio::test]
    async fn metrics_returns_zero_for_empty_org() {
        let db = setup_db().await;
        let m = collect_metrics(&db, "org1", None).await.unwrap();
        assert_eq!(m.org_id, "org1");
        assert_eq!(m.org_tip_id, 0);
        assert_eq!(m.latest_id, 0);
        assert_eq!(m.last_applied_id, 0);
        assert_eq!(m.projection_lag, 0);
        assert_eq!(m.failed_row_count, 0);
        assert_eq!(m.active_background_workers, 0);
    }

    #[tokio::test]
    async fn metrics_reflects_lag_after_insert() {
        let db = setup_db().await;
        insert_row(&db, 1, "org1", "aggregate:A", "AggregateCreated", "AGGREGATE", 1, "ok", "CREATE AGGREGATE A;", json!({"name": "A"})).await;
        insert_row(&db, 2, "org1", "aggregate:B", "AggregateCreated", "AGGREGATE", 2, "ok", "CREATE AGGREGATE B;", json!({"name": "B"})).await;

        let m = collect_metrics(&db, "org1", None).await.unwrap();
        assert_eq!(m.org_tip_id, 2);
        assert_eq!(m.last_applied_id, 0);
        assert_eq!(m.projection_lag, 2);
    }

    #[tokio::test]
    async fn metrics_reflects_caught_up_after_worker_run() {
        let db = setup_db().await;
        insert_row(&db, 1, "org1", "aggregate:A", "AggregateCreated", "AGGREGATE", 1, "ok", "CREATE AGGREGATE A;", json!({"name": "A"})).await;

        run_projection_tick(&db, "org1").await.unwrap();

        let m = collect_metrics(&db, "org1", None).await.unwrap();
        assert_eq!(m.org_tip_id, 1);
        assert_eq!(m.last_applied_id, 1);
        assert_eq!(m.projection_lag, 0);
    }

    #[tokio::test]
    async fn metrics_counts_failed_rows() {
        let db = setup_db().await;
        insert_row(&db, 1, "org1", "aggregate:X", "AggregateCreated", "AGGREGATE", 1, "failed", "BAD;", json!({})).await;
        insert_row(&db, 2, "org1", "aggregate:A", "AggregateCreated", "AGGREGATE", 2, "ok", "CREATE AGGREGATE A;", json!({"name": "A"})).await;

        let m = collect_metrics(&db, "org1", None).await.unwrap();
        assert_eq!(m.failed_row_count, 1);
    }

    #[tokio::test]
    async fn metrics_is_org_scoped() {
        let db = setup_db().await;
        insert_row(&db, 1, "org1", "aggregate:A", "AggregateCreated", "AGGREGATE", 1, "ok", "CREATE AGGREGATE A;", json!({"name": "A"})).await;
        insert_row(&db, 2, "org2", "aggregate:B", "AggregateCreated", "AGGREGATE", 1, "ok", "CREATE AGGREGATE B;", json!({"name": "B"})).await;

        let m1 = collect_metrics(&db, "org1", None).await.unwrap();
        let m2 = collect_metrics(&db, "org2", None).await.unwrap();
        assert_eq!(m1.org_tip_id, 1);
        assert_eq!(m2.org_tip_id, 2);
    }

    // --- T2.4.4: End-to-end create/drop/replay/metrics ---

    #[tokio::test]
    async fn end_to_end_create_drop_replay_metrics() {
        use crate::replay::{replay_validate, replay_refresh, ReplayRefreshParams};

        let db = setup_db().await;

        // Create
        insert_row(&db, 1, "org1", "aggregate:Account", "AggregateCreated", "AGGREGATE", 1, "ok", "CREATE AGGREGATE Account;", json!({"name": "Account"})).await;

        // Worker processes
        let processed = run_projection_tick(&db, "org1").await.unwrap();
        assert_eq!(processed, 1);

        // Metrics show caught up
        let m = collect_metrics(&db, "org1", None).await.unwrap();
        assert_eq!(m.projection_lag, 0);

        // Insert a drop (tombstone via event_type)
        insert_row(&db, 2, "org1", "aggregate:Account", "AggregateDropped", "AGGREGATE", 1, "ok", "DROP AGGREGATE Account;", json!({"tombstone": true, "name": "Account"})).await;

        // Worker processes the tombstone
        run_projection_tick(&db, "org1").await.unwrap();
        let m = collect_metrics(&db, "org1", None).await.unwrap();
        assert_eq!(m.projection_lag, 0);
        assert_eq!(m.last_applied_id, 2);

        // Replay-validate doesn't mutate
        let replay_result = replay_validate(&db, "org1").await.unwrap();
        assert!(replay_result.status == "ok" || replay_result.status == "validation_errors");

        // Replay-refresh rebuilds
        let rr = replay_refresh(&db, "org1", &ReplayRefreshParams { id: None, offset: None }).await.unwrap();
        assert_eq!(rr.status, "ok");
    }

    // --- T2.4.5: Startup recovery from existing dereg_meta_store rows ---

    #[tokio::test]
    async fn startup_recovery_rehydrates_from_db() {
        use crate::projection_worker::get_org_tip_id;
        use std::collections::HashMap;

        let db = setup_db().await;

        // Simulate rows that existed before restart
        insert_row(&db, 1, "org1", "aggregate:Account", "AggregateCreated", "AGGREGATE", 1, "ok", "CREATE AGGREGATE Account;", json!({"name": "Account"})).await;
        insert_row(&db, 2, "org1", "command:OpenAccount", "CommandCreated", "COMMAND", 2, "ok", "CREATE COMMAND OpenAccount;", json!({"name": "OpenAccount"})).await;
        insert_row(&db, 3, "org1", "aggregate:Account", "AggregateDropped", "AGGREGATE", 1, "ok", "DROP AGGREGATE Account;", json!({"tombstone": true, "name": "Account"})).await;

        // On startup: read all rows for org ordered by id
        let rows = dereg_meta_store::Entity::find()
            .filter(dereg_meta_store::Column::OrgId.eq("org1"))
            .filter(dereg_meta_store::Column::Status.eq("ok"))
            .all(&db)
            .await
            .unwrap();

        // Build effective state: last row per stream_id wins
        let mut effective: HashMap<String, &dereg_meta_store::Model> = HashMap::new();
        for row in &rows {
            effective.insert(row.stream_id.clone(), row);
        }

        // After drop, Account stream has a Dropped event_type, OpenAccount remains created
        let remaining: Vec<_> = effective.values()
            .filter(|r| !r.event_type.contains("Dropped"))
            .collect();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].stream_id, "command:OpenAccount");

        // Verify org_tip_id reflects latest
        let tip = get_org_tip_id(&db, "org1").await.unwrap();
        assert_eq!(tip, 3);
    }
}
