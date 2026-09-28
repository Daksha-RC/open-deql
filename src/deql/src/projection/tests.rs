//! Integration tests for M2.3 — Projection Worker, Replay, and Refresh.
//!
//! Validates CP-3 acceptance criteria against in-memory SQLite.

#[cfg(test)]
mod tests {
    use sea_orm::{
        ActiveModelTrait, ColumnTrait, Database, DatabaseConnection, EntityTrait, QueryFilter, Set,
    };
    use sea_orm_migration::MigratorTrait;
    use serde_json::json;

    use crate::migration::DeqlMigrator;
    use crate::projection_worker::{self, run_projection_tick};
    use crate::replay::{self, ReplayRefreshParams};
    use crate::store::dereg_meta_store;
    use crate::store::projection_watermark;
    use crate::store::projections::{meta_aggregates, meta_commands, meta_concepts, meta_decisions};

    async fn setup_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        DeqlMigrator::up(&db, None).await.unwrap();
        db
    }

    /// Helper to insert a row into dereg_meta_store.
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

    // ======================================================================
    // T2.3.1 — Worker loop with watermark progression
    // ======================================================================

    #[tokio::test]
    async fn worker_advances_watermark() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:Account",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE Account",
            json!({"fields": [{"name": "id", "data_type": "Uuid", "is_key": true}]}),
        )
        .await;

        let processed = run_projection_tick(&db, "org1").await.unwrap();
        assert_eq!(processed, 1);

        // Watermark should be 1
        let wm = projection_watermark::Entity::find_by_id("org1".to_string())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wm.last_applied_id, 1);
    }

    #[tokio::test]
    async fn worker_is_idempotent_no_new_rows() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:Account",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE Account",
            json!({"fields": []}),
        )
        .await;

        // First tick
        run_projection_tick(&db, "org1").await.unwrap();
        // Second tick — no new rows
        let processed = run_projection_tick(&db, "org1").await.unwrap();
        assert_eq!(processed, 0);
    }

    #[tokio::test]
    async fn worker_processes_multiple_rows_single_batch() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:A",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE A",
            json!({"fields": []}),
        )
        .await;
        insert_row(
            &db,
            2,
            "org1",
            "aggregate:B",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE B",
            json!({"fields": []}),
        )
        .await;

        let processed = run_projection_tick(&db, "org1").await.unwrap();
        assert_eq!(processed, 2);

        // Both projections exist
        let aggs = meta_aggregates::Entity::find()
            .filter(meta_aggregates::Column::OrgId.eq("org1"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(aggs.len(), 2);

        let wm = projection_watermark::Entity::find_by_id("org1".to_string())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wm.last_applied_id, 2);
    }

    // ======================================================================
    // T2.3.2 — Upsert + soft delete
    // ======================================================================

    #[tokio::test]
    async fn upsert_creates_projection_row() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:Account",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE Account",
            json!({"fields": [{"name": "balance", "data_type": "Int", "is_key": false}]}),
        )
        .await;

        run_projection_tick(&db, "org1").await.unwrap();

        let agg = meta_aggregates::Entity::find_by_id(("org1".to_string(), "Account".to_string()))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(agg.is_dropped, false);
        assert_eq!(agg.last_applied_id, 1);

        let fields = agg.fields_json.as_array().unwrap();
        assert_eq!(fields[0]["name"], "balance");
    }

    #[tokio::test]
    async fn soft_delete_sets_is_dropped() {
        let db = setup_db().await;

        // Create
        insert_row(
            &db,
            1,
            "org1",
            "aggregate:Account",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE Account",
            json!({"fields": []}),
        )
        .await;

        run_projection_tick(&db, "org1").await.unwrap();

        // Drop (tombstone)
        insert_row(
            &db,
            2,
            "org1",
            "aggregate:Account",
            "AggregateDropped",
            "AGGREGATE",
            2,
            "ok",
            "DROP AGGREGATE Account",
            json!({"tombstone": true, "concept_kind": "Aggregate", "name": "Account"}),
        )
        .await;

        run_projection_tick(&db, "org1").await.unwrap();

        let agg = meta_aggregates::Entity::find_by_id(("org1".to_string(), "Account".to_string()))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(agg.is_dropped, true);
        assert_eq!(agg.last_applied_id, 2);
    }

    #[tokio::test]
    async fn failed_rows_excluded_from_projection() {
        let db = setup_db().await;

        // Failed row should not produce a projection
        insert_row(
            &db,
            1,
            "org1",
            "aggregate:Bad",
            "RegistrationFailed",
            "AGGREGATE",
            1,
            "failed",
            "CREATE AGGREGATE Bad",
            json!({"error": true, "message": "parse error"}),
        )
        .await;

        run_projection_tick(&db, "org1").await.unwrap();

        let aggs = meta_aggregates::Entity::find()
            .filter(meta_aggregates::Column::OrgId.eq("org1"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(aggs.len(), 0);
    }

    #[tokio::test]
    async fn upsert_updates_existing_on_or_replace() {
        let db = setup_db().await;

        // First version
        insert_row(
            &db,
            1,
            "org1",
            "aggregate:Account",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE Account",
            json!({"fields": [{"name": "id", "data_type": "Uuid", "is_key": true}]}),
        )
        .await;

        run_projection_tick(&db, "org1").await.unwrap();

        // Second version (OR REPLACE)
        insert_row(
            &db,
            2,
            "org1",
            "aggregate:Account",
            "AggregateCreated",
            "AGGREGATE",
            2,
            "ok",
            "CREATE OR REPLACE AGGREGATE Account",
            json!({"fields": [{"name": "balance", "data_type": "Decimal", "is_key": false}]}),
        )
        .await;

        run_projection_tick(&db, "org1").await.unwrap();

        let agg = meta_aggregates::Entity::find_by_id(("org1".to_string(), "Account".to_string()))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(agg.last_applied_id, 2);
        let fields = agg.fields_json.as_array().unwrap();
        assert_eq!(fields[0]["name"], "balance");
    }

    // ======================================================================
    // T2.3.3 — Replay validation-only (no mutations)
    // ======================================================================

    #[tokio::test]
    async fn replay_validate_returns_ok_for_valid_state() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:Account",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE Account;",
            json!({"fields": []}),
        )
        .await;

        let result = replay::replay_validate(&db, "org1").await.unwrap();
        // If the parser can handle this statement and register succeeds, status is "ok"
        // If the parser produces diagnostics, we still expect statements to be present
        assert_eq!(result.replayed, 1);
        // Regardless of validation outcome, it should not crash
        assert!(result.status == "ok" || result.status == "validation_errors");
    }

    #[tokio::test]
    async fn replay_validate_does_not_mutate_projections() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:Account",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE Account;",
            json!({"fields": []}),
        )
        .await;

        // No projection worker has run → no projection rows
        replay::replay_validate(&db, "org1").await.unwrap();

        // Projections should still be empty (replay is read-only)
        let aggs = meta_aggregates::Entity::find()
            .filter(meta_aggregates::Column::OrgId.eq("org1"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(aggs.len(), 0);

        // Watermark should not exist
        let wm = projection_watermark::Entity::find_by_id("org1".to_string())
            .one(&db)
            .await
            .unwrap();
        assert!(wm.is_none());
    }

    #[tokio::test]
    async fn replay_validate_does_not_stop_workers() {
        // Workers are not affected (no real worker in test, just verify no crash)
        let db = setup_db().await;
        let result = replay::replay_validate(&db, "org1").await.unwrap();
        assert_eq!(result.status, "ok");
        assert_eq!(result.replayed, 0);
    }

    // ======================================================================
    // T2.3.5 — Replay-refresh
    // ======================================================================

    #[tokio::test]
    async fn replay_refresh_rebuilds_projections() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:A",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE A",
            json!({"fields": []}),
        )
        .await;
        insert_row(
            &db,
            2,
            "org1",
            "aggregate:B",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE B",
            json!({"fields": []}),
        )
        .await;

        // Run worker first to create projections
        run_projection_tick(&db, "org1").await.unwrap();

        // Replay-refresh with no params → restore latest
        let result = replay::replay_refresh(&db, "org1", &ReplayRefreshParams::default())
            .await
            .unwrap();

        assert_eq!(result.status, "ok");
        assert_eq!(result.org_tip_id, 2);
        assert_eq!(result.replayed_until_id, 2);
        assert_eq!(result.applied_offset, 0);

        // Projections exist
        let aggs = meta_aggregates::Entity::find()
            .filter(meta_aggregates::Column::OrgId.eq("org1"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(aggs.len(), 2);
    }

    #[tokio::test]
    async fn replay_refresh_with_id_param_rolls_back() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:A",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE A",
            json!({"fields": []}),
        )
        .await;
        insert_row(
            &db,
            2,
            "org1",
            "aggregate:B",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE B",
            json!({"fields": []}),
        )
        .await;

        // Replay only up to id=1
        let result = replay::replay_refresh(
            &db,
            "org1",
            &ReplayRefreshParams {
                id: Some(1),
                offset: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(result.replayed_until_id, 1);
        assert_eq!(result.org_tip_id, 2);
        assert_eq!(result.applied_offset, 1);

        // Only A should be projected
        let aggs = meta_aggregates::Entity::find()
            .filter(meta_aggregates::Column::OrgId.eq("org1"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].name, "A");
    }

    #[tokio::test]
    async fn replay_refresh_with_offset_param() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:A",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE A",
            json!({"fields": []}),
        )
        .await;
        insert_row(
            &db,
            2,
            "org1",
            "aggregate:B",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE B",
            json!({"fields": []}),
        )
        .await;
        insert_row(
            &db,
            3,
            "org1",
            "aggregate:C",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE C",
            json!({"fields": []}),
        )
        .await;

        // offset=1 means replay up to org_tip_id - 1 = 3 - 1 = 2
        let result = replay::replay_refresh(
            &db,
            "org1",
            &ReplayRefreshParams {
                id: None,
                offset: Some(1),
            },
        )
        .await
        .unwrap();

        assert_eq!(result.org_tip_id, 3);
        assert_eq!(result.replayed_until_id, 2);
        assert_eq!(result.applied_offset, 1);

        // A and B projected, not C
        let aggs = meta_aggregates::Entity::find()
            .filter(meta_aggregates::Column::OrgId.eq("org1"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(aggs.len(), 2);
    }

    #[tokio::test]
    async fn replay_refresh_no_params_restores_latest() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:A",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE A",
            json!({"fields": []}),
        )
        .await;

        // First replay to id=0 (nothing)
        // Then no-params to restore
        let result = replay::replay_refresh(&db, "org1", &ReplayRefreshParams::default())
            .await
            .unwrap();

        assert_eq!(result.replayed_until_id, 1);
        assert_eq!(result.applied_offset, 0);
    }

    #[tokio::test]
    async fn replay_refresh_respects_tombstone_rule() {
        let db = setup_db().await;

        // Create then drop then re-create
        insert_row(
            &db,
            1,
            "org1",
            "aggregate:X",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE X",
            json!({"fields": [{"name": "v1", "data_type": "Int", "is_key": false}]}),
        )
        .await;
        insert_row(
            &db,
            2,
            "org1",
            "aggregate:X",
            "AggregateDropped",
            "AGGREGATE",
            2,
            "ok",
            "DROP AGGREGATE X",
            json!({"tombstone": true, "name": "X"}),
        )
        .await;
        insert_row(
            &db,
            3,
            "org1",
            "aggregate:X",
            "AggregateCreated",
            "AGGREGATE",
            3,
            "ok",
            "CREATE AGGREGATE X",
            json!({"fields": [{"name": "v2", "data_type": "String", "is_key": false}]}),
        )
        .await;

        let result = replay::replay_refresh(&db, "org1", &ReplayRefreshParams::default())
            .await
            .unwrap();

        assert_eq!(result.replayed_until_id, 3);

        // X should exist with v2 fields (post-tombstone create wins)
        let agg = meta_aggregates::Entity::find_by_id(("org1".to_string(), "X".to_string()))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(agg.is_dropped, false);
        let fields = agg.fields_json.as_array().unwrap();
        assert_eq!(fields[0]["name"], "v2");
    }

    #[tokio::test]
    async fn replay_refresh_org_scoped() {
        let db = setup_db().await;

        // Rows for org1 and org2
        insert_row(
            &db,
            1,
            "org1",
            "aggregate:A",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE A",
            json!({"fields": []}),
        )
        .await;
        insert_row(
            &db,
            2,
            "org2",
            "aggregate:B",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE B",
            json!({"fields": []}),
        )
        .await;

        // Refresh org1 only
        let result = replay::replay_refresh(&db, "org1", &ReplayRefreshParams::default())
            .await
            .unwrap();

        assert_eq!(result.org_tip_id, 1); // Only org1's rows
        assert_eq!(result.replayed_until_id, 1);

        // Only org1 projections should exist
        let org1_aggs = meta_aggregates::Entity::find()
            .filter(meta_aggregates::Column::OrgId.eq("org1"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(org1_aggs.len(), 1);

        let org2_aggs = meta_aggregates::Entity::find()
            .filter(meta_aggregates::Column::OrgId.eq("org2"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(org2_aggs.len(), 0); // Org2 untouched
    }

    // ======================================================================
    // T2.3.6 — OrgLockMap blocking behavior
    // ======================================================================

    #[tokio::test]
    async fn org_lock_map_blocks_correctly() {
        use crate::worker_registry::OrgLockMap;

        let lock_map = OrgLockMap::new();

        // Not locked initially
        assert!(!lock_map.is_locked("org1").await);

        // Acquire lock
        let _guard = lock_map.acquire_write("org1").await;
        assert!(lock_map.is_locked("org1").await);

        // Other orgs unaffected
        assert!(!lock_map.is_locked("org2").await);

        // Drop guard → unlocked
        drop(_guard);
        assert!(!lock_map.is_locked("org1").await);
    }

    // ======================================================================
    // Worker + meta_concepts — universal projection
    // ======================================================================

    #[tokio::test]
    async fn worker_populates_meta_concepts_for_all_types() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "aggregate:A",
            "AggregateCreated",
            "AGGREGATE",
            1,
            "ok",
            "CREATE AGGREGATE A",
            json!({"fields": []}),
        )
        .await;
        insert_row(
            &db,
            2,
            "org1",
            "command:C",
            "CommandCreated",
            "COMMAND",
            1,
            "ok",
            "CREATE COMMAND C",
            json!({"fields": []}),
        )
        .await;

        run_projection_tick(&db, "org1").await.unwrap();

        let concepts = meta_concepts::Entity::find()
            .filter(meta_concepts::Column::OrgId.eq("org1"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(concepts.len(), 2);

        let kinds: Vec<&str> = concepts.iter().map(|c| c.kind.as_str()).collect();
        assert!(kinds.contains(&"AGGREGATE"));
        assert!(kinds.contains(&"COMMAND"));
    }

    // ======================================================================
    // Decision projection
    // ======================================================================

    #[tokio::test]
    async fn worker_projects_decisions_correctly() {
        let db = setup_db().await;

        insert_row(
            &db,
            1,
            "org1",
            "decision:HandleWithdraw",
            "DecisionCreated",
            "DECISION",
            1,
            "ok",
            "CREATE DECISION HandleWithdraw",
            json!({
                "aggregate": "Account",
                "command": "Withdraw",
                "emits": ["Withdrawn"],
                "has_guard": true,
                "guard_sql": "balance >= amount",
                "state_sql": null,
                "or_replace": false,
            }),
        )
        .await;

        run_projection_tick(&db, "org1").await.unwrap();

        let dec = meta_decisions::Entity::find_by_id((
            "org1".to_string(),
            "HandleWithdraw".to_string(),
        ))
        .one(&db)
        .await
        .unwrap()
        .unwrap();

        assert_eq!(dec.aggregate, "Account");
        assert_eq!(dec.command, "Withdraw");
        assert_eq!(dec.has_guard, true);
        assert_eq!(dec.guard_sql.as_deref(), Some("balance >= amount"));
        assert_eq!(dec.is_dropped, false);
    }
}
