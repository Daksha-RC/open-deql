//! Integration tests for Phase 2 SeaORM entities and migrations.
//!
//! These tests run against an in-memory SQLite database to validate:
//! - Migrations apply cleanly on fresh DB
//! - Migrations are idempotent on restart
//! - Basic CRUD operations on all entities

#[cfg(test)]
mod tests {
    use sea_orm::{ActiveModelTrait, Database, DatabaseConnection, EntityTrait, Set};
    use sea_orm_migration::MigratorTrait;
    use serde_json::json;

    use crate::migration::DeqlMigrator;
    use crate::store::dereg_meta_store;
    use crate::store::projection_watermark;
    use crate::store::projections::{
        meta_aggregates, meta_commands, meta_concepts, meta_decisions, meta_events,
        meta_inspections, meta_templates, meta_templates_instances,
    };

    async fn setup_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        DeqlMigrator::up(&db, None).await.unwrap();
        db
    }

    #[tokio::test]
    async fn migrations_apply_cleanly_on_fresh_db() {
        let _db = setup_db().await;
        // If we get here, migrations applied successfully
    }

    #[tokio::test]
    async fn migrations_are_idempotent() {
        let db = setup_db().await;
        // Running again should not error (idempotent with IF NOT EXISTS)
        DeqlMigrator::up(&db, None).await.unwrap();
    }

    #[tokio::test]
    async fn crud_dereg_meta_store() {
        let db = setup_db().await;

        let row = dereg_meta_store::ActiveModel {
            id: Set(1),
            org_id: Set("org1".to_string()),
            stream_id: Set("aggregate:BankAccount".to_string()),
            event_type: Set("AggregateCreated".to_string()),
            concept_type: Set("AGGREGATE".to_string()),
            concept_key: Set(1),
            occurred_at: Set(chrono::Utc::now().fixed_offset()),
            status: Set("ok".to_string()),
            error_message: Set(None),
            statement: Set("CREATE AGGREGATE BankAccount (id UUID)".to_string()),
            meta: Set(json!({"name": "BankAccount"})),
        };
        let inserted = row.insert(&db).await.unwrap();
        assert_eq!(inserted.id, 1);
        assert_eq!(inserted.org_id, "org1");
        assert_eq!(inserted.status, "ok");

        // Read back
        let found = dereg_meta_store::Entity::find_by_id(1)
            .one(&db)
            .await
            .unwrap();
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.stream_id, "aggregate:BankAccount");
        assert_eq!(found.concept_key, 1);
    }

    #[tokio::test]
    async fn crud_projection_watermark() {
        let db = setup_db().await;

        let row = projection_watermark::ActiveModel {
            org_id: Set("org1".to_string()),
            last_applied_id: Set(0),
            updated_at: Set(chrono::Utc::now().fixed_offset()),
        };
        row.insert(&db).await.unwrap();

        let found = projection_watermark::Entity::find_by_id("org1".to_string())
            .one(&db)
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().last_applied_id, 0);
    }

    #[tokio::test]
    async fn crud_meta_concepts() {
        let db = setup_db().await;

        let row = meta_concepts::ActiveModel {
            org_id: Set("org1".to_string()),
            kind: Set("AGGREGATE".to_string()),
            name: Set("BankAccount".to_string()),
            json_source: Set(json!({"fields": []})),
            last_applied_id: Set(1),
            is_dropped: Set(false),
        };
        row.insert(&db).await.unwrap();

        let all = meta_concepts::Entity::find().all(&db).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "BankAccount");
        assert!(!all[0].is_dropped);
    }

    #[tokio::test]
    async fn crud_meta_aggregates() {
        let db = setup_db().await;

        let row = meta_aggregates::ActiveModel {
            org_id: Set("org1".to_string()),
            name: Set("BankAccount".to_string()),
            fields_json: Set(json!([{"name": "id", "type": "UUID"}])),
            last_applied_id: Set(1),
            is_dropped: Set(false),
        };
        row.insert(&db).await.unwrap();

        let all = meta_aggregates::Entity::find().all(&db).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "BankAccount");
    }

    #[tokio::test]
    async fn crud_meta_decisions() {
        let db = setup_db().await;

        let row = meta_decisions::ActiveModel {
            org_id: Set("org1".to_string()),
            name: Set("HandleWithdraw".to_string()),
            aggregate: Set("BankAccount".to_string()),
            command: Set("Withdraw".to_string()),
            emits_json: Set(json!(["WithdrawAccepted"])),
            has_guard: Set(true),
            guard_sql: Set(Some("balance >= :amount".to_string())),
            state_sql: Set(None),
            full_sql: Set("CREATE DECISION HandleWithdraw ...".to_string()),
            last_applied_id: Set(1),
            is_dropped: Set(false),
        };
        row.insert(&db).await.unwrap();

        let all = meta_decisions::Entity::find().all(&db).await.unwrap();
        assert_eq!(all.len(), 1);
        assert!(all[0].has_guard);
    }

    #[tokio::test]
    async fn crud_meta_inspections() {
        let db = setup_db().await;

        let row = meta_inspections::ActiveModel {
            org_id: Set("org1".to_string()),
            name: Set("GetBalance".to_string()),
            decision_name: Set("HandleWithdraw".to_string()),
            input_output_json: Set(json!({"input": "BankAccount", "output": "BalanceSnapshot"})),
            full_sql: Set("INSPECT DECISION ...".to_string()),
            last_applied_id: Set(1),
            is_dropped: Set(false),
        };
        row.insert(&db).await.unwrap();

        let all = meta_inspections::Entity::find().all(&db).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].decision_name, "HandleWithdraw");
    }

    #[tokio::test]
    async fn crud_meta_commands() {
        let db = setup_db().await;

        let row = meta_commands::ActiveModel {
            org_id: Set("org1".to_string()),
            name: Set("Withdraw".to_string()),
            aggregate: Set("BankAccount".to_string()),
            attributes_json: Set(json!([{"name": "amount", "type": "decimal"}])),
            full_sql: Set("CREATE COMMAND Withdraw ...".to_string()),
            last_applied_id: Set(1),
            is_dropped: Set(false),
        };
        row.insert(&db).await.unwrap();

        let all = meta_commands::Entity::find().all(&db).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].aggregate, "BankAccount");
    }

    #[tokio::test]
    async fn crud_meta_events() {
        let db = setup_db().await;

        let row = meta_events::ActiveModel {
            org_id: Set("org1".to_string()),
            name: Set("WithdrawAccepted".to_string()),
            aggregate: Set("BankAccount".to_string()),
            attributes_json: Set(json!([{"name": "amount", "type": "decimal"}])),
            full_sql: Set("CREATE EVENT WithdrawAccepted ...".to_string()),
            last_applied_id: Set(1),
            is_dropped: Set(false),
        };
        row.insert(&db).await.unwrap();

        let all = meta_events::Entity::find().all(&db).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn crud_meta_templates() {
        let db = setup_db().await;

        let row = meta_templates::ActiveModel {
            org_id: Set("org1".to_string()),
            name: Set("AuditLog".to_string()),
            parameters_json: Set(json!(["entity_type", "entity_id"])),
            full_sql: Set("CREATE TEMPLATE AuditLog ...".to_string()),
            last_applied_id: Set(1),
            is_dropped: Set(false),
        };
        row.insert(&db).await.unwrap();

        let all = meta_templates::Entity::find().all(&db).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn crud_meta_templates_instances() {
        let db = setup_db().await;

        let row = meta_templates_instances::ActiveModel {
            org_id: Set("org1".to_string()),
            template_name: Set("AuditLog".to_string()),
            args_json: Set(json!({})),
            generated_names_json: Set(json!(["AuditLogEvents"])),
            last_applied_id: Set(1),
            is_dropped: Set(false),
        };
        row.insert(&db).await.unwrap();

        let all = meta_templates_instances::Entity::find()
            .all(&db)
            .await
            .unwrap();
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn soft_delete_via_is_dropped_toggle() {
        let db = setup_db().await;

        // Insert concept
        let row = meta_aggregates::ActiveModel {
            org_id: Set("org1".to_string()),
            name: Set("Account".to_string()),
            fields_json: Set(json!([])),
            last_applied_id: Set(1),
            is_dropped: Set(false),
        };
        row.insert(&db).await.unwrap();

        use sea_orm::IntoActiveModel;
        let found = meta_aggregates::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let mut active: meta_aggregates::ActiveModel = found.into_active_model();
        active.is_dropped = Set(true);
        active.last_applied_id = Set(2);
        active.update(&db).await.unwrap();

        // Verify is_dropped persists
        let found = meta_aggregates::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(found.is_dropped);
        assert_eq!(found.last_applied_id, 2);
    }
}
