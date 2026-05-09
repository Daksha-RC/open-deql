//! Write-path integration tests for M2.2.
//!
//! Tests exercise CREATE/DROP flows with in-memory DeReg + persistence,
//! validating CP-2 acceptance criteria.

#[cfg(test)]
mod tests {
    use sea_orm::{Database, DatabaseConnection, EntityTrait, TransactionTrait};
    use sea_orm_migration::MigratorTrait;

    use crate::allocator;
    use crate::dereg::DeReg;
    use crate::error::ConceptKind;
    use crate::meta_json;
    use crate::migration::DeqlMigrator;
    use crate::org_registry::OrgDeRegMap;
    use crate::parser::ast::*;
    use crate::parser::token::Span;
    use crate::store::dereg_meta_store;

    async fn setup_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        DeqlMigrator::up(&db, None).await.unwrap();
        db
    }

    fn span() -> Span {
        Span { start: 0, end: 0 }
    }

    fn spanned<T>(node: T) -> Spanned<T> {
        Spanned { node, span: span() }
    }

    fn make_aggregate(name: &str) -> DeqlStatement {
        DeqlStatement::CreateAggregate(CreateAggregate {
            or_replace: false,
            name: spanned(name.to_string()),
            fields: Some(vec![FieldDef {
                name: spanned("id".to_string()),
                data_type: spanned(DeqlType::Uuid),
                is_key: true,
                annotation: None,
            }]),
        })
    }

    fn make_command(name: &str) -> DeqlStatement {
        DeqlStatement::CreateCommand(CreateCommand {
            or_replace: false,
            name: spanned(name.to_string()),
            fields: vec![FieldDef {
                name: spanned("amount".to_string()),
                data_type: spanned(DeqlType::Int),
                is_key: false,
                annotation: None,
            }],
        })
    }

    fn make_event(name: &str) -> DeqlStatement {
        DeqlStatement::CreateEvent(CreateEvent {
            or_replace: false,
            name: spanned(name.to_string()),
            fields: vec![FieldDef {
                name: spanned("amount".to_string()),
                data_type: spanned(DeqlType::Int),
                is_key: false,
                annotation: None,
            }],
        })
    }

    fn make_decision(name: &str, agg: &str, cmd: &str, event: &str) -> DeqlStatement {
        DeqlStatement::CreateDecision(CreateDecision {
            or_replace: false,
            name: spanned(name.to_string()),
            aggregate: spanned(agg.to_string()),
            command: spanned(cmd.to_string()),
            state_as: None,
            branches: vec![DecisionBranch {
                branch_index: 0,
                rule_name: None,
                guard: None,
                emit_items: vec![EmitItem {
                    event_type: spanned(event.to_string()),
                    assignments: vec![],
                    span: span(),
                }],
                span: span(),
            }],
        })
    }

    /// Simulates the full write path: parse → register in-memory → allocate ids → persist.
    async fn write_definition(
        db: &DatabaseConnection,
        dereg: &mut DeReg,
        org_id: &str,
        stmt: &DeqlStatement,
        raw_sql: &str,
    ) -> Result<i64, String> {
        let result = dereg.register_statement(stmt);

        let txn = db.begin().await.unwrap();
        let id = allocator::allocate_next_id_txn(&txn).await.unwrap();

        match result {
            Ok(reg) => {
                let stream_id = format!(
                    "{}:{}",
                    format!("{:?}", reg.concept_type).to_lowercase(),
                    reg.concept_name
                );
                let concept_key =
                    allocator::allocate_concept_key_txn(&txn, org_id, &stream_id)
                        .await
                        .unwrap();
                let meta = meta_json::build_meta(stmt);

                use sea_orm::ActiveModelTrait;
                use sea_orm::Set;
                let model = dereg_meta_store::ActiveModel {
                    id: Set(id),
                    org_id: Set(org_id.to_string()),
                    stream_id: Set(stream_id),
                    event_type: Set(reg.event_type.to_string()),
                    concept_type: Set(format!("{:?}", reg.concept_type).to_uppercase()),
                    concept_key: Set(concept_key),
                    occurred_at: Set(chrono::Utc::now().into()),
                    status: Set("ok".to_string()),
                    error_message: Set(None),
                    statement: Set(raw_sql.to_string()),
                    meta: Set(meta),
                };
                model.insert(&txn).await.unwrap();
                txn.commit().await.unwrap();
                Ok(id)
            }
            Err(e) => {
                let meta = meta_json::build_error_meta(&e.to_string());
                use sea_orm::ActiveModelTrait;
                use sea_orm::Set;
                let model = dereg_meta_store::ActiveModel {
                    id: Set(id),
                    org_id: Set(org_id.to_string()),
                    stream_id: Set("unknown".to_string()),
                    event_type: Set("RegistrationFailed".to_string()),
                    concept_type: Set("UNKNOWN".to_string()),
                    concept_key: Set(0),
                    occurred_at: Set(chrono::Utc::now().into()),
                    status: Set("failed".to_string()),
                    error_message: Set(Some(e.to_string())),
                    statement: Set(raw_sql.to_string()),
                    meta: Set(meta),
                };
                model.insert(&txn).await.unwrap();
                txn.commit().await.unwrap();
                Err(e.to_string())
            }
        }
    }

    // --- CREATE flow tests ---

    #[tokio::test]
    async fn create_aggregate_persists_row_with_meta() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();
        let stmt = make_aggregate("BankAccount");

        let id = write_definition(&db, &mut dereg, "org1", &stmt, "CREATE AGGREGATE BankAccount")
            .await
            .unwrap();

        assert_eq!(id, 1);

        // Verify row in DB
        let rows = dereg_meta_store::Entity::find()
            .all(&db)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].org_id, "org1");
        assert_eq!(rows[0].event_type, "AggregateCreated");
        assert_eq!(rows[0].concept_type, "AGGREGATE");
        assert_eq!(rows[0].status, "ok");

        // Verify meta JSON has expected fields
        let meta = &rows[0].meta;
        assert!(meta.get("fields").is_some());
        assert_eq!(meta["or_replace"], false);
    }

    #[tokio::test]
    async fn create_decision_validates_refs_and_persists() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        // Register prerequisites
        write_definition(
            &db,
            &mut dereg,
            "org1",
            &make_aggregate("Account"),
            "CREATE AGGREGATE Account",
        )
        .await
        .unwrap();
        write_definition(
            &db,
            &mut dereg,
            "org1",
            &make_command("Withdraw"),
            "CREATE COMMAND Withdraw",
        )
        .await
        .unwrap();
        write_definition(
            &db,
            &mut dereg,
            "org1",
            &make_event("Withdrawn"),
            "CREATE EVENT Withdrawn",
        )
        .await
        .unwrap();

        // Now create decision referencing them
        let stmt = make_decision("HandleWithdraw", "Account", "Withdraw", "Withdrawn");
        let id = write_definition(
            &db,
            &mut dereg,
            "org1",
            &stmt,
            "CREATE DECISION HandleWithdraw",
        )
        .await
        .unwrap();

        assert_eq!(id, 4);
        let rows = dereg_meta_store::Entity::find()
            .all(&db)
            .await
            .unwrap();
        let decision_row = rows.iter().find(|r| r.event_type == "DecisionCreated").unwrap();
        assert_eq!(decision_row.status, "ok");
        assert_eq!(decision_row.meta["aggregate"], "Account");
        assert_eq!(decision_row.meta["command"], "Withdraw");
    }

    #[tokio::test]
    async fn create_decision_fails_validation_persists_error() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        // Decision references non-existent aggregate
        let stmt = make_decision("BadDecision", "NoSuchAgg", "NoCmd", "NoEvent");
        let err = write_definition(&db, &mut dereg, "org1", &stmt, "CREATE DECISION BadDecision")
            .await
            .unwrap_err();

        assert!(err.contains("MissingReferences") || err.contains("missing"));

        // Row persisted as failed
        let rows = dereg_meta_store::Entity::find()
            .all(&db)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "failed");
        assert!(rows[0].error_message.is_some());
    }

    #[tokio::test]
    async fn duplicate_create_without_replace_fails() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        write_definition(&db, &mut dereg, "org1", &make_aggregate("X"), "CREATE AGGREGATE X")
            .await
            .unwrap();

        let err =
            write_definition(&db, &mut dereg, "org1", &make_aggregate("X"), "CREATE AGGREGATE X")
                .await
                .unwrap_err();

        assert!(err.contains("Duplicate name") || err.contains("duplicate"));
    }

    // --- Id monotonicity ---

    #[tokio::test]
    async fn ids_are_globally_monotonic_across_orgs() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        let id1 = write_definition(&db, &mut dereg, "org1", &make_aggregate("A"), "A")
            .await
            .unwrap();
        let id2 = write_definition(&db, &mut dereg, "org2", &make_aggregate("B"), "B")
            .await
            .unwrap();
        let id3 = write_definition(&db, &mut dereg, "org1", &make_command("C"), "C")
            .await
            .unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    // --- DROP flow tests ---

    #[tokio::test]
    async fn drop_removes_from_memory_and_writes_tombstone() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        write_definition(&db, &mut dereg, "org1", &make_aggregate("X"), "CREATE AGGREGATE X")
            .await
            .unwrap();

        // DROP
        let drop_result = dereg.drop_concept(ConceptKind::Aggregate, "X").unwrap();
        assert_eq!(drop_result.concept_name, "X");

        // Verify removed from memory
        assert!(dereg.get_aggregate("X").is_none());

        // Write tombstone
        let txn = db.begin().await.unwrap();
        let id = allocator::allocate_next_id_txn(&txn).await.unwrap();
        let meta = meta_json::build_tombstone_meta(ConceptKind::Aggregate, "X");
        use sea_orm::ActiveModelTrait;
        use sea_orm::Set;
        let model = dereg_meta_store::ActiveModel {
            id: Set(id),
            org_id: Set("org1".to_string()),
            stream_id: Set("aggregate:X".to_string()),
            event_type: Set("AggregateDropped".to_string()),
            concept_type: Set("AGGREGATE".to_string()),
            concept_key: Set(0),
            occurred_at: Set(chrono::Utc::now().into()),
            status: Set("ok".to_string()),
            error_message: Set(None),
            statement: Set("DROP AGGREGATE X".to_string()),
            meta: Set(meta),
        };
        model.insert(&txn).await.unwrap();
        txn.commit().await.unwrap();

        // Verify tombstone in DB
        let rows = dereg_meta_store::Entity::find()
            .all(&db)
            .await
            .unwrap();
        let tombstone = rows.iter().find(|r| r.event_type == "AggregateDropped").unwrap();
        assert_eq!(tombstone.meta["tombstone"], true);
    }

    #[tokio::test]
    async fn double_drop_returns_not_found() {
        let mut dereg = DeReg::new();

        // Register then drop
        let stmt = make_aggregate("X");
        dereg.register_statement(&stmt).unwrap();
        dereg.drop_concept(ConceptKind::Aggregate, "X").unwrap();

        // Second drop → NotFound
        let err = dereg.drop_concept(ConceptKind::Aggregate, "X").unwrap_err();
        match err {
            crate::error::DeRegError::NotFound { concept_kind, name } => {
                assert_eq!(concept_kind, ConceptKind::Aggregate);
                assert_eq!(name, "X");
            }
            _ => panic!("Expected NotFound, got {:?}", err),
        }
    }

    #[tokio::test]
    async fn drop_never_created_returns_not_found() {
        let mut dereg = DeReg::new();

        let err = dereg
            .drop_concept(ConceptKind::Aggregate, "NeverCreated")
            .unwrap_err();
        match err {
            crate::error::DeRegError::NotFound { name, .. } => {
                assert_eq!(name, "NeverCreated");
            }
            _ => panic!("Expected NotFound"),
        }
    }

    #[tokio::test]
    async fn drop_decision_removes_command_binding() {
        let mut dereg = DeReg::new();

        // Setup
        dereg
            .register_statement(&make_aggregate("Account"))
            .unwrap();
        dereg
            .register_statement(&make_command("Withdraw"))
            .unwrap();
        dereg
            .register_statement(&make_event("Withdrawn"))
            .unwrap();
        dereg
            .register_statement(&make_decision(
                "HandleWithdraw",
                "Account",
                "Withdraw",
                "Withdrawn",
            ))
            .unwrap();

        // Command binding exists
        assert!(dereg.command_map.contains_key("Withdraw"));

        // DROP decision
        dereg
            .drop_concept(ConceptKind::Decision, "HandleWithdraw")
            .unwrap();

        // Command binding removed
        assert!(!dereg.command_map.contains_key("Withdraw"));
    }

    // --- OrgDeRegMap tests ---

    #[tokio::test]
    async fn org_map_isolates_orgs() {
        let map = OrgDeRegMap::new();

        let org1 = map.get_or_init("org1").await;
        let org2 = map.get_or_init("org2").await;

        // Register aggregate in org1
        {
            let mut d = org1.write().await;
            d.register_statement(&make_aggregate("X")).unwrap();
        }

        // org2 should not see it
        {
            let d = org2.read().await;
            assert!(d.get_aggregate("X").is_none());
        }

        // org1 should see it
        {
            let d = org1.read().await;
            assert!(d.get_aggregate("X").is_some());
        }
    }

    #[tokio::test]
    async fn org_map_returns_same_instance() {
        let map = OrgDeRegMap::new();

        let first = map.get_or_init("org1").await;
        {
            let mut d = first.write().await;
            d.register_statement(&make_aggregate("Y")).unwrap();
        }

        let second = map.get_or_init("org1").await;
        {
            let d = second.read().await;
            assert!(d.get_aggregate("Y").is_some());
        }
    }

    // --- Meta JSON tests ---

    #[tokio::test]
    async fn meta_json_aggregate_has_fields() {
        let stmt = make_aggregate("Acct");
        let meta = meta_json::build_meta(&stmt);
        let fields = meta["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0]["name"], "id");
        assert_eq!(fields[0]["is_key"], true);
    }

    #[tokio::test]
    async fn meta_json_decision_has_emits() {
        let stmt = make_decision("D", "Agg", "Cmd", "Evt");
        let meta = meta_json::build_meta(&stmt);
        assert_eq!(meta["aggregate"], "Agg");
        assert_eq!(meta["command"], "Cmd");
        let emits = meta["emits"].as_array().unwrap();
        assert_eq!(emits[0], "Evt");
    }

    #[tokio::test]
    async fn meta_json_tombstone_has_required_fields() {
        let meta = meta_json::build_tombstone_meta(ConceptKind::Aggregate, "X");
        assert_eq!(meta["tombstone"], true);
        assert_eq!(meta["name"], "X");
    }

    #[tokio::test]
    async fn meta_json_error_has_message() {
        let meta = meta_json::build_error_meta("something went wrong");
        assert_eq!(meta["error"], true);
        assert_eq!(meta["message"], "something went wrong");
    }

    // --- concept_key tests ---

    #[tokio::test]
    async fn concept_key_increments_per_stream_in_write_path() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        // Two aggregates → same concept type, different stream_id
        let id1 = write_definition(&db, &mut dereg, "org1", &make_aggregate("A"), "A")
            .await
            .unwrap();
        let id2 = write_definition(&db, &mut dereg, "org1", &make_aggregate("B"), "B")
            .await
            .unwrap();

        let rows = dereg_meta_store::Entity::find()
            .all(&db)
            .await
            .unwrap();
        // Both should have concept_key=1 since they have different stream_ids
        assert_eq!(rows[0].concept_key, 1);
        assert_eq!(rows[1].concept_key, 1);
        assert_ne!(rows[0].stream_id, rows[1].stream_id);
        let _ = (id1, id2);
    }
}
