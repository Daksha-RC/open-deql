//! Write-path integration tests for M2.2.
//!
//! Tests exercise CREATE/DROP flows with in-memory DeReg + persistence,
//! validating CP-2 acceptance criteria.

#[cfg(test)]
mod tests {
    use sea_orm::{
        ConnectionTrait, Database, DatabaseConnection, EntityTrait, Statement, TransactionTrait,
    };
    use sea_orm_migration::MigratorTrait;

    use crate::{
        allocator,
        dereg::DeReg,
        error::ConceptKind,
        meta_json,
        migration::DeqlMigrator,
        org_registry::OrgDeRegMap,
        parser::{ast::*, error::Diagnostic, token::Span},
        store::dereg_meta_store,
    };

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
        let mut temp_dereg = dereg.clone();
        let id = allocator::allocate_next_id(db).await.unwrap();
        let result = temp_dereg.register_statement(stmt);

        let registration = match result {
            Ok(reg) => reg,
            Err(e) => return Err(e.to_string()),
        };

        let txn = db.begin().await.unwrap();

        let stream_id = format!(
            "{}:{}",
            format!("{:?}", registration.concept_type).to_lowercase(),
            registration.concept_name.as_str()
        );
        let concept_key = allocator::allocate_concept_key_txn(&txn, org_id, &stream_id)
            .await
            .unwrap();
        let meta = meta_json::build_meta(stmt);

        use sea_orm::{ActiveModelTrait, Set};
        let model = dereg_meta_store::ActiveModel {
            id: Set(id),
            org_id: Set(org_id.to_string()),
            stream_id: Set(stream_id),
            event_type: Set(registration.event_type.to_string()),
            concept_type: Set(format!("{:?}", registration.concept_type).to_uppercase()),
            concept_key: Set(concept_key),
            occurred_at: Set(chrono::Utc::now().into()),
            status: Set("ok".to_string()),
            error_message: Set(None),
            statement: Set(raw_sql.to_string()),
            meta: Set(meta),
        };
        model.insert(&txn).await.unwrap();
        txn.commit().await.unwrap();
        *dereg = temp_dereg;
        Ok(id)
    }

    async fn write_parse_error(
        db: &DatabaseConnection,
        org_id: &str,
        raw_sql: &str,
        diagnostics: &[Diagnostic],
    ) -> i64 {
        let txn = db.begin().await.unwrap();
        let id = allocator::allocate_next_id_txn(&txn).await.unwrap();
        let error_message = if diagnostics.is_empty() {
            "empty script".to_string()
        } else {
            diagnostics
                .iter()
                .map(|diag| diag.display(raw_sql))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let meta = meta_json::build_parse_error_meta(raw_sql, diagnostics);

        use sea_orm::{ActiveModelTrait, Set};
        let model = dereg_meta_store::ActiveModel {
            id: Set(id),
            org_id: Set(org_id.to_string()),
            stream_id: Set("ERROR:PARSE".to_string()),
            event_type: Set("ParseError".to_string()),
            concept_type: Set("PARSE_ERROR".to_string()),
            concept_key: Set(0),
            occurred_at: Set(chrono::Utc::now().into()),
            status: Set("parse_error".to_string()),
            error_message: Set(Some(error_message)),
            statement: Set(raw_sql.to_string()),
            meta: Set(meta),
        };
        model.insert(&txn).await.unwrap();
        txn.commit().await.unwrap();
        id
    }

    async fn write_definitions_atomic(
        db: &DatabaseConnection,
        dereg: &mut DeReg,
        org_id: &str,
        statements: &[(&DeqlStatement, &str)],
    ) -> Result<Vec<i64>, String> {
        let mut temp_dereg = dereg.clone();
        let mut prepared = Vec::with_capacity(statements.len());

        for (stmt, raw_sql) in statements {
            let id = match allocator::allocate_next_id(db).await {
                Ok(id) => id,
                Err(e) => return Err(e.to_string()),
            };

            let registration = match temp_dereg.register_statement(stmt) {
                Ok(reg) => reg,
                Err(e) => return Err(e.to_string()),
            };
            let stream_id = format!(
                "{}:{}",
                format!("{:?}", registration.concept_type).to_lowercase(),
                registration.concept_name.as_str()
            );
            prepared.push((
                id,
                raw_sql.to_string(),
                registration,
                stream_id,
                meta_json::build_meta(stmt),
            ));
        }

        let txn = db.begin().await.map_err(|e| e.to_string())?;
        let mut ids = Vec::with_capacity(prepared.len());

        for (id, raw_sql, registration, stream_id, meta) in prepared {
            let concept_key = allocator::allocate_concept_key_txn(&txn, org_id, &stream_id)
                .await
                .map_err(|e| e.to_string())?;

            use sea_orm::{ActiveModelTrait, Set};
            let model = dereg_meta_store::ActiveModel {
                id: Set(id),
                org_id: Set(org_id.to_string()),
                stream_id: Set(stream_id),
                event_type: Set(registration.event_type.to_string()),
                concept_type: Set(format!("{:?}", registration.concept_type).to_uppercase()),
                concept_key: Set(concept_key),
                occurred_at: Set(chrono::Utc::now().into()),
                status: Set("ok".to_string()),
                error_message: Set(None),
                statement: Set(raw_sql),
                meta: Set(meta),
            };
            model.insert(&txn).await.map_err(|e| e.to_string())?;
            ids.push(id);
        }

        txn.commit().await.map_err(|e| e.to_string())?;
        *dereg = temp_dereg;
        Ok(ids)
    }

    async fn write_definitions_atomic_with_forced_db_failure(
        db: &DatabaseConnection,
        dereg: &mut DeReg,
        org_id: &str,
        statements: &[(&DeqlStatement, &str)],
    ) -> Result<(), String> {
        let mut temp_dereg = dereg.clone();
        let mut prepared = Vec::with_capacity(statements.len());

        for (stmt, raw_sql) in statements {
            let id = match allocator::allocate_next_id(db).await {
                Ok(id) => id,
                Err(e) => return Err(e.to_string()),
            };

            let registration = match temp_dereg.register_statement(stmt) {
                Ok(reg) => reg,
                Err(e) => return Err(e.to_string()),
            };
            let stream_id = format!(
                "{}:{}",
                format!("{:?}", registration.concept_type).to_lowercase(),
                registration.concept_name.as_str()
            );
            prepared.push((
                id,
                raw_sql.to_string(),
                registration,
                stream_id,
                meta_json::build_meta(stmt),
            ));
        }

        let txn = db.begin().await.map_err(|e| e.to_string())?;

        for (index, (id, raw_sql, registration, stream_id, meta)) in
            prepared.into_iter().enumerate()
        {
            let concept_key = allocator::allocate_concept_key_txn(&txn, org_id, &stream_id)
                .await
                .map_err(|e| e.to_string())?;

            use sea_orm::{ActiveModelTrait, Set};
            let model = dereg_meta_store::ActiveModel {
                id: Set(id),
                org_id: Set(org_id.to_string()),
                stream_id: Set(stream_id),
                event_type: Set(registration.event_type.to_string()),
                concept_type: Set(format!("{:?}", registration.concept_type).to_uppercase()),
                concept_key: Set(concept_key),
                occurred_at: Set(chrono::Utc::now().into()),
                status: Set("ok".to_string()),
                error_message: Set(None),
                statement: Set(raw_sql),
                meta: Set(meta),
            };
            model.insert(&txn).await.map_err(|e| e.to_string())?;

            if index == 0 {
                let forced_error = txn
                    .execute(Statement::from_string(
                        sea_orm::DatabaseBackend::Sqlite,
                        String::from("INSRT INTO definitely_not_a_table VALUES (1)"),
                    ))
                    .await;

                if forced_error.is_err() {
                    let _ = txn.rollback().await;
                    return Err("forced db failure".to_string());
                }
            }
        }

        txn.commit().await.map_err(|e| e.to_string())?;
        *dereg = temp_dereg;
        Ok(())
    }

    // --- CREATE flow tests ---

    #[tokio::test]
    async fn create_aggregate_persists_row_with_meta() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();
        let stmt = make_aggregate("BankAccount");

        let id = write_definition(
            &db,
            &mut dereg,
            "org1",
            &stmt,
            "CREATE AGGREGATE BankAccount",
        )
        .await
        .unwrap();

        assert_eq!(id, 1);

        // Verify row in DB
        let rows = dereg_meta_store::Entity::find().all(&db).await.unwrap();
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
        let rows = dereg_meta_store::Entity::find().all(&db).await.unwrap();
        let decision_row = rows
            .iter()
            .find(|r| r.event_type == "DecisionCreated")
            .unwrap();
        assert_eq!(decision_row.status, "ok");
        assert_eq!(decision_row.meta["aggregate"], "Account");
        assert_eq!(decision_row.meta["command"], "Withdraw");
    }

    #[tokio::test]
    async fn create_decision_fails_validation_without_persisting_row() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        // Decision references non-existent aggregate
        let stmt = make_decision("BadDecision", "NoSuchAgg", "NoCmd", "NoEvent");
        let err = write_definition(
            &db,
            &mut dereg,
            "org1",
            &stmt,
            "CREATE DECISION BadDecision",
        )
        .await
        .unwrap_err();

        assert!(err.contains("MissingReferences") || err.contains("missing"));

        let rows = dereg_meta_store::Entity::find().all(&db).await.unwrap();
        assert!(rows.is_empty());

        let next_id = allocator::allocate_next_id(&db).await.unwrap();
        assert_eq!(next_id, 2);
    }

    #[tokio::test]
    async fn parse_error_persists_single_row_with_full_sql_and_diagnostics() {
        let db = setup_db().await;
        let raw_sql = "CRATE AGGREGATE Foo;";
        let (parsed, diagnostics) = crate::parse(raw_sql);

        assert!(parsed.statements.is_empty());
        assert!(!diagnostics.is_empty());

        let main_txn = db.begin().await.unwrap();
        main_txn
            .execute(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                String::from("INSERT INTO dereg_meta_store (id, org_id, stream_id, event_type, concept_type, concept_key, occurred_at, status, statement, meta) VALUES (99, 'org1', 'aggregate:Temp', 'AggregateCreated', 'AGGREGATE', 1, '2026-01-01T00:00:00Z', 'ok', 'CREATE AGGREGATE Temp;', '{}')"),
            ))
            .await
            .unwrap();
        main_txn.rollback().await.unwrap();

        let id = write_parse_error(&db, "org1", raw_sql, &diagnostics).await;

        assert_eq!(id, 1);

        let rows = dereg_meta_store::Entity::find().all(&db).await.unwrap();
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(row.status, "parse_error");
        assert_eq!(row.statement, raw_sql);
        assert_eq!(row.stream_id, "ERROR:PARSE");
        assert_eq!(row.concept_type, "PARSE_ERROR");
        assert_eq!(row.concept_key, 0);
        let expected_message = diagnostics[0].display(raw_sql);
        assert_eq!(
            row.error_message.as_deref(),
            Some(expected_message.as_str())
        );
        assert_eq!(row.meta["code"], "PARSE_ERROR");
        assert_eq!(row.meta["line"], 1);
        assert_eq!(row.meta["column"], 1);
        assert_eq!(row.meta["snippet"], raw_sql);
        assert_eq!(row.meta["diagnostics"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn multi_statement_success_persists_all_rows_atomically() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        let account_stmt = make_aggregate("Account");
        let withdraw_stmt = make_command("Withdraw");

        let statements = [
            (&account_stmt, "CREATE AGGREGATE Account;"),
            (&withdraw_stmt, "CREATE COMMAND Withdraw (amount INT);"),
        ];

        let ids = write_definitions_atomic(&db, &mut dereg, "org1", &statements)
            .await
            .unwrap();

        assert_eq!(ids, vec![1, 2]);

        let rows = dereg_meta_store::Entity::find().all(&db).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].status, "ok");
        assert_eq!(rows[1].status, "ok");
        assert_eq!(rows[0].statement, "CREATE AGGREGATE Account;");
        assert_eq!(rows[1].statement, "CREATE COMMAND Withdraw (amount INT);");
    }

    #[tokio::test]
    async fn multi_statement_validation_failure_rolls_back_everything() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        let account_stmt = make_aggregate("Account");
        let bad_decision_stmt = make_decision("BadDecision", "MissingAgg", "Withdraw", "Withdrawn");

        let statements = [
            (&account_stmt, "CREATE AGGREGATE Account;"),
            (&bad_decision_stmt, "CREATE DECISION BadDecision;"),
        ];

        let err = write_definitions_atomic(&db, &mut dereg, "org1", &statements)
            .await
            .unwrap_err();

        assert!(err.contains("MissingReferences") || err.contains("missing"));

        let rows = dereg_meta_store::Entity::find().all(&db).await.unwrap();
        assert!(rows.is_empty());
        assert!(dereg.get_aggregate("Account").is_none());

        let next_id = allocator::allocate_next_id(&db).await.unwrap();
        assert_eq!(next_id, 3);
    }

    #[tokio::test]
    async fn multi_statement_db_failure_rolls_back_everything() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        let account_stmt = make_aggregate("Account");
        let withdraw_stmt = make_command("Withdraw");

        let statements = [
            (&account_stmt, "CREATE AGGREGATE Account;"),
            (&withdraw_stmt, "CREATE COMMAND Withdraw (amount INT);"),
        ];

        let err =
            write_definitions_atomic_with_forced_db_failure(&db, &mut dereg, "org1", &statements)
                .await
                .unwrap_err();

        assert_eq!(err, "forced db failure");

        let rows = dereg_meta_store::Entity::find().all(&db).await.unwrap();
        assert!(rows.is_empty());
        assert!(dereg.get_aggregate("Account").is_none());

        let next_id = allocator::allocate_next_id(&db).await.unwrap();
        assert_eq!(next_id, 3);
    }

    #[tokio::test]
    async fn duplicate_create_without_replace_fails() {
        let db = setup_db().await;
        let mut dereg = DeReg::new();

        write_definition(
            &db,
            &mut dereg,
            "org1",
            &make_aggregate("X"),
            "CREATE AGGREGATE X",
        )
        .await
        .unwrap();

        let err = write_definition(
            &db,
            &mut dereg,
            "org1",
            &make_aggregate("X"),
            "CREATE AGGREGATE X",
        )
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

        write_definition(
            &db,
            &mut dereg,
            "org1",
            &make_aggregate("X"),
            "CREATE AGGREGATE X",
        )
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
        use sea_orm::{ActiveModelTrait, Set};
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
        let rows = dereg_meta_store::Entity::find().all(&db).await.unwrap();
        let tombstone = rows
            .iter()
            .find(|r| r.event_type == "AggregateDropped")
            .unwrap();
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
        dereg.register_statement(&make_command("Withdraw")).unwrap();
        dereg.register_statement(&make_event("Withdrawn")).unwrap();
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

        let rows = dereg_meta_store::Entity::find().all(&db).await.unwrap();
        // Both should have concept_key=1 since they have different stream_ids
        assert_eq!(rows[0].concept_key, 1);
        assert_eq!(rows[1].concept_key, 1);
        assert_ne!(rows[0].stream_id, rows[1].stream_id);
        let _ = (id1, id2);
    }
}
