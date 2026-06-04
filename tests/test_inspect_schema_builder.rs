#![cfg(feature = "deql")]

use arrow::datatypes::DataType;
use o2_deql::parser::ast::{CreateDecision, CreateEvent, DecisionBranch, DeqlType, EmitItem, FieldDef, Spanned};
use o2_deql::parser::token::Span;

// Import the schema builder functions from the service module
use openobserve::service::deql_inspect::{build_branching_schema, build_inspect_output_schema};

fn test_span() -> Span {
    Span { start: 0, end: 0 }
}

fn spanned<T>(value: T) -> Spanned<T> {
    Spanned {
        node: value,
        span: test_span(),
    }
}

#[test]
fn test_build_inspect_output_schema_with_fixed_columns() {
    // Create a minimal decision
    let decision = CreateDecision {
        or_replace: false,
        name: spanned("TestDecision".to_string()),
        aggregate: spanned("TestAggregate".to_string()),
        command: spanned("TestCommand".to_string()),
        state_as: None,
        branches: vec![],
    };

    let dereg = o2_deql::dereg::DeReg::new();
    let schema = build_inspect_output_schema(&decision, &dereg);

    // Verify all fixed columns are present
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    assert_eq!(field_names.len(), 10, "Should have exactly 10 fixed columns when no events");
    assert!(field_names.contains(&"_timestamp"));
    assert!(field_names.contains(&"_row"));
    assert!(field_names.contains(&"_status"));
    assert!(field_names.contains(&"_event_type"));
    assert!(field_names.contains(&"_decision"));
    assert!(field_names.contains(&"_aggregate_id"));
    assert!(field_names.contains(&"_branch_index"));
    assert!(field_names.contains(&"_guard_expression"));
    assert!(field_names.contains(&"_reason"));
    assert!(field_names.contains(&"_run_id"));
}

#[test]
fn test_build_inspect_output_schema_with_payload_fields() {
    // Create an event with fields
    let event = CreateEvent {
        or_replace: false,
        name: spanned("TestEvent".to_string()),
        fields: vec![
            FieldDef {
                name: spanned("amount".to_string()),
                data_type: spanned(DeqlType::Int),
                is_key: false,
                annotation: None,
            },
            FieldDef {
                name: spanned("account_id".to_string()),
                data_type: spanned(DeqlType::String),
                is_key: false,
                annotation: None,
            },
        ],
    };

    // Create a decision with an emit item
    let decision = CreateDecision {
        or_replace: false,
        name: spanned("TestDecision".to_string()),
        aggregate: spanned("TestAggregate".to_string()),
        command: spanned("TestCommand".to_string()),
        state_as: None,
        branches: vec![DecisionBranch {
            branch_index: 0,
            rule_name: None,
            guard: None,
            emit_items: vec![EmitItem {
                event_type: spanned("TestEvent".to_string()),
                assignments: vec![],
                span: test_span(),
            }],
            span: test_span(),
        }],
    };

    // Register the event in DeReg
    let mut dereg = o2_deql::dereg::DeReg::new();
    let _ = dereg.register_statement(&o2_deql::parser::ast::DeqlStatement::CreateEvent(event));

    let schema = build_inspect_output_schema(&decision, &dereg);

    // Should have 10 fixed columns + 2 payload columns
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names.len(), 12);

    // Verify payload fields are present
    assert!(field_names.contains(&"amount"));
    assert!(field_names.contains(&"account_id"));

    // Verify payload fields are nullable Utf8
    let amount_field = schema.field_with_name("amount").unwrap();
    assert_eq!(amount_field.data_type(), &DataType::Utf8);
    assert!(amount_field.is_nullable());

    let account_id_field = schema.field_with_name("account_id").unwrap();
    assert_eq!(account_id_field.data_type(), &DataType::Utf8);
    assert!(account_id_field.is_nullable());
}

#[test]
fn test_build_inspect_output_schema_no_duplicate_fields() {
    // Create an event
    let event = CreateEvent {
        or_replace: false,
        name: spanned("TestEvent".to_string()),
        fields: vec![FieldDef {
            name: spanned("amount".to_string()),
            data_type: spanned(DeqlType::Int),
            is_key: false,
            annotation: None,
        }],
    };

    // Create a decision where the same event is emitted in multiple branches
    let decision = CreateDecision {
        or_replace: false,
        name: spanned("TestDecision".to_string()),
        aggregate: spanned("TestAggregate".to_string()),
        command: spanned("TestCommand".to_string()),
        state_as: None,
        branches: vec![
            DecisionBranch {
                branch_index: 0,
                rule_name: None,
                guard: None,
                emit_items: vec![EmitItem {
                    event_type: spanned("TestEvent".to_string()),
                    assignments: vec![],
                    span: test_span(),
                }],
                span: test_span(),
            },
            DecisionBranch {
                branch_index: 1,
                rule_name: None,
                guard: None,
                emit_items: vec![EmitItem {
                    event_type: spanned("TestEvent".to_string()),
                    assignments: vec![],
                    span: test_span(),
                }],
                span: test_span(),
            },
        ],
    };

    let mut dereg = o2_deql::dereg::DeReg::new();
    let _ = dereg.register_statement(&o2_deql::parser::ast::DeqlStatement::CreateEvent(event));

    let schema = build_inspect_output_schema(&decision, &dereg);

    // Count occurrences of "amount"
    let amount_count = schema
        .fields()
        .iter()
        .filter(|f| f.name() == "amount")
        .count();

    // Should appear exactly once, not duplicated
    assert_eq!(amount_count, 1, "Payload field should not be duplicated across branches");
}

#[test]
fn test_build_branching_schema_structure() {
    let schema = build_branching_schema();

    // Verify all required columns
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    assert_eq!(field_names.len(), 10);
    assert!(field_names.contains(&"_timestamp"));
    assert!(field_names.contains(&"inspect_row_index"));
    assert!(field_names.contains(&"decision_name"));
    assert!(field_names.contains(&"branch_id"));
    assert!(field_names.contains(&"branch_index"));
    assert!(field_names.contains(&"branch_rule_name"));
    assert!(field_names.contains(&"branch_guard"));
    assert!(field_names.contains(&"branch_status"));
    assert!(field_names.contains(&"event_type"));
    assert!(field_names.contains(&"stream_id"));
}

#[test]
fn test_build_branching_schema_nullability() {
    let schema = build_branching_schema();

    // branch_rule_name should be nullable
    let branch_rule_name = schema.field_with_name("branch_rule_name").unwrap();
    assert!(branch_rule_name.is_nullable());

    // event_type should be nullable
    let event_type = schema.field_with_name("event_type").unwrap();
    assert!(event_type.is_nullable());

    // inspect_row_index should NOT be nullable
    let inspect_row_index = schema.field_with_name("inspect_row_index").unwrap();
    assert!(!inspect_row_index.is_nullable());

    // decision_name should NOT be nullable
    let decision_name = schema.field_with_name("decision_name").unwrap();
    assert!(!decision_name.is_nullable());
}

#[test]
fn test_build_inspect_output_schema_fixed_columns_nullability() {
    let decision = CreateDecision {
        or_replace: false,
        name: spanned("TestDecision".to_string()),
        aggregate: spanned("TestAggregate".to_string()),
        command: spanned("TestCommand".to_string()),
        state_as: None,
        branches: vec![],
    };

    let dereg = o2_deql::dereg::DeReg::new();
    let schema = build_inspect_output_schema(&decision, &dereg);

    // These should NOT be nullable
    assert!(!schema.field_with_name("_timestamp").unwrap().is_nullable());
    assert!(!schema.field_with_name("_row").unwrap().is_nullable());
    assert!(!schema.field_with_name("_status").unwrap().is_nullable());
    assert!(!schema.field_with_name("_decision").unwrap().is_nullable());
    assert!(!schema.field_with_name("_run_id").unwrap().is_nullable());

    // _aggregate_id is nullable because error rows may not have an aggregate_id
    assert!(schema.field_with_name("_aggregate_id").unwrap().is_nullable());

    // These SHOULD be nullable
    assert!(schema.field_with_name("_event_type").unwrap().is_nullable());
    assert!(schema.field_with_name("_branch_index").unwrap().is_nullable());
    assert!(schema.field_with_name("_guard_expression").unwrap().is_nullable());
    assert!(schema.field_with_name("_reason").unwrap().is_nullable());
}

/// REQ-INSP-10: Integration test validating parity with deql-cli implementation.
/// 
/// This test verifies that the `build_branching_schema()` function produces a schema
/// that matches the deql-cli/deql-dereg implementation exactly in:
/// - Column names and order
/// - Data types for each column
/// - Nullability flags
/// 
/// This ensures inspection decision branching tables are compatible across both
/// implementations.
#[test]
fn test_branching_schema_matches_deql_cli_implementation() {
    let schema = build_branching_schema();

    // Expected schema from deql-cli/deql-dereg/src/inspector.rs:build_branches_schema()
    // This is the source of truth for branching table schema.
    let expected_columns = vec![
        ("_timestamp", DataType::Int64, false),
        ("inspect_row_index", DataType::UInt64, false),
        ("decision_name", DataType::Utf8, false),
        ("branch_id", DataType::Utf8, false),
        ("branch_index", DataType::UInt64, false),
        ("branch_rule_name", DataType::Utf8, true),
        ("branch_guard", DataType::Utf8, false),
        ("branch_status", DataType::Utf8, false),
        ("event_type", DataType::Utf8, true),
        ("stream_id", DataType::Utf8, false),
    ];

    // Verify column count matches exactly
    assert_eq!(
        schema.fields().len(),
        expected_columns.len(),
        "Branching schema must have exactly {} columns",
        expected_columns.len()
    );

    // Verify each column matches in order: name, type, and nullability
    for (idx, (expected_name, expected_type, expected_nullable)) in expected_columns.iter().enumerate() {
        let actual_field = &schema.fields()[idx];

        // Column name must match
        assert_eq!(
            actual_field.name(),
            *expected_name,
            "Column {} name mismatch: expected '{}', got '{}'",
            idx,
            expected_name,
            actual_field.name()
        );

        // Data type must match
        assert_eq!(
            actual_field.data_type(),
            expected_type,
            "Column '{}' type mismatch: expected {:?}, got {:?}",
            expected_name,
            expected_type,
            actual_field.data_type()
        );

        // Nullability must match
        assert_eq!(
            actual_field.is_nullable(),
            *expected_nullable,
            "Column '{}' nullability mismatch: expected nullable={}, got nullable={}",
            expected_name,
            expected_nullable,
            actual_field.is_nullable()
        );
    }

    // Extra validation: verify critical nullable columns
    // These are the only two columns that should be nullable in branching table
    assert!(schema.field_with_name("branch_rule_name").unwrap().is_nullable(),
        "branch_rule_name must be nullable (optional rule name)");
    assert!(schema.field_with_name("event_type").unwrap().is_nullable(),
        "event_type must be nullable (guard_failed rows have no event)");

    // Verify non-nullable columns
    let non_nullable_cols = vec![
        "inspect_row_index",
        "decision_name",
        "branch_id",
        "branch_index",
        "branch_guard",
        "branch_status",
        "stream_id",
    ];
    for col_name in non_nullable_cols {
        assert!(!schema.field_with_name(col_name).unwrap().is_nullable(),
            "Column '{}' must not be nullable", col_name);
    }
}
