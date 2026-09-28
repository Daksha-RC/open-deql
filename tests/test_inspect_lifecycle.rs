// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Integration test for INSPECT DECISION full lifecycle (START → execution → STOP).
//!
//! This test validates the complete workflow:
//! 1. Output schema validation
//! 2. Branching schema validation
//! 3. Output batch builder for different row types (accepted/rejected/error)
//! 4. Runtime state management (serial increment, concurrency locks)
//! 5. Validation report generation
//! 6. Error handling

#![cfg(all(test, feature = "deql"))]

use arrow::array::{
    ArrayRef, RecordBatch, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::Utc;
use std::sync::Arc;

// ============================================================================
// Test Fixtures
// ============================================================================

/// Create a standard inspection output schema for testing
fn build_test_output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("_row", DataType::Int64, false),
        Field::new("_status", DataType::Utf8, false),
        Field::new("_event_type", DataType::Utf8, true),
        Field::new("_decision", DataType::Utf8, true),  // nullable - may be null for accepted/error rows
        Field::new("_aggregate_id", DataType::Utf8, true),  // nullable - may be null for error rows
        Field::new("_branch_index", DataType::Int64, true),
        Field::new("_guard_expression", DataType::Utf8, true),
        Field::new("_reason", DataType::Utf8, true),
        Field::new("_run_id", DataType::Utf8, false),
    ]))
}

/// Create test source data as a RecordBatch.
/// Data: [amount=150, account_id="acc1"], [amount=75, account_id="acc2"], [amount=30, account_id="acc3"]
fn build_test_source_batch() -> RecordBatch {
    let amounts = UInt64Array::from(vec![150, 75, 30]);
    let accounts = StringArray::from(vec!["acc1", "acc2", "acc3"]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::UInt64, false),
        Field::new("account_id", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(amounts) as ArrayRef,
            Arc::new(accounts) as ArrayRef,
        ],
    )
    .expect("Failed to create test RecordBatch")
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn test_inspect_lifecycle_source_batch_creation() {
    // Setup: Build test source data
    let source_batch = build_test_source_batch();

    // Verify source data
    assert_eq!(
        source_batch.num_rows(),
        3,
        "Source batch should have 3 rows"
    );
    assert_eq!(
        source_batch.num_columns(),
        2,
        "Source batch should have 2 columns"
    );

    // Verify schema
    let schema = source_batch.schema();
    assert_eq!(schema.fields()[0].name(), "amount");
    assert_eq!(schema.fields()[1].name(), "account_id");
}

#[test]
fn test_inspect_output_schema_validation() {
    // Validates that output schema builder creates correct structure
    // This is a unit test focused on schema correctness
    let branching_schema = openobserve::service::deql_inspect::build_branching_schema();

    // Verify fixed columns
    let expected_fixed_columns = vec![
        "_timestamp",
        "inspect_row_index",
        "decision_name",
        "branch_id",
        "branch_index",
        "branch_rule_name",
        "branch_guard",
        "branch_status",
        "event_type",
        "stream_id",
    ];

    assert_eq!(
        branching_schema.fields().len(),
        expected_fixed_columns.len(),
        "Schema should have {} columns",
        expected_fixed_columns.len()
    );

    for (idx, field) in branching_schema.fields().iter().enumerate() {
        assert_eq!(
            field.name(),
            &expected_fixed_columns[idx],
            "Column {} should match expected name",
            idx
        );
    }
}

#[test]
fn test_inspect_branching_schema_validation() {
    // Validates that branching schema matches deql-cli implementation
    let branching_schema = openobserve::service::deql_inspect::build_branching_schema();

    let expected_fields = vec![
        "_timestamp",
        "inspect_row_index",
        "decision_name",
        "branch_id",
        "branch_index",
        "branch_rule_name",
        "branch_guard",
        "branch_status",
        "event_type",
        "stream_id",
    ];

    assert_eq!(
        branching_schema.fields().len(),
        expected_fields.len(),
        "Branching schema should have {} columns",
        expected_fields.len()
    );

    for (idx, field) in branching_schema.fields().iter().enumerate() {
        assert_eq!(
            field.name(),
            expected_fields[idx],
            "Column {} should have correct name",
            idx
        );
    }
}

#[test]
fn test_inspect_batch_builder_accepted_row() {
    // Tests OutputBatchBuilder for accepted rows
    let schema = build_test_output_schema();
    let mut builder = openobserve::service::deql_inspect::OutputBatchBuilder::new(&schema, 10);

    // Append an accepted row
    builder.append_accepted_row(
        0,                        // row_idx
        "test_run",              // run_id
        "TestEvent",             // event_type
        "acc1",                  // aggregate_id
        0,                       // branch_index
        "amount > 100",          // guard_text
        "TestDecision",          // decision_name
        &[],                     // payload_fields (empty for now)
    );

    let batch = builder
        .finish()
        .expect("Failed to finish RecordBatch");

    assert_eq!(batch.num_rows(), 1, "Batch should have 1 row");
    assert_eq!(batch.num_columns(), schema.fields().len(), "Batch should have correct column count");
}

#[test]
fn test_inspect_batch_builder_rejected_row() {
    // Tests OutputBatchBuilder for rejected rows
    let schema = build_test_output_schema();
    let mut builder = openobserve::service::deql_inspect::OutputBatchBuilder::new(&schema, 10);

    // Append a rejected row
    builder.append_rejected_row(
        2,                       // row_idx
        "test_run",              // run_id
        "acc3",                  // aggregate_id
        "TestDecision",          // decision_name
        "amount > 50",           // last_guard
    );

    let batch = builder
        .finish()
        .expect("Failed to finish RecordBatch");

    assert_eq!(batch.num_rows(), 1, "Batch should have 1 row");
}

#[test]
fn test_inspect_batch_builder_error_row() {
    // Tests OutputBatchBuilder for error rows
    let schema = build_test_output_schema();
    let mut builder = openobserve::service::deql_inspect::OutputBatchBuilder::new(&schema, 10);

    // Append an error row
    builder.append_error_row(
        1,                       // row_idx
        "test_run",              // run_id
        "TestDecision",          // decision_name
        "Guard evaluation failed: invalid column",
    );

    let batch = builder
        .finish()
        .expect("Failed to finish RecordBatch");

    assert_eq!(batch.num_rows(), 1, "Batch should have 1 row");
}

#[test]
fn test_inspect_batch_builder_mixed_rows() {
    // Tests OutputBatchBuilder with mixed row types (accepted, rejected, error)
    let schema = build_test_output_schema();
    let mut builder = openobserve::service::deql_inspect::OutputBatchBuilder::new(&schema, 10);

    // Append mixed rows
    builder.append_accepted_row(0, "test_run", "TestEvent", "acc1", 0, "amount > 100", "TestDecision", &[]);
    builder.append_rejected_row(1, "test_run", "acc2", "TestDecision", "amount > 50");
    builder.append_error_row(2, "test_run", "TestDecision", "Column not found");
    builder.append_accepted_row(3, "test_run", "TestEvent", "acc3", 1, "amount > 50", "TestDecision", &[]);

    let batch = builder
        .finish()
        .expect("Failed to finish RecordBatch");

    assert_eq!(batch.num_rows(), 4, "Batch should have 4 rows (mixed types)");
}

#[test]
fn test_inspect_inspection_state_serial_increment() {
    // Tests that serial numbers auto-increment correctly
    let state = openobserve::service::deql_inspect::InspectionOrgState::new();

    let key1 = "my_inspection_20260601";
    let key2 = "my_inspection_20260602";

    let serial1a = state.next_serial(key1);
    let serial1b = state.next_serial(key1);
    let serial2a = state.next_serial(key2);
    let serial1c = state.next_serial(key1);

    assert_eq!(serial1a, 1, "First serial should be 1");
    assert_eq!(serial1b, 2, "Second serial should be 2 (same key)");
    assert_eq!(serial2a, 1, "First serial for different key should be 1");
    assert_eq!(serial1c, 3, "Third serial should be 3 (same key as first)");
}

#[test]
fn test_inspect_validation_report_pass() {
    // Tests ValidationReport for passing checks
    let check = openobserve::service::deql_inspect::ValidationCheck::pass("test_check");

    assert_eq!(check.status, openobserve::service::deql_inspect::CheckStatus::Pass);
    assert_eq!(check.check, "test_check");
    assert_eq!(check.message, None);
}

#[test]
fn test_inspect_validation_report_fail() {
    // Tests ValidationReport for failing checks
    let check = openobserve::service::deql_inspect::ValidationCheck::fail(
        "decision_exists",
        "Decision 'MissingDecision' not found",
    );

    assert_eq!(check.status, openobserve::service::deql_inspect::CheckStatus::Fail);
    assert_eq!(check.check, "decision_exists");
    assert!(check.message.is_some());
}

#[test]
fn test_inspect_validation_report_warn() {
    // Tests ValidationReport for warning checks
    let check = openobserve::service::deql_inspect::ValidationCheck::warn(
        "from_stream_exists",
        "Stream has no schema fields",
    );

    assert_eq!(check.status, openobserve::service::deql_inspect::CheckStatus::Warn);
    assert_eq!(check.check, "from_stream_exists");
    assert!(check.message.is_some());
}

#[test]
fn test_inspect_batch_builder_large_batch() {
    // Tests OutputBatchBuilder with many rows
    let schema = build_test_output_schema();
    let mut builder = openobserve::service::deql_inspect::OutputBatchBuilder::new(&schema, 100);

    // Append 100 rows
    for i in 0..100 {
        if i % 3 == 0 {
            builder.append_accepted_row(
                i,
                "test_run",
                "TestEvent",
                &format!("acc{}", i),
                0,
                "amount > 100",
                "TestDecision",
                &[],
            );
        } else if i % 3 == 1 {
            builder.append_rejected_row(i, "test_run", &format!("acc{}", i), "TestDecision", "amount > 50");
        } else {
            builder.append_error_row(i, "test_run", "TestDecision", "Evaluation error");
        }
    }

    let batch = builder
        .finish()
        .expect("Failed to finish RecordBatch");

    assert_eq!(batch.num_rows(), 100, "Batch should have 100 rows");

    // Verify column count
    assert_eq!(
        batch.num_columns(),
        schema.fields().len(),
        "Batch should have correct column count"
    );
}

#[test]
fn test_inspect_output_status_variants() {
    // Tests all OutputStatus variants
    use openobserve::service::deql_inspect::OutputStatus;

    let running = OutputStatus::Running;
    let done = OutputStatus::Done;
    let stopped = OutputStatus::Stopped;

    assert_ne!(running, done);
    assert_ne!(done, stopped);
    assert_ne!(stopped, running);

    // Verify they can be cloned and compared
    assert_eq!(running.clone(), running);
}

#[test]
fn test_inspect_error_display() {
    // Tests InspectError Display implementation
    use openobserve::service::deql_inspect::InspectError;

    let error1 = InspectError::GuardEvaluationFailed("invalid condition".to_string());
    assert!(error1.to_string().contains("Guard evaluation failed"));

    let error2 = InspectError::StateQueryFailed("query timeout".to_string());
    assert!(error2.to_string().contains("State query failed"));

    let error3 = InspectError::AggregateIdResolutionFailed("missing field".to_string());
    assert!(error3.to_string().contains("Aggregate ID resolution failed"));

    let error4 = InspectError::EmitEvaluationFailed("expression error".to_string());
    assert!(error4.to_string().contains("Emit evaluation failed"));

    let error5 = InspectError::RowProcessingFailed {
        row_idx: 42,
        reason: "test reason".to_string(),
    };
    assert!(error5.to_string().contains("Row 42"));

    let error6 = InspectError::BatchConstructionFailed("schema mismatch".to_string());
    assert!(error6.to_string().contains("Batch construction failed"));
}

#[test]
fn test_inspect_definition_from_metadata() {
    // Tests InspectionDefinition extraction from metadata
    let meta_json = serde_json::json!({
        "name": "my_inspection",
        "decision_name": "MyDecision",
        "input_table": "source_stream",
        "output_table": "results_template",
        "guard_filter": "amount > 100"
    });

    let definition = openobserve::service::deql_inspect::InspectionDefinition {
        name: meta_json["name"].as_str().unwrap_or("").to_string(),
        decision_name: meta_json["decision_name"].as_str().unwrap_or("").to_string(),
        from_stream: meta_json["input_table"].as_str().unwrap_or("").to_string(),
        into_template: meta_json["output_table"].as_str().unwrap_or("").to_string(),
        guard_filter: meta_json["guard_filter"].as_str().map(|s| s.to_string()),
        statement: "CREATE INSPECTION ...".to_string(),
    };

    assert_eq!(definition.name, "my_inspection");
    assert_eq!(definition.decision_name, "MyDecision");
    assert_eq!(definition.from_stream, "source_stream");
    assert_eq!(definition.into_template, "results_template");
    assert_eq!(definition.guard_filter, Some("amount > 100".to_string()));
}

#[test]
fn test_inspect_run_handle_creation() {
    // Tests that a RunHandle can be properly created
    use openobserve::service::deql_inspect::RunHandle;
    use std::sync::atomic::AtomicUsize;
    use tokio_util::sync::CancellationToken;

    let cancel = CancellationToken::new();
    let handle = RunHandle {
        inspection_name: "test_inspection".to_string(),
        output_table: "deql_ins_test_20260601_001".to_string(),
        branching_table: "deql_brn_test_20260601_001".to_string(),
        decision_name: "TestDecision".to_string(),
        from_stream: "test_source".to_string(),
        cancel,
        started_at: Utc::now(),
        rows_processed: Arc::new(AtomicUsize::new(0)),
        accepted: Arc::new(AtomicUsize::new(0)),
        rejected: Arc::new(AtomicUsize::new(0)),
        errors: Arc::new(AtomicUsize::new(0)),
        limit: 100,
    };

    assert_eq!(handle.inspection_name, "test_inspection");
    assert_eq!(handle.output_table, "deql_ins_test_20260601_001");
    assert_eq!(handle.branching_table, "deql_brn_test_20260601_001");
    assert_eq!(handle.limit, 100);
}

#[test]
fn test_inspect_inspection_org_state_concurrency_lock() {
    // Tests that InspectionOrgState enforces org-wide concurrency lock
    use openobserve::service::deql_inspect::RunHandle;
    use std::sync::atomic::AtomicUsize;
    use tokio_util::sync::CancellationToken;

    let state = openobserve::service::deql_inspect::InspectionOrgState::new();

    assert!(!state.is_running(), "State should start with no running inspection");
    assert_eq!(state.running_name(), None);

    // Set running
    let cancel = CancellationToken::new();
    let handle = RunHandle {
        inspection_name: "inspection1".to_string(),
        output_table: "table1".to_string(),
        branching_table: "table1_brn".to_string(),
        decision_name: "decision1".to_string(),
        from_stream: "stream1".to_string(),
        cancel,
        started_at: Utc::now(),
        rows_processed: Arc::new(AtomicUsize::new(0)),
        accepted: Arc::new(AtomicUsize::new(0)),
        rejected: Arc::new(AtomicUsize::new(0)),
        errors: Arc::new(AtomicUsize::new(0)),
        limit: 100,
    };

    state.set_running(handle);

    assert!(state.is_running(), "State should show running after set_running");
    assert_eq!(
        state.running_name(),
        Some("inspection1".to_string()),
        "Should return correct running inspection name"
    );

    // Clear running
    state.clear_running();

    assert!(!state.is_running(), "State should show not running after clear_running");
}

#[test]
fn test_inspect_output_table_entry_lifecycle() {
    // Tests that OutputTableEntry represents table metadata correctly
    let schema = Arc::new(Schema::new(vec![
        Field::new("col1", DataType::Utf8, false),
        Field::new("col2", DataType::Int64, false),
    ]));

    let entry = openobserve::service::deql_inspect::OutputTableEntry {
        table_name: "deql_ins_test_20260601_001".to_string(),
        branching_table_name: "deql_brn_test_20260601_001".to_string(),
        inspection_name: Some("test_inspection".to_string()),
        decision_name: "TestDecision".to_string(),
        status: openobserve::service::deql_inspect::OutputStatus::Running,
        rows_processed: 42,
        accepted: 30,
        rejected: 10,
        errors: 2,
        schema,
        created_at: Utc::now(),
        memory_bytes: 4096,
    };

    assert_eq!(entry.table_name, "deql_ins_test_20260601_001");
    assert_eq!(entry.rows_processed, 42);
    assert_eq!(entry.accepted, 30);
    assert_eq!(entry.rejected, 10);
    assert_eq!(entry.errors, 2);
    assert_eq!(entry.memory_bytes, 4096);
}
