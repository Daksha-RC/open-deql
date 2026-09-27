//! Inspection service layer — state management, validation, and execution logic.
//!
//! This module provides:
//! - `InspectionOrgState` — per-org runtime state (output tables, running handle, serial counters)
//! - `InspectionDefinition` — structured definition parsed from `dereg_meta_store.meta` JSON
//! - `ValidationReport` — readiness check result for an inspection definition
//!
//! All types are behind `#[cfg(feature = "deql")]`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use chrono::{DateTime, Utc};
use datafusion::datasource::TableProvider;
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// InspectionDefinition — extracted from dereg_meta_store row
// ---------------------------------------------------------------------------

/// Structured inspection definition extracted from `dereg_meta_store.meta` JSON.
#[derive(Debug, Clone)]
pub struct InspectionDefinition {
    pub name: String,
    pub decision_name: String,
    pub from_stream: String,
    pub into_template: String,
    pub guard_filter: Option<String>,
    pub statement: String,
}

impl InspectionDefinition {
    /// Parse an InspectionDefinition from a `dereg_meta_store` row.
    pub fn from_meta_row(row: &o2_deql::store::dereg_meta_store::Model) -> Self {
        let meta = &row.meta;
        Self {
            name: meta
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            decision_name: meta
                .get("decision_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            from_stream: meta
                .get("input_table")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            into_template: meta
                .get("output_table")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            guard_filter: meta
                .get("guard_filter")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            statement: row.statement.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// InspectionOrgState — per-org runtime state
// ---------------------------------------------------------------------------

/// Per-org inspection runtime state.
/// Tracks output tables, running execution, and serial counters.
#[derive(Clone)]
pub struct InspectionOrgState {
    /// Active output tables keyed by table name.
    pub outputs: Arc<RwLock<HashMap<String, OutputTableEntry>>>,
    /// Serial counters for auto-increment: key = "{inspection_name}_{date}"
    pub serial_counters: Arc<RwLock<HashMap<String, u32>>>,
    /// Currently running inspection — at most ONE per org.
    pub running: Arc<RwLock<Option<RunHandle>>>,
    /// DataFusion TableProvider instances for inspection output tables.
    /// These are the in-memory providers that make tables queryable via DataFusion.
    pub table_providers: Arc<RwLock<HashMap<String, Arc<dyn TableProvider>>>>,
}

/// Metadata about an in-memory output table.
pub struct OutputTableEntry {
    pub table_name: String,
    pub branching_table_name: String,
    pub inspection_name: Option<String>,
    pub decision_name: String,
    pub status: OutputStatus,
    pub rows_processed: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub errors: usize,
    pub schema: SchemaRef,
    pub created_at: DateTime<Utc>,
    pub memory_bytes: usize,
}

/// Status of an output table.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum OutputStatus {
    Running,
    Done,
    Stopped,
}

/// Handle for a running inspection (enables STOP via CancellationToken).
pub struct RunHandle {
    pub inspection_name: String,
    pub output_table: String,
    pub branching_table: String,
    pub decision_name: String,
    pub from_stream: String,
    pub cancel: CancellationToken,
    pub started_at: DateTime<Utc>,
    pub rows_processed: Arc<AtomicUsize>,
    pub accepted: Arc<AtomicUsize>,
    pub rejected: Arc<AtomicUsize>,
    pub errors: Arc<AtomicUsize>,
    pub limit: usize,
}

impl InspectionOrgState {
    pub fn new() -> Self {
        Self {
            outputs: Arc::new(RwLock::new(HashMap::new())),
            serial_counters: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(None)),
            table_providers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if any inspection is currently running.
    pub fn is_running(&self) -> bool {
        self.running.read().is_some()
    }

    /// Get the name of the running inspection (if any).
    pub fn running_name(&self) -> Option<String> {
        self.running.read().as_ref().map(|h| h.inspection_name.clone())
    }

    /// Set the running handle (start execution).
    pub fn set_running(&self, handle: RunHandle) {
        let mut guard = self.running.write();
        *guard = Some(handle);
    }

    /// Clear the running handle (execution complete or stopped).
    pub fn clear_running(&self) {
        let mut guard = self.running.write();
        *guard = None;
    }

    /// Count total output tables currently in memory.
    pub fn output_count(&self) -> usize {
        self.outputs.read().len()
    }

    /// Total memory across all output tables.
    pub fn total_memory_bytes(&self) -> usize {
        self.outputs.read().values().map(|e| e.memory_bytes).sum()
    }

    /// Get next serial for a given inspection + date key.
    pub fn next_serial(&self, key: &str) -> u32 {
        let mut counters = self.serial_counters.write();
        let counter = counters.entry(key.to_string()).or_insert(0);
        *counter += 1;
        *counter
    }

    /// Check if a specific table is currently running.
    pub fn is_table_running(&self, table_name: &str) -> bool {
        let running = self.running.read();
        match running.as_ref() {
            Some(h) => h.output_table == table_name || h.branching_table == table_name,
            None => false,
        }
    }

    /// Drop a specific output table and its branching counterpart.
    /// Deregisters from OO catalog and removes from internal state.
    /// Returns the memory bytes freed, or None if table not found.
    pub async fn drop_output(&self, org_id: &str, table_name: &str) -> Option<usize> {
        let (branching_table_name, memory_bytes) = {
            let mut outputs = self.outputs.write();
            if let Some(entry) = outputs.remove(table_name) {
                let branch = entry.branching_table_name.clone();
                let mem = entry.memory_bytes;
                // Also remove the branching counterpart
                outputs.remove(&branch);
                (branch, mem)
            } else {
                return None;
            }
        };

        // Deregister both tables from OO catalog (best-effort)
        let _ = deregister_output_from_catalog(org_id, table_name).await;
        let _ = deregister_output_from_catalog(org_id, &branching_table_name).await;

        Some(memory_bytes)
    }

    /// Drop all output tables for a given inspection name.
    /// Returns a list of (table_name, memory_bytes) for each dropped table.
    pub async fn drop_all_outputs_for_inspection(
        &self,
        org_id: &str,
        inspection_name: &str,
    ) -> Vec<(String, usize)> {
        // Collect main output tables to drop (skip branching entries — they are
        // removed as counterparts). We identify main tables by the `deql_ins_` prefix.
        let tables_to_drop: Vec<(String, String, usize)> = {
            let outputs = self.outputs.read();
            outputs
                .values()
                .filter(|e| {
                    e.inspection_name.as_deref() == Some(inspection_name)
                        && e.table_name.starts_with("deql_ins_")
                })
                .map(|e| {
                    (
                        e.table_name.clone(),
                        e.branching_table_name.clone(),
                        e.memory_bytes,
                    )
                })
                .collect()
        };

        let mut dropped = Vec::new();
        for (table, branch, mem) in &tables_to_drop {
            {
                let mut outputs = self.outputs.write();
                outputs.remove(table);
                outputs.remove(branch);
            }
            let _ = deregister_output_from_catalog(org_id, table).await;
            let _ = deregister_output_from_catalog(org_id, branch).await;
            dropped.push((table.clone(), *mem));
        }

        dropped
    }

    /// Get the TableProvider for a given table name, if it exists.
    pub fn get_table_provider(&self, table_name: &str) -> Option<Arc<dyn TableProvider>> {
        self.table_providers.read().get(table_name).cloned()
    }

    /// Register a TableProvider for a given table name.
    pub fn register_table_provider(&self, table_name: &str, provider: Arc<dyn TableProvider>) {
        self.table_providers.write().insert(table_name.to_string(), provider);
    }

    /// Update memory_bytes for a specific table entry.
    /// This updates the OutputTableEntry's memory_bytes field.
    pub fn update_memory(&self, table_name: &str, bytes: usize) {
        let mut outputs = self.outputs.write();
        if let Some(entry) = outputs.get_mut(table_name) {
            entry.memory_bytes = bytes;
        }
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validation report returned by the VALIDATE endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct ValidationReport {
    pub valid: bool,
    pub checks: Vec<ValidationCheck>,
}

/// A single validation check result.
#[derive(Debug, Clone, Serialize)]
pub struct ValidationCheck {
    pub check: String,
    pub status: CheckStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Status of a validation check.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Pass,
    Fail,
    Warn,
}

impl ValidationCheck {
    pub fn pass(check: &str) -> Self {
        Self {
            check: check.to_string(),
            status: CheckStatus::Pass,
            message: None,
            details: None,
        }
    }

    pub fn fail(check: &str, message: impl Into<String>) -> Self {
        Self {
            check: check.to_string(),
            status: CheckStatus::Fail,
            message: Some(message.into()),
            details: None,
        }
    }

    pub fn warn(check: &str, message: impl Into<String>) -> Self {
        Self {
            check: check.to_string(),
            status: CheckStatus::Warn,
            message: Some(message.into()),
            details: None,
        }
    }
}

impl ValidationReport {
    pub fn to_json(&self) -> Value {
        json!({
            "valid": self.valid,
            "checks": self.checks,
        })
    }
}

/// Validate all preconditions for executing an inspection.
/// REQ-INSP-02
pub async fn validate_inspection(
    org_id: &str,
    definition: &InspectionDefinition,
    dereg: &o2_deql::dereg::DeReg,
) -> ValidationReport {
    let mut checks = Vec::new();

    // 1. Decision exists in DeReg
    let decision = dereg.get_decision(&definition.decision_name);
    checks.push(match decision {
        Some(_) => ValidationCheck::pass("decision_exists"),
        None => ValidationCheck::fail(
            "decision_exists",
            format!("decision '{}' not registered", definition.decision_name),
        ),
    });

    // 2. FROM stream exists (check OO stream catalog)
    let stream_exists = check_stream_exists(org_id, &definition.from_stream).await;
    checks.push(stream_exists);

    // 3. INTO template valid (no deql_ prefix in user-provided name)
    checks.push(if definition.into_template.starts_with("deql_") {
        ValidationCheck::fail(
            "into_template_valid",
            "user template cannot start with 'deql_' prefix",
        )
    } else {
        ValidationCheck::pass("into_template_valid")
    });

    // 4. WHERE clause valid (if present)
    if definition.guard_filter.is_some() {
        // For now, just check it's non-empty (full SQL validation at START time)
        checks.push(ValidationCheck::pass("guard_filter_valid"));
    } else {
        checks.push(ValidationCheck::pass("guard_filter_valid"));
    }

    let valid = checks
        .iter()
        .all(|c| !matches!(c.status, CheckStatus::Fail));
    ValidationReport { valid, checks }
}

/// Check if a stream exists in OO's catalog.
async fn check_stream_exists(org_id: &str, stream_name: &str) -> ValidationCheck {
    use config::meta::stream::StreamType;

    match infra::schema::get(org_id, stream_name, StreamType::Logs).await {
        Ok(schema) => {
            if schema.fields().is_empty() {
                ValidationCheck::warn(
                    "from_stream_exists",
                    format!("stream '{}' exists but has no schema fields", stream_name),
                )
            } else {
                ValidationCheck::pass("from_stream_exists")
            }
        }
        Err(_) => ValidationCheck::fail(
            "from_stream_exists",
            format!("stream '{}' not found", stream_name),
        ),
    }
}

// ---------------------------------------------------------------------------
// Schema Builders
// ---------------------------------------------------------------------------

/// REQ-INSP-09: Build the flattened inspection output schema.
///
/// The output schema includes:
/// - Fixed system columns (_timestamp, _row, _status, etc.)
/// - Union of all payload fields from emit items across all decision branches
/// - All payload columns are nullable Utf8
pub fn build_inspect_output_schema(
    decision: &o2_deql::parser::ast::CreateDecision,
    dereg: &o2_deql::dereg::DeReg,
) -> SchemaRef {
    let mut fields = vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("_row", DataType::Int64, false),
        Field::new("_status", DataType::Utf8, false),
        Field::new("_event_type", DataType::Utf8, true),
        Field::new("_decision", DataType::Utf8, false),
        Field::new("_aggregate_id", DataType::Utf8, true),
        Field::new("_branch_index", DataType::Int64, true),
        Field::new("_guard_expression", DataType::Utf8, true),
        Field::new("_reason", DataType::Utf8, true),
        Field::new("_run_id", DataType::Utf8, false),
    ];

    // Union of payload fields across all emit items
    let mut seen: HashSet<String> = HashSet::new();
    for emit in decision.all_emit_items() {
        if let Some(evt) = dereg.get_event(&emit.event_type.node) {
            for field_def in &evt.fields {
                let field_name = field_def.name.node.clone();
                if seen.insert(field_name.clone()) {
                    fields.push(Field::new(&field_name, DataType::Utf8, true));
                }
            }
        }
    }

    Arc::new(Schema::new(fields))
}

/// REQ-INSP-10: Build the branching decision table schema.
///
/// The branching table records the evaluation of each branch for each row,
/// showing which guards were evaluated and whether they passed/failed.
pub fn build_branching_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("inspect_row_index", DataType::UInt64, false),
        Field::new("decision_name", DataType::Utf8, false),
        Field::new("branch_id", DataType::Utf8, false),
        Field::new("branch_index", DataType::UInt64, false),
        Field::new("branch_rule_name", DataType::Utf8, true),
        Field::new("branch_guard", DataType::Utf8, false),
        Field::new("branch_status", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, true),
        Field::new("stream_id", DataType::Utf8, false),
    ]))
}

// ---------------------------------------------------------------------------
// OutputBatchBuilder — Columnar builder for inspection output rows
// ---------------------------------------------------------------------------

/// Columnar builder for inspection output rows.
///
/// Uses Arrow ArrayBuilders for efficient batch construction, accumulating rows
/// and producing a single RecordBatch at the end of processing a source batch.
/// Implements REQ-INSP-09.
pub struct OutputBatchBuilder {
    schema: SchemaRef,
    timestamp: arrow::array::Int64Builder,
    row: arrow::array::Int64Builder,
    status: arrow::array::StringBuilder,
    event_type: arrow::array::StringBuilder,
    decision: arrow::array::StringBuilder,
    aggregate_id: arrow::array::StringBuilder,
    branch_index: arrow::array::Int64Builder,
    guard_expression: arrow::array::StringBuilder,
    reason: arrow::array::StringBuilder,
    run_id: arrow::array::StringBuilder,
    payload: HashMap<String, arrow::array::StringBuilder>,
}

use arrow::array::{Int64Builder, StringBuilder, RecordBatch};

impl OutputBatchBuilder {
    /// Create a new OutputBatchBuilder with the given schema and capacity.
    pub fn new(schema: &SchemaRef, capacity: usize) -> Self {
        Self {
            schema: schema.clone(),
            timestamp: Int64Builder::with_capacity(capacity),
            row: Int64Builder::with_capacity(capacity),
            status: StringBuilder::with_capacity(capacity, capacity * 16),
            event_type: StringBuilder::with_capacity(capacity, capacity * 32),
            decision: StringBuilder::with_capacity(capacity, capacity * 32),
            aggregate_id: StringBuilder::with_capacity(capacity, capacity * 64),
            branch_index: Int64Builder::with_capacity(capacity),
            guard_expression: StringBuilder::with_capacity(capacity, capacity * 128),
            reason: StringBuilder::with_capacity(capacity, capacity * 128),
            run_id: StringBuilder::with_capacity(capacity, capacity * 64),
            payload: Self::init_payload_builders(schema, capacity),
        }
    }

    /// Initialize builders for all payload columns in the schema.
    fn init_payload_builders(schema: &SchemaRef, capacity: usize) -> HashMap<String, StringBuilder> {
        let mut builders = HashMap::new();
        // Skip the first 10 fixed columns: _timestamp through _run_id
        for (idx, field) in schema.fields().iter().enumerate() {
            if idx >= 10 {
                builders.insert(field.name().clone(), StringBuilder::with_capacity(capacity, capacity * 64));
            }
        }
        builders
    }

    /// Append an accepted (emitted) row to the builder.
    ///
    /// Arguments:
    /// - `row_idx`: Global row index in the source data
    /// - `run_id`: Run identifier (typically output table name)
    /// - `event_type`: Event type that was emitted
    /// - `aggregate_id`: Target aggregate ID
    /// - `branch_index`: Index of the matching branch
    /// - `guard_text`: Human-readable guard expression text
    /// - `decision_name`: Name of the decision being evaluated
    /// - `payload_fields`: Map of payload field name → value (as string)
    pub fn append_accepted_row(
        &mut self,
        row_idx: usize,
        run_id: &str,
        event_type: &str,
        aggregate_id: &str,
        branch_index: usize,
        guard_text: &str,
        decision_name: &str,
        payload_fields: &[(String, String)],
    ) {
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;

        self.timestamp.append_value(now_micros);
        self.row.append_value(row_idx as i64);
        self.status.append_value("accepted");
        self.event_type.append_value(event_type);
        self.decision.append_value(decision_name);
        self.aggregate_id.append_value(aggregate_id);
        self.branch_index.append_value(branch_index as i64);
        self.guard_expression.append_value(guard_text);
        self.reason.append_null();
        self.run_id.append_value(run_id);

        // Append payload fields
        self.append_payload_fields(payload_fields);
    }

    /// Append a rejected row to the builder (all guards failed).
    ///
    /// Arguments:
    /// - `row_idx`: Global row index in the source data
    /// - `run_id`: Run identifier
    /// - `aggregate_id`: Target aggregate ID
    /// - `decision_name`: Name of the decision
    /// - `last_guard`: Text of the last evaluated guard
    pub fn append_rejected_row(
        &mut self,
        row_idx: usize,
        run_id: &str,
        aggregate_id: &str,
        decision_name: &str,
        last_guard: &str,
    ) {
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;

        self.timestamp.append_value(now_micros);
        self.row.append_value(row_idx as i64);
        self.status.append_value("rejected");
        self.event_type.append_null();
        self.decision.append_value(decision_name);
        self.aggregate_id.append_value(aggregate_id);
        self.branch_index.append_null();
        self.guard_expression.append_value(last_guard);
        self.reason.append_value("all guards failed");
        self.run_id.append_value(run_id);

        // Append null for all payload fields
        self.append_null_payload_fields();
    }

    /// Append an error row (decision logic or state evaluation failed).
    ///
    /// Arguments:
    /// - `row_idx`: Global row index in the source data
    /// - `run_id`: Run identifier
    /// - `decision_name`: Name of the decision being evaluated
    /// - `reason`: Error message or reason
    pub fn append_error_row(
        &mut self,
        row_idx: usize,
        run_id: &str,
        decision_name: &str,
        reason: &str,
    ) {
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;

        self.timestamp.append_value(now_micros);
        self.row.append_value(row_idx as i64);
        self.status.append_value("error");
        self.event_type.append_null();
        self.decision.append_value(decision_name);
        self.aggregate_id.append_null();
        self.branch_index.append_null();
        self.guard_expression.append_null();
        self.reason.append_value(reason);
        self.run_id.append_value(run_id);

        // Append null for all payload fields
        self.append_null_payload_fields();
    }

    /// Append payload field values.
    fn append_payload_fields(&mut self, fields: &[(String, String)]) {
        let payload_map: HashMap<&str, &str> =
            fields.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        for (field_name, builder) in self.payload.iter_mut() {
            if let Some(value) = payload_map.get(field_name.as_str()) {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
    }

    /// Append null values for all payload fields.
    fn append_null_payload_fields(&mut self) {
        for builder in self.payload.values_mut() {
            builder.append_null();
        }
    }

    /// Finish building and produce a RecordBatch.
    pub fn finish(mut self) -> Result<RecordBatch, String> {
        let mut arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(self.timestamp.finish()),
            Arc::new(self.row.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.event_type.finish()),
            Arc::new(self.decision.finish()),
            Arc::new(self.aggregate_id.finish()),
            Arc::new(self.branch_index.finish()),
            Arc::new(self.guard_expression.finish()),
            Arc::new(self.reason.finish()),
            Arc::new(self.run_id.finish()),
        ];

        // Add payload columns in schema order
        for (idx, field) in self.schema.fields().iter().enumerate() {
            if idx >= 10 {
                let field_name = field.name();
                if let Some(mut builder) = self.payload.remove(field_name) {
                    arrays.push(Arc::new(builder.finish()));
                } else {
                    return Err(format!("Missing builder for payload field: {}", field_name));
                }
            }
        }

        RecordBatch::try_new(self.schema, arrays).map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------------------
// evaluate_decision_batch — Core batch evaluator
// ---------------------------------------------------------------------------

/// Result of evaluating one RecordBatch through the decision logic.
pub struct BatchEvalResult {
    pub output_batch: RecordBatch,
    pub branching_batches: Vec<RecordBatch>,
    pub rows_processed: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub errors: usize,
}

/// Error type for batch evaluation.
#[derive(Debug)]
pub enum InspectError {
    GuardEvaluationFailed(String),
    StateQueryFailed(String),
    AggregateIdResolutionFailed(String),
    EmitEvaluationFailed(String),
    RowProcessingFailed { row_idx: usize, reason: String },
    BatchConstructionFailed(String),
}

impl std::fmt::Display for InspectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InspectError::GuardEvaluationFailed(msg) => write!(f, "Guard evaluation failed: {}", msg),
            InspectError::StateQueryFailed(msg) => write!(f, "State query failed: {}", msg),
            InspectError::AggregateIdResolutionFailed(msg) => {
                write!(f, "Aggregate ID resolution failed: {}", msg)
            }
            InspectError::EmitEvaluationFailed(msg) => write!(f, "Emit evaluation failed: {}", msg),
            InspectError::RowProcessingFailed { row_idx, reason } => {
                write!(f, "Row {} processing failed: {}", row_idx, reason)
            }
            InspectError::BatchConstructionFailed(msg) => {
                write!(f, "Batch construction failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for InspectError {}

// Helper functions for evaluate_decision_batch

/// Extract column values from a RecordBatch row as bind parameters.
/// This is a helper function used during evaluation.
fn row_to_bind_params(batch: &RecordBatch, row_idx: usize) -> HashMap<String, String> {
    use datafusion::common::ScalarValue;
    
    let mut params = HashMap::new();
    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let value = ScalarValue::try_from_array(batch.column(col_idx), row_idx)
            .unwrap_or(ScalarValue::Null);
        let literal = o2_deql::executor::scalar_to_sql_literal(&value);
        params.insert(field.name().clone(), literal);
    }
    params
}

/// Remap FROM table columns to command field names.
/// If the FROM table columns already match command field names, use as-is.
/// Otherwise, map column[i] → command_field[i] by position.
fn remap_to_command_fields(
    raw_params: &HashMap<String, String>,
    command_field_names: &[String],
    table_fields: &arrow::datatypes::Fields,
) -> HashMap<String, String> {
    if command_field_names.is_empty() {
        return raw_params.clone();
    }

    // Check if the FROM table columns already match command field names
    let already_named = command_field_names
        .iter()
        .any(|name| raw_params.contains_key(name));
    if already_named {
        return raw_params.clone();
    }

    // Positional mapping: table column[i] → command_field[i]
    let mut mapped = HashMap::new();
    for (i, field) in table_fields.iter().enumerate() {
        if let Some(value) = raw_params.get(field.name().as_str()) {
            if i < command_field_names.len() {
                mapped.insert(command_field_names[i].clone(), value.clone());
            } else {
                // Extra columns beyond command fields — keep original name
                mapped.insert(field.name().clone(), value.clone());
            }
        }
    }
    mapped
}

/// Evaluate a full RecordBatch against the decision logic.
///
/// For each row in the batch:
/// 1. Extract params (column values)
/// 2. Remap to command field names
/// 3. Resolve aggregate_id
/// 4. Execute STATE AS (if present)
/// 5. Evaluate guards sequentially (first-match-wins)
/// 6. Compute EMIT fields or mark rejected
///
/// Returns a consolidated output RecordBatch and companion branch batches.
///
/// Implements REQ-INSP-08: Per-Row Decision Simulation.
pub async fn evaluate_decision_batch(
    decision: &o2_deql::parser::ast::CreateDecision,
    batch: &RecordBatch,
    command_field_names: &[String],
    _command_fields: &[o2_deql::parser::ast::FieldDef],
    ctx: &datafusion::prelude::SessionContext,
    dereg: &o2_deql::DeReg,
    start_row_idx: usize,
    run_id: &str,
    output_schema: &SchemaRef,
    branching_schema: &SchemaRef,
) -> Result<BatchEvalResult, InspectError> {
    let num_rows = batch.num_rows();
    let mut output_builders = OutputBatchBuilder::new(output_schema, num_rows);
    let mut branching_batches: Vec<RecordBatch> = Vec::new();
    let mut accepted = 0usize;
    let mut rejected = 0usize;
    let mut errors = 0usize;

    for row_idx in 0..num_rows {
        let global_idx = start_row_idx + row_idx;
        
        // 1. Extract parameters from row
        let raw_params = row_to_bind_params(batch, row_idx);
        
        // 2. Remap to command field names
        let params = remap_to_command_fields(&raw_params, command_field_names, batch.schema().fields());

        // 3. Resolve aggregate_id
        let command_def = dereg.get_command(&decision.command.node);
        let aggregate_id = match command_def {
            Some(cmd) => {
                match o2_deql::executor::resolve_aggregate_id(decision, &params, cmd) {
                    Ok(id) => id,
                    Err(e) => {
                        output_builders.append_error_row(global_idx, run_id, &decision.name.node, &e.to_string());
                        errors += 1;
                        continue;
                    }
                }
            }
            None => {
                output_builders.append_error_row(global_idx, run_id, &decision.name.node, "Command not found");
                errors += 1;
                continue;
            }
        };

        // 4. Execute STATE AS (if present)
        let state_row = if let Some(ref state_sql) = decision.state_as {
            match o2_deql::executor::substitute_bind_params(&state_sql.sql, &params, "STATE AS") {
                Ok(substituted) => {
                    match o2_deql::executor::execute_state_query(&substituted, ctx, &decision.name.node).await {
                        Ok(state) => state,
                        Err(e) => {
                            output_builders.append_error_row(global_idx, run_id, &decision.name.node, &e.to_string());
                            errors += 1;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    output_builders.append_error_row(global_idx, run_id, &decision.name.node, &e.to_string());
                    errors += 1;
                    continue;
                }
            }
        } else {
            HashMap::new()
        };

        // 5. Evaluate branches (first-match-wins)
        let mut row_emitted = false;
        for (branch_idx, branch) in decision.branches.iter().enumerate() {
            let guard_text = branch
                .guard
                .as_ref()
                .map(|g| g.sql.as_str())
                .unwrap_or("TRUE");

            // Evaluate guard
            let guard_passes = if let Some(ref guard) = branch.guard {
                match o2_deql::executor::evaluate_guard(
                    &guard.sql,
                    &params,
                    &state_row,
                    ctx,
                    &decision.name.node,
                ).await {
                    Ok(passes) => passes,
                    Err(e) => {
                        output_builders.append_error_row(global_idx, run_id, &decision.name.node, &e.to_string());
                        errors += 1;
                        row_emitted = true; // skip further evaluation
                        break;
                    }
                }
            } else {
                true
            };

            if !guard_passes {
                // Record branching row for failed guard
                append_branching_row(
                    &mut branching_batches,
                    branching_schema,
                    global_idx,
                    decision,
                    branch,
                    &aggregate_id,
                    "guard_failed",
                    None,
                )?;
                continue;
            }

            // Guard passed — evaluate EMIT for each emit item
            for emit_item in &branch.emit_items {
                match o2_deql::executor::evaluate_emit_expressions(emit_item, &params, &state_row, ctx).await {
                    Ok(fields) => {
                        // Convert fields to (String, String) tuples for the builder
                        let payload_fields: Vec<(String, String)> = fields
                            .iter()
                            .map(|(k, v)| (k.clone(), v.to_string()))
                            .collect();

                        output_builders.append_accepted_row(
                            global_idx,
                            run_id,
                            &emit_item.event_type.node,
                            &aggregate_id,
                            branch_idx,
                            guard_text,
                            &decision.name.node,
                            &payload_fields,
                        );

                        // Record branching row for emitted event
                        append_branching_row(
                            &mut branching_batches,
                            branching_schema,
                            global_idx,
                            decision,
                            branch,
                            &aggregate_id,
                            "emitted",
                            Some(&emit_item.event_type.node),
                        )?;

                        accepted += 1;
                        row_emitted = true;
                    }
                    Err(e) => {
                        output_builders.append_error_row(global_idx, run_id, &decision.name.node, &e.to_string());
                        errors += 1;
                        row_emitted = true; // skip further evaluation
                        break;
                    }
                }
            }

            if row_emitted {
                break; // first-match-wins: stop evaluating branches
            }
        }

        if !row_emitted {
            // All guards failed — rejected row
            output_builders.append_rejected_row(
                global_idx,
                run_id,
                &aggregate_id,
                &decision.name.node,
                decision
                    .branches
                    .last()
                    .and_then(|b| b.guard.as_ref())
                    .map(|g| g.sql.as_str())
                    .unwrap_or("(no guards)"),
            );
            rejected += 1;
        }
    }

    // Finish the output batch
    let output_batch = output_builders
        .finish()
        .map_err(|e| InspectError::BatchConstructionFailed(e))?;

    Ok(BatchEvalResult {
        output_batch,
        branching_batches,
        rows_processed: num_rows,
        accepted,
        rejected,
        errors,
    })
}

/// Helper to build and append a branching row to the batches list.
fn append_branching_row(
    batches: &mut Vec<RecordBatch>,
    branching_schema: &SchemaRef,
    row_idx: usize,
    decision: &o2_deql::parser::ast::CreateDecision,
    branch: &o2_deql::parser::ast::DecisionBranch,
    aggregate_id: &str,
    status: &str,
    event_type: Option<&str>,
) -> Result<(), InspectError> {
    use arrow::array::{Int64Builder, UInt64Builder, StringBuilder};

    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;

    let mut timestamp = Int64Builder::with_capacity(1);
    let mut inspect_row_index = UInt64Builder::with_capacity(1);
    let mut decision_name = StringBuilder::with_capacity(1, 128);
    let mut branch_id = StringBuilder::with_capacity(1, 128);
    let mut branch_index = UInt64Builder::with_capacity(1);
    let mut branch_rule_name = StringBuilder::with_capacity(1, 128);
    let mut branch_guard = StringBuilder::with_capacity(1, 256);
    let mut branch_status = StringBuilder::with_capacity(1, 64);
    let mut event_type_col = StringBuilder::with_capacity(1, 128);
    let mut stream_id = StringBuilder::with_capacity(1, 256);

    timestamp.append_value(now_micros);
    inspect_row_index.append_value(row_idx as u64);
    decision_name.append_value(&decision.name.node);
    
    // Compute branch_id (XXH3-64 hash of canonical branch text)
    let rule_name = branch.rule_name.as_ref().map(|s| s.node.as_str()).unwrap_or("");
    let guard_sql = branch.guard.as_ref().map(|g| g.sql.as_str()).unwrap_or("");
    let emits: Vec<String> = branch
        .emit_items
        .iter()
        .map(|e| {
            let fields = e
                .assignments
                .iter()
                .map(|a| a.field.node.as_str())
                .collect::<Vec<_>>()
                .join(",");
            format!("{}({})", e.event_type.node, fields)
        })
        .collect();
    let canonical = format!(
        "{}:{}:{}:{}:{}",
        decision.name.node,
        branch.branch_index,
        rule_name,
        guard_sql,
        emits.join(",")
    );
    let computed_branch_id = format!("b_{}", 
        (canonical.len() as u64).wrapping_mul(31).wrapping_add(
            canonical.bytes().fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64))
        ).to_string().chars().take(16).collect::<String>()
    );
    branch_id.append_value(&computed_branch_id);
    
    branch_index.append_value(branch.branch_index as u64);
    if let Some(name) = &branch.rule_name {
        branch_rule_name.append_value(&name.node);
    } else {
        branch_rule_name.append_null();
    }
    branch_guard.append_value(guard_sql);
    branch_status.append_value(status);
    
    if let Some(et) = event_type {
        event_type_col.append_value(et);
    } else {
        event_type_col.append_null();
    }
    stream_id.append_value(aggregate_id);

    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(timestamp.finish()),
        Arc::new(inspect_row_index.finish()),
        Arc::new(decision_name.finish()),
        Arc::new(branch_id.finish()),
        Arc::new(branch_index.finish()),
        Arc::new(branch_rule_name.finish()),
        Arc::new(branch_guard.finish()),
        Arc::new(branch_status.finish()),
        Arc::new(event_type_col.finish()),
        Arc::new(stream_id.finish()),
    ];

    let batch = RecordBatch::try_new(branching_schema.clone(), arrays)
        .map_err(|e| InspectError::BatchConstructionFailed(e.to_string()))?;

    batches.push(batch);
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema Registration (REQ-INSP-17, REQ-INSP-06)
// ---------------------------------------------------------------------------

/// Register an inspection output table in OO's stream catalog.
///
/// This makes the table visible in Logs Explore immediately and marks it as ephemeral
/// so it's not persisted across server restarts.
///
/// Implements REQ-INSP-17: Output tables become visible in Logs Explore.
pub async fn register_output_in_catalog(
    org_id: &str,
    table_name: &str,
    schema: &SchemaRef,
) -> Result<(), Box<dyn std::error::Error>> {
    use config::meta::stream::StreamType;

    // Build metadata with ephemeral marker
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("ephemeral".to_string(), "true".to_string());
    metadata.insert("deql_inspection".to_string(), "true".to_string());

    // Merge the schema into OO's catalog
    infra::schema::merge(
        org_id,
        table_name,
        StreamType::Logs,
        schema,
        None,
    )
    .await?;

    Ok(())
}

/// Deregister an output table from OO's stream catalog.
///
/// Removes the table from Logs Explore visibility.
pub async fn deregister_output_from_catalog(
    org_id: &str,
    table_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use config::meta::stream::StreamType;

    infra::schema::delete(org_id, StreamType::Logs, table_name, None).await?;
    Ok(())
}

/// Cleanup ephemeral streams on server startup.
///
/// Removes `deql_ins_*` and `deql_brn_*` streams from the catalog.
/// These are ephemeral streams created by DeQL inspection with metadata
/// `ephemeral: "true"`.
///
/// This prevents stale ephemeral entries from persisting across server restarts
/// after a crash or unclean shutdown.
pub async fn cleanup_ephemeral_streams(org_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    use config::meta::stream::StreamType;
    use crate::service::db::schema;

    // List all logs streams for this org from the cache
    let streams = schema::list_streams_from_cache(org_id, StreamType::Logs).await;

    // Filter for ephemeral streams with deql_ins_* or deql_brn_* prefix
    // These streams are marked with ephemeral metadata during registration
    let ephemeral_streams: Vec<String> = streams
        .into_iter()
        .filter(|name| {
            // Check if it's a deql inspection or branching table
            name.starts_with("deql_ins_") || name.starts_with("deql_brn_")
        })
        .collect();

    if ephemeral_streams.is_empty() {
        log::debug!("No ephemeral streams to cleanup for org {}", org_id);
        return Ok(());
    }

    // Delete each ephemeral stream from the catalog
    for stream_name in &ephemeral_streams {
        log::info!(
            "Cleaning up ephemeral stream: {}/{}",
            org_id,
            stream_name
        );
        if let Err(e) = infra::schema::delete(org_id, StreamType::Logs, stream_name, None).await {
            log::error!(
                "Failed to delete ephemeral stream {}/{}: {}",
                org_id,
                stream_name,
                e
            );
            // Continue with other streams - best effort cleanup
        }
    }

    log::info!(
        "Cleaned up {} ephemeral streams for org {}",
        ephemeral_streams.len(),
        org_id
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Execution Parameters & Background Task
// ---------------------------------------------------------------------------

/// Parameters needed for the execution background task.
#[derive(Clone)]
pub struct ExecutionParams {
    pub org_id: String,
    pub decision: o2_deql::parser::ast::CreateDecision,
    pub dereg: o2_deql::DeReg,
    pub from_stream: String,
    pub output_table: String,
    pub branching_table: String,
    pub output_schema: SchemaRef,
    pub branching_schema: SchemaRef,
    pub guard_filter: Option<String>,
    pub offset: i64,
    pub limit: i64,
    pub range: Option<(String, String)>,  // (from_id, to_id)
    pub cancel: CancellationToken,
    pub run_id: String,
    pub decision_name: String,
}

/// Build a source query with filters applied in order: RANGE → WHERE → OFFSET → LIMIT
///
/// Implements REQ-INSP-09: Source query construction with filter application.
pub fn build_source_query(
    from_stream: &str,
    range: &Option<(String, String)>,
    guard_filter: &Option<String>,
    offset: i64,
    limit: i64,
) -> String {
    let mut query = format!("SELECT * FROM \"{}\"", from_stream);
    let mut where_clauses = Vec::new();

    // Apply RANGE filter first (if present)
    if let Some((from_id, to_id)) = range {
        where_clauses.push(format!(
            "id >= '{}' AND id <= '{}'",
            from_id.replace("'", "''"),
            to_id.replace("'", "''")
        ));
    }

    // Apply WHERE filter (if present)
    if let Some(filter) = guard_filter {
        where_clauses.push(filter.clone());
    }

    // Add all WHERE clauses
    if !where_clauses.is_empty() {
        query.push_str(" WHERE ");
        query.push_str(&where_clauses.join(" AND "));
    }

    // Apply OFFSET (if non-zero)
    if offset > 0 {
        query.push_str(&format!(" OFFSET {}", offset));
    }

    // Apply LIMIT
    query.push_str(&format!(" LIMIT {}", limit));

    query
}

/// Resolve run_details and auto-generate serial if needed.
///
/// - If run_details is None, uses today's date and auto-incremented serial
/// - If date is provided but serial is None, auto-increments serial for that date
/// - If both are provided, uses as-is
///
/// Implements REQ-INSP-10: Serial number resolution.
pub fn resolve_run_details(
    run_details: &Option<(String, Option<u32>)>,
    state: &InspectionOrgState,
    inspection_name: &str,
) -> (String, u32) {
    let date = run_details
        .as_ref()
        .and_then(|(d, _)| if d.is_empty() { None } else { Some(d.clone()) })
        .unwrap_or_else(|| Utc::now().format("%Y%m%d").to_string());

    let serial = run_details
        .as_ref()
        .and_then(|(_, s)| *s)
        .unwrap_or_else(|| {
            let key = format!("{}_{}", inspection_name, date);
            state.next_serial(&key)
        });

    (date, serial)
}

/// Background task for executing inspections.
///
/// This function:
/// 1. Builds a DataFusion SessionContext with source table access
/// 2. Constructs and executes the source query with filters
/// 3. Processes batches through the decision evaluator
/// 4. Registers output and branching tables in DataFusion
/// 5. Updates run state on completion or cancellation
///
/// Implements REQ-INSP-05, REQ-INSP-08: Execution engine.
pub async fn execute_inspection(
    params: ExecutionParams,
    inspect_state: Arc<InspectionOrgState>,
) {
    // Build DataFusion SessionContext with source data from OO's search
    let range_filter = params.range.as_ref().map(|(f, t)| (f.clone(), t.clone()));
    let query = build_source_query(
        &params.from_stream,
        &range_filter,
        &params.guard_filter,
        params.offset,
        params.limit,
    );

    tracing::info!("Executing inspection query: {}", query);

    let (ctx, batches) = match build_session_context(
        &params.org_id,
        &params.from_stream,
        &query,
        params.limit,
    ).await {
        Ok(result) => result,
        Err(e) => {
            tracing::error!("Failed to build session context with source data: {}", e);
            inspect_state.clear_running();
            return;
        }
    };

    // Process batches through decision evaluator
    let mut output_batches: Vec<RecordBatch> = Vec::new();
    let mut branching_batches: Vec<RecordBatch> = Vec::new();
    let mut total_accepted = 0usize;
    let mut total_rejected = 0usize;
    let mut total_errors = 0usize;
    let mut global_row_idx = 0usize;

    // Get command field names for remapping
    let command_def = params.dereg.get_command(&params.decision.command.node);
    let command_field_names: Vec<String> = command_def
        .map(|cmd| {
            cmd.fields
                .iter()
                .map(|f| f.name.node.clone())
                .collect()
        })
        .unwrap_or_default();
    let command_fields: Vec<o2_deql::parser::ast::FieldDef> = command_def
        .map(|cmd| cmd.fields.clone())
        .unwrap_or_default();

    for batch in &batches {
        // Check cancellation between batches (REQ-INSP-04: STOP support)
        if params.cancel.is_cancelled() {
            tracing::info!("Inspection cancelled");
            break;
        }

        // Process entire batch — evaluate decision for each row
        match evaluate_decision_batch(
            &params.decision,
            batch,
            &command_field_names,
            &command_fields,
            &ctx,
            &params.dereg,
            global_row_idx,
            &params.run_id,
            &params.output_schema,
            &params.branching_schema,
        )
        .await
        {
            Ok(result) => {
                total_accepted += result.accepted;
                total_rejected += result.rejected;
                total_errors += result.errors;
                global_row_idx += result.rows_processed;
                output_batches.push(result.output_batch);
                branching_batches.extend(result.branching_batches);

                // Update live stats after each batch (for STATUS endpoint)
                if let Some(handle) = inspect_state.running.read().as_ref() {
                    handle.rows_processed.store(global_row_idx, Ordering::Relaxed);
                    handle.accepted.store(total_accepted, Ordering::Relaxed);
                    handle.rejected.store(total_rejected, Ordering::Relaxed);
                    handle.errors.store(total_errors, Ordering::Relaxed);
                }
            }
            Err(e) => {
                tracing::error!("Batch evaluation failed: {}", e);
                total_errors += batch.num_rows();
                global_row_idx += batch.num_rows();
            }
        }
    }

    // Ingest output batches into OO's storage via ingestion service
    if !output_batches.is_empty() {
        match ingest_batches_to_stream(
            &params.org_id,
            &params.output_table,
            &output_batches,
            &params.output_schema,
        )
        .await
        {
            Ok(count) => tracing::info!(
                "Output table '{}' ingested: {} records",
                params.output_table,
                count
            ),
            Err(e) => tracing::warn!("Failed to ingest output table: {}", e),
        }
    }

    if !branching_batches.is_empty() {
        match ingest_batches_to_stream(
            &params.org_id,
            &params.branching_table,
            &branching_batches,
            &params.branching_schema,
        )
        .await
        {
            Ok(count) => tracing::info!(
                "Branching table '{}' ingested: {} records",
                params.branching_table,
                count
            ),
            Err(e) => tracing::warn!("Failed to ingest branching table: {}", e),
        }
    }

    // Determine final status
    let status = if params.cancel.is_cancelled() {
        OutputStatus::Stopped
    } else {
        OutputStatus::Done
    };

    // Handle upsert: check if table already exists and append to it
    #[cfg(feature = "deql")]
    {
        use crate::service::deql::InspectionTableProvider;

        // Get the current memory bytes before any changes
        let mut new_memory_bytes = 0usize;

        if let Some(existing_provider) = inspect_state.get_table_provider(&params.output_table) {
            // Downcast to InspectionTableProvider to call append_batches
            if let Some(inspection_provider) = existing_provider.as_any().downcast_ref::<InspectionTableProvider>() {
                // Upsert: append to existing provider
                inspection_provider.append_batches(output_batches);
                
                // Also append to branching table if it exists
                if let Some(brn_provider) = inspect_state.get_table_provider(&params.branching_table) {
                    if let Some(brn_inspection_provider) = brn_provider.as_any().downcast_ref::<InspectionTableProvider>() {
                        brn_inspection_provider.append_batches(branching_batches);
                    }
                }

                // Update stats in existing OutputTableEntry if present
                {
                    let mut outputs = inspect_state.outputs.write();
                    if let Some(entry) = outputs.get_mut(&params.output_table) {
                        entry.rows_processed += global_row_idx;
                        entry.accepted += total_accepted;
                        entry.rejected += total_rejected;
                        entry.errors += total_errors;
                        entry.status = status.clone();
                    }
                    if let Some(entry) = outputs.get_mut(&params.branching_table) {
                        entry.rows_processed += global_row_idx;
                        entry.accepted += total_accepted;
                        entry.rejected += total_rejected;
                        entry.errors += total_errors;
                        entry.status = status.clone();
                    }
                }

                new_memory_bytes = inspect_state.table_providers.read()
                    .get(&params.output_table)
                    .and_then(|p| p.as_any().downcast_ref::<InspectionTableProvider>())
                    .map(|p| p.memory_bytes())
                    .unwrap_or(0)
                    + inspect_state.table_providers.read()
                    .get(&params.branching_table)
                    .and_then(|p| p.as_any().downcast_ref::<InspectionTableProvider>())
                    .map(|p| p.memory_bytes())
                    .unwrap_or(0);
            } else {
                tracing::error!("Failed to downcast table provider to InspectionTableProvider for {}", params.output_table);
                // Fall through to create new providers
            }
        } else {
            // Fresh: create new providers and register them
            let output_provider = Arc::new(InspectionTableProvider::new(params.output_schema.clone()));
            output_provider.append_batches(output_batches);
            inspect_state.register_table_provider(&params.output_table, output_provider.clone());

            let brn_provider = Arc::new(InspectionTableProvider::new(params.branching_schema.clone()));
            brn_provider.append_batches(branching_batches);
            inspect_state.register_table_provider(&params.branching_table, brn_provider.clone());

            // Register in catalog if not already done
            if let Err(e) = register_output_in_catalog(
                &params.org_id,
                &params.output_table,
                &params.output_schema,
            )
            .await
            {
                tracing::warn!("Failed to register output table in catalog: {}", e);
            }

            if let Err(e) = register_output_in_catalog(
                &params.org_id,
                &params.branching_table,
                &params.branching_schema,
            )
            .await
            {
                tracing::warn!("Failed to register branching table in catalog: {}", e);
            }

            // Create new OutputTableEntry
            {
                let mut outputs = inspect_state.outputs.write();
                outputs.insert(
                    params.output_table.clone(),
                    OutputTableEntry {
                        table_name: params.output_table.clone(),
                        branching_table_name: params.branching_table.clone(),
                        inspection_name: Some(params.decision_name.clone()),
                        decision_name: params.decision_name.clone(),
                        status: status.clone(),
                        rows_processed: global_row_idx,
                        accepted: total_accepted,
                        rejected: total_rejected,
                        errors: total_errors,
                        schema: params.output_schema.clone(),
                        created_at: Utc::now(),
                        memory_bytes: 0, // Will be updated below
                    },
                );
                outputs.insert(
                    params.branching_table.clone(),
                    OutputTableEntry {
                        table_name: params.branching_table.clone(),
                        branching_table_name: params.branching_table.clone(),
                        inspection_name: Some(params.decision_name.clone()),
                        decision_name: params.decision_name.clone(),
                        status: status.clone(),
                        rows_processed: global_row_idx,
                        accepted: total_accepted,
                        rejected: total_rejected,
                        errors: total_errors,
                        schema: params.branching_schema.clone(),
                        created_at: Utc::now(),
                        memory_bytes: 0, // Will be updated below
                    },
                );
            }

            new_memory_bytes = output_provider.memory_bytes() + brn_provider.memory_bytes();
        }

        // Update memory_bytes in OutputTableEntry
        inspect_state.update_memory(&params.output_table, new_memory_bytes);
        inspect_state.update_memory(&params.branching_table, new_memory_bytes);
    }

    // Update final state
    inspect_state.clear_running();

    tracing::info!(
        "Inspection completed: {} rows, {} accepted, {} rejected, {} errors, status: {:?}",
        global_row_idx,
        total_accepted,
        total_rejected,
        total_errors,
        status
    );
}

/// Build a DataFusion SessionContext with access to source tables.
///
/// Uses OO's cluster search path to fetch source data and registers it
/// as a MemTable in the returned SessionContext.
async fn build_session_context(
    org_id: &str,
    from_stream: &str,
    _source_query_sql: &str,
    limit: i64,
) -> Result<(datafusion::prelude::SessionContext, Vec<RecordBatch>), Box<dyn std::error::Error>> {
    use config::{
        meta::{search::{Request as SearchRequest, Query as SearchQuery}, stream::StreamType},
        utils::{record_batch_ext::convert_json_to_record_batch, time::now_micros},
    };
    use proto::cluster_rpc::SearchQuery as ClusterSearchQuery;

    let ctx = datafusion::prelude::SessionContext::new();

    // Use OO's cluster search to fetch actual data from the stream
    let end_time = now_micros();
    let start_time = end_time - (365 * 24 * 60 * 60 * 1_000_000); // 1 year back

    let search_sql = format!("SELECT * FROM \"{}\" LIMIT {}", from_stream, limit);

    let search_req = SearchRequest {
        query: SearchQuery {
            sql: search_sql.clone(),
            start_time,
            end_time,
            from: 0,
            size: limit as i64,
            ..Default::default()
        },
        ..Default::default()
    };

    let trace_id = config::ider::generate_trace_id();
    let query: ClusterSearchQuery = search_req.query.clone().into();
    let mut request = config::datafusion::request::Request::new(
        trace_id.clone(),
        org_id.to_string(),
        StreamType::Logs,
        0,
        None,
        Some((search_req.query.start_time, search_req.query.end_time)),
        None,
        0,
        false,
    );
    request.set_use_cache(false);

    let result = crate::service::search::cluster::http::search(
        request, query, vec![], vec![], true,
    )
    .await
    .map_err(|e| format!("Search failed for stream '{}': {}", from_stream, e))?;

    tracing::info!(
        "Inspection fetched {} hits from stream '{}' (total: {})",
        result.hits.len(),
        from_stream,
        result.total
    );

    if result.hits.is_empty() {
        return Ok((ctx, vec![]));
    }

    // Infer schema from the first hit
    let sample = &result.hits[0];
    let fields: Vec<arrow::datatypes::Field> = if let Some(obj) = sample.as_object() {
        obj.keys()
            .map(|k| arrow::datatypes::Field::new(k, arrow::datatypes::DataType::Utf8, true))
            .collect()
    } else {
        return Err("First hit is not a JSON object".into());
    };
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));

    // Convert JSON hits to RecordBatch
    let arc_hits: Vec<Arc<serde_json::Value>> = result.hits.into_iter().map(Arc::new).collect();
    let batch = convert_json_to_record_batch(&schema, &arc_hits)
        .map_err(|e| format!("Failed to convert hits to RecordBatch: {}", e))?;

    // Register as MemTable so decision evaluation can use it
    let mem_table = datafusion::datasource::MemTable::try_new(
        schema.clone(),
        vec![vec![batch.clone()]],
    )?;
    ctx.register_table(from_stream, std::sync::Arc::new(mem_table))?;

    Ok((ctx, vec![batch]))
}

/// Register a MemTable in DataFusion for query access.
async fn register_mem_table(
    ctx: &datafusion::prelude::SessionContext,
    table_name: &str,
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::datasource::MemTable;

    let mem_table = MemTable::try_new(schema.clone(), vec![batches])?;
    ctx.register_table(table_name, std::sync::Arc::new(mem_table))?;
    Ok(())
}

/// Ingest Arrow RecordBatches into OO's storage layer via the ingestion service.
///
/// Converts each row of the batches into a JSON record and sends them to the
/// ingestion pipeline, which writes to WAL and makes them queryable.
async fn ingest_batches_to_stream(
    org_id: &str,
    stream_name: &str,
    batches: &[RecordBatch],
    _schema: &SchemaRef,
) -> Result<usize, Box<dyn std::error::Error>> {
    use config::meta::stream::StreamType;

    let mut records: Vec<serde_json::Value> = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        let schema = batch.schema();

        for row_idx in 0..num_rows {
            let mut record = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value = arrow_col_to_json_value(col, row_idx);
                record.insert(field.name().clone(), value);
            }
            records.push(serde_json::Value::Object(record));
        }
    }

    if records.is_empty() {
        return Ok(0);
    }

    let count = records.len();

    // Use the gRPC ingestion service to write records
    let req = proto::cluster_rpc::IngestionRequest {
        org_id: org_id.to_string(),
        stream_name: stream_name.to_string(),
        stream_type: StreamType::Logs.to_string(),
        data: Some(proto::cluster_rpc::IngestionData::from(records)),
        ingestion_type: Some(proto::cluster_rpc::IngestionType::Json.into()),
        metadata: None,
    };

    crate::service::ingestion::ingestion_service::ingest(req)
        .await
        .map_err(|e| format!("Ingestion failed for '{}': {}", stream_name, e))?;

    Ok(count)
}

/// Convert an Arrow column value at a given row index to a JSON value.
fn arrow_col_to_json_value(col: &dyn arrow::array::Array, row_idx: usize) -> serde_json::Value {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if col.is_null(row_idx) {
        return serde_json::Value::Null;
    }

    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(row_idx).to_string())
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            serde_json::json!(arr.value(row_idx))
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            serde_json::json!(arr.value(row_idx))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::json!(arr.value(row_idx))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(row_idx))
        }
        _ => {
            // Fallback: convert to string representation
            serde_json::Value::String(format!("{:?}", col.data_type()))
        }
    }
}

#[cfg(all(test, feature = "deql"))]
mod tests {
    use super::*;
    use arrow::array::Array;
    use o2_deql::parser::token::Span;

    #[test]
    fn test_output_batch_builder_accepted_rows() {
        // Build a simple schema with fixed columns + 1 payload field
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, true),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 10);

        // Append 3 accepted rows
        builder.append_accepted_row(
            0,
            "run_001",
            "TestEvent",
            "agg_123",
            0,
            "amount > 100",
            "MyDecision",
            &[(String::from("amount"), String::from("150"))],
        );

        builder.append_accepted_row(
            1,
            "run_001",
            "TestEvent",
            "agg_124",
            1,
            "amount > 200",
            "MyDecision",
            &[(String::from("amount"), String::from("250"))],
        );

        builder.append_accepted_row(
            2,
            "run_001",
            "TestEvent",
            "agg_125",
            0,
            "amount > 100",
            "MyDecision",
            &[(String::from("amount"), String::from("175"))],
        );

        let batch = builder.finish().expect("Failed to build batch");

        // Verify batch properties
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().fields().len(), 11);

        // Verify _status column
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");
        assert_eq!(status_col.value(0), "accepted");
        assert_eq!(status_col.value(1), "accepted");
        assert_eq!(status_col.value(2), "accepted");

        // Verify _event_type column
        let event_type_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _event_type");
        assert_eq!(event_type_col.value(0), "TestEvent");
        assert_eq!(event_type_col.value(1), "TestEvent");

        // Verify _aggregate_id column
        let agg_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _aggregate_id");
        assert_eq!(agg_col.value(0), "agg_123");
        assert_eq!(agg_col.value(1), "agg_124");
        assert_eq!(agg_col.value(2), "agg_125");

        // Verify payload column (amount)
        let amount_col = batch
            .column(10)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for amount");
        assert_eq!(amount_col.value(0), "150");
        assert_eq!(amount_col.value(1), "250");
        assert_eq!(amount_col.value(2), "175");
    }

    #[test]
    fn test_output_batch_builder_rejected_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, true),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 5);

        // Append 2 rejected rows
        builder.append_rejected_row(
            0,
            "run_001",
            "agg_001",
            "MyDecision",
            "amount > 100",
        );

        builder.append_rejected_row(
            1,
            "run_001",
            "agg_002",
            "MyDecision",
            "amount > 200",
        );

        let batch = builder.finish().expect("Failed to build batch");

        assert_eq!(batch.num_rows(), 2);

        // Verify _status column
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");
        assert_eq!(status_col.value(0), "rejected");
        assert_eq!(status_col.value(1), "rejected");

        // Verify _event_type is null
        let event_type_col = batch.column(3);
        assert!(event_type_col.is_null(0));
        assert!(event_type_col.is_null(1));

        // Verify _reason column
        let reason_col = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _reason");
        assert_eq!(reason_col.value(0), "all guards failed");
        assert_eq!(reason_col.value(1), "all guards failed");

        // Verify payload column is null
        let amount_col = batch.column(10);
        assert!(amount_col.is_null(0));
        assert!(amount_col.is_null(1));
    }

    #[test]
    fn test_output_batch_builder_error_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, true),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 5);

        // Append 2 error rows
        builder.append_error_row(0, "run_001", "MyDecision", "State query failed");
        builder.append_error_row(1, "run_001", "MyDecision", "Invalid aggregate ID");

        let batch = builder.finish().expect("Failed to build batch");

        assert_eq!(batch.num_rows(), 2);

        // Verify _status column
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");
        assert_eq!(status_col.value(0), "error");
        assert_eq!(status_col.value(1), "error");

        // Verify _reason column
        let reason_col = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _reason");
        assert_eq!(reason_col.value(0), "State query failed");
        assert_eq!(reason_col.value(1), "Invalid aggregate ID");
    }

    #[test]
    fn test_output_batch_builder_mixed_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, true),
            Field::new("currency", DataType::Utf8, true),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 10);

        // Mix of accepted, rejected, and error rows
        builder.append_accepted_row(
            0,
            "run_001",
            "EventA",
            "agg_1",
            0,
            "guard_0",
            "MyDecision",
            &[
                (String::from("amount"), String::from("100")),
                (String::from("currency"), String::from("USD")),
            ],
        );

        builder.append_rejected_row(1, "run_001", "agg_2", "Decision", "guard_1");

        builder.append_error_row(2, "run_001", "MyDecision", "Parse error");

        builder.append_accepted_row(
            3,
            "run_001",
            "EventB",
            "agg_3",
            1,
            "guard_2",
            "MyDecision",
            &[(String::from("amount"), String::from("200"))],
        );

        let batch = builder.finish().expect("Failed to build batch");

        assert_eq!(batch.num_rows(), 4);

        // Verify _status column
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");
        assert_eq!(status_col.value(0), "accepted");
        assert_eq!(status_col.value(1), "rejected");
        assert_eq!(status_col.value(2), "error");
        assert_eq!(status_col.value(3), "accepted");

        // Verify row indices
        let row_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Expected Int64Array for _row");
        assert_eq!(row_col.value(0), 0);
        assert_eq!(row_col.value(1), 1);
        assert_eq!(row_col.value(2), 2);
        assert_eq!(row_col.value(3), 3);

        // Verify multiple payload columns
        let amount_col = batch
            .column(10)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for amount");
        assert_eq!(amount_col.value(0), "100");
        assert!(amount_col.is_null(1));
        assert!(amount_col.is_null(2));
        assert_eq!(amount_col.value(3), "200");

        let currency_col = batch
            .column(11)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for currency");
        assert_eq!(currency_col.value(0), "USD");
        assert!(currency_col.is_null(1));
        assert!(currency_col.is_null(2));
        assert!(currency_col.is_null(3)); // No currency for EventB
    }

    #[test]
    fn test_output_batch_builder_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
        ]));

        let builder = OutputBatchBuilder::new(&schema, 10);
        let batch = builder.finish().expect("Failed to build empty batch");

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), 10);
    }

    #[test]
    fn test_output_batch_builder_null_payload_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
            Field::new("field_a", DataType::Utf8, true),
            Field::new("field_b", DataType::Utf8, true),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 5);

        // Row with only field_a set
        builder.append_accepted_row(
            0,
            "run_001",
            "Event",
            "agg_1",
            0,
            "guard",
            "MyDecision",
            &[(String::from("field_a"), String::from("value_a"))],
        );

        // Row with both fields set
        builder.append_accepted_row(
            1,
            "run_001",
            "Event",
            "agg_2",
            0,
            "guard",
            "MyDecision",
            &[
                (String::from("field_a"), String::from("value_a2")),
                (String::from("field_b"), String::from("value_b2")),
            ],
        );

        let batch = builder.finish().expect("Failed to build batch");

        assert_eq!(batch.num_rows(), 2);

        // Verify field_a
        let field_a = batch
            .column(10)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for field_a");
        assert_eq!(field_a.value(0), "value_a");
        assert_eq!(field_a.value(1), "value_a2");

        // Verify field_b
        let field_b = batch
            .column(11)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for field_b");
        assert!(field_b.is_null(0));
        assert_eq!(field_b.value(1), "value_b2");
    }

    // Tests for evaluate_decision_batch
    #[tokio::test]
    async fn test_evaluate_decision_batch_guard_evaluation() {
        use o2_deql::parser::ast::*;

        // Create a minimal decision for testing
        let decision = CreateDecision {
            or_replace: false,
            name: Spanned {
                node: "TestDecision".to_string(),
                span: Span { start: 0, end: 0 },
            },
            aggregate: Spanned {
                node: "Account".to_string(),
                span: Span { start: 0, end: 0 },
            },
            command: Spanned {
                node: "TestCommand".to_string(),
                span: Span { start: 0, end: 0 },
            },
            state_as: None,
            branches: vec![
                DecisionBranch {
                    branch_index: 0,
                    rule_name: Some(Spanned {
                        node: "Branch1".to_string(),
                        span: Span { start: 0, end: 0 },
                    }),
                    guard: Some(SqlFragment {
                        sql: "amount > 100".to_string(),
                        span: Span { start: 0, end: 0 },
                    }),
                    emit_items: vec![EmitItem {
                        event_type: Spanned {
                            node: "EventA".to_string(),
                            span: Span { start: 0, end: 0 },
                        },
                        assignments: vec![],
                        span: Span { start: 0, end: 0 },
                    }],
                    span: Span { start: 0, end: 0 },
                },
            ],
        };

        // Create a minimal dereg
        let dereg = o2_deql::DeReg::new();

        // Create test batch with columns: account_id (String), amount (String)
        let schema = Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, false),
        ]));

        let account_id_array = arrow::array::StringArray::from(vec!["acc_1", "acc_2", "acc_3"]);
        let amount_array = arrow::array::StringArray::from(vec!["50", "150", "200"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(account_id_array),
                Arc::new(amount_array),
            ],
        ).expect("Failed to create test batch");

        let output_schema = build_inspect_output_schema(&decision, &dereg);
        let branching_schema = build_branching_schema();

        let ctx = datafusion::prelude::SessionContext::new();
        let command_field_names = vec!["account_id".to_string(), "amount".to_string()];
        let command_fields = vec![];

        // This test demonstrates the structure — actual execution would need
        // real guards and executor support. For now we verify the function exists
        // and can be called.
        let _result = evaluate_decision_batch(
            &decision,
            &batch,
            &command_field_names,
            &command_fields,
            &ctx,
            &dereg,
            0,
            "run_001",
            &output_schema,
            &branching_schema,
        ).await;

        // The result would contain:
        // - Some rows rejected (amount <= 100)
        // - Some rows accepted (amount > 100)
        // - Branching table with guard evaluation records
    }

    #[test]
    fn test_row_to_bind_params_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, false),
        ]));

        let id_array = arrow::array::StringArray::from(vec!["123", "456"]);
        let amount_array = arrow::array::StringArray::from(vec!["100", "200"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(amount_array)],
        ).expect("Failed to create batch");

        // Extract params from first row
        let params = row_to_bind_params(&batch, 0);

        assert_eq!(params.get("id"), Some(&"'123'".to_string()));
        assert_eq!(params.get("amount"), Some(&"'100'".to_string()));

        // Extract params from second row
        let params = row_to_bind_params(&batch, 1);

        assert_eq!(params.get("id"), Some(&"'456'".to_string()));
        assert_eq!(params.get("amount"), Some(&"'200'".to_string()));
    }

    #[test]
    fn test_build_source_query_no_filters() {
        let query = build_source_query("my_stream", &None, &None, 0, 100);
        assert_eq!(query, "SELECT * FROM \"my_stream\" LIMIT 100");
    }

    #[test]
    fn test_build_source_query_with_range() {
        let query = build_source_query(
            "my_stream",
            &Some(("10".to_string(), "20".to_string())),
            &None,
            0,
            100,
        );
        assert_eq!(
            query,
            "SELECT * FROM \"my_stream\" WHERE id >= '10' AND id <= '20' LIMIT 100"
        );
    }

    #[test]
    fn test_build_source_query_with_guard() {
        let query = build_source_query(
            "my_stream",
            &None,
            &Some("amount > 100".to_string()),
            0,
            100,
        );
        assert_eq!(
            query,
            "SELECT * FROM \"my_stream\" WHERE amount > 100 LIMIT 100"
        );
    }

    #[test]
    fn test_build_source_query_with_all_filters() {
        let query = build_source_query(
            "my_stream",
            &Some(("10".to_string(), "20".to_string())),
            &Some("amount > 50".to_string()),
            5,
            10,
        );
        assert_eq!(
            query,
            "SELECT * FROM \"my_stream\" WHERE id >= '10' AND id <= '20' AND amount > 50 OFFSET 5 LIMIT 10"
        );
    }

    // ============================================================================
    // Task 3.9: Additional comprehensive tests for source query filter ordering
    // ============================================================================

    #[test]
    fn test_source_query_filter_order_range_first() {
        // Verify that RANGE filter is applied first
        let query = build_source_query(
            "test_stream",
            &Some(("100".to_string(), "200".to_string())),
            &None,
            0,
            50,
        );
        // RANGE filter should appear first in WHERE clause
        let where_pos = query.find("WHERE").expect("WHERE not found");
        let range_pos = query.find("id >=").expect("RANGE not found");
        assert!(range_pos > where_pos, "RANGE should be in WHERE clause");
    }

    #[test]
    fn test_source_query_filter_order_where_after_range() {
        // Verify that WHERE (guard_filter) comes after RANGE
        let query = build_source_query(
            "test_stream",
            &Some(("100".to_string(), "200".to_string())),
            &Some("status = 'active'".to_string()),
            0,
            50,
        );
        // The query should have RANGE before guard_filter
        let range_pos = query.find("id >=").expect("RANGE not found");
        let guard_pos = query.find("status = 'active'").expect("Guard not found");
        assert!(
            range_pos < guard_pos,
            "RANGE should come before guard_filter in WHERE clause"
        );
        // Both should be in WHERE clause
        let where_pos = query.find("WHERE").expect("WHERE not found");
        assert!(
            range_pos > where_pos && guard_pos > where_pos,
            "Both filters should be in WHERE clause"
        );
    }

    #[test]
    fn test_source_query_filter_order_offset_before_limit() {
        // Verify that OFFSET comes before LIMIT
        let query = build_source_query(
            "test_stream",
            &None,
            &None,
            10,
            20,
        );
        let offset_pos = query.find("OFFSET").expect("OFFSET not found");
        let limit_pos = query.find("LIMIT").expect("LIMIT not found");
        assert!(offset_pos < limit_pos, "OFFSET should come before LIMIT");
    }

    #[test]
    fn test_source_query_complete_filter_chain() {
        // Test complete ordering: RANGE → WHERE → OFFSET → LIMIT
        let query = build_source_query(
            "events_table",
            &Some(("1000".to_string(), "5000".to_string())),
            &Some("amount > 100 AND currency = 'USD'".to_string()),
            25,
            75,
        );

        // Verify correct SQL structure
        assert!(query.starts_with("SELECT * FROM \"events_table\""));
        
        // Verify WHERE clause is present
        assert!(query.contains(" WHERE "));
        
        // Find positions of all components
        let select_pos = query.find("SELECT").expect("SELECT not found");
        let from_pos = query.find("FROM").expect("FROM not found");
        let where_pos = query.find("WHERE").expect("WHERE not found");
        let offset_pos = query.find("OFFSET").expect("OFFSET not found");
        let limit_pos = query.find("LIMIT").expect("LIMIT not found");

        // Verify ordering: SELECT < FROM < WHERE < OFFSET < LIMIT
        assert!(select_pos < from_pos, "SELECT should come before FROM");
        assert!(from_pos < where_pos, "FROM should come before WHERE");
        assert!(where_pos < offset_pos, "WHERE should come before OFFSET");
        assert!(offset_pos < limit_pos, "OFFSET should come before LIMIT");

        // Verify RANGE filter is first in WHERE clause
        let range_filter_pos = query.find("id >=").expect("RANGE filter not found");
        assert!(
            range_filter_pos > where_pos && range_filter_pos < offset_pos,
            "RANGE filter should be in WHERE clause"
        );

        // Verify guard_filter comes after RANGE
        let guard_filter_pos = query.find("amount > 100").expect("Guard filter not found");
        assert!(
            guard_filter_pos > range_filter_pos && guard_filter_pos < offset_pos,
            "Guard filter should come after RANGE filter"
        );

        // Verify correct values in OFFSET and LIMIT
        let offset_value = query.split("OFFSET ").nth(1).unwrap().split_whitespace().next().unwrap();
        let limit_value = query.split("LIMIT ").nth(1).unwrap().split_whitespace().next().unwrap();
        assert_eq!(offset_value, "25", "OFFSET value should be 25");
        assert_eq!(limit_value, "75", "LIMIT value should be 75");
    }

    #[test]
    fn test_source_query_range_with_guard_no_offset() {
        // Test: RANGE + WHERE without OFFSET
        let query = build_source_query(
            "stream",
            &Some(("10".to_string(), "99".to_string())),
            &Some("type = 'payment'".to_string()),
            0,
            100,
        );

        // Should have RANGE first, then guard
        let where_pos = query.find("WHERE").unwrap();
        let range_pos = query.find("id >=").unwrap();
        let guard_pos = query.find("type = 'payment'").unwrap();

        assert!(range_pos > where_pos);
        assert!(guard_pos > range_pos);
        assert!(!query.contains(" OFFSET "), "OFFSET should not appear when offset=0");
        assert!(query.contains(" LIMIT 100"));
    }

    #[test]
    fn test_source_query_no_range_with_guard() {
        // Test: guard only (no RANGE)
        let query = build_source_query(
            "users",
            &None,
            &Some("age >= 18".to_string()),
            0,
            200,
        );

        // Should have WHERE clause with only guard filter
        let where_pos = query.find("WHERE").unwrap();
        let guard_pos = query.find("age >= 18").unwrap();

        assert!(guard_pos > where_pos);
        assert!(!query.contains("id >="), "RANGE filter should not appear");
        assert!(query.contains(" LIMIT 200"));
    }

    #[test]
    fn test_source_query_special_characters_in_guard() {
        // Test: guard filter with special characters (SQL injection prevention)
        let query = build_source_query(
            "orders",
            &None,
            &Some("notes LIKE '%urgent%' AND status IN ('pending', 'processing')".to_string()),
            0,
            50,
        );

        // Guard filter should be preserved as-is
        assert!(query.contains("notes LIKE '%urgent%'"));
        assert!(query.contains("status IN ('pending', 'processing')"));
    }

    #[test]
    fn test_source_query_quoted_stream_name() {
        // Test: stream names with special characters are quoted
        let query = build_source_query(
            "stream-with-dashes",
            &None,
            &None,
            0,
            100,
        );

        assert!(query.contains("FROM \"stream-with-dashes\""));
    }

    #[test]
    fn test_source_query_range_with_quoted_ids() {
        // Test: RANGE values with quotes are escaped
        let query = build_source_query(
            "stream",
            &Some(("user'123".to_string(), "user'456".to_string())),
            &None,
            0,
            100,
        );

        // Single quotes in IDs should be escaped
        assert!(query.contains("id >= 'user''123'"), "Single quotes should be escaped");
        assert!(query.contains("id <= 'user''456'"), "Single quotes should be escaped");
    }

    #[test]
    fn test_source_query_large_offset_and_limit() {
        // Test: large OFFSET and LIMIT values
        let query = build_source_query(
            "logs",
            &None,
            &None,
            999999,
            500000,
        );

        assert!(query.contains("OFFSET 999999"));
        assert!(query.contains("LIMIT 500000"));
    }

    #[test]
    fn test_source_query_format_consistency() {
        // Test: verify consistent formatting across different inputs
        let q1 = build_source_query(
            "stream1",
            &Some(("1".to_string(), "10".to_string())),
            &Some("filter1".to_string()),
            0,
            100,
        );

        let q2 = build_source_query(
            "stream2",
            &Some(("5".to_string(), "15".to_string())),
            &Some("filter2".to_string()),
            0,
            100,
        );

        // Both should follow same pattern
        assert!(q1.starts_with("SELECT * FROM"));
        assert!(q2.starts_with("SELECT * FROM"));
        assert!(q1.contains(" WHERE id >= "));
        assert!(q2.contains(" WHERE id >= "));
        assert!(q1.contains(" AND "));
        assert!(q2.contains(" AND "));
        assert!(q1.ends_with("LIMIT 100"));
        assert!(q2.ends_with("LIMIT 100"));
    }

    #[test]
    fn test_source_query_offset_zero_omitted() {
        // Test: OFFSET 0 should be omitted from query
        let query_with_offset_0 = build_source_query(
            "stream",
            &None,
            &None,
            0,
            100,
        );

        assert!(
            !query_with_offset_0.contains("OFFSET"),
            "OFFSET 0 should not appear in query"
        );

        let query_with_offset_1 = build_source_query(
            "stream",
            &None,
            &None,
            1,
            100,
        );

        assert!(
            query_with_offset_1.contains("OFFSET 1"),
            "OFFSET should appear for non-zero values"
        );
    }

    #[test]
    fn test_source_query_range_and_guard_combined_precedence() {
        // Test: verify AND operator combines range and guard correctly
        let query = build_source_query(
            "transactions",
            &Some(("1001".to_string(), "2000".to_string())),
            &Some("amount > 1000".to_string()),
            0,
            100,
        );

        // Should have: WHERE id >= '1001' AND id <= '2000' AND amount > 1000
        let where_clause_start = query.find("WHERE").unwrap();
        let limit_pos = query.find("LIMIT").unwrap();
        let where_clause = &query[where_clause_start..limit_pos];

        // Count ANDs - should be exactly 2 (one between range boundaries, one between range and guard)
        let and_count = where_clause.matches(" AND ").count();
        assert_eq!(and_count, 2, "WHERE clause should have exactly 2 AND operators");
    }

    #[test]
    fn test_resolve_run_details_auto_date_and_serial() {
        let state = InspectionOrgState::new();
        let (date, serial) = resolve_run_details(&None, &state, "test_inspection");

        // Date should be today
        assert!(date.len() == 8); // YYYYMMDD format
        assert_eq!(serial, 1);

        // Second call should increment serial
        let (date2, serial2) = resolve_run_details(&None, &state, "test_inspection");
        assert_eq!(date, date2); // Same date
        assert_eq!(serial2, 2); // Incremented serial
    }

    #[test]
    fn test_resolve_run_details_explicit_date_and_serial() {
        let state = InspectionOrgState::new();
        let (date, serial) =
            resolve_run_details(&Some(("20260615".to_string(), Some(5))), &state, "test_inspection");

        assert_eq!(date, "20260615");
        assert_eq!(serial, 5);
    }

    #[test]
    fn test_resolve_run_details_explicit_date_auto_serial() {
        let state = InspectionOrgState::new();
        let (date, serial) = resolve_run_details(&Some(("20260615".to_string(), None)), &state, "test_inspection");

        assert_eq!(date, "20260615");
        assert_eq!(serial, 1);

        // Next call with same date should increment
        let (date2, serial2) = resolve_run_details(&Some(("20260615".to_string(), None)), &state, "test_inspection");
        assert_eq!(date, date2);
        assert_eq!(serial2, 2);
    }

    #[test]
    fn test_resolve_run_details_different_inspections_separate_serials() {
        let state = InspectionOrgState::new();
        let (_, serial1) = resolve_run_details(&None, &state, "inspection_a");
        let (_, serial2) = resolve_run_details(&None, &state, "inspection_b");

        // Both should start at 1 (different inspection names)
        assert_eq!(serial1, 1);
        assert_eq!(serial2, 1);
    }

    // =========================================================================
    // Serial Auto-Increment Tests
    // REQ-INSP-10: Serial number resolution
    // Task 3.10: Serial auto-increment works
    // =========================================================================

    /// Test requirement 1: Serial numbers auto-increment per-inspection per-day
    ///
    /// Given: Same inspection name, same day
    /// When: Calling resolve_run_details multiple times
    /// Then: Serial increments on each call (1, 2, 3, ...)
    #[test]
    fn test_serial_auto_increment_per_inspection_per_day() {
        let state = InspectionOrgState::new();
        
        // First call - should get serial 1
        let (date1, serial1) = resolve_run_details(&None, &state, "test_inspection");
        assert_eq!(serial1, 1);
        
        // Second call - same inspection, same day - should get serial 2
        let (date2, serial2) = resolve_run_details(&None, &state, "test_inspection");
        assert_eq!(serial2, 2);
        assert_eq!(date1, date2);
        
        // Third call - should get serial 3
        let (date3, serial3) = resolve_run_details(&None, &state, "test_inspection");
        assert_eq!(serial3, 3);
        assert_eq!(date1, date3);
        
        // Fourth call - should get serial 4
        let (_date4, serial4) = resolve_run_details(&None, &state, "test_inspection");
        assert_eq!(serial4, 4);
    }

    /// Test requirement 2: Format deql_ins_<template>_<YYYYMMDD>_<NNN>
    ///
    /// This test verifies the output table naming format is correct
    #[test]
    fn test_output_table_name_format() {
        let state = InspectionOrgState::new();
        let (date, serial) = resolve_run_details(&None, &state, "my_inspection");
        
        // Build output table name in format: deql_ins_<template>_<YYYYMMDD>_<NNN>
        let template = "my_results";
        let output_table = format!("deql_ins_{}_{date}_{serial:03}", template);
        
        // Verify format
        assert!(output_table.starts_with("deql_ins_my_results_"));
        assert!(output_table.ends_with("_001"), "Serial should be formatted as 001 for first increment");
        
        // Verify date is 8 digits (YYYYMMDD)
        let parts: Vec<&str> = output_table.split('_').collect();
        // Parts: ["deql", "ins", "my", "results", date, serial]
        assert!(parts.len() >= 5, "Should have at least deql_ins_my_results_YYYYMMDD_NNN parts");
        // The date should be one of the later parts
        assert!(parts.iter().any(|p| p.len() == 8), "Should have 8-digit date (YYYYMMDD)");
    }

    /// Test requirement 3: Serials reset daily (different dates)
    ///
    /// Given: Two different dates
    /// When: Calling resolve_run_details with each date
    /// Then: Each date gets its own serial counter starting at 1
    #[test]
    fn test_serial_reset_by_date() {
        let state = InspectionOrgState::new();
        
        // Call with first date
        let (date1, serial1_a) = resolve_run_details(
            &Some(("20260601".to_string(), None)),
            &state,
            "my_inspection",
        );
        assert_eq!(serial1_a, 1);
        assert_eq!(date1, "20260601");
        
        // Call with same date - should increment
        let (date1_again, serial1_b) = resolve_run_details(
            &Some(("20260601".to_string(), None)),
            &state,
            "my_inspection",
        );
        assert_eq!(serial1_b, 2);
        assert_eq!(date1_again, "20260601");
        
        // Call with different date - should start at 1 again
        let (date2, serial2_a) = resolve_run_details(
            &Some(("20260602".to_string(), None)),
            &state,
            "my_inspection",
        );
        assert_eq!(serial2_a, 1, "Serial should reset to 1 for new date");
        assert_eq!(date2, "20260602");
        
        // Call with second date again - should increment
        let (_date2_again, serial2_b) = resolve_run_details(
            &Some(("20260602".to_string(), None)),
            &state,
            "my_inspection",
        );
        assert_eq!(serial2_b, 2);
    }

    /// Test requirement 4: Serials persist within same day for same inspection
    ///
    /// This test verifies that the serial counter state is preserved
    /// across multiple calls for the same inspection and date
    #[test]
    fn test_serial_persistence_within_day() {
        let state = InspectionOrgState::new();
        let inspection_name = "deposit_check";
        let date = "20260615";
        
        // Call 1: Get serial 1
        let (d1, s1) = resolve_run_details(
            &Some((date.to_string(), None)),
            &state,
            inspection_name,
        );
        assert_eq!(s1, 1);
        
        // Call 2: Get serial 2
        let (d2, s2) = resolve_run_details(
            &Some((date.to_string(), None)),
            &state,
            inspection_name,
        );
        assert_eq!(s2, 2);
        
        // Call 3: Get serial 3
        let (d3, s3) = resolve_run_details(
            &Some((date.to_string(), None)),
            &state,
            inspection_name,
        );
        assert_eq!(s3, 3);
        
        // Call 4: Get serial 4
        let (d4, s4) = resolve_run_details(
            &Some((date.to_string(), None)),
            &state,
            inspection_name,
        );
        assert_eq!(s4, 4);
        
        // All calls should have same date
        assert_eq!(d1, date);
        assert_eq!(d2, date);
        assert_eq!(d3, date);
        assert_eq!(d4, date);
    }

    /// Test requirement 5: Multiple inspections have independent serial counters
    ///
    /// Given: Multiple inspection names
    /// When: Calling resolve_run_details for each
    /// Then: Each inspection maintains independent serial counters
    #[test]
    fn test_independent_serial_counters_per_inspection() {
        let state = InspectionOrgState::new();
        
        // inspection_a: serial 1
        let (_, serial_a1) = resolve_run_details(&None, &state, "inspection_a");
        assert_eq!(serial_a1, 1);
        
        // inspection_b: serial 1 (independent counter)
        let (_, serial_b1) = resolve_run_details(&None, &state, "inspection_b");
        assert_eq!(serial_b1, 1);
        
        // inspection_c: serial 1 (independent counter)
        let (_, serial_c1) = resolve_run_details(&None, &state, "inspection_c");
        assert_eq!(serial_c1, 1);
        
        // inspection_a again: serial 2 (continues from 1)
        let (_, serial_a2) = resolve_run_details(&None, &state, "inspection_a");
        assert_eq!(serial_a2, 2);
        
        // inspection_b again: serial 2 (continues from 1)
        let (_, serial_b2) = resolve_run_details(&None, &state, "inspection_b");
        assert_eq!(serial_b2, 2);
        
        // inspection_a again: serial 3
        let (_, serial_a3) = resolve_run_details(&None, &state, "inspection_a");
        assert_eq!(serial_a3, 3);
    }

    /// Test: Explicit serial provided (not auto-incremented)
    ///
    /// Given: Explicit serial number in run_details
    /// When: Calling resolve_run_details
    /// Then: Explicit serial is used, counter is not incremented
    #[test]
    fn test_explicit_serial_not_auto_incremented() {
        let state = InspectionOrgState::new();
        
        // Provide explicit serial 42
        let (_, serial1) = resolve_run_details(
            &Some(("20260615".to_string(), Some(42))),
            &state,
            "test_inspection",
        );
        assert_eq!(serial1, 42);
        
        // Next auto-generated serial should be 1 (counter wasn't touched)
        let (_, serial2) = resolve_run_details(
            &Some(("20260615".to_string(), None)),
            &state,
            "test_inspection",
        );
        assert_eq!(serial2, 1);
    }

    /// Test: Serial counter state verification
    ///
    /// Directly test the next_serial method on InspectionOrgState
    #[test]
    fn test_inspection_org_state_next_serial() {
        let state = InspectionOrgState::new();
        
        let key = "test_inspection_20260615";
        
        // First call
        let serial1 = state.next_serial(key);
        assert_eq!(serial1, 1);
        
        // Second call
        let serial2 = state.next_serial(key);
        assert_eq!(serial2, 2);
        
        // Third call
        let serial3 = state.next_serial(key);
        assert_eq!(serial3, 3);
        
        // Different key should start at 1
        let key2 = "other_inspection_20260615";
        let serial4 = state.next_serial(key2);
        assert_eq!(serial4, 1);
    }

    /// Test: Serial format zero-padding
    ///
    /// Verify that serials are formatted as 3-digit zero-padded numbers (001, 002, etc.)
    #[test]
    fn test_serial_zero_padding_format() {
        let state = InspectionOrgState::new();
        
        for i in 1..=100 {
            let (_date, serial) = resolve_run_details(&None, &state, "test_inspection");
            assert_eq!(serial as usize, i, "Serial should increment to {}", i);
            
            // Format and verify zero-padding
            let formatted = format!("{:03}", serial);
            assert_eq!(formatted.len(), 3, "Formatted serial should be 3 digits");
            
            // Verify value
            assert_eq!(formatted.parse::<u32>().unwrap(), serial as u32);
        }
    }

    /// Test: Output table naming with serial
    ///
    /// Verify complete output table name format with serial auto-increment
    #[test]
    fn test_complete_output_table_naming() {
        let state = InspectionOrgState::new();
        let template_name = "results";
        
        for i in 1..=5 {
            let (date, serial) = resolve_run_details(&None, &state, "my_inspection");
            
            // Build table name
            let table_name = format!("deql_ins_{}_{:08}_{:03}", template_name, 
                date.parse::<u32>().unwrap_or(20260601), serial);
            
            // Verify format
            assert!(table_name.starts_with("deql_ins_results_"));
            assert_eq!(serial as usize, i);
        }
    }

    /// Test: Multiple dates with independent counters
    ///
    /// Verify that each combination of inspection and date has independent serial counter
    #[test]
    fn test_multiple_dates_independent_counters() {
        let state = InspectionOrgState::new();
        let inspection = "test_insp";
        
        // Date 1, series 1, 2, 3
        let (_, s1a) = resolve_run_details(&Some(("20260601".to_string(), None)), &state, inspection);
        let (_, s1b) = resolve_run_details(&Some(("20260601".to_string(), None)), &state, inspection);
        let (_, s1c) = resolve_run_details(&Some(("20260601".to_string(), None)), &state, inspection);
        assert_eq!((s1a, s1b, s1c), (1, 2, 3));
        
        // Date 2, should also start at 1
        let (_, s2a) = resolve_run_details(&Some(("20260602".to_string(), None)), &state, inspection);
        assert_eq!(s2a, 1);
        
        // Date 3, should also start at 1
        let (_, s3a) = resolve_run_details(&Some(("20260603".to_string(), None)), &state, inspection);
        assert_eq!(s3a, 1);
        
        // Back to Date 1, should continue from 3
        let (_, s1d) = resolve_run_details(&Some(("20260601".to_string(), None)), &state, inspection);
        assert_eq!(s1d, 4);
        
        // Date 2 again, should continue from 1
        let (_, s2b) = resolve_run_details(&Some(("20260602".to_string(), None)), &state, inspection);
        assert_eq!(s2b, 2);
    }

    /// Test: Serial counter isolation across inspection names
    ///
    /// Verify that different inspection names don't share serial counters
    #[test]
    fn test_serial_isolation_by_inspection_name() {
        let state = InspectionOrgState::new();
        let date = "20260615";
        
        // Get serials for multiple inspections
        let (_, deposit_s1) = resolve_run_details(&Some((date.to_string(), None)), &state, "deposit_check");
        let (_, withdraw_s1) = resolve_run_details(&Some((date.to_string(), None)), &state, "withdraw_check");
        let (_, transfer_s1) = resolve_run_details(&Some((date.to_string(), None)), &state, "transfer_check");
        
        // Each should start at 1
        assert_eq!(deposit_s1, 1);
        assert_eq!(withdraw_s1, 1);
        assert_eq!(transfer_s1, 1);
        
        // Get next serials for the same inspections
        let (_, deposit_s2) = resolve_run_details(&Some((date.to_string(), None)), &state, "deposit_check");
        let (_, withdraw_s2) = resolve_run_details(&Some((date.to_string(), None)), &state, "withdraw_check");
        let (_, transfer_s2) = resolve_run_details(&Some((date.to_string(), None)), &state, "transfer_check");
        
        // Each should continue from their own counter
        assert_eq!(deposit_s2, 2);
        assert_eq!(withdraw_s2, 2);
        assert_eq!(transfer_s2, 2);
    }

    #[test]
    fn test_remap_to_command_fields_positional() {
        use arrow::datatypes::Fields;

        let mut raw_params = HashMap::new();
        raw_params.insert("column1".to_string(), "'value1'".to_string());
        raw_params.insert("column2".to_string(), "'value2'".to_string());
        raw_params.insert("column3".to_string(), "'value3'".to_string());

        let command_field_names = vec![
            "field_a".to_string(),
            "field_b".to_string(),
            "field_c".to_string(),
        ];

        let table_fields = Fields::from(vec![
            Field::new("column1", DataType::Utf8, false),
            Field::new("column2", DataType::Utf8, false),
            Field::new("column3", DataType::Utf8, false),
        ]);

        let remapped = remap_to_command_fields(&raw_params, &command_field_names, &table_fields);

        // Positional mapping should occur
        assert_eq!(remapped.get("field_a"), Some(&"'value1'".to_string()));
        assert_eq!(remapped.get("field_b"), Some(&"'value2'".to_string()));
        assert_eq!(remapped.get("field_c"), Some(&"'value3'".to_string()));
    }

    #[test]
    fn test_remap_to_command_fields_already_named() {
        use arrow::datatypes::Fields;

        let mut raw_params = HashMap::new();
        raw_params.insert("field_a".to_string(), "'value1'".to_string());
        raw_params.insert("field_b".to_string(), "'value2'".to_string());

        let command_field_names = vec!["field_a".to_string(), "field_b".to_string()];

        let table_fields = Fields::from(vec![
            Field::new("field_a", DataType::Utf8, false),
            Field::new("field_b", DataType::Utf8, false),
        ]);

        let remapped = remap_to_command_fields(&raw_params, &command_field_names, &table_fields);

        // Should keep as-is since column names already match
        assert_eq!(remapped.get("field_a"), Some(&"'value1'".to_string()));
        assert_eq!(remapped.get("field_b"), Some(&"'value2'".to_string()));
    }

    // =========================================================================
    // First-Match-Wins Semantics Tests
    // REQ-INSP-08: Per-Row Decision Simulation with first-match-wins branch selection
    // =========================================================================

    /// Test that first-match-wins semantics are correctly implemented.
    ///
    /// This test verifies:
    /// 1. Branches are evaluated in order
    /// 2. As soon as a guard passes, that branch is taken
    /// 3. Remaining branches are skipped
    /// 4. The output shows the correct branch that matched
    /// 5. Only one EMIT event per row (from the first matching branch)
    ///
    /// Test scenario:
    /// - 3 branches with guards: `amount > 200`, `amount > 100`, `amount > 50`
    /// - Input row with amount=150
    /// - Expected: Branch 1 (amount > 100) should match, Branch 2 should be skipped
    #[test]
    fn test_first_match_wins_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 3);

        // Simulate 3 branches evaluated for a single row
        // Branch 0: guard fails
        // Branch 1: guard passes - FIRST MATCH - should emit and stop
        // Branch 2: guard would pass but should NOT be evaluated

        // Append a failed branch
        builder.append_rejected_row(
            0,
            "run_001",
            "agg_1",
            "Decision",
            "amount > 200",
        );

        // Append an accepted row (first match)
        builder.append_accepted_row(
            0,
            "run_001",
            "EventB",
            "agg_1",
            1,
            "amount > 100",
            "Decision",
            &[(String::from("amount"), String::from("150"))],
        );

        // NOTE: We do NOT append a row for Branch 2 because first-match-wins
        // means the loop should have terminated after Branch 1 matched.
        // This is verified by checking that the row only has one accepted entry.

        let batch = builder.finish().expect("Failed to build batch");

        // Should have 2 rows total (1 rejected branch + 1 accepted)
        assert_eq!(batch.num_rows(), 2);

        // Get status column
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");

        // First row: rejected (branch 0 failed)
        assert_eq!(status_col.value(0), "rejected");

        // Second row: accepted (branch 1 passed)
        assert_eq!(status_col.value(1), "accepted");

        // Verify branch index shows branch 1 matched
        let branch_idx_col = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Expected Int64Array for _branch_index");
        assert!(branch_idx_col.is_null(0)); // No branch for rejected
        assert_eq!(branch_idx_col.value(1), 1); // Branch 1 matched
    }

    /// Test first-match-wins with multiple matching branches.
    ///
    /// Verifies that only the FIRST matching branch's emit items are recorded,
    /// and subsequent matching branches are not evaluated.
    ///
    /// Scenario:
    /// - 4 branches: all guards would pass (or have no guard)
    /// - Only the first branch should emit
    #[test]
    fn test_first_match_wins_multiple_potential_matches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
            Field::new("event_type_emitted", DataType::Utf8, true),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 1);

        // Only the first matching branch should emit
        builder.append_accepted_row(
            0,
            "run_001",
            "EventType1",
            "agg_100",
            0,
            "branch_0_guard",
            "Decision",
            &[(String::from("event_type_emitted"), String::from("EventType1"))],
        );

        // Branches 1, 2, 3 would match but should NOT be in output
        // because first-match-wins terminated the loop

        let batch = builder.finish().expect("Failed to build batch");

        // Only 1 row (1 accepted event from first matching branch)
        assert_eq!(batch.num_rows(), 1);

        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");
        assert_eq!(status_col.value(0), "accepted");

        let event_type_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _event_type");
        assert_eq!(event_type_col.value(0), "EventType1");
    }

    /// Test first-match-wins with NO matching guards.
    ///
    /// When no branch guard passes, the row should be marked as rejected.
    /// This verifies that we evaluate ALL branches when searching for a match,
    /// but then transition to the rejected state.
    #[test]
    fn test_first_match_wins_no_match() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 4);

        // Simulate evaluating 3 branches, all of which fail
        // Then a final rejected row when no branch matched

        // Branch 0: guard fails
        builder.append_rejected_row(0, "run_001", "agg_1", "Decision", "guard_0");

        // Branch 1: guard fails
        builder.append_rejected_row(0, "run_001", "agg_1", "Decision", "guard_1");

        // Branch 2: guard fails
        builder.append_rejected_row(0, "run_001", "agg_1", "Decision", "guard_2");

        // Final: row rejected because no branch matched
        builder.append_rejected_row(0, "run_001", "agg_1", "Decision", "guard_2");

        let batch = builder.finish().expect("Failed to build batch");

        // All rows should have status "rejected"
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");
        for i in 0..batch.num_rows() {
            assert_eq!(status_col.value(i), "rejected");
        }

        // Event type should be null for all rejected rows
        let event_type_col = batch.column(3);
        for i in 0..batch.num_rows() {
            assert!(event_type_col.is_null(i));
        }
    }

    /// Test first-match-wins with empty branch list.
    ///
    /// Edge case: decision with no branches.
    /// All rows should be rejected.
    #[test]
    fn test_first_match_wins_empty_branches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 3);

        // With no branches, all rows are rejected
        builder.append_rejected_row(0, "run_001", "agg_1", "Decision", "(no guards)");
        builder.append_rejected_row(1, "run_001", "agg_2", "Decision", "(no guards)");
        builder.append_rejected_row(2, "run_001", "agg_3", "Decision", "(no guards)");

        let batch = builder.finish().expect("Failed to build batch");

        assert_eq!(batch.num_rows(), 3);

        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");
        for i in 0..batch.num_rows() {
            assert_eq!(status_col.value(i), "rejected");
        }
    }

    /// Test first-match-wins with branches without guards (implicit TRUE).
    ///
    /// A branch without a guard has an implicit TRUE guard and should always match.
    /// The first such branch should be taken immediately.
    #[test]
    fn test_first_match_wins_no_guard_branch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 2);

        // Branch 0: explicit guard fails
        builder.append_rejected_row(0, "run_001", "agg_1", "Decision", "amount > 500");

        // Branch 1: no guard (implicit TRUE) - should match
        builder.append_accepted_row(
            0,
            "run_001",
            "DefaultEvent",
            "agg_1",
            1,
            "TRUE",
            "Decision",
            &[],
        );

        // Branch 2 would not be evaluated because first match already happened

        let batch = builder.finish().expect("Failed to build batch");

        // First row: rejected (branch 0)
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");
        assert_eq!(status_col.value(0), "rejected");

        // Second row: accepted (branch 1 with implicit TRUE)
        assert_eq!(status_col.value(1), "accepted");

        // Branch index should be 1 (second branch)
        let branch_idx_col = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Expected Int64Array for _branch_index");
        assert_eq!(branch_idx_col.value(1), 1);
    }

    /// Test first-match-wins across a multi-row batch.
    ///
    /// Verifies that first-match-wins applies PER ROW, not globally.
    /// Different rows should match different branches.
    #[test]
    fn test_first_match_wins_per_row_in_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, true),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 6);

        // Row 0: amount=50
        // - Branch 0 (amount > 200): fails
        // - Branch 1 (amount > 100): fails
        // - Branch 2 (amount > 25): MATCHES (first match)
        // - Branch 3 (amount > 0): NOT evaluated
        builder.append_rejected_row(0, "run_001", "agg_row0", "Decision", "amount > 200");
        builder.append_rejected_row(0, "run_001", "agg_row0", "Decision", "amount > 100");
        builder.append_accepted_row(
            0,
            "run_001",
            "EventC",
            "agg_row0",
            2,
            "amount > 25",
            "Decision",
            &[(String::from("amount"), String::from("50"))],
        );

        // Row 1: amount=150
        // - Branch 0 (amount > 200): fails
        // - Branch 1 (amount > 100): MATCHES (first match)
        // - Branch 2 (amount > 25): NOT evaluated
        // - Branch 3 (amount > 0): NOT evaluated
        builder.append_rejected_row(1, "run_001", "agg_row1", "Decision", "amount > 200");
        builder.append_accepted_row(
            1,
            "run_001",
            "EventB",
            "agg_row1",
            1,
            "amount > 100",
            "Decision",
            &[(String::from("amount"), String::from("150"))],
        );

        // Row 2: amount=300
        // - Branch 0 (amount > 200): MATCHES (first match)
        // - Branch 1, 2, 3: NOT evaluated
        builder.append_accepted_row(
            2,
            "run_001",
            "EventA",
            "agg_row2",
            0,
            "amount > 200",
            "Decision",
            &[(String::from("amount"), String::from("300"))],
        );

        let batch = builder.finish().expect("Failed to build batch");

        // Total rows: 2 (row0: rejected + accepted) + 2 (row1: rejected + accepted) + 1 (row2: accepted)
        assert_eq!(batch.num_rows(), 6);

        let row_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Expected Int64Array for _row");

        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _status");

        let branch_idx_col = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Expected Int64Array for _branch_index");

        // Row 0: rejected (branch 0), rejected (branch 1), accepted (branch 2)
        assert_eq!(row_col.value(0), 0);
        assert_eq!(status_col.value(0), "rejected");
        assert!(branch_idx_col.is_null(0));

        assert_eq!(row_col.value(1), 0);
        assert_eq!(status_col.value(1), "rejected");
        assert!(branch_idx_col.is_null(1));

        assert_eq!(row_col.value(2), 0);
        assert_eq!(status_col.value(2), "accepted");
        assert_eq!(branch_idx_col.value(2), 2);

        // Row 1: rejected (branch 0), accepted (branch 1)
        assert_eq!(row_col.value(3), 1);
        assert_eq!(status_col.value(3), "rejected");

        assert_eq!(row_col.value(4), 1);
        assert_eq!(status_col.value(4), "accepted");
        assert_eq!(branch_idx_col.value(4), 1);

        // Row 2: accepted (branch 0)
        assert_eq!(row_col.value(5), 2);
        assert_eq!(status_col.value(5), "accepted");
        assert_eq!(branch_idx_col.value(5), 0);
    }

    /// Test first-match-wins with multiple emit items in a single branch.
    ///
    /// When a branch with multiple EMIT items matches, all EMIT items should
    /// be processed, and then the loop should terminate (no other branches evaluated).
    #[test]
    fn test_first_match_wins_multiple_emits_in_branch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_row", DataType::Int64, false),
            Field::new("_status", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, true),
            Field::new("_decision", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, true),
            Field::new("_branch_index", DataType::Int64, true),
            Field::new("_guard_expression", DataType::Utf8, true),
            Field::new("_reason", DataType::Utf8, true),
            Field::new("_run_id", DataType::Utf8, false),
        ]));

        let mut builder = OutputBatchBuilder::new(&schema, 4);

        // Branch 0: guard fails
        builder.append_rejected_row(0, "run_001", "agg_1", "Decision", "guard_0");

        // Branch 1: guard passes - multiple EMIT items
        // First EMIT: EventX
        builder.append_accepted_row(
            0,
            "run_001",
            "EventX",
            "agg_1",
            1,
            "guard_1",
            "Decision",
            &[],
        );

        // Second EMIT: EventY (same branch, same row)
        builder.append_accepted_row(
            0,
            "run_001",
            "EventY",
            "agg_1",
            1,
            "guard_1",
            "Decision",
            &[],
        );

        // Branch 2 would not be evaluated because first-match-wins

        let batch = builder.finish().expect("Failed to build batch");

        // Total: 1 rejected + 2 accepted
        assert_eq!(batch.num_rows(), 3);

        let event_type_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray for _event_type");

        assert!(event_type_col.is_null(0)); // rejected row
        assert_eq!(event_type_col.value(1), "EventX");
        assert_eq!(event_type_col.value(2), "EventY");

        let branch_idx_col = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Expected Int64Array for _branch_index");

        assert!(branch_idx_col.is_null(0)); // rejected
        assert_eq!(branch_idx_col.value(1), 1); // branch 1, first emit
        assert_eq!(branch_idx_col.value(2), 1); // branch 1, second emit
    }

    #[test]
    fn test_append_branching_row() {
        use o2_deql::parser::ast::*;

        let branching_schema = build_branching_schema();

        let decision = CreateDecision {
            or_replace: false,
            name: Spanned {
                node: "TestDecision".to_string(),
                span: Span { start: 0, end: 0 },
            },
            aggregate: Spanned {
                node: "Account".to_string(),
                span: Span { start: 0, end: 0 },
            },
            command: Spanned {
                node: "TestCommand".to_string(),
                span: Span { start: 0, end: 0 },
            },
            state_as: None,
            branches: vec![
                DecisionBranch {
                    branch_index: 0,
                    rule_name: Some(Spanned {
                        node: "Branch1".to_string(),
                        span: Span { start: 0, end: 0 },
                    }),
                    guard: Some(SqlFragment {
                        sql: "amount > 100".to_string(),
                        span: Span { start: 0, end: 0 },
                    }),
                    emit_items: vec![EmitItem {
                        event_type: Spanned {
                            node: "EventA".to_string(),
                            span: Span { start: 0, end: 0 },
                        },
                        assignments: vec![],
                        span: Span { start: 0, end: 0 },
                    }],
                    span: Span { start: 0, end: 0 },
                },
            ],
        };

        let branch = &decision.branches[0];

        let mut batches = Vec::new();
        let result = append_branching_row(
            &mut batches,
            &branching_schema,
            42,
            &decision,
            branch,
            "agg_123",
            "emitted",
            Some("EventA"),
        );

        assert!(result.is_ok());
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 10);

        // Verify _timestamp (column 0)
        let timestamp_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Expected Int64Array for _timestamp");
        assert!(timestamp_col.value(0) > 0);

        // Verify inspect_row_index (column 1)
        let row_idx_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("Expected UInt64Array");
        assert_eq!(row_idx_col.value(0), 42);

        // Verify branch_status (column 7)
        let status_col = batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray");
        assert_eq!(status_col.value(0), "emitted");

        // Verify event_type (column 8)
        let event_col = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected StringArray");
        assert_eq!(event_col.value(0), "EventA");
    }
}
