//! Decision Executor — processes EXECUTE statements through DeQL decisions.
//!
//! Ported and adapted from `deql-cli/deql-dereg/src/executor.rs` for OpenObserve fork.
//! Key OO-specific changes:
//! - Takes `Arc<RwLock<DeReg>>` to manage own lock acquisitions (REQ-CMD-010)
//! - Explicitly handles SENSITIVE annotation (real in ingest, null in response)
//! - Uses `now_micros()` for timestamp generation (OO-standard)
//! - Builds DataFusion execution context internally

use std::{collections::HashMap, sync::Arc};

use datafusion::{error::DataFusionError, prelude::SessionContext, scalar::ScalarValue};
use regex::Regex;
use tokio::sync::RwLock;

use crate::{
    dereg::DeReg,
    event_table_provider::DeqlSearchBackend,
    parser::ast::{CreateDecision, EmitItem, Execute, FieldAnnotation},
    schema_provider::DeQlSchemaProvider,
};

// ============================================================================
// Result and Error Types
// ============================================================================

/// Successful execution — events were emitted.
#[derive(Debug, Clone)]
pub struct ExecutionSuccess {
    /// Emitted events; includes annotation metadata for handler to apply
    pub events: Vec<EmittedEvent>,
}

/// A single emitted event with full metadata.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EmittedEvent {
    /// Event type name (e.g., "AccountCreated")
    pub event_type: String,
    /// Aggregate/stream ID
    pub stream_id: String,
    /// All field values (real values before any redaction)
    pub fields: Vec<(String, serde_json::Value)>,
    /// Field names marked with VOLATILE annotation
    /// (null in ingest, real in response)
    pub volatile_fields: Vec<String>,
    /// Field names marked with SENSITIVE annotation
    /// (real in ingest, null in response)
    pub sensitive_fields: Vec<String>,
}

/// Business-level rejection — a guard evaluated to false.
#[derive(Debug, Clone)]
pub struct ExecutionRejection {
    /// Decision name that was executed
    pub decision_name: String,
    /// Guard expression (all guards joined with " | ")
    pub guard_expression: String,
    /// State variable values at time of rejection
    pub state_values: HashMap<String, serde_json::Value>,
    /// Command parameter values at time of rejection
    pub command_values: HashMap<String, serde_json::Value>,
}

/// Infrastructure-level error.
#[derive(Debug, Clone)]
pub enum ExecutionError {
    /// No decision registered for the command
    NoDecision { command_name: String },
    /// Decision definition not found
    DecisionNotFound { decision_name: String },
    /// Unresolved `:field` reference in SQL
    UnresolvedBindParam { param_name: String, context: String },
    /// Could not determine aggregate_id
    NoAggregateId { decision_name: String },
    /// STATE AS query execution failed
    StateQueryFailed {
        decision_name: String,
        source: String,
    },
    /// EMIT AS expression evaluation failed
    EmitEvalFailed { event_type: String, source: String },
    /// Guard evaluation failed
    GuardEvalFailed {
        decision_name: String,
        source: String,
    },
    /// Type mismatch
    TypeMismatch {
        context: String,
        field_name: String,
        expected_type: String,
        actual_value: String,
    },
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionError::NoDecision { command_name } => {
                write!(f, "No decision registered for command '{command_name}'")
            }
            ExecutionError::DecisionNotFound { decision_name } => {
                write!(f, "Decision '{decision_name}' not found")
            }
            ExecutionError::UnresolvedBindParam {
                param_name,
                context,
            } => {
                write!(f, "Unresolved bind parameter ':{param_name}' in {context}")
            }
            ExecutionError::NoAggregateId { decision_name } => {
                write!(f, "Cannot determine aggregate_id for '{decision_name}'")
            }
            ExecutionError::StateQueryFailed {
                decision_name,
                source,
            } => {
                write!(f, "STATE AS query failed for '{decision_name}': {source}")
            }
            ExecutionError::EmitEvalFailed { event_type, source } => {
                write!(
                    f,
                    "EMIT AS evaluation failed for event '{event_type}': {source}"
                )
            }
            ExecutionError::GuardEvalFailed {
                decision_name,
                source,
            } => {
                write!(f, "Guard evaluation failed for '{decision_name}': {source}")
            }
            ExecutionError::TypeMismatch {
                context,
                field_name,
                expected_type,
                actual_value,
            } => {
                write!(
                    f,
                    "Type mismatch in {context}: field '{field_name}' expected {expected_type}, got {actual_value}"
                )
            }
        }
    }
}

impl From<DataFusionError> for ExecutionError {
    fn from(err: DataFusionError) -> Self {
        ExecutionError::StateQueryFailed {
            decision_name: "unknown".to_string(),
            source: err.to_string(),
        }
    }
}

impl std::error::Error for ExecutionError {}

/// Execution outcome.
pub enum ExecutionResult {
    Success(ExecutionSuccess),
    Rejected(ExecutionRejection),
}

// ============================================================================
// DeQL → OpenObserve column name rewriting
// ============================================================================

/// Rewrite DeQL-lang canonical column names to the prefixed names used by the
/// OpenObserve virtual-table schema.
///
/// Handles two classes of mismatch between the deql-cli prototype and OO:
///
/// 1. **`data.` prefix** — the prototype stored payload in a nested `data` object, so STATE AS SQL
///    uses `data.grade`, `data.balance`, etc.  In OO the payload fields are stored flat, so
///    `data.grade` → `grade`.
///
/// 2. **Reserved column prefixes** — the prototype used bare `stream_id` and `event_type`; the OO
///    virtual table prefixes them with `deql_` to avoid collisions with user payload fields.
///
/// Word-boundary matching prevents false-positive rewrites inside longer
/// identifiers such as `my_stream_id` or `deql_event_type`.
fn rewrite_deql_column_names(sql: &str) -> String {
    // 1. Strip `data.` prefix from payload field references (e.g. data.grade → grade). Only matches
    //    bare `data.`, not `my_data.` or `some_data.`. The SQL is reconstructed from tokens joined
    //    with spaces, so `data.grade` appears as `data . grade` — allow optional whitespace around
    //    the dot.
    let re_data_prefix = Regex::new(r"\bdata\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)").unwrap();
    let sql = re_data_prefix.replace_all(sql, "$1");
    // 2. Rewrite reserved virtual-table column names.
    let re_stream = Regex::new(r"\bstream_id\b").unwrap();
    let re_event_type = Regex::new(r"\bevent_type\b").unwrap();
    let re_seq = Regex::new(r"\bseq\b").unwrap();
    let re_event_id = Regex::new(r"\bevent_id\b").unwrap();
    let sql = re_stream.replace_all(&sql, "_aggregate_id");
    let sql = re_event_type.replace_all(&sql, "_event_type");
    let sql = re_seq.replace_all(&sql, "_offset");
    let sql = re_event_id.replace_all(&sql, "_event_id");
    sql.into_owned()
}

// ============================================================================
// Bind Parameter Extraction & Substitution
// ============================================================================

/// Extract bind parameters from Execute assignments.
fn extract_bind_params(execute: &Execute) -> HashMap<String, String> {
    execute
        .assignments
        .iter()
        .map(|a| (a.field.node.clone(), a.value.node.clone()))
        .collect()
}

/// Substitute `:field` references in SQL with literal values.
pub fn substitute_bind_params(
    sql: &str,
    params: &HashMap<String, String>,
    context: &str,
) -> Result<String, ExecutionError> {
    let re = Regex::new(r":([a-zA-Z_][a-zA-Z0-9_]*)").unwrap();

    // Validate all params are resolvable
    for cap in re.captures_iter(sql) {
        let field_name = &cap[1];
        if !params.contains_key(field_name) {
            return Err(ExecutionError::UnresolvedBindParam {
                param_name: field_name.to_string(),
                context: context.to_string(),
            });
        }
    }

    // Replace all
    let result = re.replace_all(sql, |caps: &regex::Captures| params[&caps[1]].clone());

    Ok(result.into_owned())
}

// ============================================================================
// Aggregate ID Extraction
// ============================================================================

/// Extract aggregate_id field name from STATE AS SQL.
/// Matches `aggregate_id = :field` or `stream_id = :field`, taking the LAST match.
fn extract_aggregate_id_field(state_sql: &str) -> Option<String> {
    let re = Regex::new(r"(?i)(?:aggregate_id|stream_id)\s*=\s*:(\w+)").unwrap();
    let mut last_match = None;
    for cap in re.captures_iter(state_sql) {
        last_match = Some(cap[1].to_string());
    }
    last_match
}

/// Resolve aggregate ID from decision and bind params.
pub fn resolve_aggregate_id(
    decision: &CreateDecision,
    params: &HashMap<String, String>,
    command_fields: &crate::parser::ast::CreateCommand,
) -> Result<String, ExecutionError> {
    // Try STATE AS pattern first
    if let Some(ref state) = decision.state_as {
        if let Some(field_name) = extract_aggregate_id_field(&state.sql) {
            if let Some(value) = params.get(&field_name) {
                let clean = value.trim_matches('\'').to_string();
                if !clean.is_empty() {
                    return Ok(clean);
                }
            }
        }
    }

    // Fallback: first field ending with _id
    for field in &command_fields.fields {
        if field.name.node.ends_with("_id") {
            if let Some(value) = params.get(&field.name.node) {
                let clean = value.trim_matches('\'').to_string();
                if !clean.is_empty() {
                    return Ok(clean);
                }
            }
        }
    }

    // Fallback: first field
    if let Some(field) = command_fields.fields.first() {
        if let Some(value) = params.get(&field.name.node) {
            let clean = value.trim_matches('\'').to_string();
            if !clean.is_empty() {
                return Ok(clean);
            }
        }
    }

    Err(ExecutionError::NoAggregateId {
        decision_name: decision.name.node.clone(),
    })
}

// ============================================================================
// ScalarValue → JSON conversion
// ============================================================================

/// Convert a ScalarValue to a serde_json::Value.
fn scalar_to_json(value: &ScalarValue) -> serde_json::Value {
    match value {
        ScalarValue::Null => serde_json::json!(null),
        ScalarValue::Boolean(Some(b)) => serde_json::json!(b),
        ScalarValue::Int8(Some(i)) => serde_json::json!(i),
        ScalarValue::Int16(Some(i)) => serde_json::json!(i),
        ScalarValue::Int32(Some(i)) => serde_json::json!(i),
        ScalarValue::Int64(Some(i)) => serde_json::json!(i),
        ScalarValue::UInt8(Some(u)) => serde_json::json!(u),
        ScalarValue::UInt16(Some(u)) => serde_json::json!(u),
        ScalarValue::UInt32(Some(u)) => serde_json::json!(u),
        ScalarValue::UInt64(Some(u)) => serde_json::json!(u),
        ScalarValue::Float32(Some(f)) => serde_json::json!(f),
        ScalarValue::Float64(Some(f)) => serde_json::json!(f),
        ScalarValue::Utf8(Some(s)) => serde_json::json!(s),
        ScalarValue::LargeUtf8(Some(s)) => serde_json::json!(s),
        _ => serde_json::json!(null),
    }
}

/// Convert a ScalarValue to a SQL literal string.
pub fn scalar_to_sql_literal(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Utf8(Some(s)) => {
            let escaped = s.replace('\'', "''");
            format!("'{escaped}'")
        }
        ScalarValue::Int64(Some(i)) => i.to_string(),
        ScalarValue::Float64(Some(f)) => f.to_string(),
        ScalarValue::Boolean(Some(b)) => b.to_string(),
        ScalarValue::Null => "NULL".to_string(),
        other => format!("{other}"),
    }
}

// ============================================================================
// Guard Evaluation
// ============================================================================

/// Evaluate guard expression. Returns true if the branch should execute.
pub async fn evaluate_guard(
    guard_sql: &str,
    params: &HashMap<String, String>,
    state_row: &HashMap<String, ScalarValue>,
    ctx: &SessionContext,
    decision_name: &str,
) -> Result<bool, ExecutionError> {
    let substituted = substitute_bind_params(guard_sql, params, "WHERE guard")?;

    // Substitute state column values
    let mut final_sql = substituted;
    for (col_name, value) in state_row {
        final_sql = final_sql.replace(col_name, &scalar_to_sql_literal(value));
    }

    // If state_row is empty, replace remaining bare identifiers with NULL
    // (except SQL keywords) to enable `WHERE exists IS NULL` patterns.
    // Skip content inside single-quoted string literals to avoid mangling
    // bound parameter values like 'A3' → 'NULL'.
    if state_row.is_empty() {
        final_sql = replace_bare_identifiers_with_null(&final_sql);
    }

    // Execute: SELECT <expr> AS guard_result
    let query = format!("SELECT {final_sql} AS guard_result");
    tracing::debug!(
        decision = %decision_name,
        guard_sql = %guard_sql,
        final_query = %query,
        state_empty = state_row.is_empty(),
        "Guard evaluation"
    );

    let df = ctx
        .sql(&query)
        .await
        .map_err(|e| ExecutionError::GuardEvalFailed {
            decision_name: decision_name.to_string(),
            source: e.to_string(),
        })?;

    let batches = df
        .collect()
        .await
        .map_err(|e| ExecutionError::GuardEvalFailed {
            decision_name: decision_name.to_string(),
            source: e.to_string(),
        })?;

    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Ok(false);
    }

    let value = ScalarValue::try_from_array(batches[0].column(0), 0).map_err(|e| {
        ExecutionError::GuardEvalFailed {
            decision_name: decision_name.to_string(),
            source: e.to_string(),
        }
    })?;

    match value {
        ScalarValue::Boolean(Some(b)) => Ok(b),
        _ => Ok(false),
    }
}

/// Replace bare SQL identifiers with NULL, skipping single-quoted string
/// literals and SQL keywords. Used when state_row is empty to produce a
/// valid SQL expression (all unknown columns → NULL).
fn replace_bare_identifiers_with_null(sql: &str) -> String {
    // Tokenize respecting single-quoted strings: split into alternating
    // segments of "outside quotes" and "inside quotes (including delimiters)".
    let sql_keywords = [
        "IS", "NULL", "NOT", "AND", "OR", "TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END",
        "IN", "BETWEEN", "LIKE", "AS", "SELECT",
    ];
    let re = regex::Regex::new(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();

    let mut result = String::with_capacity(sql.len());
    let mut in_quote = false;
    let mut segment_start = 0;
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if !in_quote {
                // Process the unquoted segment before this quote
                let segment = &sql[segment_start..i];
                let replaced = re.replace_all(segment, |caps: &regex::Captures| {
                    let word = &caps[1];
                    if sql_keywords.iter().any(|&kw| kw.eq_ignore_ascii_case(word)) {
                        word.to_string()
                    } else {
                        "NULL".to_string()
                    }
                });
                result.push_str(&replaced);
                segment_start = i;
                in_quote = true;
                i += 1;
            } else {
                // Check for escaped quote ('')
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    // Skip both quotes — stay in_quote
                    i += 2;
                    continue;
                }
                // End of quoted string — include the closing quote
                let segment = &sql[segment_start..=i];
                result.push_str(segment);
                segment_start = i + 1;
                in_quote = false;
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    // Process remaining segment
    if segment_start < sql.len() {
        let segment = &sql[segment_start..];
        if in_quote {
            // Unterminated quote — pass through as-is
            result.push_str(segment);
        } else {
            let replaced = re.replace_all(segment, |caps: &regex::Captures| {
                let word = &caps[1];
                if sql_keywords.iter().any(|&kw| kw.eq_ignore_ascii_case(word)) {
                    word.to_string()
                } else {
                    "NULL".to_string()
                }
            });
            result.push_str(&replaced);
        }
    }

    result
}

// ============================================================================
// EMIT AS Expression Evaluation
// ============================================================================

/// Evaluate EMIT AS expressions for an emit item.
pub async fn evaluate_emit_expressions(
    emit_item: &EmitItem,
    params: &HashMap<String, String>,
    state_row: &HashMap<String, ScalarValue>,
    ctx: &SessionContext,
) -> Result<Vec<(String, ScalarValue)>, ExecutionError> {
    if emit_item.assignments.is_empty() {
        return Ok(Vec::new());
    }

    let select_parts: Vec<String> = emit_item
        .assignments
        .iter()
        .map(|a| {
            let mut expr = a.value.node.clone();
            for (name, value) in params {
                expr = expr.replace(&format!(":{name}"), value);
            }
            for (col_name, value) in state_row {
                expr = expr.replace(col_name, &scalar_to_sql_literal(value));
            }
            format!("{expr} AS \"{}\"", a.field.node)
        })
        .collect();

    let query = format!("SELECT {}", select_parts.join(", "));
    let df = ctx
        .sql(&query)
        .await
        .map_err(|e| ExecutionError::EmitEvalFailed {
            event_type: emit_item.event_type.node.clone(),
            source: e.to_string(),
        })?;

    let batches = df
        .collect()
        .await
        .map_err(|e| ExecutionError::EmitEvalFailed {
            event_type: emit_item.event_type.node.clone(),
            source: e.to_string(),
        })?;

    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Err(ExecutionError::EmitEvalFailed {
            event_type: emit_item.event_type.node.clone(),
            source: "expression evaluation returned no rows".to_string(),
        });
    }

    let batch = &batches[0];
    let schema = batch.schema();
    let mut fields = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        let value = ScalarValue::try_from_array(batch.column(i), 0).map_err(|e| {
            ExecutionError::EmitEvalFailed {
                event_type: emit_item.event_type.node.clone(),
                source: e.to_string(),
            }
        })?;
        fields.push((field.name().clone(), value));
    }

    Ok(fields)
}

// ============================================================================
// Main execute_command Function
// ============================================================================

/// Execute a command through its registered decision.
///
/// Takes ownership of the DeReg lock acquisition to satisfy REQ-CMD-010:
/// releases the lock before any async I/O operations.
///
/// `search_backend` — optional OO search backend injected by the main crate.
/// When present, STATE AS queries can resolve `$Events` and `$Agg` tables
/// (`IMPL-03`, `IMPL-04`). When `None`, those tables resolve to `Ok(None)` so
/// guard-free decisions still work.
pub async fn execute_command(
    execute: &Execute,
    dereg: Arc<RwLock<DeReg>>,
    org_id: &str,
    search_backend: Option<Arc<dyn DeqlSearchBackend>>,
) -> Result<ExecutionResult, ExecutionError> {
    let command_name = execute.command.node.clone();
    let command_name_lower = command_name.to_lowercase();

    // Acquire read lock only for registry lookups, then release before async work
    let (_decision_name, decision, command_def, event_defs) = {
        let dereg_read = dereg.read().await;

        // Find decision by searching for one where command field matches (case-insensitive)
        let decision_name = dereg_read
            .registry
            .decisions
            .values()
            .find(|d| d.command.node.to_lowercase() == command_name_lower)
            .map(|d| d.name.node.clone())
            .ok_or_else(|| ExecutionError::NoDecision {
                command_name: command_name.clone(),
            })?;

        let decision = dereg_read
            .get_decision(&decision_name)
            .ok_or_else(|| ExecutionError::DecisionNotFound {
                decision_name: decision_name.clone(),
            })?
            .clone();

        let command_def = dereg_read
            .get_command_ci(&command_name)
            .ok_or_else(|| ExecutionError::NoDecision {
                command_name: command_name.clone(),
            })?
            .clone();

        // Pre-fetch all event definitions with explicit type annotation
        let mut event_defs: HashMap<String, crate::parser::ast::CreateEvent> = HashMap::new();
        for branch in &decision.branches {
            for emit_item in &branch.emit_items {
                if !event_defs.contains_key(&emit_item.event_type.node) {
                    if let Some(event_def) = dereg_read.get_event_ci(&emit_item.event_type.node) {
                        event_defs.insert(emit_item.event_type.node.clone(), event_def.clone());
                    }
                }
            }
        }

        (decision_name, decision, command_def, event_defs)
    };

    // Build DataFusion context for query execution.
    // IMPL-03/04: inject search backend so STATE AS can resolve $Events/$Agg.
    let schema_provider = match search_backend {
        Some(backend) => {
            DeQlSchemaProvider::with_search_backend(dereg.clone(), org_id.to_string(), backend)
        }
        None => DeQlSchemaProvider::new(dereg.clone(), org_id.to_string()),
    };
    let ctx = schema_provider.build_state_context().await?;

    // Extract and validate bind params
    let params = extract_bind_params(execute);

    // Resolve aggregate ID
    let aggregate_id = resolve_aggregate_id(&decision, &params, &command_def)?;

    // Execute STATE AS if present
    let state_row = if let Some(ref state_sql) = decision.state_as {
        let substituted = substitute_bind_params(&state_sql.sql, &params, "STATE AS")?;
        let substituted = rewrite_deql_column_names(&substituted);
        tracing::debug!(
            decision = %decision.name.node,
            sql = %substituted,
            "STATE AS executing rewritten SQL"
        );
        execute_state_query(&substituted, &ctx, &decision.name.node).await?
    } else {
        HashMap::new()
    };

    tracing::debug!(
        decision = %decision.name.node,
        state_row_empty = state_row.is_empty(),
        state_keys = ?state_row.keys().collect::<Vec<_>>(),
        "STATE AS result"
    );

    // Evaluate branches
    let mut emitted_events = Vec::new();

    for branch in &decision.branches {
        // Check branch guard
        let guard_passes = if let Some(ref guard) = branch.guard {
            evaluate_guard(&guard.sql, &params, &state_row, &ctx, &decision.name.node).await?
        } else {
            true
        };

        if !guard_passes {
            continue;
        }

        // Evaluate EMIT AS expressions
        for emit_item in &branch.emit_items {
            let fields = evaluate_emit_expressions(emit_item, &params, &state_row, &ctx).await?;

            // Build annotation metadata
            let mut volatile_fields = Vec::new();
            let mut sensitive_fields = Vec::new();

            if let Some(event_def) = event_defs.get(&emit_item.event_type.node) {
                for field in &event_def.fields {
                    if let Some(FieldAnnotation::Volatile) = field.annotation {
                        volatile_fields.push(field.name.node.clone());
                    }
                    if let Some(FieldAnnotation::Sensitive) = field.annotation {
                        sensitive_fields.push(field.name.node.clone());
                    }
                }
            }

            // Convert fields to JSON
            let json_fields: Vec<(String, serde_json::Value)> = fields
                .iter()
                .map(|(name, value)| (name.clone(), scalar_to_json(value)))
                .collect();

            emitted_events.push(EmittedEvent {
                event_type: emit_item.event_type.node.clone(),
                stream_id: aggregate_id.clone(),
                fields: json_fields,
                volatile_fields,
                sensitive_fields,
            });
        }
    }

    // Build rejection if no events were emitted
    if emitted_events.is_empty() {
        let guard_expression = decision
            .branches
            .iter()
            .filter_map(|b| b.guard.as_ref())
            .map(|g| g.sql.clone())
            .collect::<Vec<_>>()
            .join(" | ");

        let state_values: HashMap<String, serde_json::Value> = state_row
            .iter()
            .map(|(k, v)| (k.clone(), scalar_to_json(v)))
            .collect();

        let command_values: HashMap<String, serde_json::Value> = params
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(v.trim_matches('\''))))
            .collect();

        return Ok(ExecutionResult::Rejected(ExecutionRejection {
            decision_name: decision.name.node.clone(),
            guard_expression,
            state_values,
            command_values,
        }));
    }

    Ok(ExecutionResult::Success(ExecutionSuccess {
        events: emitted_events,
    }))
}

/// Execute STATE AS query and return state row.
pub async fn execute_state_query(
    sql: &str,
    ctx: &SessionContext,
    decision_name: &str,
) -> Result<HashMap<String, ScalarValue>, ExecutionError> {
    let df = ctx
        .sql(sql)
        .await
        .map_err(|e| ExecutionError::StateQueryFailed {
            decision_name: decision_name.to_string(),
            source: e.to_string(),
        })?;

    let batches = df
        .collect()
        .await
        .map_err(|e| ExecutionError::StateQueryFailed {
            decision_name: decision_name.to_string(),
            source: e.to_string(),
        })?;

    let mut state_row = HashMap::new();

    if !batches.is_empty() && batches[0].num_rows() > 0 {
        let batch = &batches[0];
        let schema = batch.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            let col = batch.column(i);
            let value = ScalarValue::try_from_array(col, 0).map_err(|e| {
                ExecutionError::StateQueryFailed {
                    decision_name: decision_name.to_string(),
                    source: e.to_string(),
                }
            })?;
            state_row.insert(field.name().clone(), value);
        }
    }

    Ok(state_row)
}

#[cfg(test)]
mod tests {
    use super::rewrite_deql_column_names;

    #[test]
    fn test_rewrite_stream_id_to_aggregate_id() {
        let sql = "SELECT stream_id AS employee_id, LAST(grade) AS grade \
                   FROM dereg.\"Employee$Events\" WHERE stream_id = 'EMP-001' \
                   GROUP BY stream_id";
        let rewritten = rewrite_deql_column_names(sql);
        assert!(
            !rewritten.contains(" stream_id"),
            "bare stream_id should be rewritten"
        );
        assert!(
            rewritten.contains("_aggregate_id"),
            "_aggregate_id should appear after rewrite"
        );
    }

    #[test]
    fn test_rewrite_event_type() {
        let sql = "SELECT event_type, LAST(name) AS name FROM dereg.\"Employee$Events\"";
        let rewritten = rewrite_deql_column_names(sql);
        assert!(!rewritten.contains(" event_type,"));
        assert!(rewritten.contains("_event_type"));
    }

    #[test]
    fn test_rewrite_seq_to_offset() {
        let sql = "SELECT * FROM dereg.\"Employee$Events\" ORDER BY seq ASC";
        let rewritten = rewrite_deql_column_names(sql);
        assert!(!rewritten.contains(" seq "));
        assert!(rewritten.contains("_offset"));
    }

    #[test]
    fn test_rewrite_event_id() {
        let sql = "SELECT event_id FROM dereg.\"Employee$Events\" WHERE event_id = 'abc'";
        let rewritten = rewrite_deql_column_names(sql);
        assert!(!rewritten.contains(" event_id"));
        assert!(rewritten.contains("_event_id"));
    }

    #[test]
    fn test_rewrite_does_not_double_rewrite_already_prefixed() {
        // If somehow the SQL already has _aggregate_id it should not be altered.
        let sql = "SELECT _aggregate_id FROM dereg.\"Employee$Events\"";
        let rewritten = rewrite_deql_column_names(sql);
        assert_eq!(rewritten, sql, "already-prefixed name must not be altered");
    }

    #[test]
    fn test_rewrite_data_prefix_stripped() {
        // Compact form (direct string usage)
        let sql = "SELECT data.grade AS grade, LAST(data.name) AS name \
                   FROM dereg.\"Employee$Events\" WHERE deql_stream_id = 'EMP-001' \
                   GROUP BY deql_stream_id";
        let rewritten = rewrite_deql_column_names(sql);
        assert!(
            !rewritten.contains("data."),
            "data. prefix should be stripped; got: {rewritten}"
        );
        assert!(rewritten.contains("grade"), "field name must survive");
        assert!(rewritten.contains("name"), "field name must survive");
    }

    #[test]
    fn test_rewrite_data_prefix_spaced_form() {
        // Token-joined form: the parser reconstructs SQL as `data . grade`
        let sql = "SELECT data . grade AS grade , LAST ( data . name ) AS name \
                   FROM dereg . \"Employee$Events\" WHERE deql_stream_id = 'EMP-001' \
                   GROUP BY deql_stream_id";
        let rewritten = rewrite_deql_column_names(sql);
        assert!(
            !rewritten.contains("data . ") && !rewritten.contains("data."),
            "spaced data . prefix should be stripped; got: {rewritten}"
        );
    }

    #[test]
    fn test_rewrite_data_prefix_does_not_affect_other_qualified_names() {
        // `mydata.grade` or `some_data.x` must NOT be rewritten.
        let sql = "SELECT mydata.grade, some_data.x FROM t";
        let rewritten = rewrite_deql_column_names(sql);
        assert_eq!(rewritten, sql, "only bare `data.` must be stripped");
    }

    #[test]
    fn test_replace_bare_identifiers_preserves_string_literals() {
        use super::replace_bare_identifiers_with_null;

        // Simulates guard: `'A3' <> current_grade` when state_row is empty.
        // The 'A3' inside quotes must NOT be replaced.
        let sql = "'A3' <> current_grade";
        let result = replace_bare_identifiers_with_null(sql);
        assert_eq!(result, "'A3' <> NULL", "got: {result}");
    }

    #[test]
    fn test_replace_bare_identifiers_preserves_keywords() {
        use super::replace_bare_identifiers_with_null;

        let sql = "status IS NOT NULL AND active = TRUE";
        let result = replace_bare_identifiers_with_null(sql);
        assert!(result.contains("IS"), "IS keyword must survive");
        assert!(result.contains("NOT"), "NOT keyword must survive");
        assert!(result.contains("NULL"), "NULL keyword must survive");
        assert!(result.contains("AND"), "AND keyword must survive");
        assert!(result.contains("TRUE"), "TRUE keyword must survive");
        // `status` and `active` should become NULL
        assert!(
            !result.contains("status"),
            "bare identifier 'status' should be NULL"
        );
        assert!(
            !result.contains("active"),
            "bare identifier 'active' should be NULL"
        );
    }

    #[test]
    fn test_replace_bare_identifiers_escaped_quotes() {
        use super::replace_bare_identifiers_with_null;

        // SQL with escaped single quote: WHERE name = 'O''Brien'
        let sql = "'O''Brien' <> current_name";
        let result = replace_bare_identifiers_with_null(sql);
        assert!(
            result.contains("O''Brien"),
            "escaped quote content must be preserved; got: {result}"
        );
        assert!(result.contains("NULL"), "current_name should become NULL");
    }

    #[test]
    fn test_scalar_to_sql_literal_escaped_quotes() {
        use crate::executor::scalar_to_sql_literal;
        use datafusion::scalar::ScalarValue;

        // Test single quote escaping
        let value = ScalarValue::Utf8(Some("O'Brien".to_string()));
        let result = scalar_to_sql_literal(&value);
        assert_eq!(result, "'O''Brien'");

        // Test multiple quotes
        let value = ScalarValue::Utf8(Some("It's a 'test'".to_string()));
        let result = scalar_to_sql_literal(&value);
        assert_eq!(result, "'It''s a ''test'''");
    }

    #[test]
    fn test_scalar_to_sql_literal_utf8_multibyte() {
        use crate::executor::scalar_to_sql_literal;
        use datafusion::scalar::ScalarValue;

        // Test Chinese characters
        let value = ScalarValue::Utf8(Some("世界".to_string()));
        let result = scalar_to_sql_literal(&value);
        assert_eq!(result, "'世界'");

        // Test emoji
        let value = ScalarValue::Utf8(Some("Hello 🌍".to_string()));
        let result = scalar_to_sql_literal(&value);
        assert_eq!(result, "'Hello 🌍'");

        // Test mixed with escaped quote
        let value = ScalarValue::Utf8(Some("It's 世界 🌍".to_string()));
        let result = scalar_to_sql_literal(&value);
        assert_eq!(result, "'It''s 世界 🌍'");
    }

    #[test]
    fn test_scalar_to_sql_literal_other_types() {
        use crate::executor::scalar_to_sql_literal;
        use datafusion::scalar::ScalarValue;

        assert_eq!(scalar_to_sql_literal(&ScalarValue::Int64(Some(42))), "42");
        assert_eq!(scalar_to_sql_literal(&ScalarValue::Float64(Some(3.14))), "3.14");
        assert_eq!(scalar_to_sql_literal(&ScalarValue::Boolean(Some(true))), "true");
        assert_eq!(scalar_to_sql_literal(&ScalarValue::Null), "NULL");
    }
}
