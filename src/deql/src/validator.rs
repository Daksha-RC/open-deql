//! Validator — cross-reference validation logic.
//!
//! Ported from `deql-cli/deql-dereg/src/validator.rs`.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::{
    error::{ConceptKind, DeRegError},
    parser::ast::{CreateCommand, CreateDecision, CreateProjection, DeqlType, FieldDef},
    registry::Registry,
};

/// Validate a command payload against its schema.
///
/// Checks that:
/// - No unknown fields are present in the payload (REQ-CMD-003)
/// - Each field type matches the command definition's expected type
///
/// Returns a list of validation errors (empty if valid).
pub fn validate_command_payload(
    command: &CreateCommand,
    payload: &Map<String, Value>,
) -> Vec<String> {
    let mut errors = Vec::new();

    // Build a map of field name -> FieldDef for quick lookup
    let command_fields: HashMap<String, &FieldDef> = command
        .fields
        .iter()
        .map(|f| (f.name.node.clone(), f))
        .collect();

    // Check for unknown fields
    for key in payload.keys() {
        if !command_fields.contains_key(key) {
            errors.push(format!("Unknown field: {}", key));
        }
    }

    // Validate type compatibility for each field in the payload
    for (key, value) in payload.iter() {
        if let Some(field_def) = command_fields.get(key) {
            if !is_type_compatible(value, &field_def.data_type.node) {
                errors.push(format!(
                    "Type mismatch for field '{}': expected {:?}, got {}",
                    key,
                    field_def.data_type.node,
                    value_type_name(value)
                ));
            }
        }
    }

    errors
}

/// Get the human-readable type name of a JSON value.
fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Null => "null",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Check if a serde_json::Value is compatible with a DeqlType.
fn is_type_compatible(value: &Value, deql_type: &DeqlType) -> bool {
    match (value, deql_type) {
        (Value::String(s), DeqlType::Uuid) => {
            // Validate UUID format: must be 36 chars and valid hex with dashes
            s.len() == 36 && is_valid_uuid(s)
        }
        (Value::String(_), DeqlType::String) => true,
        (Value::Number(n), DeqlType::Int) => {
            // Must be an integer (no fractional part)
            n.is_i64()
        }
        (Value::Number(n), DeqlType::Decimal { .. }) => {
            // Decimal can be any valid number (skip NaN check which doesn't exist for
            // serde_json::Number)
            n.is_f64() || n.is_i64() || n.is_u64()
        }
        (Value::String(s), DeqlType::Timestamp) => {
            // Validate RFC3339 timestamp format
            chrono::DateTime::parse_from_rfc3339(s).is_ok()
        }
        (Value::Bool(_), DeqlType::Boolean) => true,
        (Value::Null, _) => {
            // Null is generally not allowed for command fields (no optional fields in commands)
            false
        }
        _ => false,
    }
}

/// Check if a string is a valid UUID.
fn is_valid_uuid(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }

    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 5 {
        return false;
    }

    // Check standard UUID format: 8-4-4-4-12
    if parts[0].len() != 8
        || parts[1].len() != 4
        || parts[2].len() != 4
        || parts[3].len() != 4
        || parts[4].len() != 12
    {
        return false;
    }

    parts
        .iter()
        .all(|part| part.chars().all(|c| c.is_ascii_hexdigit()))
}

/// Validate a decision's cross-references against the registry.
pub fn validate_decision(decision: &CreateDecision, registry: &Registry) -> Result<(), DeRegError> {
    let mut missing = Vec::new();

    if !registry.contains_aggregate(&decision.aggregate.node) {
        missing.push((ConceptKind::Aggregate, decision.aggregate.node.clone()));
    }

    if !registry.contains_command(&decision.command.node) {
        missing.push((ConceptKind::Command, decision.command.node.clone()));
    }

    for emit_item in decision.all_emit_items() {
        if !registry.contains_event(&emit_item.event_type.node) {
            missing.push((ConceptKind::Event, emit_item.event_type.node.clone()));
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(DeRegError::MissingReferences {
            source_kind: ConceptKind::Decision,
            source_name: decision.name.node.clone(),
            missing,
        })
    }
}

/// Validate a projection's cross-references.
/// Extracts `DeReg.<Name>$Events` and `DeReg.<Name>$Agg` references from SQL
/// and checks that the referenced aggregates exist.
pub fn validate_projection(
    projection: &CreateProjection,
    registry: &Registry,
) -> Result<(), DeRegError> {
    let mut missing = Vec::new();

    let sql = &projection.body.sql;
    for ref_name in extract_dereg_refs(sql) {
        if !registry.contains_aggregate(&ref_name) {
            missing.push((ConceptKind::Aggregate, ref_name));
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(DeRegError::MissingReferences {
            source_kind: ConceptKind::Projection,
            source_name: projection.name.node.clone(),
            missing,
        })
    }
}

/// Re-validate every cross-reference in the registry.
pub fn validate_all(
    registry: &Registry,
    command_map: &HashMap<String, String>,
) -> Result<(), Vec<DeRegError>> {
    let mut errors = Vec::new();

    for decision in registry.decisions.values() {
        if let Err(e) = validate_decision(decision, registry) {
            errors.push(e);
        }
    }

    for projection in registry.projections.values() {
        if let Err(e) = validate_projection(projection, registry) {
            errors.push(e);
        }
    }

    // Check for duplicate command bindings
    let mut seen_commands: HashMap<&str, &str> = HashMap::new();
    for decision in registry.decisions.values() {
        let cmd = decision.command.node.as_str();
        let dec = decision.name.node.as_str();
        if let Some(&existing) = seen_commands.get(cmd) {
            if existing != dec {
                errors.push(DeRegError::DuplicateCommandBinding {
                    command_name: cmd.to_string(),
                    existing_decision: existing.to_string(),
                    new_decision: dec.to_string(),
                });
            }
        } else {
            seen_commands.insert(cmd, dec);
        }
    }

    let _ = command_map; // reserved for future checks

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Extract aggregate names from `DeReg.<Name>$Events` and `DeReg.<Name>$Agg`
/// references in SQL text.
fn extract_dereg_refs(sql: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let lower = sql.to_lowercase();
    let mut search_from = 0;

    while let Some(pos) = lower[search_from..].find("dereg.") {
        let abs_pos = search_from + pos;
        let after_dot = abs_pos + "dereg.".len();

        let name_start = after_dot;
        let mut name_end = name_start;
        for ch in sql[name_start..].chars() {
            if ch.is_alphanumeric() || ch == '_' {
                name_end += ch.len_utf8();
            } else {
                break;
            }
        }

        if name_end > name_start {
            let name = sql[name_start..name_end].to_string();
            let remainder = &lower[name_end..];
            if remainder.starts_with("$events") || remainder.starts_with("$agg") {
                refs.push(name);
            }
        }

        search_from = if name_end > abs_pos {
            name_end
        } else {
            abs_pos + 1
        };
    }

    refs
}
