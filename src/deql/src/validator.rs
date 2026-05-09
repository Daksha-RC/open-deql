//! Validator — cross-reference validation logic.
//!
//! Ported from `deql-cli/deql-dereg/src/validator.rs`.

use std::collections::HashMap;

use crate::error::{ConceptKind, DeRegError};
use crate::parser::ast::{CreateDecision, CreateProjection};
use crate::registry::Registry;

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
