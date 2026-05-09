//! Meta JSON builder — concept-specific JSON payloads for `dereg_meta_store.meta`.
//!
//! Each concept type stores different metadata. This module provides a
//! builder that generates the correct JSON payload for each concept type
//! based on the parsed AST.
//!
//! [REQ-028] Design doc section 5.1 table.

use serde_json::{json, Value};

use crate::error::ConceptKind;
use crate::parser::ast::*;

/// Build the `meta` JSON column value for a successful CREATE statement.
pub fn build_meta(stmt: &DeqlStatement) -> Value {
    match stmt {
        DeqlStatement::CreateAggregate(a) => build_aggregate_meta(a),
        DeqlStatement::CreateCommand(c) => build_command_meta(c),
        DeqlStatement::CreateEvent(e) => build_event_meta(e),
        DeqlStatement::CreateDecision(d) => build_decision_meta(d),
        DeqlStatement::CreateProjection(p) => build_projection_meta(p),
        DeqlStatement::CreateEventStore(es) => build_eventstore_meta(es),
        DeqlStatement::CreateTemplate(t) => build_template_meta(t),
        _ => json!({}),
    }
}

/// Build tombstone meta for a DROP event.
pub fn build_tombstone_meta(concept_kind: ConceptKind, name: &str) -> Value {
    json!({
        "tombstone": true,
        "concept_kind": format!("{:?}", concept_kind),
        "name": name,
    })
}

/// Build error meta for a failed registration.
pub fn build_error_meta(error: &str) -> Value {
    json!({
        "error": true,
        "message": error,
    })
}

fn build_aggregate_meta(a: &CreateAggregate) -> Value {
    let fields: Vec<Value> = a
        .fields
        .as_ref()
        .map(|fs| fs.iter().map(field_to_json).collect())
        .unwrap_or_default();

    json!({
        "fields": fields,
        "or_replace": a.or_replace,
    })
}

fn build_command_meta(c: &CreateCommand) -> Value {
    let fields: Vec<Value> = c.fields.iter().map(field_to_json).collect();
    json!({
        "fields": fields,
        "or_replace": c.or_replace,
    })
}

fn build_event_meta(e: &CreateEvent) -> Value {
    let fields: Vec<Value> = e.fields.iter().map(field_to_json).collect();
    json!({
        "fields": fields,
        "or_replace": e.or_replace,
    })
}

fn build_decision_meta(d: &CreateDecision) -> Value {
    let emits: Vec<String> = d
        .all_emit_items()
        .map(|e| e.event_type.node.clone())
        .collect();

    json!({
        "aggregate": d.aggregate.node,
        "command": d.command.node,
        "emits": emits,
        "has_guard": d.has_guards(),
        "guard_sql": d.single_guard().map(|g| &g.sql),
        "state_sql": d.state_as.as_ref().map(|s| &s.sql),
        "or_replace": d.or_replace,
    })
}

fn build_projection_meta(p: &CreateProjection) -> Value {
    json!({
        "body_sql": p.body.sql,
        "or_replace": p.or_replace,
    })
}

fn build_eventstore_meta(es: &CreateEventStore) -> Value {
    let config: Vec<Value> = es
        .config
        .iter()
        .map(|cp| {
            json!({
                "key": cp.key.node,
                "value": config_value_to_json(&cp.value.node),
            })
        })
        .collect();

    json!({
        "config": config,
    })
}

fn build_template_meta(t: &CreateTemplate) -> Value {
    let params: Vec<Value> = t
        .params
        .iter()
        .map(|p| {
            json!({
                "name": p.name.node,
                "data_type": p.data_type.as_ref().map(|dt| format!("{:?}", dt.node)),
            })
        })
        .collect();

    json!({
        "parameters": params,
        "or_replace": t.or_replace,
    })
}

fn field_to_json(f: &FieldDef) -> Value {
    json!({
        "name": f.name.node,
        "data_type": format!("{:?}", f.data_type.node),
        "is_key": f.is_key,
    })
}

fn config_value_to_json(cv: &ConfigValue) -> Value {
    match cv {
        ConfigValue::StringLit(s) => json!(s),
        ConfigValue::IntLit(i) => json!(i),
        ConfigValue::DecimalLit(d) => json!(d),
        ConfigValue::BoolLit(b) => json!(b),
        ConfigValue::List(l) => json!(l),
    }
}
