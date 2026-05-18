//! Meta JSON builder — concept-specific JSON payloads for `dereg_meta_store.meta`.
//!
//! Each concept type stores different metadata. This module provides a
//! builder that generates the correct JSON payload for each concept type
//! based on the parsed AST.
//!
//! [REQ-028] Design doc section 5.1 table.

use serde_json::{Value, json};

use crate::{
    error::ConceptKind,
    parser::{ast::*, error::Diagnostic},
};

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

/// Build error meta for a parser failure using the full diagnostic context.
pub fn build_parse_error_meta(source: &str, diagnostics: &[Diagnostic]) -> Value {
    let diagnostics_json: Vec<Value> = diagnostics
        .iter()
        .map(|diagnostic| {
            let (line, column) = byte_offset_to_line_col(source, diagnostic.span.start);
            json!({
                "severity": format!("{:?}", diagnostic.severity),
                "message": diagnostic.message.clone(),
                "line": line,
                "column": column,
                "snippet": get_source_line(source, diagnostic.span.start),
                "span": {
                    "start": diagnostic.span.start,
                    "end": diagnostic.span.end,
                },
                "display": diagnostic.display(source),
            })
        })
        .collect();

    let message = diagnostics
        .iter()
        .map(|diagnostic| diagnostic.display(source))
        .collect::<Vec<_>>()
        .join("\n");

    let (line, column, snippet) = diagnostics
        .first()
        .map(|diagnostic| {
            let (line, column) = byte_offset_to_line_col(source, diagnostic.span.start);
            (
                Some(line),
                Some(column),
                Some(get_source_line(source, diagnostic.span.start)),
            )
        })
        .unwrap_or((None, None, None));

    let root_message = if message.is_empty() {
        "parse error"
    } else {
        message.as_str()
    };

    json!({
        "code": "PARSE_ERROR",
        "message": root_message,
        "line": line,
        "column": column,
        "snippet": snippet,
        "diagnostics": diagnostics_json,
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

fn byte_offset_to_line_col(source: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(source.len());
    let mut line = 1;
    let mut line_start = 0;

    for (i, ch) in source.char_indices() {
        if i >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            line_start = i + 1;
        }
    }

    let column = offset - line_start + 1;
    (line, column)
}

fn get_source_line(source: &str, offset: usize) -> &str {
    let offset = offset.min(source.len());
    let line_start = source[..offset].rfind('\n').map_or(0, |idx| idx + 1);
    let line_end = source[offset..]
        .find('\n')
        .map_or(source.len(), |idx| offset + idx);

    &source[line_start..line_end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{error::Severity, token::Span};

    #[test]
    fn parse_error_meta_includes_diagnostic_details() {
        let source = "-- comment\nCRATE AGGREGATE Foo;";
        let diagnostics = vec![Diagnostic {
            span: Span { start: 11, end: 16 },
            message: "unexpected token 'CRATE'".to_string(),
            severity: Severity::Error,
        }];

        let meta = build_parse_error_meta(source, &diagnostics);

        assert_eq!(meta["code"], "PARSE_ERROR");
        assert_eq!(meta["line"], 2);
        assert_eq!(meta["column"], 1);
        assert_eq!(meta["snippet"], "CRATE AGGREGATE Foo;");
        assert_eq!(meta["diagnostics"].as_array().unwrap().len(), 1);
        assert_eq!(
            meta["diagnostics"][0]["display"],
            diagnostics[0].display(source)
        );
    }
}
