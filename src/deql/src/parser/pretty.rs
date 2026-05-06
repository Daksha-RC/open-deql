// Pretty-printer: AST → DeQL source text.

use crate::parser::ast::*;

/// Format an entire parsed source back into DeQL text.
pub fn pretty_print(source: &ParsedSource) -> String {
    source
        .statements
        .iter()
        .map(|s| pretty_print_statement(&s.node))
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Format a single statement back into DeQL text.
pub fn pretty_print_statement(stmt: &DeqlStatement) -> String {
    match stmt {
        DeqlStatement::CreateAggregate(a) => format_create_aggregate(a),
        DeqlStatement::CreateCommand(c) => format_create_command(c),
        DeqlStatement::CreateEvent(e) => format_create_event(e),
        DeqlStatement::CreateDecision(d) => format_create_decision(d),
        DeqlStatement::CreateProjection(p) => format_create_projection(p),
        DeqlStatement::CreateEventStore(es) => format_create_eventstore(es),
        DeqlStatement::CreateTemplate(t) => format_create_template(t),
        DeqlStatement::Execute(ex) => format_execute(ex),
        DeqlStatement::InspectDecision(i) => format_inspect_decision(i),
        DeqlStatement::InspectProjection(i) => format_inspect_projection(i),
        DeqlStatement::Describe(d) => format_describe(d),
        DeqlStatement::ApplyTemplate(u) => format_apply_template(u),
        DeqlStatement::ExportDeReg(e) => match &e.path {
            Some(p) => format!("EXPORT DEREG TO '{}';", p.node),
            None => "EXPORT DEREG;".to_string(),
        },
        DeqlStatement::ExportMetadata(e) => match &e.path {
            Some(p) => format!("EXPORT METADATA TO '{}';", p.node),
            None => "EXPORT METADATA;".to_string(),
        },
        DeqlStatement::ValidateDeReg => "VALIDATE DEREG;".to_string(),
    }
}

fn or_replace_str(or_replace: bool) -> &'static str {
    if or_replace {
        "OR REPLACE "
    } else {
        ""
    }
}

fn format_type(dt: &DeqlType) -> String {
    match dt {
        DeqlType::Uuid => "UUID".to_string(),
        DeqlType::String => "STRING".to_string(),
        DeqlType::Int => "INT".to_string(),
        DeqlType::Decimal { precision, scale } => format!("DECIMAL({},{})", precision, scale),
        DeqlType::Timestamp => "TIMESTAMP".to_string(),
        DeqlType::Boolean => "BOOLEAN".to_string(),
    }
}

fn format_fields(fields: &[FieldDef]) -> String {
    let lines: Vec<String> = fields
        .iter()
        .map(|f| {
            let key_str = if f.is_key { " KEY" } else { "" };
            format!(
                "    {} {}{}",
                f.name.node,
                format_type(&f.data_type.node),
                key_str
            )
        })
        .collect();
    format!("(\n{}\n)", lines.join(",\n"))
}

fn format_config_value(val: &ConfigValue) -> String {
    match val {
        ConfigValue::StringLit(s) => format!("'{}'", s),
        ConfigValue::IntLit(i) => i.to_string(),
        ConfigValue::DecimalLit(d) => {
            let s = d.to_string();
            if s.contains('.') {
                s
            } else {
                format!("{}.0", s)
            }
        }
        ConfigValue::BoolLit(b) => b.to_string(),
        ConfigValue::List(items) => {
            let inner: Vec<String> = items.iter().map(|s| format!("'{}'", s)).collect();
            format!("({})", inner.join(","))
        }
    }
}

fn format_template_arg_value(val: &TemplateArgValue) -> String {
    match val {
        TemplateArgValue::StringLit(s) => format!("'{}'", s),
        TemplateArgValue::IntLit(i) => i.to_string(),
        TemplateArgValue::DecimalLit(d) => {
            let s = d.to_string();
            if s.contains('.') {
                s
            } else {
                format!("{}.0", s)
            }
        }
        TemplateArgValue::BoolLit(b) => b.to_string(),
        TemplateArgValue::FieldList(fields) => format_fields(fields),
    }
}

fn format_create_aggregate(a: &CreateAggregate) -> String {
    match &a.fields {
        Some(fields) if !fields.is_empty() => {
            format!(
                "CREATE {}AGGREGATE {} {};",
                or_replace_str(a.or_replace),
                a.name.node,
                format_fields(fields)
            )
        }
        _ => {
            format!(
                "CREATE {}AGGREGATE {};",
                or_replace_str(a.or_replace),
                a.name.node
            )
        }
    }
}

fn format_create_command(c: &CreateCommand) -> String {
    format!(
        "CREATE {}COMMAND {} {};",
        or_replace_str(c.or_replace),
        c.name.node,
        format_fields(&c.fields)
    )
}

fn format_create_event(e: &CreateEvent) -> String {
    format!(
        "CREATE {}EVENT {} {};",
        or_replace_str(e.or_replace),
        e.name.node,
        format_fields(&e.fields)
    )
}

fn format_create_decision(d: &CreateDecision) -> String {
    let mut out = format!(
        "CREATE {}DECISION {}\nFOR {}\nON COMMAND {}",
        or_replace_str(d.or_replace),
        d.name.node,
        d.aggregate.node,
        d.command.node
    );

    if let Some(ref state) = d.state_as {
        out.push_str("\nSTATE AS\n    ");
        out.push_str(state.sql.trim());
    }

    out.push_str("\nEMIT AS\n");

    for (bi, branch) in d.branches.iter().enumerate() {
        if bi > 0 {
            out.push_str("\n\n    UNION ALL\n\n");
        }

        if let Some(ref rule) = branch.rule_name {
            out.push_str("    BRANCH ");
            out.push_str(&rule.node);
            out.push('\n');
        }

        for (i, emit) in branch.emit_items.iter().enumerate() {
            if i > 0 {
                out.push_str(",\n");
            }
            out.push_str("    SELECT EVENT ");
            out.push_str(&emit.event_type.node);
            out.push_str(" (\n");
            for (j, assign) in emit.assignments.iter().enumerate() {
                out.push_str("        ");
                out.push_str(&assign.field.node);
                out.push_str(" := ");
                out.push_str(&assign.value.node);
                if j + 1 < emit.assignments.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str("    )");
        }

        if let Some(ref guard) = branch.guard {
            let trimmed = guard.sql.trim();
            if !trimmed.is_empty() {
                out.push_str("\n    WHERE ");
                out.push_str(trimmed);
            }
        }
    }

    out.push(';');
    out
}

fn format_create_projection(p: &CreateProjection) -> String {
    format!(
        "CREATE {}PROJECTION {} AS\n{};",
        or_replace_str(p.or_replace),
        p.name.node,
        p.body.sql.trim()
    )
}

fn format_create_eventstore(es: &CreateEventStore) -> String {
    let mut out = format!(
        "CREATE {}EVENTSTORE {}\nWITH (\n",
        or_replace_str(es.or_replace),
        es.name.node
    );

    for (i, pair) in es.config.iter().enumerate() {
        out.push_str("    ");
        out.push_str(&pair.key.node);
        out.push_str(" = ");
        out.push_str(&format_config_value(&pair.value.node));
        if i + 1 < es.config.len() {
            out.push(',');
        }
        out.push('\n');
    }

    out.push_str(");");
    out
}

fn format_create_template(t: &CreateTemplate) -> String {
    let mut out = format!(
        "CREATE {}TEMPLATE {}",
        or_replace_str(t.or_replace),
        t.name.node
    );

    // Determine if typed params (WITH variant) or untyped
    let has_typed = t.params.iter().any(|p| p.data_type.is_some());
    if has_typed {
        out.push_str(" WITH (");
        let params: Vec<String> = t
            .params
            .iter()
            .map(|p| {
                if let Some(ref dt) = p.data_type {
                    format!("{} {}", p.name.node, format_type(&dt.node))
                } else {
                    p.name.node.clone()
                }
            })
            .collect();
        out.push_str(&params.join(", "));
        out.push(')');
    } else {
        out.push_str(" (");
        let params: Vec<String> = t.params.iter().map(|p| p.name.node.clone()).collect();
        out.push_str(&params.join(", "));
        out.push(')');
    }

    out.push_str(" AS (\n");

    if let Some(ref raw) = t.raw_body {
        // Use raw body text, indented
        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                out.push('\n');
            } else {
                out.push_str("    ");
                out.push_str(trimmed);
                out.push('\n');
            }
        }
    } else {
        // Format from parsed body statements
        for s in &t.body {
            let formatted = pretty_print_statement(&s.node);
            for line in formatted.lines() {
                out.push_str("    ");
                out.push_str(line);
                out.push('\n');
            }
            out.push('\n');
        }
    }

    out.push_str(");");
    out
}

fn format_execute(ex: &Execute) -> String {
    if ex.assignments.is_empty() {
        return format!("EXECUTE {}();", ex.command.node);
    }
    let args: Vec<String> = ex
        .assignments
        .iter()
        .map(|a| format!("{} := {}", a.field.node, a.value.node))
        .collect();
    format!("EXECUTE {}({});", ex.command.node, args.join(", "))
}

fn format_inspect_decision(i: &InspectDecision) -> String {
    let mut out = format!(
        "INSPECT DECISION {} FROM {} INTO {}",
        i.name.node, i.from.node, i.into.node
    );
    if let Some(ref offset) = i.offset {
        out.push_str(&format!(" OFFSET {}", offset.node));
    }
    if let Some(ref guard) = i.guard {
        let trimmed = guard.sql.trim();
        if !trimmed.is_empty() {
            out.push_str(&format!(" WHERE {}", trimmed));
        }
    }
    if let Some(ref limit) = i.limit {
        out.push_str(&format!(" LIMIT {}", limit.node));
    }
    out.push(';');
    out
}

fn format_inspect_projection(i: &InspectProjection) -> String {
    let mut out = format!(
        "INSPECT PROJECTION {} FROM {} INTO {}",
        i.name.node, i.from.node, i.into.node
    );
    if let Some(ref offset) = i.offset {
        out.push_str(&format!(" OFFSET {}", offset.node));
    }
    if let Some(ref guard) = i.guard {
        let trimmed = guard.sql.trim();
        if !trimmed.is_empty() {
            out.push_str(&format!(" WHERE {}", trimmed));
        }
    }
    if let Some(ref limit) = i.limit {
        out.push_str(&format!(" LIMIT {}", limit.node));
    }
    out.push(';');
    out
}

fn format_describe(d: &Describe) -> String {
    match &d.target {
        DescribeTarget::Single { concept, name } => {
            let concept_str = match concept.node {
                ConceptType::Aggregate => "AGGREGATE",
                ConceptType::Command => "COMMAND",
                ConceptType::Event => "EVENT",
                ConceptType::Decision => "DECISION",
                ConceptType::Projection => "PROJECTION",
                ConceptType::Inspection => "INSPECTION",
                ConceptType::EventStore => "EVENTSTORE",
                ConceptType::Template => "TEMPLATE",
            };
            format!("DESCRIBE {} {};", concept_str, name.node)
        }
        DescribeTarget::ListAll { concept } => {
            let concept_str = match concept.node {
                PluralConceptType::Aggregates => "AGGREGATES",
                PluralConceptType::Commands => "COMMANDS",
                PluralConceptType::Events => "EVENTS",
                PluralConceptType::Decisions => "DECISIONS",
                PluralConceptType::Projections => "PROJECTIONS",
                PluralConceptType::Inspections => "INSPECTIONS",
                PluralConceptType::EventStores => "EVENTSTORES",
                PluralConceptType::Templates => "TEMPLATES",
            };
            format!("DESCRIBE {};", concept_str)
        }
    }
}

fn format_apply_template(u: &ApplyTemplate) -> String {
    let mut out = format!("APPLY TEMPLATE {} WITH (\n", u.name.node);
    for (i, arg) in u.params.iter().enumerate() {
        out.push_str("    ");
        out.push_str(&arg.name.node);
        out.push_str(" = ");
        out.push_str(&format_template_arg_value(&arg.value.node));
        if i + 1 < u.params.len() {
            out.push(',');
        }
        out.push('\n');
    }
    out.push_str(");");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::token::Span;

    fn dummy_span() -> Span {
        Span { start: 0, end: 0 }
    }

    fn spanned<T>(node: T) -> Spanned<T> {
        Spanned {
            node,
            span: dummy_span(),
        }
    }

    #[test]
    fn test_create_aggregate_no_fields() {
        let stmt = DeqlStatement::CreateAggregate(CreateAggregate {
            or_replace: false,
            name: spanned("Inventory".to_string()),
            fields: None,
        });
        assert_eq!(pretty_print_statement(&stmt), "CREATE AGGREGATE Inventory;");
    }

    #[test]
    fn test_create_aggregate_with_fields() {
        let stmt = DeqlStatement::CreateAggregate(CreateAggregate {
            or_replace: true,
            name: spanned("Inventory".to_string()),
            fields: Some(vec![
                FieldDef {
                    name: spanned("sku".to_string()),
                    data_type: spanned(DeqlType::Uuid),
                    is_key: true,
                    annotation: None,
                },
                FieldDef {
                    name: spanned("name".to_string()),
                    data_type: spanned(DeqlType::String),
                    is_key: false,
                    annotation: None,
                },
            ]),
        });
        let result = pretty_print_statement(&stmt);
        assert!(result.starts_with("CREATE OR REPLACE AGGREGATE Inventory"));
        assert!(result.contains("sku UUID KEY"));
        assert!(result.contains("name STRING"));
    }

    #[test]
    fn test_create_command() {
        let stmt = DeqlStatement::CreateCommand(CreateCommand {
            or_replace: false,
            name: spanned("RegisterProduct".to_string()),
            fields: vec![
                FieldDef {
                    name: spanned("sku".to_string()),
                    data_type: spanned(DeqlType::Uuid),
                    is_key: false,
                    annotation: None,
                },
                FieldDef {
                    name: spanned("price".to_string()),
                    data_type: spanned(DeqlType::Decimal {
                        precision: 10,
                        scale: 2,
                    }),
                    is_key: false,
                    annotation: None,
                },
            ],
        });
        let result = pretty_print_statement(&stmt);
        assert!(result.starts_with("CREATE COMMAND RegisterProduct"));
        assert!(result.contains("sku UUID"));
        assert!(result.contains("price DECIMAL(10,2)"));
    }

    #[test]
    fn test_create_event() {
        let stmt = DeqlStatement::CreateEvent(CreateEvent {
            or_replace: false,
            name: spanned("ProductRegistered".to_string()),
            fields: vec![FieldDef {
                name: spanned("name".to_string()),
                data_type: spanned(DeqlType::String),
                is_key: false,
                annotation: None,
            }],
        });
        let result = pretty_print_statement(&stmt);
        assert!(result.starts_with("CREATE EVENT ProductRegistered"));
        assert!(result.contains("name STRING"));
    }

    #[test]
    fn test_create_decision() {
        let stmt = DeqlStatement::CreateDecision(CreateDecision {
            or_replace: false,
            name: spanned("RegisterNewProduct".to_string()),
            aggregate: spanned("Inventory".to_string()),
            command: spanned("RegisterProduct".to_string()),
            state_as: Some(SqlFragment {
                sql: "SELECT is_active FROM DeReg.Inventory$Agg WHERE aggregate_id = :sku"
                    .to_string(),
                span: dummy_span(),
            }),
            branches: vec![DecisionBranch {
                branch_index: 1,
                rule_name: None,
                guard: Some(SqlFragment {
                    sql: "is_active IS NULL OR is_active = FALSE".to_string(),
                    span: dummy_span(),
                }),
                emit_items: vec![EmitItem {
                    event_type: spanned("ProductRegistered".to_string()),
                    assignments: vec![
                        Assignment {
                            field: spanned("name".to_string()),
                            value: spanned(":name".to_string()),
                        },
                        Assignment {
                            field: spanned("category".to_string()),
                            value: spanned(":category".to_string()),
                        },
                    ],
                    span: dummy_span(),
                }],
                span: dummy_span(),
            }],
        });
        let result = pretty_print_statement(&stmt);
        assert!(result.contains("CREATE DECISION RegisterNewProduct"));
        assert!(result.contains("FOR Inventory"));
        assert!(result.contains("ON COMMAND RegisterProduct"));
        assert!(result.contains("STATE AS"));
        assert!(result.contains("EMIT AS"));
        assert!(result.contains("SELECT EVENT ProductRegistered"));
        assert!(result.contains("name := :name"));
        assert!(result.contains("WHERE is_active IS NULL OR is_active = FALSE"));
        assert!(result.ends_with(';'));
    }

    #[test]
    fn test_create_projection() {
        let stmt = DeqlStatement::CreateProjection(CreateProjection {
            or_replace: true,
            name: spanned("StockLevels".to_string()),
            body: SqlFragment {
                sql: "SELECT sku, name FROM events".to_string(),
                span: dummy_span(),
            },
        });
        let result = pretty_print_statement(&stmt);
        assert_eq!(
            result,
            "CREATE OR REPLACE PROJECTION StockLevels AS\nSELECT sku, name FROM events;"
        );
    }

    #[test]
    fn test_create_eventstore() {
        let stmt = DeqlStatement::CreateEventStore(CreateEventStore {
            or_replace: false,
            name: spanned("inventory_local".to_string()),
            config: vec![
                ConfigPair {
                    key: spanned("durable.type".to_string()),
                    value: spanned(ConfigValue::StringLit("parquet".to_string())),
                },
                ConfigPair {
                    key: spanned("partition.enforce".to_string()),
                    value: spanned(ConfigValue::BoolLit(true)),
                },
            ],
        });
        let result = pretty_print_statement(&stmt);
        assert!(result.contains("CREATE EVENTSTORE inventory_local"));
        assert!(result.contains("WITH ("));
        assert!(result.contains("durable.type = 'parquet',"));
        assert!(result.contains("partition.enforce = true"));
    }

    #[test]
    fn test_execute() {
        let stmt = DeqlStatement::Execute(Execute {
            command: spanned("RegisterProduct".to_string()),
            assignments: vec![
                Assignment {
                    field: spanned("sku".to_string()),
                    value: spanned("'BOLT-001'".to_string()),
                },
                Assignment {
                    field: spanned("name".to_string()),
                    value: spanned("'Bolt'".to_string()),
                },
            ],
        });
        let result = pretty_print_statement(&stmt);
        assert_eq!(
            result,
            "EXECUTE RegisterProduct(sku := 'BOLT-001', name := 'Bolt');"
        );
    }

    #[test]
    fn test_inspect_decision() {
        let stmt = DeqlStatement::InspectDecision(InspectDecision {
            name: spanned("RegisterNewProduct".to_string()),
            from: spanned("test_data".to_string()),
            into: spanned("results".to_string()),
            offset: None,
            guard: None,
            limit: None,
        });
        let result = pretty_print_statement(&stmt);
        assert_eq!(
            result,
            "INSPECT DECISION RegisterNewProduct FROM test_data INTO results;"
        );
    }

    #[test]
    fn test_inspect_projection_with_clauses() {
        let stmt = DeqlStatement::InspectProjection(InspectProjection {
            name: spanned("StockLevels".to_string()),
            from: spanned("events".to_string()),
            into: spanned("output".to_string()),
            offset: Some(spanned(5000)),
            guard: Some(SqlFragment {
                sql: "warehouse = 'WH-EAST'".to_string(),
                span: dummy_span(),
            }),
            limit: Some(spanned(1000)),
        });
        let result = pretty_print_statement(&stmt);
        assert_eq!(
            result,
            "INSPECT PROJECTION StockLevels FROM events INTO output OFFSET 5000 WHERE warehouse = 'WH-EAST' LIMIT 1000;"
        );
    }

    #[test]
    fn test_describe_single() {
        let stmt = DeqlStatement::Describe(Describe {
            target: DescribeTarget::Single {
                concept: spanned(ConceptType::Aggregate),
                name: spanned("Inventory".to_string()),
            },
        });
        assert_eq!(
            pretty_print_statement(&stmt),
            "DESCRIBE AGGREGATE Inventory;"
        );
    }

    #[test]
    fn test_describe_list_all() {
        let stmt = DeqlStatement::Describe(Describe {
            target: DescribeTarget::ListAll {
                concept: spanned(PluralConceptType::Events),
            },
        });
        assert_eq!(pretty_print_statement(&stmt), "DESCRIBE EVENTS;");
    }

    #[test]
    fn test_apply_template() {
        let stmt = DeqlStatement::ApplyTemplate(ApplyTemplate {
            name: spanned("SoftDeletable".to_string()),
            params: vec![TemplateArg {
                name: spanned("EntityName".to_string()),
                value: spanned(TemplateArgValue::StringLit("Inventory".to_string())),
            }],
        });
        let result = pretty_print_statement(&stmt);
        assert!(result.contains("APPLY TEMPLATE SoftDeletable WITH ("));
        assert!(result.contains("EntityName = 'Inventory'"));
        assert!(result.ends_with(");"));
    }

    #[test]
    fn test_pretty_print_multiple_statements() {
        let source = ParsedSource {
            statements: vec![
                spanned(DeqlStatement::CreateAggregate(CreateAggregate {
                    or_replace: false,
                    name: spanned("Foo".to_string()),
                    fields: None,
                })),
                spanned(DeqlStatement::CreateAggregate(CreateAggregate {
                    or_replace: false,
                    name: spanned("Bar".to_string()),
                    fields: None,
                })),
            ],
        };
        let result = pretty_print(&source);
        assert_eq!(
            result,
            "CREATE AGGREGATE Foo;\n\nCREATE AGGREGATE Bar;"
        );
    }

    #[test]
    fn test_config_value_list() {
        let val = ConfigValue::List(vec!["dt".to_string(), "stream_type".to_string()]);
        assert_eq!(format_config_value(&val), "('dt','stream_type')");
    }

    #[test]
    fn test_decision_multiple_emit_items() {
        let stmt = DeqlStatement::CreateDecision(CreateDecision {
            or_replace: false,
            name: spanned("MultiEmit".to_string()),
            aggregate: spanned("Agg".to_string()),
            command: spanned("Cmd".to_string()),
            state_as: None,
            branches: vec![DecisionBranch {
                branch_index: 1,
                rule_name: None,
                guard: None,
                emit_items: vec![
                    EmitItem {
                        event_type: spanned("EventA".to_string()),
                        assignments: vec![Assignment {
                            field: spanned("x".to_string()),
                            value: spanned(":x".to_string()),
                        }],
                        span: dummy_span(),
                    },
                    EmitItem {
                        event_type: spanned("EventB".to_string()),
                        assignments: vec![Assignment {
                            field: spanned("y".to_string()),
                            value: spanned(":y".to_string()),
                        }],
                        span: dummy_span(),
                    },
                ],
                span: dummy_span(),
            }],
        });
        let result = pretty_print_statement(&stmt);
        assert!(result.contains("SELECT EVENT EventA"));
        assert!(result.contains("SELECT EVENT EventB"));
    }

    #[test]
    fn test_create_template_untyped() {
        let stmt = DeqlStatement::CreateTemplate(CreateTemplate {
            or_replace: false,
            name: spanned("SoftDeletable".to_string()),
            params: vec![TemplateParam {
                name: spanned("EntityName".to_string()),
                data_type: None,
            }],
            body: Vec::new(),
            raw_body: Some("CREATE AGGREGATE {{EntityName}};".to_string()),
        });
        let result = pretty_print_statement(&stmt);
        assert!(result.contains("CREATE TEMPLATE SoftDeletable (EntityName) AS ("));
        assert!(result.contains("CREATE AGGREGATE {{EntityName}};"));
        assert!(result.ends_with(");"));
    }

    #[test]
    fn test_create_template_typed() {
        let stmt = DeqlStatement::CreateTemplate(CreateTemplate {
            or_replace: false,
            name: spanned("wallet_aggregate".to_string()),
            params: vec![
                TemplateParam {
                    name: spanned("wallet_name".to_string()),
                    data_type: Some(spanned(DeqlType::String)),
                },
                TemplateParam {
                    name: spanned("currency".to_string()),
                    data_type: Some(spanned(DeqlType::String)),
                },
            ],
            body: Vec::new(),
            raw_body: Some("CREATE AGGREGATE {{wallet_name}}Wallet;".to_string()),
        });
        let result = pretty_print_statement(&stmt);
        assert!(result.contains("WITH (wallet_name STRING, currency STRING)"));
    }

    #[test]
    fn test_export_dereg_no_path() {
        let stmt = DeqlStatement::ExportDeReg(ExportDeReg { path: None });
        assert_eq!(pretty_print_statement(&stmt), "EXPORT DEREG;");
    }

    #[test]
    fn test_export_dereg_with_path() {
        let stmt = DeqlStatement::ExportDeReg(ExportDeReg {
            path: Some(spanned("output.deql".to_string())),
        });
        assert_eq!(
            pretty_print_statement(&stmt),
            "EXPORT DEREG TO 'output.deql';"
        );
    }

    #[test]
    fn test_validate_dereg() {
        let stmt = DeqlStatement::ValidateDeReg;
        assert_eq!(pretty_print_statement(&stmt), "VALIDATE DEREG;");
    }
}
