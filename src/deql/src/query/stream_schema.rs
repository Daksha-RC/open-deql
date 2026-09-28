//! Stream schema derivation and registration for `deql_events`.
//!
//! Implements IMPL-15, IMPL-16, IMPL-19, IMPL-22, IMPL-23 from the
//! `deql-events-agg` spec. Adds methods to [`DeReg`] that:
//!  - enumerate event types reachable from a named aggregate's decisions,
//!  - build the sorted, deduplicated union of payload fields,
//!  - derive the Arrow schema for a per-aggregate `$Events` DataFusion view,
//!  - write the org-level `deql_events` schema to the OO schema registry.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::{dereg::DeReg, parser::ast::DeqlType};

// ---------------------------------------------------------------------------
// SS-07: DeQL type → Arrow DataType mapping
// ---------------------------------------------------------------------------

/// Map a DeQL type to its canonical Arrow [`DataType`] per specification SS-07.
///
/// | DeQL type         | Arrow type                                    |
/// |-------------------|-----------------------------------------------|
/// | `Uuid`            | `Utf8`                                        |
/// | `String`          | `Utf8`                                        |
/// | `Int`             | `Int64`                                       |
/// | `Decimal(p, s)`   | `Decimal128(p, s)`                            |
/// | `Timestamp`       | `Timestamp(Microsecond, Some("UTC".into()))`  |
/// | `Boolean`         | `Boolean`                                     |
pub fn deql_type_to_arrow(dt: &DeqlType) -> DataType {
    match dt {
        DeqlType::Uuid | DeqlType::String => DataType::Utf8,
        DeqlType::Int => DataType::Int64,
        DeqlType::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale as i8),
        DeqlType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DeqlType::Boolean => DataType::Boolean,
    }
}

// ---------------------------------------------------------------------------
// SS-01 / SS-09: fixed metadata field definitions
// ---------------------------------------------------------------------------

/// Fixed metadata columns for the DataFusion `$Events` schema (SS-01, SS-09).
///
/// All metadata columns are **non-nullable**: the ingest handler (IMPL-18)
/// always populates them. `deql_org_id` is intentionally excluded here because
/// it is a storage-only column (SS-09) projected out by [`EventTableProvider`].
fn metadata_fields() -> Vec<Field> {
    vec![
        Field::new("_event_id", DataType::Utf8, false),
        Field::new("_event_type", DataType::Utf8, false),
        Field::new("_aggregate_type", DataType::Utf8, false),
        Field::new("_aggregate_id", DataType::Utf8, false),
        Field::new("_aggregate_version", DataType::Int64, false),
        Field::new("_offset", DataType::Utf8, false),
        Field::new("_timestamp", DataType::Int64, false),
    ]
}

/// Stored metadata columns for the physical stream schema registration.
/// With the canonical naming scheme, `_org_id` is derived from the storage
/// path at query time — no extra column is needed.
fn stored_metadata_fields() -> Vec<Field> {
    metadata_fields()
}

// ---------------------------------------------------------------------------
// impl DeReg — schema derivation
// ---------------------------------------------------------------------------

impl DeReg {
    // IMPL-19 -----------------------------------------------------------------
    /// Return the distinct event type names emitted by all decisions whose
    /// `aggregate` field matches `agg_name`.
    ///
    /// Names are collected from `EMIT AS` clauses across all branches of every
    /// matching decision and returned in **alphabetical order** for
    /// deterministic output.
    pub fn event_types_for_aggregate<'a>(&'a self, agg_name: &str) -> Vec<&'a str> {
        let mut seen = std::collections::HashSet::new();
        let mut names: Vec<&str> = self
            .registry
            .decisions
            .values()
            .filter(|d| d.aggregate.node == agg_name)
            .flat_map(|d| d.all_emit_items())
            .map(|item| item.event_type.node.as_str())
            .filter(|n| seen.insert(*n))
            .collect();
        names.sort_unstable();
        names
    }

    // IMPL-22 -----------------------------------------------------------------
    /// Build the sorted, deduplicated union of payload [`FieldDef`]s for the
    /// named aggregate.
    ///
    /// Steps:
    /// 1. Collect event type names via [`event_types_for_aggregate`].
    /// 2. For each event type, look up the registered `CreateEvent.fields`.
    /// 3. Flatten into a single list: first-occurrence of a given field name wins; sort the result
    ///    alphabetically by field name.
    ///
    /// Callers can map each `data_type.node` to an Arrow type with
    /// [`deql_type_to_arrow`].
    pub fn payload_fields_for_aggregate(
        &self,
        agg_name: &str,
    ) -> Vec<&crate::parser::ast::FieldDef> {
        let event_types = self.event_types_for_aggregate(agg_name);
        let mut seen_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut fields: Vec<&crate::parser::ast::FieldDef> = Vec::new();
        for et in event_types {
            if let Some(event) = self.registry.get_event(et) {
                for field in &event.fields {
                    if seen_names.insert(field.name.node.as_str()) {
                        fields.push(field);
                    }
                }
            }
        }
        fields.sort_by(|a, b| a.name.node.cmp(&b.name.node));
        fields
    }

    // Phase 7 helpers ---------------------------------------------------------
    /// Return the names of `SENSITIVE`-annotated fields for the given aggregate.
    ///
    /// Collects the set across all event types for the aggregate (union, first-wins).
    /// These names are used by `EventTableProvider::scan()` to null-mask SENSITIVE
    /// columns in query results (7.2).
    pub fn sensitive_fields_for_aggregate(&self, agg_name: &str) -> Vec<String> {
        use crate::parser::ast::FieldAnnotation;
        let event_types = self.event_types_for_aggregate(agg_name);
        let mut seen = std::collections::HashSet::new();
        let mut names: Vec<String> = Vec::new();
        for et in event_types {
            if let Some(event) = self.registry.get_event(et) {
                for field in &event.fields {
                    if matches!(field.annotation, Some(FieldAnnotation::Sensitive))
                        && seen.insert(field.name.node.clone())
                    {
                        names.push(field.name.node.clone());
                    }
                }
            }
        }
        names
    }

    // IMPL-15 -----------------------------------------------------------------
    /// Build the Arrow [`Schema`] for the `$Events` DataFusion view of the
    /// given aggregate (SS-01, SS-09, SS-07).
    ///
    /// Layout:
    /// 1. Fixed metadata columns (non-nullable) per SS-09, excluding `deql_org_id` which is a
    ///    storage-only column.
    /// 2. Payload columns (nullable) derived from the aggregate's event types, sorted
    ///    alphabetically and mapped via SS-07.
    ///
    /// This is the source of truth consumed by [`EventTableProvider`].
    pub fn stream_schema_for_org(&self, _org_id: &str, agg_name: &str) -> Arc<Schema> {
        let mut fields = metadata_fields();
        for field_def in self.payload_fields_for_aggregate(agg_name) {
            let arrow_type = deql_type_to_arrow(&field_def.data_type.node);
            // Payload columns are nullable (IMPL-17: absent fields → Arrow null).
            fields.push(Field::new(&field_def.name.node, arrow_type, true));
        }
        Arc::new(Schema::new(fields))
    }

    // IMPL-16 -----------------------------------------------------------------
    /// Write the org-level `deql_events` Arrow schema to the OO schema
    /// registry via `infra::schema::merge`.
    ///
    /// The written schema is the union across **all** aggregates in the
    /// registry so that a single physical stream stores events for every
    /// aggregate in the org. `defined_schema_fields` (SS-05) is set to the
    /// full column list. `flatten_level` is set to `0` (SS-04) as a
    /// declarative marker; the ingest handler (IMPL-18) bypasses
    /// `flatten_with_level` entirely.
    ///
    /// # Errors
    /// Propagates storage errors from `infra::schema::merge`.
    pub async fn register_stream_schema(
        &self,
        org_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use config::meta::stream::{StreamSettings, StreamType};

        // Build the union of all payload fields across every aggregate.
        // First-occurrence wins; final list is sorted alphabetically.
        let mut all_fields: std::collections::HashMap<String, &crate::parser::ast::FieldDef> =
            std::collections::HashMap::new();
        for agg_name in self.list_aggregate_names() {
            for field_def in self.payload_fields_for_aggregate(agg_name) {
                all_fields
                    .entry(field_def.name.node.clone())
                    .or_insert(field_def);
            }
        }
        let mut sorted_payload: Vec<(&String, &&crate::parser::ast::FieldDef)> =
            all_fields.iter().collect();
        sorted_payload.sort_by_key(|(name, _)| name.as_str());

        let mut fields = stored_metadata_fields();
        for (_, field_def) in &sorted_payload {
            let arrow_type = deql_type_to_arrow(&field_def.data_type.node);
            fields.push(Field::new(&field_def.name.node, arrow_type, true));
        }

        // SS-05: defined_schema_fields = all column names.
        let defined_schema_fields: Vec<String> =
            fields.iter().map(|f| f.name().to_string()).collect();

        // Embed StreamSettings in schema metadata (key = "settings").
        let settings = StreamSettings {
            flatten_level: Some(0), // SS-04
            defined_schema_fields,  // SS-05
            ..Default::default()
        };
        let settings_json = serde_json::to_string(&settings)?;

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("settings".to_string(), settings_json);

        let schema = Schema::new_with_metadata(fields, metadata);

        // IMPL-23: never call check_for_schema for deql_events.
        // We write the schema directly — no JSON inference, no check_for_schema.
        infra::schema::merge(org_id, "deql_events", StreamType::Logs, &schema, None)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // R3.5: Register virtual $Agg stream schema for each aggregate.
        // This makes the stream visible in Logs Explore sidebar with all fields
        // in the field selector. No physical data is stored — queries are rewritten
        // to fold SQL against deql_events at search time.
        for agg_name in self.list_aggregate_names() {
            let agg_stream_name = format!("deql_agg_{}", agg_name.to_lowercase());

            // Build schema: metadata fields + payload fields for this aggregate
            let mut agg_fields: Vec<Field> = vec![
                Field::new("_timestamp", DataType::Int64, false),
                Field::new("aggregate_id", DataType::Utf8, false),
                Field::new("_offset", DataType::Utf8, false),
                Field::new("_aggregate_version", DataType::Int64, false),
            ];
            for field_def in self.payload_fields_for_aggregate(agg_name) {
                let arrow_type = deql_type_to_arrow(&field_def.data_type.node);
                agg_fields.push(Field::new(&field_def.name.node, arrow_type, true));
            }

            let agg_defined_fields: Vec<String> =
                agg_fields.iter().map(|f| f.name().to_string()).collect();

            let agg_settings = StreamSettings {
                flatten_level: Some(0),
                defined_schema_fields: agg_defined_fields,
                ..Default::default()
            };
            let agg_settings_json = serde_json::to_string(&agg_settings)?;

            let mut agg_metadata = std::collections::HashMap::new();
            agg_metadata.insert("settings".to_string(), agg_settings_json);

            let agg_schema = Schema::new_with_metadata(agg_fields, agg_metadata);

            infra::schema::merge(org_id, &agg_stream_name, StreamType::Logs, &agg_schema, None)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        }

        // R3.6: Register virtual projection stream schema for each projection.
        // Parse the projection SQL to extract output column names for the schema.
        for proj_name in self.list_projection_names() {
            if let Some(proj) = self.get_projection(proj_name) {
                let proj_stream_name = format!("deql_prj_{}", proj_name.to_lowercase());
                let proj_sql = &proj.body.sql;

                // Extract output column names from the projection SQL
                let output_columns = extract_select_columns(proj_sql);

                // Build schema with extracted columns
                let mut proj_fields: Vec<Field> = Vec::new();
                
                // Always include _timestamp for time-axis compatibility
                let has_timestamp = output_columns.iter().any(|c| c == "_timestamp");
                if !has_timestamp {
                    proj_fields.push(Field::new("_timestamp", DataType::Int64, true));
                }

                // Add all output columns from the projection SQL
                for col_name in &output_columns {
                    // Use Utf8 as default type since we don't know the actual types
                    // The actual types will be determined at query time
                    let data_type = if col_name == "_timestamp" || col_name == "_aggregate_version" || col_name == "_offset" {
                        DataType::Int64
                    } else {
                        DataType::Utf8
                    };
                    proj_fields.push(Field::new(col_name, data_type, true));
                }

                let proj_defined_fields: Vec<String> =
                    proj_fields.iter().map(|f| f.name().to_string()).collect();

                let proj_settings = StreamSettings {
                    flatten_level: Some(0),
                    defined_schema_fields: proj_defined_fields,
                    ..Default::default()
                };
                let proj_settings_json = serde_json::to_string(&proj_settings)?;

                let mut proj_metadata = std::collections::HashMap::new();
                proj_metadata.insert("settings".to_string(), proj_settings_json);

                let proj_schema = Schema::new_with_metadata(proj_fields, proj_metadata);

                infra::schema::merge(org_id, &proj_stream_name, StreamType::Logs, &proj_schema, None)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helper: Extract SELECT column names from SQL
// ---------------------------------------------------------------------------

/// Extract output column names from a SQL SELECT statement.
/// Uses regex-based parsing to extract column aliases and names.
/// Returns the alias if present (AS clause), otherwise the column name.
fn extract_select_columns(sql: &str) -> Vec<String> {
    let mut columns = Vec::new();
    
    // Find the SELECT clause - everything between SELECT and FROM
    let sql_upper = sql.to_uppercase();
    let select_pos = match sql_upper.find("SELECT") {
        Some(pos) => pos + 6,
        None => return columns,
    };
    let from_pos = match sql_upper.find("FROM") {
        Some(pos) => pos,
        None => return columns,
    };
    
    if select_pos >= from_pos {
        return columns;
    }
    
    let select_clause = &sql[select_pos..from_pos].trim();
    
    // Split by comma, handling nested parentheses
    let items = split_select_items(select_clause);
    
    for item in items {
        let item = item.trim();
        if item.is_empty() || item == "*" {
            continue;
        }
        
        // Check for AS alias (case-insensitive)
        let item_upper = item.to_uppercase();
        if let Some(as_pos) = item_upper.rfind(" AS ") {
            let alias = item[as_pos + 4..].trim();
            // Remove quotes if present
            let alias = alias.trim_matches('"').trim_matches('\'').trim_matches('`');
            if !alias.is_empty() {
                columns.push(alias.to_string());
                continue;
            }
        }
        
        // No alias - try to extract the column name
        // For simple columns like "name" or "table.name"
        let name = extract_simple_column_name(item);
        if !name.is_empty() {
            columns.push(name);
        }
    }
    
    columns
}

/// Split SELECT items by comma, respecting parentheses nesting.
fn split_select_items(select_clause: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut paren_depth: i32 = 0;
    
    for ch in select_clause.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if paren_depth == 0 => {
                items.push(current.trim().to_string());
                current = String::new();
            }
            _ => {
                current.push(ch);
            }
        }
    }
    
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    
    items
}

/// Extract a simple column name from an expression.
/// For "table.column" returns "column", for "column" returns "column".
/// For complex expressions, returns empty string.
fn extract_simple_column_name(expr: &str) -> String {
    let expr = expr.trim();
    
    // If it contains parentheses, it's a function - skip
    if expr.contains('(') {
        return String::new();
    }
    
    // If it contains a dot, take the last part
    if let Some(dot_pos) = expr.rfind('.') {
        let name = expr[dot_pos + 1..].trim();
        return name.trim_matches('"').trim_matches('\'').trim_matches('`').to_string();
    }
    
    // Simple identifier
    expr.trim_matches('"').trim_matches('\'').trim_matches('`').to_string()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dereg::DeReg;

    /// Build a `DeReg` with an `Employee` aggregate that has two decisions
    /// emitting two different event types with partially overlapping fields.
    fn make_employee_dereg() -> DeReg {
        let mut dereg = DeReg::new();
        let (parsed, diags) = crate::parse(
            r#"
            CREATE AGGREGATE Employee (id UUID KEY, name STRING);
            CREATE EVENT EmployeeHired (name STRING, role STRING);
            CREATE EVENT EmployeePromoted (name STRING, grade INT);
            CREATE COMMAND HireEmployee (id UUID, name STRING, role STRING);
            CREATE DECISION HireDecision FOR Employee ON COMMAND HireEmployee
                EMIT AS SELECT EVENT EmployeeHired (name := :name, role := :role);
            CREATE COMMAND PromoteEmployee (id UUID, grade INT);
            CREATE DECISION PromoteDecision FOR Employee ON COMMAND PromoteEmployee
                EMIT AS SELECT EVENT EmployeePromoted (name := :name, grade := :grade);
            "#,
        );
        assert!(diags.is_empty(), "parse diagnostics: {diags:?}");
        for spanned in &parsed.statements {
            dereg
                .register_statement(&spanned.node)
                .expect("register error");
        }
        dereg
    }

    // 1.2 — event_types_for_aggregate ------------------------------------------

    #[test]
    fn test_event_types_alphabetical() {
        let dereg = make_employee_dereg();
        let types = dereg.event_types_for_aggregate("Employee");
        assert_eq!(types, vec!["EmployeeHired", "EmployeePromoted"]);
    }

    #[test]
    fn test_event_types_unknown_aggregate_is_empty() {
        let dereg = make_employee_dereg();
        assert!(dereg.event_types_for_aggregate("Ghost").is_empty());
    }

    // 1.1 — payload_fields_for_aggregate ----------------------------------------

    #[test]
    fn test_payload_fields_dedup_first_wins_and_sorted() {
        let dereg = make_employee_dereg();
        let fields = dereg.payload_fields_for_aggregate("Employee");
        let names: Vec<&str> = fields.iter().map(|f| f.name.node.as_str()).collect();
        // alphabetical: grade < name < role
        // "name" appears in both events; first-wins → type from EmployeeHired (String)
        assert_eq!(names, vec!["grade", "name", "role"]);
    }

    #[test]
    fn test_payload_fields_first_wins_type() {
        let dereg = make_employee_dereg();
        let fields = dereg.payload_fields_for_aggregate("Employee");
        let name_field = fields.iter().find(|f| f.name.node == "name").unwrap();
        // "name" first appears in EmployeeHired as STRING
        assert_eq!(name_field.data_type.node, DeqlType::String);
    }

    // 1.3 — stream_schema_for_org -----------------------------------------------

    #[test]
    fn test_schema_metadata_fields_present_and_non_nullable() {
        let dereg = make_employee_dereg();
        let schema = dereg.stream_schema_for_org("default", "Employee");
        let required_meta = [
            "_event_id",
            "_event_type",
            "_aggregate_type",
            "_aggregate_id",
            "_aggregate_version",
            "_offset",
            "_timestamp",
        ];
        for name in &required_meta {
            let field = schema
                .field_with_name(name)
                .unwrap_or_else(|_| panic!("missing metadata field: {name}"));
            assert!(
                !field.is_nullable(),
                "metadata field `{name}` must be non-nullable"
            );
        }
    }

    #[test]
    fn test_schema_payload_fields_nullable() {
        let dereg = make_employee_dereg();
        let schema = dereg.stream_schema_for_org("default", "Employee");
        let meta_names = [
            "_event_id",
            "_event_type",
            "_aggregate_type",
            "_aggregate_id",
            "_aggregate_version",
            "_offset",
            "_timestamp",
        ];
        for field in schema.fields() {
            let n = field.name().as_str();
            if meta_names.contains(&n) {
                assert!(!field.is_nullable(), "metadata `{n}` must be non-nullable");
            } else {
                assert!(field.is_nullable(), "payload `{n}` must be nullable");
            }
        }
    }

    #[test]
    fn test_schema_ss07_type_mapping() {
        let dereg = make_employee_dereg();
        let schema = dereg.stream_schema_for_org("default", "Employee");

        // grade: INT → Int64
        assert_eq!(
            schema.field_with_name("grade").unwrap().data_type(),
            &DataType::Int64
        );
        // name: STRING → Utf8
        assert_eq!(
            schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        // _offset: fixed Utf8 (non-nullable, string-serialized snowflake ID)
        assert_eq!(
            schema.field_with_name("_offset").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_schema_field_count() {
        let dereg = make_employee_dereg();
        let schema = dereg.stream_schema_for_org("default", "Employee");
        // 7 metadata + 3 payload (grade, name, role)
        assert_eq!(schema.fields().len(), 10);
    }

    // SS-07 type round-trip for all variants ------------------------------------

    #[test]
    fn test_deql_type_to_arrow_all_variants() {
        assert_eq!(deql_type_to_arrow(&DeqlType::Uuid), DataType::Utf8);
        assert_eq!(deql_type_to_arrow(&DeqlType::String), DataType::Utf8);
        assert_eq!(deql_type_to_arrow(&DeqlType::Int), DataType::Int64);
        assert_eq!(
            deql_type_to_arrow(&DeqlType::Decimal {
                precision: 10,
                scale: 2
            }),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            deql_type_to_arrow(&DeqlType::Timestamp),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(deql_type_to_arrow(&DeqlType::Boolean), DataType::Boolean);
    }

    // Property: schema derivation invariants ------------------------------------

    #[test]
    fn test_schema_no_deql_org_id_in_datafusion_view() {
        let dereg = make_employee_dereg();
        let schema = dereg.stream_schema_for_org("default", "Employee");
        // deql_org_id must NOT appear in the DataFusion $Events schema (SS-09)
        assert!(
            schema.field_with_name("deql_org_id").is_err(),
            "deql_org_id must be excluded from $Events DataFusion schema"
        );
    }

    #[test]
    fn test_schema_no_deql_command_in_datafusion_view() {
        let dereg = make_employee_dereg();
        let schema = dereg.stream_schema_for_org("default", "Employee");
        // deql_command must NOT appear in the DataFusion $Events schema (SS-09)
        assert!(
            schema.field_with_name("deql_command").is_err(),
            "deql_command must be excluded from $Events DataFusion schema"
        );
    }

    // Phase 7: sensitive_fields_for_aggregate ------------------------------------

    fn make_employee_dereg_with_sensitive() -> DeReg {
        let mut dereg = DeReg::default();
        let (parsed, diags) = crate::parse(
            r#"
            CREATE AGGREGATE Employee (id UUID KEY);
            CREATE EVENT EmployeeHired (name STRING, ssn STRING SENSITIVE, role STRING);
            CREATE EVENT EmployeePromoted (name STRING, salary STRING SENSITIVE);
            CREATE COMMAND Hire (id UUID, name STRING, ssn STRING, role STRING);
            CREATE DECISION HireDecision FOR Employee ON COMMAND Hire
                EMIT AS SELECT EVENT EmployeeHired (name := :name, ssn := :ssn, role := :role);
            CREATE COMMAND Promote (id UUID, salary STRING);
            CREATE DECISION PromoteDecision FOR Employee ON COMMAND Promote
                EMIT AS SELECT EVENT EmployeePromoted (name := :name, salary := :salary);
            "#,
        );
        assert!(diags.is_empty(), "parse diagnostics: {diags:?}");
        for stmt in &parsed.statements {
            dereg.register_statement(&stmt.node).expect("register");
        }
        dereg
    }

    #[test]
    fn test_sensitive_fields_returns_annotated_names() {
        let dereg = make_employee_dereg_with_sensitive();
        let mut fields = dereg.sensitive_fields_for_aggregate("Employee");
        fields.sort();
        // ssn (EmployeeHired) and salary (EmployeePromoted) are SENSITIVE
        assert_eq!(fields, vec!["salary", "ssn"]);
    }

    #[test]
    fn test_sensitive_fields_empty_for_non_sensitive_aggregate() {
        let dereg = make_employee_dereg(); // no SENSITIVE fields
        let fields = dereg.sensitive_fields_for_aggregate("Employee");
        assert!(fields.is_empty(), "expected no sensitive fields, got {fields:?}");
    }

    #[test]
    fn test_sensitive_fields_unknown_aggregate_is_empty() {
        let dereg = make_employee_dereg_with_sensitive();
        assert!(dereg.sensitive_fields_for_aggregate("Ghost").is_empty());
    }

    // extract_select_columns tests ----------------------------------------------

    #[test]
    fn test_extract_select_columns_with_aliases() {
        let sql = "SELECT _aggregate_id AS employee_id, name AS employee_name FROM deql_events";
        let cols = extract_select_columns(sql);
        assert_eq!(cols, vec!["employee_id", "employee_name"]);
    }

    #[test]
    fn test_extract_select_columns_simple() {
        let sql = "SELECT name, grade, role FROM deql_events";
        let cols = extract_select_columns(sql);
        assert_eq!(cols, vec!["name", "grade", "role"]);
    }

    #[test]
    fn test_extract_select_columns_mixed() {
        let sql = "SELECT _aggregate_id AS employee_id, name, LAST_VALUE(grade ORDER BY _offset ASC) AS grade FROM deql_events";
        let cols = extract_select_columns(sql);
        assert_eq!(cols, vec!["employee_id", "name", "grade"]);
    }

    #[test]
    fn test_extract_select_columns_qualified() {
        let sql = "SELECT t.name, t.grade FROM deql_events t";
        let cols = extract_select_columns(sql);
        assert_eq!(cols, vec!["name", "grade"]);
    }

    #[test]
    fn test_extract_select_columns_empty_on_wildcard() {
        let sql = "SELECT * FROM deql_events";
        let cols = extract_select_columns(sql);
        assert!(cols.is_empty());
    }
}
