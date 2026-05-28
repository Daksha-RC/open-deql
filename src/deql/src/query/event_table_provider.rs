//! EventTableProvider — DataFusion `TableProvider` for the `{Aggregate}$Events` virtual table.
//!
//! Fetches typed event rows from the `deql_events` OO stream via an injected
//! `DeqlSearchBackend`, presenting them as a DataFusion table whose schema is
//! derived from `DeReg::stream_schema_for_org` (`IMPL-06`, `IMPL-09`, `IMPL-24`).
//!
//! Call graph:
//! ```text
//! DeQlSchemaProvider::table("Employee$Events")
//!   └─ EventTableProvider::new(org_id, agg_name, schema, backend)
//!        └─ scan() → backend.fetch_events() → JSON hits → RecordBatch
//! ```
//!
//! The `deql_command` and `deql_org_id` columns are projected out at scan time
//! (`IMPL-24`): they are stored in Parquet but excluded from the exposed schema.
//! Historical Parquet files that predate `_event_id` or `_offset` columns have
//! those fields null-filled (`IMPL-17`).

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use config::utils::record_batch_ext::convert_json_to_record_batch;
use datafusion::{
    arrow::{
        array::{ArrayRef, RecordBatch, new_null_array},
        datatypes::SchemaRef,
    },
    catalog::Session,
    common::ScalarValue,
    datasource::{TableProvider, memory::MemorySourceConfig},
    error::{DataFusionError, Result as DFResult},
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};

// ─── Search backend trait ────────────────────────────────────────────────────

/// Abstraction over the OO search API for `deql_events` queries.
///
/// Defined here so that `o2-deql` (a library crate) does not take a hard
/// dependency on the `openobserve` binary crate. The main crate injects a
/// concrete implementation that calls `service::search::search()`.
#[async_trait]
pub trait DeqlSearchBackend: Send + Sync + std::fmt::Debug {
    /// Fetch raw event rows from the `deql_events` log stream.
    ///
    /// - `org_id`          — organisation identifier (tenant isolation).
    /// - `agg_name`        — used to build `WHERE _aggregate_type = '<agg_name>'` filter
    ///   (`IMPL-07`).
    /// - `event_id_filter` — optional `_event_id` equality literal; if present the backend adds
    ///   `AND _event_id = '<value>'` (`IMPL-08`).
    ///
    /// Returns raw JSON objects, one per event row, as `serde_json::Value`
    /// (`Object` variant). Empty vec if no rows match.
    async fn fetch_events(
        &self,
        org_id: &str,
        agg_name: &str,
        event_id_filter: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>;

    /// Fetch events with access to the DataFusion filters that were applied
    /// at scan time. Default implementation extracts a `_event_id = '<lit>'`
    /// equality if present and forwards to `fetch_events()` for backward
    /// compatibility.
    async fn fetch_events_with_filters(
        &self,
        org_id: &str,
        agg_name: &str,
        filters: &[Expr],
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        // Default behavior: extract _event_id equality if present and call
        // the existing `fetch_events` implementation.
        let mut event_id: Option<String> = None;
        for filter in filters {
            if let Expr::BinaryExpr(binary) = filter {
                if binary.op == Operator::Eq {
                    if let (Expr::Column(col), Expr::Literal(ScalarValue::Utf8(Some(value)), _)) =
                        (binary.left.as_ref(), binary.right.as_ref())
                    {
                        if col.name == "_event_id" {
                            event_id = Some(value.clone());
                            break;
                        }
                    }
                    if let (Expr::Literal(ScalarValue::Utf8(Some(value)), _), Expr::Column(col)) =
                        (binary.left.as_ref(), binary.right.as_ref())
                    {
                        if col.name == "_event_id" {
                            event_id = Some(value.clone());
                            break;
                        }
                    }
                }
            }
        }
        self.fetch_events(org_id, agg_name, event_id.as_deref())
            .await
    }
}

// ─── Phase 7 masking helper ───────────────────────────────────────────────────

/// Replace each column whose name is in `sensitive_names` with a null array of
/// the same type and length (Phase 7 / 7.2).
///
/// Columns not in the list are passed through unchanged.  Returns the same
/// batch schema — only the column data changes.
fn mask_sensitive_columns(batch: RecordBatch, sensitive_names: &[String]) -> DFResult<RecordBatch> {
    if sensitive_names.is_empty() {
        return Ok(batch);
    }
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, col)| -> ArrayRef {
            if sensitive_names.iter().any(|n| n == field.name()) {
                // Return a typed all-null array matching the column's data type.
                new_null_array(field.data_type(), num_rows)
            } else {
                col.clone()
            }
        })
        .collect();
    RecordBatch::try_new(schema, columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

// ─── EventTableProvider ──────────────────────────────────────────────────────

/// DataFusion `TableProvider` that backs the `{Aggregate}$Events` virtual table.
///
/// The schema is pre-built from `DeReg::stream_schema_for_org` and excludes
/// `deql_command` / `deql_org_id` per `SS-09` (`IMPL-09`). On each `scan()` call the
/// provider fetches all matching rows from OO, converts them to typed
/// `RecordBatch`es, and serves them via `MemorySourceConfig`.
#[derive(Debug)]
pub struct EventTableProvider {
    org_id: String,
    agg_name: String,
    /// Schema as presented to DataFusion — no `deql_command` / `deql_org_id`.
    schema: SchemaRef,
    backend: Arc<dyn DeqlSearchBackend>,
    /// Names of `SENSITIVE`-annotated payload fields (Phase 7 / 7.2).
    /// These columns are overwritten with `null` in every `scan()` result
    /// because DataFusion providers do not carry per-request auth context.
    sensitive_field_names: Vec<String>,
}

impl EventTableProvider {
    /// Create a new provider.
    ///
    /// `schema` must be the schema returned by
    /// `DeReg::stream_schema_for_org(org_id, agg_name)` which already
    /// excludes `deql_command` and `deql_org_id` (`IMPL-09`, `SS-09`).
    pub fn new(
        org_id: String,
        agg_name: String,
        schema: SchemaRef,
        backend: Arc<dyn DeqlSearchBackend>,
    ) -> Self {
        Self {
            org_id,
            agg_name,
            schema,
            backend,
            sensitive_field_names: Vec::new(),
        }
    }

    /// Set the list of `SENSITIVE`-annotated field names that will be
    /// null-masked in every `scan()` result (Phase 7 / 7.2).
    pub fn with_sensitive_fields(mut self, names: Vec<String>) -> Self {
        self.sensitive_field_names = names;
        self
    }
}

/// Extract a `_event_id = '<literal>'` filter from DataFusion expressions
/// for pushdown (`IMPL-08`, `AC-07`).
fn extract_event_id_filter(filters: &[Expr]) -> Option<String> {
    for filter in filters {
        if let Expr::BinaryExpr(binary) = filter {
            if binary.op == Operator::Eq {
                // _event_id = '<literal>'
                if let (Expr::Column(col), Expr::Literal(ScalarValue::Utf8(Some(value)), _)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    if col.name == "_event_id" {
                        return Some(value.clone());
                    }
                }
                // '<literal>' = _event_id
                if let (Expr::Literal(ScalarValue::Utf8(Some(value)), _), Expr::Column(col)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    if col.name == "_event_id" {
                        return Some(value.clone());
                    }
                }
            }
        }
    }
    None
}

#[async_trait]
impl TableProvider for EventTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Return `Exact` for `_event_id` equality predicates (`IMPL-08`, `AC-07`);
    /// `Unsupported` for everything else.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if let Expr::BinaryExpr(binary) = f {
                    if binary.op == Operator::Eq {
                        let is_event_id = matches!(
                            (binary.left.as_ref(), binary.right.as_ref()),
                            (Expr::Column(c), Expr::Literal(ScalarValue::Utf8(Some(_)), _))
                                if c.name == "_event_id"
                        ) || matches!(
                            (binary.left.as_ref(), binary.right.as_ref()),
                            (Expr::Literal(ScalarValue::Utf8(Some(_)), _), Expr::Column(c))
                                if c.name == "_event_id"
                        );
                        if is_event_id {
                            return TableProviderFilterPushDown::Exact;
                        }
                    }
                }
                TableProviderFilterPushDown::Unsupported
            })
            .collect())
    }

    /// Fetch events, convert to typed `RecordBatch`es, and return via
    /// `MemorySourceConfig` (`IMPL-06`, `IMPL-07`, `IMPL-24`).
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let event_id_filter = extract_event_id_filter(filters);

        // Fetch JSON rows from OO search (IMPL-06, IMPL-07).
        let hits = self
            .backend
            .fetch_events_with_filters(&self.org_id, &self.agg_name, filters)
            .await
            .map_err(|e| {
                DataFusionError::External(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("deql_events fetch failed for '{}': {}", self.agg_name, e),
                )))
            })?;

        tracing::debug!(
            agg = %self.agg_name,
            org = %self.org_id,
            hit_count = hits.len(),
            event_id_filter = ?event_id_filter,
            "$Events scan: fetched events from search backend"
        );

        // Convert JSON objects to a typed RecordBatch using the registry schema.
        // IMPL-24: the schema already excludes deql_command / deql_org_id.
        // IMPL-17: columns absent from a row (e.g. old Parquet without deql_event_id)
        //   are null-filled by convert_json_to_record_batch.
        let batch = if hits.is_empty() {
            RecordBatch::new_empty(self.schema.clone())
        } else {
            let arc_hits: Vec<Arc<serde_json::Value>> = hits.into_iter().map(Arc::new).collect();
            convert_json_to_record_batch(&self.schema, &arc_hits)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
        };

        // Phase 7 / 7.2: null-mask SENSITIVE columns.
        // DataFusion providers do not carry per-request auth context, so we
        // apply a default-deny policy: SENSITIVE fields are always returned as
        // null from the virtual table.
        let batch = mask_sensitive_columns(batch, &self.sensitive_field_names)?;

        let batches = vec![vec![batch]];
        MemorySourceConfig::try_new_exec(&batches, self.schema.clone(), projection.cloned())
            .map(|exec| exec as Arc<dyn ExecutionPlan>)
            .map_err(|e| {
                DataFusionError::Context(
                    format!("building execution plan for '{}'$Events", self.agg_name),
                    Box::new(e),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::ScalarValue,
        logical_expr::{BinaryExpr, Expr, Operator},
    };

    use super::*;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_event_id", DataType::Utf8, false),
            Field::new("_event_type", DataType::Utf8, false),
            Field::new("_aggregate_type", DataType::Utf8, false),
            Field::new("_aggregate_id", DataType::Utf8, false),
            Field::new("_aggregate_version", DataType::Int64, false),
            Field::new("_offset", DataType::Utf8, false),
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    // ── supports_filters_pushdown ──────────────────────────────────────────

    #[test]
    fn test_pushdown_exact_for_event_id() {
        let schema = make_schema();

        #[derive(Debug)]
        struct NoopBackend;
        #[async_trait]
        impl DeqlSearchBackend for NoopBackend {
            async fn fetch_events(
                &self,
                _org_id: &str,
                _agg_name: &str,
                _event_id_filter: Option<&str>,
            ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(vec![])
            }
        }

        let provider = EventTableProvider::new(
            "org".to_string(),
            "Employee".to_string(),
            schema,
            Arc::new(NoopBackend),
        );

        // _event_id = 'x'
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "_event_id",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("EVT-1".to_string())),
                None,
            )),
        });
        let result = provider
            .supports_filters_pushdown(&[&expr])
            .expect("pushdown check failed");
        assert_eq!(result, vec![TableProviderFilterPushDown::Exact]);
    }

    #[test]
    fn test_pushdown_unsupported_for_other_cols() {
        let schema = make_schema();

        #[derive(Debug)]
        struct NoopBackend;
        #[async_trait]
        impl DeqlSearchBackend for NoopBackend {
            async fn fetch_events(
                &self,
                _org_id: &str,
                _agg_name: &str,
                _event_id_filter: Option<&str>,
            ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(vec![])
            }
        }

        let provider = EventTableProvider::new(
            "org".to_string(),
            "Employee".to_string(),
            schema,
            Arc::new(NoopBackend),
        );

        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "_offset",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)), None)),
        });
        let result = provider
            .supports_filters_pushdown(&[&expr])
            .expect("pushdown check failed");
        assert_eq!(result, vec![TableProviderFilterPushDown::Unsupported]);
    }

    // ── schema ──────────────────────────────────────────────────────────────

    #[test]
    fn test_schema_excludes_deql_command_and_org_id() {
        // The schema we pass in should already exclude those; verify the provider
        // exposes exactly what it was given.
        let schema = make_schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(!field_names.contains(&"deql_command"));
        assert!(!field_names.contains(&"deql_org_id"));

        #[derive(Debug)]
        struct NoopBackend;
        #[async_trait]
        impl DeqlSearchBackend for NoopBackend {
            async fn fetch_events(
                &self,
                _: &str,
                _: &str,
                _: Option<&str>,
            ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(vec![])
            }
        }

        let provider = EventTableProvider::new(
            "org".to_string(),
            "Employee".to_string(),
            schema.clone(),
            Arc::new(NoopBackend),
        );
        let exposed = provider.schema();
        assert_eq!(exposed, schema);
    }

    // ── extract_event_id_filter ──────────────────────────────────────────────

    #[test]
    fn test_extract_event_id_filter_found() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "_event_id",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("EVT-42".to_string())),
                None,
            )),
        });
        assert_eq!(extract_event_id_filter(&[expr]), Some("EVT-42".to_string()));
    }

    #[test]
    fn test_extract_event_id_filter_not_found() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "_offset",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)), None)),
        });
        assert_eq!(extract_event_id_filter(&[expr]), None);
    }

    // ── Phase 7: SENSITIVE masking ────────────────────────────────────────────

    /// 7.1 / 7.2: mask_sensitive_columns replaces the named column with a
    /// typed all-null array while leaving other columns unchanged.
    #[test]
    fn test_mask_sensitive_columns_nulls_named_column() {
        use datafusion::arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("salary", DataType::Utf8, true),
            Field::new("_offset", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob"])) as _,
                Arc::new(StringArray::from(vec!["50000", "60000"])) as _,
                Arc::new(StringArray::from(vec!["1", "2"])) as _,
            ],
        )
        .unwrap();

        let masked = mask_sensitive_columns(batch, &["salary".to_string()]).unwrap();

        // `name` and `_offset` unchanged
        let names = masked
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");

        // `salary` must be all-null
        let salary_col = masked.column_by_name("salary").unwrap();
        assert_eq!(salary_col.null_count(), 2);
        assert!(salary_col.is_null(0));
        assert!(salary_col.is_null(1));
    }

    /// mask_sensitive_columns with an empty sensitive list is a no-op.
    #[test]
    fn test_mask_sensitive_columns_no_op_when_empty() {
        use datafusion::arrow::{
            array::StringArray,
            datatypes::{DataType, Field, Schema},
        };

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["Alice"])) as _],
        )
        .unwrap();
        let result = mask_sensitive_columns(batch.clone(), &[]).unwrap();
        assert_eq!(result.num_rows(), 1);
        let col = result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "Alice");
    }
}
