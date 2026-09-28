//! AggProvider — DataFusion `TableProvider` for the `{Aggregate}$Agg` virtual table.
//!
//! Computes current aggregate state by folding events from an `EventTableProvider`
//! using `LAST_VALUE(field ORDER BY _offset ASC) IGNORE NULLS` per payload field
//! (`IMPL-11`–`IMPL-14`). State is recomputed on every `scan()` call (no caching in MVP).
//!
//! Output schema (`IMPL-05`):
//! - `aggregate_id` (Utf8, non-nullable) — aliased from `_aggregate_id`
//! - payload fields — alphabetically sorted, all nullable, from `payload_fields_for_aggregate`

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::ScalarValue,
    datasource::TableProvider,
    error::{DataFusionError, Result as DFResult},
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_expr::{PhysicalExpr, expressions::Column as PhysicalColumn},
    physical_plan::{ExecutionPlan, projection::ProjectionExec},
    prelude::SessionContext,
};

/// Build the fold SQL that computes current aggregate state.
///
/// Example output for `field_names = ["name", "salary"]`, no filter:
/// ```sql
/// SELECT _aggregate_id AS aggregate_id,
///        LAST_VALUE(name ORDER BY _offset ASC) IGNORE NULLS AS name,
///        LAST_VALUE(salary ORDER BY _offset ASC) IGNORE NULLS AS salary
/// FROM __events__
/// GROUP BY _aggregate_id
/// ```
///
/// `IMPL-11`, `IMPL-12`
pub fn build_fold_query(field_names: &[&str], aggregate_id_filter: Option<&str>) -> String {
    let mut select_parts = vec!["_aggregate_id AS aggregate_id".to_string()];

    for &field in field_names {
        select_parts.push(format!(
            "LAST_VALUE({field} ORDER BY _offset ASC) IGNORE NULLS AS {field}",
        ));
    }

    let select_clause = select_parts.join(", ");

    let where_clause = match aggregate_id_filter {
        Some(value) => format!(" WHERE _aggregate_id = '{}'", value.replace('\'', "''")),
        None => String::new(),
    };

    format!("SELECT {select_clause} FROM __events__{where_clause} GROUP BY _aggregate_id")
}

/// Extract an `aggregate_id = '<literal>'` filter from DataFusion expressions.
///
/// Returns the literal string value if found, `None` otherwise (`IMPL-13`).
pub fn extract_aggregate_id_filter(filters: &[Expr]) -> Option<String> {
    for filter in filters {
        if let Expr::BinaryExpr(binary) = filter {
            if binary.op == Operator::Eq {
                // aggregate_id = '<literal>'
                if let (Expr::Column(col), Expr::Literal(ScalarValue::Utf8(Some(value)), _)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    if col.name == "aggregate_id" {
                        return Some(value.clone());
                    }
                }
                // '<literal>' = aggregate_id
                if let (Expr::Literal(ScalarValue::Utf8(Some(value)), _), Expr::Column(col)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    if col.name == "aggregate_id" {
                        return Some(value.clone());
                    }
                }
            }
        }
    }
    None
}

// ─── AggProvider ─────────────────────────────────────────────────────────────

/// DataFusion `TableProvider` backing the `{Aggregate}$Agg` virtual table.
///
/// On each `scan()` call, registers the injected `EventTableProvider` as
/// `__events__` in a temporary `SessionContext`, runs the fold SQL, and
/// returns the resulting `ExecutionPlan` (`IMPL-14`).
#[derive(Debug)]
pub struct AggProvider {
    agg_name: String,
    schema: SchemaRef,
    event_table_provider: Arc<dyn TableProvider>,
    /// Alphabetically sorted payload field names used in the fold SQL (`IMPL-22`).
    data_field_names: Vec<String>,
}

impl AggProvider {
    /// Create a new `AggProvider`.
    ///
    /// - `agg_name` — aggregate name (used in error messages)
    /// - `schema` — output schema: `aggregate_id` first, then sorted payload fields (`IMPL-05`)
    /// - `event_table_provider` — provides raw event rows (`IMPL-11`)
    /// - `data_field_names` — alphabetically sorted payload field names from
    ///   `payload_fields_for_aggregate` (`IMPL-22`)
    pub fn new(
        agg_name: String,
        schema: SchemaRef,
        event_table_provider: Arc<dyn TableProvider>,
        data_field_names: Vec<String>,
    ) -> Self {
        Self {
            agg_name,
            schema,
            event_table_provider,
            data_field_names,
        }
    }
}

#[async_trait]
impl TableProvider for AggProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    /// Return `Exact` for `aggregate_id` equality; `Unsupported` otherwise
    /// (`IMPL-13`, `AC-08`).
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if let Expr::BinaryExpr(binary) = f {
                    if binary.op == Operator::Eq {
                        let is_agg_id = matches!(
                            (binary.left.as_ref(), binary.right.as_ref()),
                            (Expr::Column(c), Expr::Literal(ScalarValue::Utf8(Some(_)), _))
                                if c.name == "aggregate_id"
                        ) || matches!(
                            (binary.left.as_ref(), binary.right.as_ref()),
                            (Expr::Literal(ScalarValue::Utf8(Some(_)), _), Expr::Column(c))
                                if c.name == "aggregate_id"
                        );
                        if is_agg_id {
                            return TableProviderFilterPushDown::Exact;
                        }
                    }
                }
                TableProviderFilterPushDown::Unsupported
            })
            .collect())
    }

    /// Register `EventTableProvider` as `__events__`, run the fold SQL, and
    /// return the `ExecutionPlan` (`IMPL-14`, `IMPL-11`).
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let agg_id_filter = extract_aggregate_id_filter(filters);

        let field_refs: Vec<&str> = self.data_field_names.iter().map(|s| s.as_str()).collect();
        let sql = build_fold_query(&field_refs, agg_id_filter.as_deref());

        tracing::debug!(
            agg = %self.agg_name,
            filter = ?agg_id_filter,
            fold_sql = %sql,
            "$Agg scan: executing fold query"
        );

        // Register LAST/FIRST UDAFs so the fold SQL resolves them (`IMPL-02`).
        let ctx = SessionContext::new();
        ctx.register_udaf(crate::udaf::create_last_udaf());
        ctx.register_udaf(crate::udaf::create_first_udaf());

        ctx.register_table("__events__", self.event_table_provider.clone())
            .map_err(|e| {
                DataFusionError::Context(
                    format!("registering __events__ for '$Agg' of '{}'", self.agg_name),
                    Box::new(e),
                )
            })?;

        let df = ctx.sql(&sql).await.map_err(|e| {
            DataFusionError::Context(
                format!("computing $Agg state for '{}'", self.agg_name),
                Box::new(e),
            )
        })?;

        let plan = df.create_physical_plan().await.map_err(|e| {
            DataFusionError::Context(
                format!("planning $Agg state for '{}'", self.agg_name),
                Box::new(e),
            )
        })?;

        // Apply column projection requested by DataFusion's query planner.
        if let Some(proj) = projection {
            let full_schema = plan.schema();
            let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                .iter()
                .filter_map(|&idx| {
                    let field = self.schema.field(idx);
                    full_schema.index_of(field.name()).ok().map(|col_idx| {
                        (
                            Arc::new(PhysicalColumn::new(field.name(), col_idx))
                                as Arc<dyn PhysicalExpr>,
                            field.name().clone(),
                        )
                    })
                })
                .collect();

            if !exprs.is_empty() {
                let projected = ProjectionExec::try_new(exprs, plan).map_err(|e| {
                    DataFusionError::Context(
                        format!("projecting $Agg state for '{}'", self.agg_name),
                        Box::new(e),
                    )
                })?;
                return Ok(Arc::new(projected));
            }
        }

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── build_fold_query ────────────────────────────────────────────────────

    #[test]
    fn test_build_fold_query_no_filter() {
        let sql = build_fold_query(&["name", "salary"], None);
        assert!(
            sql.contains("_aggregate_id AS aggregate_id"),
            "missing aggregate_id alias"
        );
        assert!(
            sql.contains("LAST_VALUE(name ORDER BY _offset ASC) IGNORE NULLS AS name"),
            "missing name fold: {sql}"
        );
        assert!(
            sql.contains("LAST_VALUE(salary ORDER BY _offset ASC) IGNORE NULLS AS salary"),
            "missing salary fold: {sql}"
        );
        assert!(sql.contains("GROUP BY _aggregate_id"), "missing GROUP BY");
        assert!(!sql.contains("WHERE"), "unexpected WHERE clause");
    }

    #[test]
    fn test_build_fold_query_with_filter() {
        let sql = build_fold_query(&["name"], Some("EMP-001"));
        assert!(
            sql.contains("WHERE _aggregate_id = 'EMP-001'"),
            "expected filter: {sql}"
        );
    }

    #[test]
    fn test_build_fold_query_sql_injection_prevention() {
        // Single quotes in the filter value must be escaped.
        let sql = build_fold_query(&["name"], Some("O'Brien"));
        assert!(
            sql.contains("WHERE _aggregate_id = 'O''Brien'"),
            "single-quote escaping failed: {sql}"
        );
    }

    #[test]
    fn test_build_fold_query_empty_fields() {
        let sql = build_fold_query(&[], None);
        assert!(
            sql.contains("_aggregate_id AS aggregate_id"),
            "missing alias"
        );
        assert!(sql.contains("GROUP BY _aggregate_id"), "missing GROUP BY");
    }

    // ── extract_aggregate_id_filter ─────────────────────────────────────────

    #[test]
    fn test_extract_filter_found() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "aggregate_id",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("EMP-001".to_string())),
                None,
            )),
        });
        assert_eq!(
            extract_aggregate_id_filter(&[expr]),
            Some("EMP-001".to_string())
        );
    }

    #[test]
    fn test_extract_filter_reversed() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("EMP-001".to_string())),
                None,
            )),
            op: Operator::Eq,
            right: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "aggregate_id",
            ))),
        });
        assert_eq!(
            extract_aggregate_id_filter(&[expr]),
            Some("EMP-001".to_string())
        );
    }

    #[test]
    fn test_extract_filter_not_found() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "_aggregate_id",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("EMP-001".to_string())),
                None,
            )),
        });
        assert_eq!(extract_aggregate_id_filter(&[expr]), None);
    }

    // ── supports_filters_pushdown ───────────────────────────────────────────

    #[test]
    fn test_agg_pushdown_exact_for_aggregate_id() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("aggregate_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        #[derive(Debug)]
        struct NoopEvents(SchemaRef);
        #[async_trait]
        impl TableProvider for NoopEvents {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn schema(&self) -> SchemaRef {
                self.0.clone()
            }
            fn table_type(&self) -> TableType {
                TableType::Base
            }
            async fn scan(
                &self,
                _: &dyn Session,
                _: Option<&Vec<usize>>,
                _: &[Expr],
                _: Option<usize>,
            ) -> DFResult<Arc<dyn ExecutionPlan>> {
                unimplemented!()
            }
        }

        let provider = AggProvider::new(
            "Employee".to_string(),
            schema.clone(),
            Arc::new(NoopEvents(schema.clone())),
            vec!["name".to_string()],
        );

        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "aggregate_id",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("EMP-001".to_string())),
                None,
            )),
        });

        let result = provider
            .supports_filters_pushdown(&[&expr])
            .expect("pushdown check failed");
        assert_eq!(result, vec![TableProviderFilterPushDown::Exact]);
    }

    // ── fold query execution ────────────────────────────────────────────────

    /// Verify that LAST_VALUE(x ORDER BY y ASC) IGNORE NULLS actually works
    /// as an aggregate in DataFusion (not just as a window function).
    #[tokio::test]
    async fn test_fold_query_executes_correctly() {
        use datafusion::{
            arrow::{
                array::{Array, Int64Array, RecordBatch, StringArray},
                datatypes::{DataType, Field, Schema},
            },
            prelude::SessionContext,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("_aggregate_id", DataType::Utf8, false),
            Field::new("_offset", DataType::Utf8, false),
            Field::new("grade", DataType::Utf8, true),
            Field::new("new_grade", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["EMP102"]))
                    as Arc<dyn datafusion::arrow::array::Array>,
                Arc::new(StringArray::from(vec!["100"]))
                    as Arc<dyn datafusion::arrow::array::Array>,
                Arc::new(StringArray::from(vec![Some("A1")]))
                    as Arc<dyn datafusion::arrow::array::Array>,
                Arc::new(StringArray::from(vec![None as Option<&str>]))
                    as Arc<dyn datafusion::arrow::array::Array>,
            ],
        )
        .expect("batch creation");

        let ctx = SessionContext::new();
        ctx.register_batch("__events__", batch)
            .expect("register batch");

        // Register our custom UDAFs (same as AggProvider::scan does)
        ctx.register_udaf(crate::udaf::create_last_udaf());
        ctx.register_udaf(crate::udaf::create_first_udaf());

        let sql = build_fold_query(&["grade", "new_grade"], Some("EMP102"));

        let df = ctx
            .sql(&sql)
            .await
            .expect(&format!("fold SQL should parse: {sql}"));

        let batches = df
            .collect()
            .await
            .expect(&format!("fold SQL should execute: {sql}"));

        assert!(!batches.is_empty(), "should return at least one batch");
        assert_eq!(batches[0].num_rows(), 1, "should return exactly one row");

        // Verify aggregate_id
        let agg_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("aggregate_id should be Utf8");
        assert_eq!(agg_col.value(0), "EMP102");

        // Verify grade = "A1" (last non-null)
        let grade_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("grade should be Utf8");
        assert_eq!(grade_col.value(0), "A1");

        // Verify new_grade is null (no non-null values exist)
        let new_grade_col = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("new_grade should be Utf8");
        assert!(new_grade_col.is_null(0), "new_grade should be null");
    }
}
