//! Pure Arrow/DataFusion batch filtering logic.
//!
//! This module provides `filter_record_batches` which evaluates a DataFusion
//! logical `Expr` against a set of `RecordBatch`es, returning only matching rows.
//! It uses a fast physical-evaluation path when possible, falling back to a
//! SessionContext-based evaluation for complex expressions.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute::take;
use arrow_schema::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Expr as DFExpr;
use datafusion::physical_plan::expressions::{
    BinaryExpr, CastExpr, Column as PColumn, InListExpr, Literal as PLiteral, NotExpr,
};
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use datafusion::scalar::ScalarValue;

/// Result of filtering a set of record batches.
pub struct FilteredBatches {
    pub batches: Vec<RecordBatch>,
}

/// Filter a list of `RecordBatch`es using a DataFusion logical `Expr`.
///
/// For each batch, attempts a fast physical-evaluation path. If that fails
/// (unsupported expression nodes), falls back to a full SessionContext evaluation.
///
/// Returns only batches with matching rows (empty batches are dropped).
pub async fn filter_record_batches(
    batches: Vec<RecordBatch>,
    filter: &DFExpr,
) -> Vec<RecordBatch> {
    let ctx = datafusion::prelude::SessionContext::new();
    let mut out = Vec::with_capacity(batches.len());

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        // Try fast physical-eval path
        if let Some(filtered) = try_physical_filter(&batch, filter) {
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
            continue;
        }

        // Fallback: SessionContext evaluation
        if let Some(filtered) = fallback_session_filter(&ctx, &batch, filter).await {
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
    }

    out
}

/// Attempt to convert a logical `Expr` to a `PhysicalExpr` and evaluate it
/// directly against the batch. Returns `Some(filtered_batch)` on success,
/// `None` if conversion or evaluation fails.
fn try_physical_filter(batch: &RecordBatch, expr: &DFExpr) -> Option<RecordBatch> {
    let schema = batch.schema();
    let phys = logical_to_physical(expr, schema.as_ref()).ok()?;

    let cv = phys.evaluate(batch).ok()?;

    match cv {
        ColumnarValue::Array(arr) => {
            if arr.len() != batch.num_rows() {
                log::warn!(
                    "batch_filter: predicate array length mismatch: {} vs {}",
                    arr.len(),
                    batch.num_rows()
                );
                return None;
            }
            let bool_arr = arr.as_any().downcast_ref::<BooleanArray>()?;
            apply_boolean_mask(batch, bool_arr)
        }
        ColumnarValue::Scalar(s) => match s {
            ScalarValue::Boolean(Some(true)) => Some(batch.clone()),
            ScalarValue::Boolean(_) => Some(RecordBatch::new_empty(batch.schema())),
            _ => {
                log::warn!("batch_filter: predicate scalar not boolean");
                None
            }
        },
    }
}

/// Apply a boolean mask to a RecordBatch, returning only rows where mask is true.
fn apply_boolean_mask(batch: &RecordBatch, mask: &BooleanArray) -> Option<RecordBatch> {
    let mut sel: Vec<u32> = Vec::with_capacity(mask.len());
    for i in 0..mask.len() {
        if mask.is_valid(i) && mask.value(i) {
            sel.push(i as u32);
        }
    }

    if sel.is_empty() {
        return Some(RecordBatch::new_empty(batch.schema()));
    }

    let indices = Arc::new(UInt32Array::from(sel)) as ArrayRef;
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for c in batch.columns() {
        match take(c.as_ref(), &indices, None) {
            Ok(new_col) => cols.push(new_col),
            Err(e) => {
                log::warn!("batch_filter: take() error: {e}");
                return None;
            }
        }
    }

    RecordBatch::try_new(batch.schema(), cols).ok()
}

/// Fallback: use a DataFusion SessionContext to evaluate the filter.
async fn fallback_session_filter(
    ctx: &datafusion::prelude::SessionContext,
    batch: &RecordBatch,
    filter_expr: &DFExpr,
) -> Option<RecordBatch> {
    let schema = batch.schema();
    let df0 = ctx.read_batch(batch.clone()).ok()?;

    // Project referenced fields (null-fill missing ones)
    let mut referenced = HashSet::new();
    extract_columns_from_expr(filter_expr, &mut referenced);

    let df1 = if !referenced.is_empty() {
        let proj: Vec<datafusion::logical_expr::Expr> = referenced
            .iter()
            .map(|name| {
                if schema.field_with_name(name).is_ok() {
                    datafusion::prelude::col(name.as_str()).alias(name.as_str())
                } else {
                    datafusion::logical_expr::Expr::Literal(ScalarValue::Utf8(None), None)
                        .alias(name.as_str())
                }
            })
            .collect();
        match df0.select(proj) {
            Ok(d) => d,
            Err(e) => {
                log::warn!("batch_filter: projection error: {e}");
                return None;
            }
        }
    } else {
        df0
    };

    let filtered_df = df1.filter(filter_expr.clone()).ok()?;
    let rbs = filtered_df.collect().await.ok()?;

    // Concatenate all result batches into one
    if rbs.is_empty() {
        return None;
    }
    if rbs.len() == 1 {
        return Some(rbs.into_iter().next().unwrap());
    }

    // Multiple batches — concat
    let schema = rbs[0].schema();
    datafusion::arrow::compute::concat_batches(&schema, &rbs).ok()
}

/// Convert a DataFusion logical `Expr` into a `PhysicalExpr`.
/// Supports a conservative subset: Column, Literal, BinaryExpr, Cast, Not, InList, Alias.
fn logical_to_physical(
    expr: &DFExpr,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, datafusion::common::DataFusionError> {
    use datafusion::logical_expr::Expr as E;
    match expr {
        E::Column(c) => {
            let idx = schema
                .index_of(&c.name)
                .map_err(|e| datafusion::common::DataFusionError::Execution(e.to_string()))?;
            Ok(Arc::new(PColumn::new(&c.name, idx)))
        }
        E::Literal(sv, _opt) => Ok(Arc::new(PLiteral::new(sv.clone()))),
        E::BinaryExpr(bin) => {
            let left = logical_to_physical(&bin.left, schema)?;
            let right = logical_to_physical(&bin.right, schema)?;
            Ok(Arc::new(BinaryExpr::new(left, bin.op, right)))
        }
        E::Cast(cast) => {
            let inner = logical_to_physical(&cast.expr, schema)?;
            Ok(Arc::new(CastExpr::new(inner, cast.data_type.clone(), None)))
        }
        E::Not(inner) => {
            let arg = logical_to_physical(inner, schema)?;
            Ok(Arc::new(NotExpr::new(arg)))
        }
        E::Alias(a) => logical_to_physical(&a.expr, schema),
        E::InList(il) => {
            let left = logical_to_physical(&il.expr, schema)?;
            let mut vals: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(il.list.len());
            for e in &il.list {
                let v = logical_to_physical(e, schema)?;
                vals.push(v);
            }
            Ok(Arc::new(InListExpr::try_new(
                left, vals, il.negated, schema,
            )?))
        }
        _ => Err(datafusion::common::DataFusionError::Execution(format!(
            "unsupported expr for physical conversion: {expr:?}"
        ))),
    }
}

/// Extract column names referenced by an expression.
pub fn extract_columns_from_expr(expr: &DFExpr, cols: &mut HashSet<String>) {
    use datafusion::logical_expr::Expr as E;
    match expr {
        E::Column(c) => {
            cols.insert(c.name.clone());
        }
        E::Alias(alias) => extract_columns_from_expr(&alias.expr, cols),
        E::BinaryExpr(bin) => {
            extract_columns_from_expr(&bin.left, cols);
            extract_columns_from_expr(&bin.right, cols);
        }
        E::ScalarFunction(f) => {
            for a in &f.args {
                extract_columns_from_expr(a, cols);
            }
        }
        E::Cast(cast) => extract_columns_from_expr(&cast.expr, cols),
        E::Not(e) | E::Negative(e) | E::IsNull(e) | E::IsNotNull(e) => {
            extract_columns_from_expr(e, cols)
        }
        E::Between(b) => {
            extract_columns_from_expr(&b.expr, cols);
            extract_columns_from_expr(&b.low, cols);
            extract_columns_from_expr(&b.high, cols);
        }
        E::Case(case) => {
            if let Some(e) = &case.expr {
                extract_columns_from_expr(e, cols);
            }
            for (w, th) in &case.when_then_expr {
                extract_columns_from_expr(w, cols);
                extract_columns_from_expr(th, cols);
            }
            if let Some(e) = &case.else_expr {
                extract_columns_from_expr(e, cols);
            }
        }
        E::InList(in_list) => {
            extract_columns_from_expr(&in_list.expr, cols);
            for e in &in_list.list {
                extract_columns_from_expr(e, cols);
            }
        }
        _ => {}
    }
}
