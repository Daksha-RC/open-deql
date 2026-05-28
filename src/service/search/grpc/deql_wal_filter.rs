//! DeQL WAL memtable predicate pushdown.
//!
//! Evaluates an `IndexCondition` as a `PhysicalExpr` against merged
//! `RecordBatch`es to perform in-memory predicate pushdown during WAL search.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, UInt32Array};
use datafusion::{
    arrow::record_batch::RecordBatch, physical_plan::ColumnarValue, scalar::ScalarValue,
};
use crate::service::search::index::IndexCondition;

/// Filter a list of `RecordBatch`es using an `IndexCondition`.
///
/// For each batch, builds a physical expression from the condition and evaluates it.
/// Rows where the predicate is true are kept; others are dropped.
/// On any error, the original batch is preserved (fail-open).
pub fn filter_batches_with_index_condition(
    record_batches: Vec<RecordBatch>,
    ic: &IndexCondition,
    fst_fields: &[String],
    trace_id: &str,
) -> Vec<RecordBatch> {
    let mut filtered_batches = Vec::with_capacity(record_batches.len());

    for rb in record_batches.into_iter() {
        // Build a physical expression for this batch schema
        let phys = match ic.to_physical_expr(rb.schema().as_ref(), fst_fields) {
            Ok(e) => e,
            Err(e) => {
                log::warn!("[trace_id {trace_id}] to_physical_expr failed: {e}");
                filtered_batches.push(rb);
                continue;
            }
        };

        // Evaluate the expression
        let cv = match phys.evaluate(&rb) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("[trace_id {trace_id}] physical expr evaluate error: {e}");
                filtered_batches.push(rb);
                continue;
            }
        };

        match cv {
            ColumnarValue::Array(arr) => {
                if arr.len() != rb.num_rows() {
                    log::warn!(
                        "[trace_id {trace_id}] predicate array length mismatch: {} vs {}",
                        arr.len(),
                        rb.num_rows()
                    );
                    filtered_batches.push(rb);
                    continue;
                }
                let bool_arr = match arr.as_any().downcast_ref::<BooleanArray>() {
                    Some(b) => b,
                    None => {
                        log::warn!(
                            "[trace_id {trace_id}] predicate did not return a boolean array"
                        );
                        filtered_batches.push(rb);
                        continue;
                    }
                };

                // Collect selected indices (treat NULL as false)
                let mut sel: Vec<u32> = Vec::with_capacity(bool_arr.len());
                for i in 0..bool_arr.len() {
                    if bool_arr.is_valid(i) && bool_arr.value(i) {
                        sel.push(i as u32);
                    }
                }
                if sel.is_empty() {
                    continue; // no rows match
                }
                let indices = Arc::new(UInt32Array::from(sel)) as arrow::array::ArrayRef;
                let mut cols: Vec<arrow::array::ArrayRef> = Vec::with_capacity(rb.num_columns());
                let mut take_failed = false;
                for c in rb.columns() {
                    match arrow::compute::take(c.as_ref(), &indices, None) {
                        Ok(new_col) => cols.push(new_col),
                        Err(e) => {
                            log::warn!("[trace_id {trace_id}] take() error: {e}");
                            take_failed = true;
                            break;
                        }
                    }
                }
                if take_failed {
                    filtered_batches.push(rb);
                } else if let Ok(new_rb) = RecordBatch::try_new(rb.schema().clone(), cols) {
                    if new_rb.num_rows() > 0 {
                        filtered_batches.push(new_rb);
                    }
                } else {
                    filtered_batches.push(rb);
                }
            }
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::Boolean(Some(true)) => {
                    filtered_batches.push(rb);
                }
                ScalarValue::Boolean(_) => {
                    // false or null -> drop all rows
                }
                _ => {
                    log::warn!("[trace_id {trace_id}] predicate scalar not boolean");
                    filtered_batches.push(rb);
                }
            },
        }
    }

    filtered_batches
}
