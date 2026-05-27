#[allow(dead_code)]
pub fn compute_selection_stats(num_rows: usize, segment_ids: &BitVec) -> (usize, usize, usize) {
    let row_group_count = num_rows.div_ceil(PARQUET_MAX_ROW_GROUP_SIZE);
    let mut selected_row_group_count: usize = 0;
    let mut selected_bits_count: usize = 0;

    for (row_group_id, chunk) in segment_ids.chunks(PARQUET_MAX_ROW_GROUP_SIZE).enumerate() {
        let remaining = num_rows - row_group_id * PARQUET_MAX_ROW_GROUP_SIZE;
        if chunk.iter().take(remaining).any(|v| *v) {
            selected_row_group_count += 1;
            let sel_bits = chunk.iter().take(remaining).filter(|v| **v).count();
            selected_bits_count += sel_bits;
        }
    }

    (
        row_group_count,
        selected_row_group_count,
        selected_bits_count,
    )
}
// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
// Instrumentation: count files with access plans and last selected_rows_estimate (for test
// assertions)
static FILES_WITH_ACCESS_PLAN: AtomicUsize = AtomicUsize::new(0);
static LAST_SELECTED_ROWS_ESTIMATE: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub fn get_files_with_access_plan() -> usize {
    FILES_WITH_ACCESS_PLAN.load(Ordering::Relaxed)
}

#[cfg(test)]
pub fn get_last_selected_rows_estimate() -> usize {
    LAST_SELECTED_ROWS_ESTIMATE.load(Ordering::Relaxed)
}

use arrow_schema::{DataType, SchemaRef};
use config::{FileFormat, PARQUET_MAX_ROW_GROUP_SIZE, TIMESTAMP_COL_NAME, meta::bitvec::BitVec};
use datafusion::{
    common::{DataFusionError, Result, project_schema, stats::Precision},
    datasource::{listing::PartitionedFile, physical_plan::parquet::ParquetAccessPlan},
    logical_expr::Operator,
    parquet::arrow::arrow_reader::{RowSelection, RowSelector},
    physical_expr::conjunction,
    physical_plan::{
        ExecutionPlan, PhysicalExpr,
        expressions::{BinaryExpr, CastExpr, Column, Literal},
        filter::FilterExecBuilder,
        projection::ProjectionExec,
    },
    scalar::ScalarValue,
};
use hashbrown::HashMap;
#[cfg(all(feature = "enterprise", feature = "vortex"))]
use o2_enterprise::enterprise::search::vortex::generate_vortex_access_plan;

use crate::service::search::{datafusion::storage, index::IndexCondition};

pub fn generate_access_plan(
    file: &PartitionedFile,
) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
    let segment_ids = storage::file_list::get_segment_ids(file.path().as_ref())?;
    let file_format = FileFormat::from_extension(file.path().as_ref())?;
    match file_format {
        FileFormat::Parquet => generate_parquet_access_plan(file, segment_ids),
        #[cfg(all(feature = "enterprise", feature = "vortex"))]
        FileFormat::Vortex => generate_vortex_access_plan(segment_ids),
        #[cfg(not(all(feature = "enterprise", feature = "vortex")))]
        FileFormat::Vortex => None,
    }
}

/// Compute basic parquet selection stats from a BitVec for testing and metrics.

fn generate_parquet_access_plan(
    file: &PartitionedFile,
    segment_ids: Arc<BitVec>,
) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
    let stats = file.statistics.as_ref()?;
    let Precision::Exact(num_rows) = stats.num_rows else {
        return None;
    };
    let row_group_count = num_rows.div_ceil(PARQUET_MAX_ROW_GROUP_SIZE);

    // Determine sampling mode based on BitVec size:
    // - If BitVec size == row_group_count: row-group-level sampling (enterprise feature)
    // - If BitVec size == num_rows: row-level sampling (original behavior)
    #[cfg(feature = "enterprise")]
    if segment_ids.len() == row_group_count {
        // Row-group-level sampling: each bit represents a row group
        return Some(
            o2_enterprise::enterprise::search::sampling::execution::generate_row_group_access_plan(
                &segment_ids,
                row_group_count,
                file.path().as_ref(),
            ),
        );
    }

    let mut access_plan = ParquetAccessPlan::new_none(row_group_count);
    let mut selected_row_group_count: usize = 0;
    let mut selected_bits_count: usize = 0;
    for (row_group_id, chunk) in segment_ids.chunks(PARQUET_MAX_ROW_GROUP_SIZE).enumerate() {
        let mut selection = Vec::new();
        let mut current_count = 0;
        let mut current_select = false;

        for val in chunk
            .iter()
            .take(num_rows - row_group_id * PARQUET_MAX_ROW_GROUP_SIZE)
        {
            if *val == current_select {
                current_count += 1;
            } else {
                if current_count > 0 {
                    if current_select {
                        selection.push(RowSelector::select(current_count));
                    } else {
                        selection.push(RowSelector::skip(current_count));
                    }
                }
                current_select = *val;
                current_count = 1;
            }
        }

        // handle the last batch
        if current_count > 0 {
            if current_select {
                selection.push(RowSelector::select(current_count));
            } else {
                selection.push(RowSelector::skip(current_count));
            }
        }

        if selection.iter().any(|s| !s.skip) {
            // record that this row-group has some selected rows
            access_plan.scan(row_group_id);
            access_plan.scan_selection(row_group_id, RowSelection::from(selection));
            selected_row_group_count += 1;
            // estimate selected bits in this chunk
            let remaining = num_rows - row_group_id * PARQUET_MAX_ROW_GROUP_SIZE;
            let sel_bits = chunk.iter().take(remaining).filter(|v| **v).count();
            selected_bits_count += sel_bits;
        }
    }

    // Instrumentation: increment counter and record last selected_rows_estimate
    FILES_WITH_ACCESS_PLAN.fetch_add(1, Ordering::Relaxed);
    LAST_SELECTED_ROWS_ESTIMATE.store(selected_bits_count, Ordering::Relaxed);

    log::info!(
        "parquet access plan: file={:?}, row_group_count={row_group_count}, selected_row_groups={}, selected_rows_estimate={}",
        file.path().as_ref(),
        selected_row_group_count,
        selected_bits_count
    );

    log::debug!(
        "file path: file={:?}, row_group_count={row_group_count}, access_plan={access_plan:?}",
        file.path().as_ref()
    );
    Some(Arc::new(access_plan))
}

pub fn apply_projection(
    schema: &SchemaRef,
    diff_rules: &HashMap<String, DataType>,
    projection: Option<&Vec<usize>>,
    memory_exec: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if diff_rules.is_empty() {
        return Ok(memory_exec);
    }
    let projected_schema = project_schema(schema, projection)?;
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(projected_schema.fields().len());
    for (idx, field) in projected_schema.fields().iter().enumerate() {
        let name = field.name().to_string();
        let col = Arc::new(datafusion::physical_expr::expressions::Column::new(
            &name, idx,
        ));
        if let Some(data_type) = diff_rules.get(&name) {
            exprs.push((Arc::new(CastExpr::new(col, data_type.clone(), None)), name));
        } else {
            exprs.push((col, name));
        }
    }
    Ok(Arc::new(ProjectionExec::try_new(exprs, memory_exec)?))
}

pub fn apply_combined_filter(
    index_condition: Option<&IndexCondition>,
    timestamp_filter: Option<(i64, i64)>,
    schema: &arrow_schema::Schema,
    fst_fields: &[String],
    exec_plan: Arc<dyn ExecutionPlan>,
    filter_projection: Option<&Vec<usize>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if index_condition.is_none() && timestamp_filter.is_none() {
        return Ok(exec_plan);
    }

    let mut filter_exprs = Vec::new();

    // add index condition filter if present
    if let Some(condition) = index_condition {
        let expr = condition
            .to_physical_expr(schema, fst_fields)
            .map_err(|e| DataFusionError::External(e.into()))?;
        filter_exprs.push(expr);
    }

    // add timestamp filter if present
    if let Some((start_time, end_time)) = timestamp_filter {
        let timestamp_idx = schema.index_of(TIMESTAMP_COL_NAME)?;
        let timestamp_col = Arc::new(Column::new(TIMESTAMP_COL_NAME, timestamp_idx));

        // create filter: _timestamp >= start_time AND _timestamp < end_time
        let ge_expr = Arc::new(BinaryExpr::new(
            timestamp_col.clone(),
            Operator::GtEq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(start_time)))),
        ));
        let le_expr = Arc::new(BinaryExpr::new(
            timestamp_col,
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(end_time)))),
        ));
        let timestamp_expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(ge_expr, Operator::And, le_expr));
        filter_exprs.push(timestamp_expr);
    }

    // combine all filters with AND
    let combined_expr = conjunction(filter_exprs);

    Ok(Arc::new(
        FilterExecBuilder::new(combined_expr, exec_plan)
            .apply_projection(filter_projection.cloned())?
            .build()?,
    ))
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_access_plan_instrumentation() {
        // Reset counters
        super::FILES_WITH_ACCESS_PLAN.store(0, std::sync::atomic::Ordering::Relaxed);
        super::LAST_SELECTED_ROWS_ESTIMATE.store(0, std::sync::atomic::Ordering::Relaxed);

        // Use the pruning test to trigger access plan creation
        test_parquet_pruning_reduces_scans();

        // Assert counters were updated
        assert!(
            super::get_files_with_access_plan() > 0,
            "files_with_access_plan should be incremented"
        );
        assert_eq!(
            super::get_last_selected_rows_estimate(),
            2,
            "selected_rows_estimate should match selected bits"
        );
    }
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn make_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
        Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema))
    }

    #[test]
    fn test_apply_combined_filter_both_none_returns_input() {
        let exec = make_exec();
        let schema = exec.schema();
        let schema_ref: arrow_schema::Schema = schema.as_ref().clone();
        let result = apply_combined_filter(None, None, &schema_ref, &[], exec.clone(), None);
        assert!(result.is_ok());
        // should return the same plan (EmptyExec) since no filters
        let plan = result.unwrap();
        assert_eq!(plan.name(), "EmptyExec");
    }

    #[test]
    fn test_apply_projection_empty_diff_rules_returns_input() {
        use hashbrown::HashMap;

        let exec = make_exec();
        let schema = exec.schema();
        let diff_rules: HashMap<String, DataType> = HashMap::new();
        let result = apply_projection(&schema, &diff_rules, None, exec.clone());
        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.name(), "EmptyExec");
    }

    #[test]
    fn test_apply_combined_filter_timestamp_only_wraps_filter() {
        use arrow_schema::{Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("col", DataType::Utf8, false),
        ]));
        let exec = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        )) as Arc<dyn ExecutionPlan>;
        let schema_ref: arrow_schema::Schema = schema.as_ref().clone();
        let result = apply_combined_filter(None, Some((1000, 2000)), &schema_ref, &[], exec, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name(), "FilterExec");
    }

    #[test]
    fn test_apply_projection_with_diff_rules_produces_projection() {
        use arrow_schema::{Field, Schema};
        use hashbrown::HashMap;
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
        let exec = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        )) as Arc<dyn ExecutionPlan>;
        let mut diff_rules: HashMap<String, DataType> = HashMap::new();
        diff_rules.insert("col".to_string(), DataType::LargeUtf8);
        let result = apply_projection(&schema, &diff_rules, None, exec);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name(), "ProjectionExec");
    }

    #[test]
    fn test_compute_selection_stats_simple() {
        use config::meta::bitvec::BitVec;

        let num_rows: usize = 10;
        let mut bv = BitVec::repeat(false, num_rows);
        // set a couple of selected rows
        bv.set(2, true);
        bv.set(7, true);

        let (row_group_count, selected_row_groups, selected_bits) =
            compute_selection_stats(num_rows, &bv);

        assert_eq!(row_group_count, 1);
        assert_eq!(selected_row_groups, 1);
        assert_eq!(selected_bits, 2);
    }

    #[tokio::test]
    async fn test_generate_access_plan_from_file_list() {
        use config::meta::{bitvec::BitVec, stream::FileKey};
        use datafusion::{
            common::{Statistics, stats::Precision},
            datasource::{listing::PartitionedFile, physical_plan::parquet::ParquetAccessPlan},
        };

        let trace_id = "trace_for_test";
        let schema_key = "schemaA";
        let filename = "file1.parquet";
        let num_rows: usize = 10;

        // prepare segment bitvec with two selected rows
        let mut bv = BitVec::repeat(false, num_rows);
        bv.set(2, true);
        bv.set(7, true);

        // create FileKey and register it in the storage file list
        let mut fk = FileKey::new(
            1,
            "".to_string(),
            filename.to_string(),
            Default::default(),
            false,
        );
        fk.with_segment_ids(bv.clone());
        storage::file_list::set(trace_id, schema_key, "parquet", vec![fk]).await;

        // build a PartitionedFile whose path matches the storage key format
        let path = format!(
            "{}/schema={}/format=parquet/$$/{}",
            trace_id, schema_key, filename
        );
        let mut pf = PartitionedFile::new(path, 0u64);
        pf.statistics = Some(Arc::new(
            Statistics::default().with_num_rows(Precision::Exact(num_rows)),
        ));

        // generate access plan and assert it is a ParquetAccessPlan
        let ap_opt = generate_access_plan(&pf);
        assert!(ap_opt.is_some());
        let ap = ap_opt.unwrap();
        assert!(ap.as_ref().downcast_ref::<ParquetAccessPlan>().is_some());

        // verify selection stats match expectations
        let (rg_count, selected_rg_count, selected_bits) = compute_selection_stats(num_rows, &bv);
        assert_eq!(selected_bits, 2);
        assert_eq!(selected_rg_count, 1);
        // basic sanity: at least one row group exists
        assert!(rg_count >= 1);
    }

    #[tokio::test]
    async fn test_memtable_parquet_parity() {
        use arrow::{
            array::{Array, ArrayRef, StringArray},
            record_batch::RecordBatch,
        };
        use arrow_schema::{Field, Schema};
        use config::meta::{bitvec::BitVec, stream::FileKey};
        use datafusion::{
            common::{Statistics, stats::Precision},
            datasource::{listing::PartitionedFile, physical_plan::parquet::ParquetAccessPlan},
            physical_plan::ColumnarValue,
            prelude::{col, lit},
            scalar::ScalarValue,
        };

        // Build a tiny in-memory RecordBatch (memtable row set)
        let values = vec!["a", "b", "a", "c", "a"];
        let arr: ArrayRef = Arc::new(StringArray::from(values));
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr.clone()]).unwrap();
        let num_rows = batch.num_rows();

        // Create a supported logical predicate: col = 'a'
        let df_expr = col("col").eq(lit("a"));

        // Duplicate conservative logical->physical conversion used by the memtable fast-path.
        fn logical_to_physical(
            expr: &datafusion::logical_expr::Expr,
            schema: &arrow_schema::Schema,
        ) -> std::result::Result<Arc<dyn PhysicalExpr>, datafusion::common::DataFusionError>
        {
            use datafusion::logical_expr::Expr as E;
            match expr {
                E::Column(c) => {
                    let idx = schema.index_of(&c.name).map_err(|e| {
                        datafusion::common::DataFusionError::Execution(e.to_string())
                    })?;
                    Ok(Arc::new(Column::new(&c.name, idx)))
                }
                E::Literal(sv, _opt) => Ok(Arc::new(Literal::new(sv.clone()))),
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
                    Ok(Arc::new(
                        datafusion::physical_plan::expressions::NotExpr::new(arg),
                    ))
                }
                E::Alias(a) => logical_to_physical(&a.expr, schema),
                E::InList(il) => {
                    let left = logical_to_physical(&il.expr, schema)?;
                    let mut vals: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(il.list.len());
                    for e in &il.list {
                        let v = logical_to_physical(e, schema)?;
                        vals.push(v);
                    }
                    Ok(Arc::new(
                        datafusion::physical_plan::expressions::InListExpr::try_new(
                            left, vals, il.negated, schema,
                        )?,
                    ))
                }
                _ => Err(datafusion::common::DataFusionError::Execution(format!(
                    "unsupported expr for physical conversion: {expr:?}"
                ))),
            }
        }

        // Convert to physical and evaluate against the RecordBatch
        let phys = logical_to_physical(&df_expr, batch.schema().as_ref()).unwrap();
        let cv = phys.evaluate(&batch).unwrap();

        // Build a BitVec representing selected rows (memtable selection)
        let mut bv = BitVec::repeat(false, num_rows);
        match cv {
            ColumnarValue::Array(arr) => {
                let bool_arr = arr
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .expect("expected boolean array");
                for i in 0..bool_arr.len() {
                    if bool_arr.is_valid(i) && bool_arr.value(i) {
                        bv.set(i, true);
                    }
                }
            }
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::Boolean(Some(true)) => {
                    for i in 0..num_rows {
                        bv.set(i, true);
                    }
                }
                _ => {}
            },
        }

        // Register the FileKey with the same BitVec and generate an access plan
        let trace_id = "trace_parity_test";
        let schema_key = "schemaParity";
        let filename = "fparquet.parquet";

        let mut fk = FileKey::new(
            1,
            "".to_string(),
            filename.to_string(),
            Default::default(),
            false,
        );
        fk.with_segment_ids(bv.clone());
        storage::file_list::set(trace_id, schema_key, "parquet", vec![fk]).await;

        let path = format!(
            "{}/schema={}/format=parquet/$$/{}",
            trace_id, schema_key, filename
        );
        let mut pf = PartitionedFile::new(path, 0u64);
        pf.statistics = Some(Arc::new(
            Statistics::default().with_num_rows(Precision::Exact(num_rows)),
        ));

        let ap_opt = generate_access_plan(&pf);
        assert!(ap_opt.is_some());
        let ap_any = ap_opt.unwrap();
        let ap = ap_any
            .as_ref()
            .downcast_ref::<ParquetAccessPlan>()
            .expect("expected ParquetAccessPlan");

        // Compare selection stats between computed BitVec and the produced plan
        let (rg_count, selected_rg_count, _selected_bits_count) =
            compute_selection_stats(num_rows, &bv);
        // count scanned row groups in the plan
        let mut plan_selected_rg = 0usize;
        for i in 0..rg_count {
            if ap.should_scan(i) {
                plan_selected_rg += 1;
            }
        }

        assert_eq!(selected_rg_count, plan_selected_rg);
    }

    #[tokio::test]
    async fn test_parquet_pruning_reduces_scans() {
        use config::meta::{bitvec::BitVec, stream::FileKey};
        use datafusion::{
            common::{Statistics, stats::Precision},
            datasource::{listing::PartitionedFile, physical_plan::parquet::ParquetAccessPlan},
        };

        // Build a sparse selection across multiple row groups.
        let row_group_count: usize = 4;
        let num_rows: usize = PARQUET_MAX_ROW_GROUP_SIZE * row_group_count;

        let mut bv = BitVec::repeat(false, num_rows);
        // select a row in group 1 and group 3
        bv.set(PARQUET_MAX_ROW_GROUP_SIZE * 1 + 10, true);
        bv.set(PARQUET_MAX_ROW_GROUP_SIZE * 3 + 20, true);

        // register in storage file list
        let filename = "prune_test.parquet";
        let mut fk = FileKey::new(
            1,
            "".to_string(),
            filename.to_string(),
            Default::default(),
            false,
        );
        fk.with_segment_ids(bv.clone());
        storage::file_list::set("trace_prune", "schemaP", "parquet", vec![fk]).await;

        let path = format!(
            "trace_prune/schema={}/format=parquet/$$/{}",
            "schemaP", filename
        );
        let mut pf = PartitionedFile::new(path, 0u64);
        pf.statistics = Some(Arc::new(
            Statistics::default().with_num_rows(Precision::Exact(num_rows)),
        ));

        let ap_opt = generate_access_plan(&pf);
        assert!(ap_opt.is_some());
        let ap_any = ap_opt.unwrap();
        let ap = ap_any
            .as_ref()
            .downcast_ref::<ParquetAccessPlan>()
            .expect("expected ParquetAccessPlan");

        let (rg_count, selected_rg_count, _selected_bits_count) =
            compute_selection_stats(num_rows, &bv);
        assert_eq!(rg_count, row_group_count);
        assert_eq!(selected_rg_count, 2);

        let plan_selected_rg = (0..rg_count).filter(|&i| ap.should_scan(i)).count();
        assert_eq!(plan_selected_rg, selected_rg_count);
        assert!(
            plan_selected_rg < rg_count,
            "pruning should reduce scanned row groups"
        );
    }
}
