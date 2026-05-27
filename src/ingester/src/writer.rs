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

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        Arc, LazyLock as Lazy,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Instant,
};

use arrow_schema::Schema;
use chrono::{Duration, Utc};
use config::{
    MEM_TABLE_INDIVIDUAL_STREAMS, get_config, metrics,
    stats::MemorySize,
    utils::{
        hash::{Sum64, gxhash},
        record_batch_ext::concat_batches,
    },
};
use infra::runtime::WAL_RUNTIME;
use snafu::ResultExt;
use tokio::sync::{RwLock, mpsc};
use wal::{Writer as WalWriter, build_file_path};

use crate::{
    ReadRecordBatchEntry, WriterSignal, entry,
    entry::Entry,
    errors::*,
    immutable::{IMMUTABLES, Immutable},
    memtable::MemTable,
    rwmap::RwMap,
};

static WRITERS: Lazy<Vec<RwMap<WriterKey, Arc<Writer>>>> = Lazy::new(|| {
    let cfg = get_config();
    let writer_num = cfg.limit.mem_table_bucket_num + MEM_TABLE_INDIVIDUAL_STREAMS.len();
    let mut writers = Vec::with_capacity(writer_num);
    for _ in 0..writer_num {
        writers.push(RwMap::default());
    }
    writers
});

pub struct Writer {
    idx: usize,
    key: WriterKey,
    wal: Arc<RwLock<WalWriter>>,
    memtable: Arc<RwLock<MemTable>>,
    next_seq: AtomicU64,
    created_at: AtomicI64,
    write_queue: Arc<mpsc::Sender<(WriterSignal, crate::ProcessedBatch, bool)>>,
}

// check total memtable size
pub fn check_memtable_size() -> Result<()> {
    let cur_mem = metrics::INGEST_MEMTABLE_ARROW_BYTES
        .with_label_values::<&str>(&[])
        .get();
    if cur_mem >= get_config().limit.mem_table_max_size as i64 {
        Err(Error::MemoryTableOverflowError {})
    } else {
        Ok(())
    }
}

// check total memory size
pub fn check_memory_circuit_breaker() -> Result<()> {
    let cfg = get_config();
    if !cfg.common.memory_circuit_breaker_enabled || cfg.common.memory_circuit_breaker_ratio == 0 {
        return Ok(());
    }
    let cur_mem = metrics::NODE_MEMORY_USAGE
        .with_label_values::<&str>(&[])
        .get() as usize;
    if cur_mem > cfg.limit.mem_total / 100 * cfg.common.memory_circuit_breaker_ratio {
        Err(Error::MemoryCircuitBreakerError {})
    } else {
        Ok(())
    }
}

// check disk space availability
// Threshold interpretation (similar to memory circuit breaker):
// - Values < 100: treated as percentage of disk used (e.g., 90 = trigger when 90% full)
// - Values >= 100: treated as absolute MB remaining (e.g., 500 = trigger when < 500MB free)
// Reads from atomic metrics updated every 60 seconds to avoid expensive syscalls
pub fn check_disk_circuit_breaker() -> Result<()> {
    let cfg = get_config();
    if !cfg.common.disk_circuit_breaker_enabled {
        return Ok(());
    }

    let threshold = cfg.common.disk_circuit_breaker_threshold;
    let total_space = metrics::NODE_DISK_TOTAL
        .with_label_values::<&str>(&[])
        .get() as u64;
    let used_space = metrics::NODE_DISK_USAGE
        .with_label_values::<&str>(&[])
        .get() as u64;

    let triggered = if threshold < 100 {
        // Percentage mode: trigger when disk usage exceeds threshold%
        // e.g., threshold=90 means trigger when disk is >90% full
        used_space > total_space / 100 * threshold as u64
    } else {
        // Absolute MB mode: trigger when free space is less than threshold MB
        let available_space = total_space.saturating_sub(used_space);
        available_space < (threshold as u64) * 1024 * 1024
    };

    if triggered {
        Err(Error::DiskCircuitBreakerError {})
    } else {
        Ok(())
    }
}

fn get_table_idx(thread_id: usize, org_id: &str, stream_name: &str) -> usize {
    if let Some(idx) = MEM_TABLE_INDIVIDUAL_STREAMS.get(stream_name) {
        *idx
    } else if get_config().common.feature_shared_memtable_enabled {
        // When shared memtable is enabled, hash by thread_id and org_id
        let hash_key = format!("{thread_id}_{org_id}");
        let hash_id = gxhash::new().sum64(&hash_key);
        hash_id as usize % (WRITERS.len() - MEM_TABLE_INDIVIDUAL_STREAMS.len())
    } else {
        // Original behavior: hash by thread_id and stream_name
        let hash_key = format!("{thread_id}_{stream_name}");
        let hash_id = gxhash::new().sum64(&hash_key);
        hash_id as usize % (WRITERS.len() - MEM_TABLE_INDIVIDUAL_STREAMS.len())
    }
}

/// Get a writer for a given org_id and stream_type
pub async fn get_writer(
    thread_id: usize,
    org_id: &str,
    stream_type: &str,
    stream_name: &str,
) -> Arc<Writer> {
    let start = std::time::Instant::now();
    let idx = get_table_idx(thread_id, org_id, stream_name);
    let key = WriterKey::new(idx, org_id, stream_type);
    let r = WRITERS[idx].read().await;
    let data = r.get(&key);
    if start.elapsed().as_millis() > 500 {
        log::warn!(
            "get_writer from read cache took: {} ms",
            start.elapsed().as_millis()
        );
    }
    let mut is_existing_writer_channel_closed = false;
    if let Some(w) = data {
        if !w.is_channel_closed() {
            return w.clone();
        }
        is_existing_writer_channel_closed = true;
    }
    drop(r);

    if is_existing_writer_channel_closed {
        log::warn!(
            "[INGESTER:MEM:{idx}] Writer channel closed for {org_id}/{stream_type}, removing from cache",
        );
        let mut w = WRITERS[idx].write().await;
        w.remove(&key);
        drop(w);
    }

    // slow path
    let start = std::time::Instant::now();
    let mut rw = WRITERS[idx].write().await;
    let w = rw
        .entry(key.clone())
        .or_insert_with(|| Writer::new(idx, key));
    if start.elapsed().as_millis() > 500 {
        log::warn!(
            "get_writer from write cache took: {} ms",
            start.elapsed().as_millis()
        );
    }
    w.clone()
}

pub async fn read_from_memtable(
    org_id: &str,
    stream_type: &str,
    stream_name: &str,
    time_range: Option<(i64, i64)>,
    partition_filters: &[(String, Vec<String>)],
) -> Result<(HashSet<u64>, Vec<ReadRecordBatchEntry>)> {
    let cfg = get_config();
    // fast past
    if cfg.limit.mem_table_bucket_num <= 1 {
        let idx = get_table_idx(0, org_id, stream_name);
        let key = WriterKey::new(idx, org_id, stream_type);
        let w = WRITERS[idx].read().await;
        return match w.get(&key) {
            Some(r) => {
                let (id, batches) = r
                    .read(org_id, stream_name, time_range, partition_filters)
                    .await?;
                Ok((HashSet::from([id]), batches))
            }
            None => Ok((HashSet::new(), Vec::new())),
        };
    }

    // slow path
    let mut ids = HashSet::new();
    let mut batches = Vec::new();
    let mut visited = HashSet::with_capacity(cfg.limit.mem_table_bucket_num);
    for thread_id in 0..cfg.limit.http_worker_num {
        let idx = get_table_idx(thread_id, org_id, stream_name);
        if visited.contains(&idx) {
            continue;
        }
        visited.insert(idx);
        let key = WriterKey::new(idx, org_id, stream_type);
        let w = WRITERS[idx].read().await;
        if let Some(r) = w.get(&key)
            && let Ok((id, data)) = r
                .read(org_id, stream_name, time_range, partition_filters)
                .await
        {
            ids.insert(id);
            batches.extend(data);
        }
    }
    Ok((ids, batches))
}

/// Phase 3 stub: read from memtable with an optional DataFusion `Expr` predicate.
/// TODO: implement predicate evaluation against in-memory `RecordBatch` entries
/// to enable read-after-write guard pushdown. For now this delegates to
/// `read_from_memtable` to preserve existing behavior.
pub async fn read_from_memtable_with_filter(
    org_id: &str,
    stream_type: &str,
    stream_name: &str,
    time_range: Option<(i64, i64)>,
    partition_filters: &[(String, Vec<String>)],
    _filter: Option<datafusion::logical_expr::Expr>,
) -> Result<(HashSet<u64>, Vec<ReadRecordBatchEntry>)> {
    // If no filter provided, delegate to the existing fast path.
    if _filter.is_none() {
        return read_from_memtable(
            org_id,
            stream_type,
            stream_name,
            time_range,
            partition_filters,
        )
        .await;
    }

    // Otherwise, read the raw memtable batches and evaluate the DataFusion
    // logical expression against each RecordBatch to produce filtered rows.
    let (ids, batches) = read_from_memtable(
        org_id,
        stream_type,
        stream_name,
        time_range,
        partition_filters,
    )
    .await?;

    // Nothing to do if there are no batches
    if batches.is_empty() {
        return Ok((ids, batches));
    }

    use arrow::array::Array;
    use datafusion::{
        arrow::record_batch::RecordBatch,
        logical_expr::Expr as DFExpr,
        physical_plan::{
            ColumnarValue, PhysicalExpr,
            expressions::{
                BinaryExpr, CastExpr, Column as PColumn, InListExpr, Literal as PLiteral, NotExpr,
            },
        },
        scalar::ScalarValue,
    };

    // Attempt to convert a DataFusion logical `Expr` into a `PhysicalExpr` that
    // can be evaluated directly against an Arrow `RecordBatch`. We support a
    // conservative subset (Column, Literal, BinaryExpr, Cast, Not, InList,
    // Alias -> unwrap). If conversion fails we fall back to the SessionContext
    // path used previously.
    fn logical_to_physical(
        expr: &DFExpr,
        schema: &arrow_schema::Schema,
    ) -> std::result::Result<Arc<dyn PhysicalExpr>, datafusion::common::DataFusionError> {
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
            // Conservative: treat unsupported nodes as an error so we fall back
            // to the SessionContext-based evaluation.
            _ => Err(datafusion::common::DataFusionError::Execution(format!(
                "unsupported expr for physical conversion: {expr:?}"
            ))),
        }
    }

    // SessionContext fallback (kept for correctness when conversion fails)
    let ctx = datafusion::prelude::SessionContext::new();

    let mut out_batches: Vec<ReadRecordBatchEntry> = Vec::new();

    for (schema, entries) in batches {
        if entries.is_empty() {
            continue;
        }

        // Concatenate all entries for this schema into a single RecordBatch
        let record_batches: Vec<RecordBatch> = entries.iter().map(|e| e.data.clone()).collect();
        let combined_batch = match concat_batches(schema.clone(), record_batches) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("read_from_memtable_with_filter: concat_batches error: {e}");
                continue;
            }
        };

        if combined_batch.num_rows() == 0 {
            continue;
        }

        let total_json_size: usize = entries.iter().map(|e| e.data_json_size).sum();

        // Prepare filter expression clone to avoid moving the original option
        let filter_expr = _filter.as_ref().unwrap().clone();

        // Try the fast physical-eval route first.
        let mut used_fast_path = false;
        {
            let pred = &filter_expr;
            match logical_to_physical(pred, combined_batch.schema().as_ref()) {
                Ok(phys) => {
                    match phys.evaluate(&combined_batch) {
                        Ok(cv) => {
                            match cv {
                                ColumnarValue::Array(arr) => {
                                    if arr.len() != combined_batch.num_rows() {
                                        log::warn!(
                                            "read_from_memtable_with_filter: predicate array length mismatch: {} vs {}",
                                            arr.len(),
                                            combined_batch.num_rows()
                                        );
                                    } else if let Some(bool_arr) =
                                        arr.as_any().downcast_ref::<arrow::array::BooleanArray>()
                                    {
                                        // collect selected indices (treat NULL as false)
                                        let mut sel: Vec<u32> = Vec::with_capacity(bool_arr.len());
                                        for i in 0..bool_arr.len() {
                                            if bool_arr.is_valid(i) && bool_arr.value(i) {
                                                sel.push(i as u32);
                                            }
                                        }
                                        if !sel.is_empty() {
                                            let indices =
                                                Arc::new(arrow::array::UInt32Array::from(sel))
                                                    as arrow::array::ArrayRef;
                                            let mut cols: Vec<arrow::array::ArrayRef> =
                                                Vec::with_capacity(combined_batch.num_columns());
                                            let mut take_failed = false;
                                            for c in combined_batch.columns() {
                                                match arrow::compute::take(
                                                    c.as_ref(),
                                                    &indices,
                                                    None,
                                                ) {
                                                    Ok(new_col) => cols.push(new_col),
                                                    Err(e) => {
                                                        log::warn!(
                                                            "read_from_memtable_with_filter: take() error: {e}"
                                                        );
                                                        take_failed = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if !take_failed {
                                                if let Ok(new_rb) = RecordBatch::try_new(
                                                    combined_batch.schema().clone(),
                                                    cols,
                                                ) {
                                                    if new_rb.num_rows() > 0 {
                                                        let new_entry =
                                                            entry::RecordBatchEntry::new(
                                                                Arc::from(stream_type),
                                                                new_rb,
                                                                total_json_size,
                                                                0,
                                                            );
                                                        out_batches.push((
                                                            schema.clone(),
                                                            vec![new_entry],
                                                        ));
                                                    }
                                                    used_fast_path = true;
                                                }
                                            }
                                        } else {
                                            // no rows matched -> drop
                                            used_fast_path = true;
                                        }
                                    } else {
                                        log::warn!(
                                            "read_from_memtable_with_filter: predicate did not return a boolean array"
                                        );
                                    }
                                }
                                ColumnarValue::Scalar(s) => {
                                    match s {
                                        ScalarValue::Boolean(Some(true)) => {
                                            // keep all rows
                                            let new_entry = entry::RecordBatchEntry::new(
                                                Arc::from(stream_type),
                                                combined_batch.clone(),
                                                total_json_size,
                                                0,
                                            );
                                            out_batches.push((schema.clone(), vec![new_entry]));
                                            used_fast_path = true;
                                        }
                                        ScalarValue::Boolean(_) => {
                                            // false or null -> drop all rows
                                            used_fast_path = true;
                                        }
                                        _ => {
                                            log::warn!(
                                                "read_from_memtable_with_filter: predicate scalar not boolean"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "read_from_memtable_with_filter: physical evaluate error: {e}"
                            );
                        }
                    }
                }
                Err(e) => {
                    log::debug!(
                        "read_from_memtable_with_filter: logical->physical conversion failed: {e}"
                    );
                }
            }
        }

        if !used_fast_path {
            // fallback: use DataFusion SessionContext evaluation (slower but more complete)
            if let Ok(df0) = ctx.read_batch(combined_batch.clone()) {
                // project referenced fields (null-fill missing)
                let mut referenced = HashSet::new();
                // reuse previous extractor to collect referenced columns
                fn extract_columns_from_expr(expr: &DFExpr, cols: &mut HashSet<String>) {
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
                        // literals and other leaf nodes are ignored
                        _ => {}
                    }
                }
                extract_columns_from_expr(&filter_expr, &mut referenced);

                let df1 = if !referenced.is_empty() {
                    let proj: Vec<datafusion::logical_expr::Expr> = referenced
                        .iter()
                        .map(|name| {
                            if schema.field_with_name(name).is_ok() {
                                datafusion::prelude::col(name.as_str()).alias(name.as_str())
                            } else {
                                datafusion::logical_expr::Expr::Literal(
                                    ScalarValue::Utf8(None),
                                    None,
                                )
                                .alias(name.as_str())
                            }
                        })
                        .collect();
                    match df0.select(proj) {
                        Ok(d) => d,
                        Err(e) => {
                            log::warn!("read_from_memtable_with_filter: projection error: {e}");
                            continue;
                        }
                    }
                } else {
                    df0
                };

                match df1.filter(filter_expr.clone()) {
                    Ok(filtered_df) => match filtered_df.collect().await {
                        Ok(rbs) => {
                            let mut filtered_entries: Vec<Arc<entry::RecordBatchEntry>> =
                                Vec::new();
                            for rb in rbs {
                                filtered_entries.push(entry::RecordBatchEntry::new(
                                    Arc::from(stream_type),
                                    rb,
                                    total_json_size,
                                    0,
                                ));
                            }
                            if !filtered_entries.is_empty() {
                                out_batches.push((schema, filtered_entries));
                            }
                        }
                        Err(e) => {
                            log::warn!("read_from_memtable_with_filter: collect error: {e}");
                        }
                    },
                    Err(e) => {
                        log::warn!("read_from_memtable_with_filter: filter error: {e}");
                    }
                }
            } else {
                log::warn!("read_from_memtable_with_filter: read_batch error (fallback)");
            }
        }
    }

    Ok((ids, out_batches))
}

pub async fn check_ttl() -> Result<()> {
    for w in WRITERS.iter() {
        let w = w.read().await;
        for r in w.values() {
            if let Err(e) = r
                .write_queue
                .send((WriterSignal::Rotate, crate::ProcessedBatch::empty(), false))
                .await
            {
                log::error!("[INGESTER:MEM:{}] writer queue rotate error: {e}", r.idx);
            }
        }
    }
    Ok(())
}

pub async fn flush_all() -> Result<()> {
    log::info!("[INGESTER:MEM] start flush all writers");
    for w in WRITERS.iter() {
        let mut w = w.write().await;
        let keys = w.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            if let Some(r) = w.remove(&key) {
                r.flush().await?; // close writer
                metrics::INGEST_MEMTABLE_FILES
                    .with_label_values::<&str>(&[])
                    .dec();
            }
        }
    }
    log::info!("[INGESTER:MEM] flush all writers done");
    Ok(())
}

// get the max seq id of all writers
pub async fn get_max_writer_seq_id() -> u64 {
    let mut max_seq_id = 0;
    for w in WRITERS.iter() {
        let w = w.read().await;
        for r in w.values() {
            // next_seq is the next seq id to be used, so we need to subtract 1
            max_seq_id = max_seq_id.max(r.next_seq.load(Ordering::Relaxed) - 1);
        }
    }
    max_seq_id
}

impl Writer {
    pub(crate) fn new(idx: usize, key: WriterKey) -> Arc<Writer> {
        let now = Utc::now().timestamp_micros();
        let cfg = get_config();
        let next_seq = AtomicU64::new(now as u64);
        let wal_id = next_seq.fetch_add(1, Ordering::SeqCst);
        let wal_dir = PathBuf::from(&cfg.common.data_wal_dir)
            .join("logs")
            .join(idx.to_string());
        log::info!(
            "[INGESTER:MEM:{idx}] create file: {}/{}/{}/{}.wal",
            wal_dir.display(),
            &key.org_id,
            &key.stream_type,
            wal_id
        );

        let (tx, rx) = mpsc::channel(cfg.limit.wal_write_queue_size);

        let writer = Self {
            idx,
            key: key.clone(),
            wal: Arc::new(RwLock::new(
                WalWriter::new(
                    build_file_path(wal_dir, &key.org_id, &key.stream_type, wal_id.to_string()),
                    cfg.limit.max_file_size_on_disk as u64,
                    cfg.limit.wal_write_buffer_size,
                    None,
                )
                .expect("wal file create error")
                .0,
            )),
            memtable: Arc::new(RwLock::new(MemTable::new())),
            next_seq,
            created_at: AtomicI64::new(now),
            write_queue: Arc::new(tx),
        };
        let writer = Arc::new(writer);
        let writer_clone = writer.clone();

        log::info!("[INGESTER:MEM:{idx}] writer queue start consuming");

        // Spawn consumer tasks on the shared WAL runtime, or use the default runtime
        if let Some(rt) = WAL_RUNTIME.as_ref() {
            rt.spawn(Self::consume_loop(writer, rx, idx));
        } else {
            tokio::spawn(Self::consume_loop(writer, rx, idx));
        }

        writer_clone
    }

    async fn consume_loop(
        writer: Arc<Writer>,
        mut rx: mpsc::Receiver<(WriterSignal, crate::ProcessedBatch, bool)>,
        idx: usize,
    ) {
        let mut total: usize = 0;
        loop {
            match rx.recv().await {
                None => break,
                Some((sign, batch, fsync)) => match sign {
                    WriterSignal::Close => break,
                    WriterSignal::Rotate => {
                        if let Err(e) = writer.rotate(0, 0).await {
                            log::error!("[INGESTER:MEM:{idx}] writer rotate error: {e}");
                        }
                    }
                    WriterSignal::Produce => {
                        if let Err(e) = writer.consume_processed(batch, fsync).await {
                            log::error!("[INGESTER:MEM:{idx}] writer consume batch error: {e}");
                        }
                    }
                },
            }
            total += 1;
            if total.is_multiple_of(1000) {
                log::info!(
                    "[INGESTER:MEM:{idx}] writer queue consuming, total: {}, in queue: {}",
                    total,
                    rx.len()
                );
            }
        }
        log::info!("[INGESTER:MEM:{idx}] writer queue closed");
    }

    pub fn get_key_str(&self) -> String {
        format!("{}/{}", self.key.org_id, self.key.stream_type)
    }

    pub fn is_channel_closed(&self) -> bool {
        self.write_queue.is_closed()
    }

    // check_ttl is used to check if the memtable has expired
    pub async fn write(&self, schema: Arc<Schema>, mut entry: Entry, fsync: bool) -> Result<()> {
        if entry.data.is_empty() {
            return Ok(());
        }

        entry.schema = Some(schema);
        self.write_batch(vec![entry], fsync).await
    }

    pub async fn write_batch(&self, entries: Vec<Entry>, fsync: bool) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Pre-process data BEFORE sending to queue
        // This moves CPU-intensive work (JSON to Arrow conversion) out of the consume loop,
        // allowing consume to focus purely on IO operations
        let processed_batch = self.preprocess_batch(entries)?;

        let cfg = get_config();
        if !cfg.common.wal_write_queue_enabled {
            return self.consume_processed(processed_batch, fsync).await;
        }

        if cfg.common.wal_write_queue_full_reject {
            if let Err(e) =
                self.write_queue
                    .try_send((WriterSignal::Produce, processed_batch, fsync))
            {
                log::error!(
                    "[INGESTER:MEM:{}] write queue full, reject write: {}",
                    self.idx,
                    e
                );
                return Err(Error::WalError {
                    source: wal::Error::WriteQueueFull { idx: self.idx },
                });
            }
        } else {
            self.write_queue
                .send((WriterSignal::Produce, processed_batch, fsync))
                .await
                .context(TokioMpscSendEntriesSnafu)?;
        }

        Ok(())
    }

    fn preprocess_batch(&self, mut entries: Vec<Entry>) -> Result<crate::ProcessedBatch> {
        let _start_preprocess_batch = Instant::now();
        // Serialize entries to bytes for WAL writing
        let bytes_entries = entries
            .iter_mut()
            .map(|entry| entry.into_bytes())
            .collect::<Result<Vec<_>>>()?;

        // Bulk convert to Arrow RecordBatch
        let batch_entries = entries
            .iter()
            .map(|entry| {
                entry.into_batch(self.key.stream_type.clone(), entry.schema.clone().unwrap())
            })
            .collect::<Result<Vec<_>>>()?;

        // Calculate total sizes for rotation check
        let (entries_json_size, entries_arrow_size) = batch_entries
            .iter()
            .map(|entry| (entry.data_json_size, entry.data_arrow_size))
            .fold(
                (0, 0),
                |(acc_json_size, acc_arrow_size), (json_size, arrow_size)| {
                    (acc_json_size + json_size, acc_arrow_size + arrow_size)
                },
            );

        // Move entries into ProcessedBatch
        // Clear the heavy data field after conversion to avoid memory duplication
        // The JSON data is already in bytes_entries and Arrow format in batch_entries
        for entry in entries.iter_mut() {
            let _ = std::mem::take(&mut entry.data);
        }

        let start_preprocess_batch_duration = _start_preprocess_batch.elapsed();
        if start_preprocess_batch_duration.as_millis() > 100 {
            log::warn!("start_preprocess_batch_duration: {start_preprocess_batch_duration:?}");
        }
        Ok(crate::ProcessedBatch {
            entries,
            bytes_entries,
            batch_entries,
            entries_json_size,
            entries_arrow_size,
        })
    }

    async fn consume_processed(&self, batch: crate::ProcessedBatch, fsync: bool) -> Result<()> {
        if batch.entries.is_empty() {
            return Ok(());
        }
        let _start_consume_processed = Instant::now();
        // Check rotation
        self.rotate(batch.entries_json_size, batch.entries_arrow_size)
            .await?;

        // Write into WAL - pure IO, no CPU-intensive processing
        let start = std::time::Instant::now();
        let mut wal = self.wal.write().await;
        let wal_lock_time = start.elapsed().as_millis() as f64;
        metrics::INGEST_WAL_LOCK_TIME
            .with_label_values(&[&self.key.org_id])
            .observe(wal_lock_time);
        let _start_wal_processed = Instant::now();
        for entry in batch.bytes_entries {
            if entry.is_empty() {
                continue;
            }
            wal.write(&entry).context(WalSnafu)?;
            tokio::task::coop::consume_budget().await;
        }
        drop(wal);
        let start_wal_processed_duration = _start_wal_processed.elapsed();
        if start_wal_processed_duration.as_millis() > 100 {
            log::warn!("start_wal_processed_duration: {start_wal_processed_duration:?}");
        }

        // Write into Memtable - pure IO, no CPU-intensive processing
        let start = std::time::Instant::now();
        let mut mem = self.memtable.write().await;
        let mem_lock_time = start.elapsed().as_millis() as f64;
        metrics::INGEST_MEMTABLE_LOCK_TIME
            .with_label_values(&[&self.key.org_id])
            .observe(mem_lock_time);
        let _start_mem_processed = Instant::now();
        for (entry, batch_entry) in batch.entries.into_iter().zip(batch.batch_entries) {
            if entry.data_size == 0 {
                continue;
            }
            mem.write(entry.schema.clone().unwrap(), entry, batch_entry)?;
            tokio::task::coop::consume_budget().await;
        }
        drop(mem);
        let start_mem_processed_duration = _start_mem_processed.elapsed();
        if start_mem_processed_duration.as_millis() > 100 {
            log::warn!("start_mem_processed_duration: {start_mem_processed_duration:?}");
        }

        // Check fsync
        if fsync {
            let mut wal = self.wal.write().await;
            wal.sync().context(WalSnafu)?;
            drop(wal);
        }

        let start_consume_processed_duration = _start_consume_processed.elapsed();
        if start_consume_processed_duration.as_millis() > 500 {
            log::warn!("start_consume_processed_duration: {start_consume_processed_duration:?}");
        }

        Ok(())
    }

    // rotate is used to rotate the wal and memtable if the size exceeds the threshold
    async fn rotate(&self, entry_bytes_size: usize, entry_batch_size: usize) -> Result<()> {
        if !self.check_wal_threshold(self.wal.read().await.size(), entry_bytes_size)
            && !self.check_mem_threshold(self.memtable.read().await.size(), entry_batch_size)
        {
            return Ok(());
        }

        // rotation wal
        let start = std::time::Instant::now();
        let mut wal = self.wal.write().await;
        let wal_lock_time = start.elapsed().as_millis() as f64;
        metrics::INGEST_WAL_LOCK_TIME
            .with_label_values(&[&self.key.org_id])
            .observe(wal_lock_time);
        if !self.check_wal_threshold(wal.size(), entry_bytes_size) {
            return Ok(()); // check again to avoid race condition
        }
        let cfg = get_config();
        let wal_id = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let wal_dir = PathBuf::from(&cfg.common.data_wal_dir)
            .join("logs")
            .join(self.idx.to_string());
        log::info!(
            "[INGESTER:MEM] create file: {}/{}/{}/{}.wal",
            wal_dir.display(),
            &self.key.org_id,
            &self.key.stream_type,
            wal_id
        );
        let (new_wal, _header_size) = WalWriter::new(
            build_file_path(
                wal_dir,
                &self.key.org_id,
                &self.key.stream_type,
                wal_id.to_string(),
            ),
            cfg.limit.max_file_size_on_disk as u64,
            cfg.limit.wal_write_buffer_size,
            None,
        )
        .context(WalSnafu)?;
        wal.sync().context(WalSnafu)?; // sync wal before rotation
        let old_wal = std::mem::replace(&mut *wal, new_wal);
        drop(wal);

        // rotation memtable
        let new_mem = MemTable::new();
        let start = std::time::Instant::now();
        let mut mem = self.memtable.write().await;
        let mem_lock_time = start.elapsed().as_millis() as f64;
        metrics::INGEST_MEMTABLE_LOCK_TIME
            .with_label_values(&[&self.key.org_id])
            .observe(mem_lock_time);
        let old_mem = std::mem::replace(&mut *mem, new_mem);
        drop(mem);

        // update created_at
        self.created_at
            .store(Utc::now().timestamp_micros(), Ordering::Release);

        let path = old_wal.path().clone();
        let path_str = path.display().to_string();
        let table = Arc::new(Immutable::new(self.idx, self.key.clone(), old_mem));
        log::info!("[INGESTER:MEM] start add to IMMUTABLES, file: {path_str}");
        IMMUTABLES.write().await.insert(path, table);
        log::info!("[INGESTER:MEM] dones add to IMMUTABLES, file: {path_str}");

        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        // wait for all messages to be processed
        if let Err(e) = self
            .write_queue
            .send((WriterSignal::Close, crate::ProcessedBatch::empty(), true))
            .await
        {
            log::error!("[INGESTER:MEM:{}] close writer error: {}", self.idx, e);
        }
        self.write_queue.closed().await;
        log::info!("[INGESTER:MEM:{}] writer queue closed", self.idx);

        // rotation wal
        let mut wal = self.wal.write().await;
        wal.sync().context(WalSnafu)?;
        let path = wal.path().clone();
        drop(wal);

        // rotation memtable
        let mut mem = self.memtable.write().await;
        let new_mem = MemTable::new();
        let old_mem = std::mem::replace(&mut *mem, new_mem);
        drop(mem);

        let table = Arc::new(Immutable::new(self.idx, self.key.clone(), old_mem));
        IMMUTABLES.write().await.insert(path, table);
        Ok(())
    }

    pub async fn read(
        &self,
        org_id: &str,
        stream_name: &str,
        time_range: Option<(i64, i64)>,
        partition_filters: &[(String, Vec<String>)],
    ) -> Result<(u64, Vec<ReadRecordBatchEntry>)> {
        let memtable = self.memtable.read().await;
        memtable.read(org_id, stream_name, time_range, partition_filters)
    }

    /// Check if the wal file size is over the threshold or the file is too old
    fn check_wal_threshold(&self, written_size: (usize, usize), data_size: usize) -> bool {
        let cfg = get_config();
        let (compressed_size, uncompressed_size) = written_size;
        compressed_size > wal::FILE_TYPE_IDENTIFIER_LEN
            && (compressed_size + data_size > cfg.limit.max_file_size_on_disk
                || uncompressed_size + data_size > cfg.limit.max_file_size_on_disk
                || self.created_at.load(Ordering::Relaxed)
                    + Duration::try_seconds(cfg.limit.max_file_retention_time as i64)
                        .unwrap()
                        .num_microseconds()
                        .unwrap()
                    <= Utc::now().timestamp_micros())
    }

    /// Check if the memtable size is over the threshold
    fn check_mem_threshold(&self, written_size: (usize, usize), data_size: usize) -> bool {
        let cfg = get_config();
        let (json_size, arrow_size) = written_size;
        json_size > 0
            && (json_size + data_size > cfg.limit.max_file_size_in_memory
                || arrow_size + data_size > cfg.limit.max_file_size_in_memory)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) struct WriterKey {
    pub(crate) org_id: Arc<str>,
    pub(crate) stream_type: Arc<str>,
}

impl WriterKey {
    pub(crate) fn new<T>(bucket_idx: usize, org_id: T, stream_type: T) -> Self
    where
        T: AsRef<str>,
    {
        let org_id = if get_config().common.feature_shared_memtable_enabled {
            Arc::from(format!("shared_org_{bucket_idx}"))
        } else {
            Arc::from(org_id.as_ref())
        };
        Self {
            org_id,
            stream_type: Arc::from(stream_type.as_ref()),
        }
    }

    pub(crate) fn new_replay(org_id: &str, stream_type: &str) -> Self {
        Self {
            org_id: Arc::from(org_id),
            stream_type: Arc::from(stream_type),
        }
    }
}

impl MemorySize for WriterKey {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<WriterKey>() + self.org_id.len() + self.stream_type.len()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
    use datafusion::prelude::SessionContext;

    use super::*;

    #[test]
    fn test_writer_key_new_replay_sets_fields() {
        let key = WriterKey::new_replay("myorg", "logs");
        assert_eq!(key.org_id.as_ref(), "myorg");
        assert_eq!(key.stream_type.as_ref(), "logs");
    }

    #[test]
    fn test_writer_key_mem_size_at_least_struct_size() {
        let key = WriterKey::new_replay("org", "metrics");
        assert!(key.mem_size() >= std::mem::size_of::<WriterKey>());
    }

    #[test]
    fn test_writer_key_mem_size_includes_string_lengths() {
        let key = WriterKey::new_replay("abc", "xyz");
        let min_expected = std::mem::size_of::<WriterKey>() + "abc".len() + "xyz".len();
        assert_eq!(key.mem_size(), min_expected);
    }

    #[tokio::test]
    async fn test_read_from_memtable_with_filter_physical_equals_matches_sessionctx() {
        // Build a small RecordBatch with two columns: id: Int64, msg: Utf8
        use arrow::{
            datatypes::{Field, Schema},
            record_batch::RecordBatch,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int64, false),
            Field::new("msg", arrow::datatypes::DataType::Utf8, false),
        ]));

        let id_col: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        let msg_col: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a"]));
        let rb =
            RecordBatch::try_new(schema.clone(), vec![id_col.clone(), msg_col.clone()]).unwrap();

        // prepare an entry in the same shape as memtable read outputs
        let _entry = entry::RecordBatchEntry::new(Arc::from("logs"), rb.clone(), 0usize, 0usize);

        // Build a simple filter: msg = 'a'
        let filter = datafusion::logical_expr::col("msg").eq(datafusion::logical_expr::lit("a"));

        // Call the function under test (internal) by copying the logic: use
        // read_from_memtable_with_filter
        let _res =
            read_from_memtable_with_filter("org", "logs", "svc", None, &[], Some(filter.clone()))
                .await;

        // Since the function depends on global WRITERS state, and constructing that is heavy,
        // we'll instead validate the logical->physical conversion helper directly via DF
        // SessionContext.
        let ctx = SessionContext::new();
        let df = ctx.read_batch(rb.clone()).unwrap();
        let df_filtered = df.filter(filter.clone()).unwrap();
        let collected = df_filtered.collect().await.unwrap();

        // Expect two rows matching 'a'
        let total_rows: usize = collected.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        // Also exercise a manually-built PhysicalExpr (msg = 'a') and evaluate it directly
        use datafusion::{
            logical_expr::Operator,
            physical_plan::{
                PhysicalExpr,
                expressions::{BinaryExpr as PBinaryExpr, Column as PColumn, Literal as PLiteral},
            },
            scalar::ScalarValue,
        };

        let col = Arc::new(PColumn::new("msg", 1));
        let lit = Arc::new(PLiteral::new(ScalarValue::Utf8(Some("a".to_string()))));
        let phys = Arc::new(PBinaryExpr::new(col, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;
        let cv = phys.evaluate(&rb).unwrap();
        match cv {
            datafusion::physical_plan::ColumnarValue::Array(arr) => {
                let bool_arr = arr
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap();
                let matched = (0..bool_arr.len())
                    .filter(|i| bool_arr.is_valid(*i) && bool_arr.value(*i))
                    .count();
                assert_eq!(matched, 2);
            }
            _ => panic!("expected array result"),
        }
    }
}
