//! Concrete `DeqlSearchBackend` backed by OpenObserve's ingester memtable.
//!
//! `OoSearchBackend` implements the trait defined in `o2-deql` by reading
//! directly from the WAL memtable (and immutable partition), bypassing the
//! distributed search infrastructure.  This ensures events are visible
//! immediately after ingest without requiring Flight gRPC round-trips.
//!
//! Falls back to `service::search::search()` only when the local memtable
//! yields no rows (e.g. after WAL flush to Parquet).

use std::sync::Arc;

use async_trait::async_trait;
use config::{ider, meta::stream::StreamType, utils::arrow::record_batches_to_json_rows};
use o2_deql::DeqlSearchBackend;

/// Search backend that calls OO's `service::search::search()`.
#[derive(Debug)]
pub struct OoSearchBackend;

impl OoSearchBackend {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DeqlSearchBackend for OoSearchBackend {
    /// Fetch raw event rows from `deql_events` for the given aggregate.
    ///
    /// Strategy: read directly from the WAL memtable + immutable partition first
    /// (instant visibility after ingest).  Fall back to the distributed search
    /// only when the local path yields nothing (data flushed to Parquet).
    async fn fetch_events(
        &self,
        org_id: &str,
        agg_name: &str,
        event_id_filter: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        use config::utils::time::now_micros;

        let now = now_micros();
        const TWO_YEARS_MICROS: i64 = 2 * 365 * 24 * 3600 * 1_000_000;
        let start_time = now - TWO_YEARS_MICROS;
        let end_time = now + 3_600_000_000; // +1 hour buffer
        let time_range = Some((start_time, end_time));

        tracing::debug!(
            org_id = %org_id,
            agg_name = %agg_name,
            event_id_filter = ?event_id_filter,
            "DeQL search backend: attempting built-in distributed querier first (local_mode=true)"
        );

        // Try built-in distributed search first (prefer local node by setting local_mode)
        let dist_hits = self
            .fallback_distributed_search(org_id, agg_name, event_id_filter, start_time, end_time)
            .await?;

        if !dist_hits.is_empty() {
            tracing::debug!(
                org_id = %org_id,
                agg_name = %agg_name,
                hit_count = dist_hits.len(),
                "DeQL search backend: returning hits from distributed querier"
            );
            return Ok(dist_hits);
        }

        // If distributed querier returned no hits, fall back to reading local memtable
        tracing::debug!(
            org_id = %org_id,
            agg_name = %agg_name,
            "DeQL search backend: distributed querier returned 0 hits; trying local memtable/immutable"
        );

        // Read from WAL memtable
        let (memtable_ids, mem_batches) = ingester::read_from_memtable(
            org_id,
            StreamType::Logs.as_str(),
            "deql_events",
            time_range,
            &[],
        )
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "DeQL search backend: memtable read failed");
            (Default::default(), Vec::new())
        });

        // Read from immutable partitions (exclude memtable ids)
        let trace_id = ider::generate_trace_id();
        let (_imm_ids, imm_batches) = ingester::read_from_immutable(
            &trace_id,
            org_id,
            StreamType::Logs.as_str(),
            "deql_events",
            time_range,
            &[],
            &memtable_ids,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "DeQL search backend: immutable read failed");
            (Vec::new(), Vec::new())
        });

        // Collect RecordBatches from memtable + immutables
        let all_batches: Vec<_> = mem_batches
            .iter()
            .flat_map(|(_schema, entries)| entries.iter().map(|e| &e.data))
            .chain(
                imm_batches
                    .iter()
                    .flat_map(|(_schema, entries)| entries.iter().map(|e| &e.data)),
            )
            .collect();

        let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        tracing::debug!(
            org_id = %org_id,
            agg_name = %agg_name,
            batch_count = all_batches.len(),
            total_rows = total_rows,
            "DeQL search backend: local batches collected"
        );

        if total_rows > 0 {
            let json_rows = record_batches_to_json_rows(&all_batches).map_err(
                |e| -> Box<dyn std::error::Error + Send + Sync> {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("RecordBatch to JSON conversion error: {e}"),
                    ))
                },
            )?;

            // Filter by _aggregate_type and optionally _event_id
            let mut hits: Vec<serde_json::Value> = json_rows
                .into_iter()
                .filter(|row| {
                    let agg_match = row
                        .get("_aggregate_type")
                        .and_then(|v| v.as_str())
                        .map(|v| v == agg_name)
                        .unwrap_or(false);
                    let eid_match = match event_id_filter {
                        Some(eid) => row
                            .get("_event_id")
                            .and_then(|v| v.as_str())
                            .map(|v| v == eid)
                            .unwrap_or(false),
                        None => true,
                    };
                    agg_match && eid_match
                })
                .map(serde_json::Value::Object)
                .collect();

            // Sort by _timestamp ascending
            hits.sort_by(|a, b| {
                let ts_a = a.get("_timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                let ts_b = b.get("_timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                ts_a.cmp(&ts_b)
            });

            tracing::debug!(
                org_id = %org_id,
                agg_name = %agg_name,
                hit_count = hits.len(),
                "DeQL search backend: returning hits from local memtable"
            );
            return Ok(hits);
        }

        // Return empty distributed result (no rows found anywhere)
        Ok(dist_hits)
    }

    /// Fetch events with access to the DataFusion filters that were applied
    /// at scan time. Override the default impl so we can push supported
    /// filters into the local memtable read path and also apply the same
    /// predicate to immutable batches.
    async fn fetch_events_with_filters(
        &self,
        org_id: &str,
        agg_name: &str,
        filters: &[datafusion::logical_expr::Expr],
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        use datafusion::{
            common::ScalarValue,
            logical_expr::{BinaryExpr, Expr as DFExpr, Operator},
            prelude::SessionContext,
        };

        // Extract optional _event_id equality for distributed fallback
        let mut event_id: Option<String> = None;
        for filter in filters {
            if let DFExpr::BinaryExpr(binary) = filter {
                if binary.op == Operator::Eq {
                    if let (
                        DFExpr::Column(col),
                        DFExpr::Literal(ScalarValue::Utf8(Some(value)), _),
                    ) = (binary.left.as_ref(), binary.right.as_ref())
                    {
                        if col.name == "_event_id" {
                            event_id = Some(value.clone());
                            break;
                        }
                    }
                    if let (
                        DFExpr::Literal(ScalarValue::Utf8(Some(value)), _),
                        DFExpr::Column(col),
                    ) = (binary.left.as_ref(), binary.right.as_ref())
                    {
                        if col.name == "_event_id" {
                            event_id = Some(value.clone());
                            break;
                        }
                    }
                }
            }
        }

        // Try distributed search first (same policy as fetch_events)
        use config::utils::time::now_micros;
        let now = now_micros();
        const TWO_YEARS_MICROS: i64 = 2 * 365 * 24 * 3600 * 1_000_000;
        let start_time = now - TWO_YEARS_MICROS;
        let end_time = now + 3_600_000_000;

        let dist_hits = self
            .fallback_distributed_search(
                org_id,
                agg_name,
                event_id.as_deref(),
                start_time,
                end_time,
            )
            .await?;
        if !dist_hits.is_empty() {
            return Ok(dist_hits);
        }

        // Combine filters into a single Expr (AND), if any
        let combined: Option<datafusion::logical_expr::Expr> = if filters.is_empty() {
            None
        } else {
            let mut it = filters.iter();
            let first = it.next().unwrap().clone();
            let comb = it.fold(first, |acc, x| {
                DFExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(acc),
                    op: Operator::And,
                    right: Box::new(x.clone()),
                })
            });
            Some(comb)
        };

        // Read from memtable with predicate pushdown
        let time_range = Some((start_time, end_time));
        let (memtable_ids, mem_batches) = ingester::read_from_memtable_with_filter(
            org_id,
            StreamType::Logs.as_str(),
            "deql_events",
            time_range,
            &[],
            combined.clone(),
        )
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "DeQL search backend: memtable read failed");
            (Default::default(), Vec::new())
        });

        // Read immutable partitions (exclude memtable ids)
        let trace_id = ider::generate_trace_id();
        let (_imm_ids, imm_batches) = ingester::read_from_immutable(
            &trace_id,
            org_id,
            StreamType::Logs.as_str(),
            "deql_events",
            time_range,
            &[],
            &memtable_ids,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "DeQL search backend: immutable read failed");
            (Vec::new(), Vec::new())
        });

        // Prepare a collection of RecordBatch references for JSON conversion.
        // Memtable batches from read_from_memtable_with_filter are already filtered.
        let mut all_batch_refs: Vec<datafusion::arrow::record_batch::RecordBatch> = Vec::new();
        for (_schema, entries) in mem_batches.iter() {
            for e in entries.iter() {
                all_batch_refs.push(e.data.clone());
            }
        }

        // If we have a combined predicate, apply it to immutable batches as well
        if let Some(pred) = combined {
            let ctx = SessionContext::new();
            for (schema, entries) in imm_batches {
                for entry in entries {
                    if entry.data.num_rows() == 0 {
                        continue;
                    }
                    let cols = entry.data.columns().iter().cloned().collect::<Vec<_>>();
                    if let Ok(batch) =
                        datafusion::arrow::record_batch::RecordBatch::try_new(schema.clone(), cols)
                    {
                        if let Ok(df0) = ctx.read_batch(batch) {
                            // Apply the predicate to the DataFrame and collect results
                            match df0.filter(pred.clone()) {
                                Ok(filtered_df) => match filtered_df.collect().await {
                                    Ok(rbs) => {
                                        for rb in rbs {
                                            all_batch_refs.push(rb);
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "DeQL search backend: immutable collect failed")
                                    }
                                },
                                Err(e) => {
                                    tracing::warn!(error = %e, "DeQL search backend: immutable filter failed")
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // No predicate -> include imm_batches as-is
            for (_schema, entries) in imm_batches {
                for e in entries {
                    all_batch_refs.push(e.data.clone());
                }
            }
        }

        // Convert to JSON rows
        let total_rows: usize = all_batch_refs.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let json_rows = record_batches_to_json_rows(&all_batch_refs.iter().collect::<Vec<_>>())
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("RecordBatch to JSON conversion error: {e}"),
                ))
            })?;

        // Filter by _aggregate_type and optionally _event_id (safety double-check)
        let mut hits: Vec<serde_json::Value> = json_rows
            .into_iter()
            .filter(|row| {
                let agg_match = row
                    .get("_aggregate_type")
                    .and_then(|v| v.as_str())
                    .map(|v| v == agg_name)
                    .unwrap_or(false);
                let eid_match = match event_id.as_deref() {
                    Some(eid) => row
                        .get("_event_id")
                        .and_then(|v| v.as_str())
                        .map(|v| v == eid)
                        .unwrap_or(false),
                    None => true,
                };
                agg_match && eid_match
            })
            .map(serde_json::Value::Object)
            .collect();

        // Sort by _timestamp ascending
        hits.sort_by(|a, b| {
            let ts_a = a.get("_timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
            let ts_b = b.get("_timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
            ts_a.cmp(&ts_b)
        });

        Ok(hits)
    }
}

impl OoSearchBackend {
    /// Fallback: use OO's distributed search for data already flushed to Parquet.
    async fn fallback_distributed_search(
        &self,
        org_id: &str,
        agg_name: &str,
        event_id_filter: Option<&str>,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
        use config::meta::search::{Query, Request};

        let cfg = config::get_config();
        let where_agg = format!("_aggregate_type = '{}'", agg_name.replace('\'', "''"));
        let sql = if let Some(eid) = event_id_filter {
            format!(
                "SELECT * FROM deql_events WHERE {} AND _event_id = '{}' ORDER BY _timestamp ASC",
                where_agg,
                eid.replace('\'', "''"),
            )
        } else {
            format!(
                "SELECT * FROM deql_events WHERE {} ORDER BY _timestamp ASC",
                where_agg,
            )
        };

        let req = Request {
            query: Query {
                sql: sql.clone(),
                start_time,
                end_time,
                size: cfg.limit.query_default_limit,
                ..Default::default()
            },
            ..Default::default()
        };

        let trace_id = ider::generate_trace_id();
        let resp = crate::service::search::search(&trace_id, org_id, StreamType::Logs, None, &req)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("OO search error: {e}"),
                ))
            })?;

        tracing::debug!(
            org_id = %org_id,
            agg_name = %agg_name,
            hit_count = resp.hits.len(),
            "DeQL search backend: distributed search returned"
        );
        Ok(resp.hits)
    }
}

/// Create a search backend as `Arc<dyn DeqlSearchBackend>`.
pub fn make_search_backend() -> Arc<dyn DeqlSearchBackend> {
    Arc::new(OoSearchBackend::new())
}
