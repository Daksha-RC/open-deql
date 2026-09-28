//! DeQL inspection table query rewrite.
//!
//! Detects when a search query targets an inspection output stream (`deql_ins_*` or `deql_brn_*`)
//! and resolves it from the in-memory `InspectionTableProvider` instead of the normal storage backend.
//!
//! Gated behind `#[cfg(feature = "deql")]`.

use std::sync::Arc;

use config::{utils::time::now_micros, utils::arrow::record_batches_to_json_rows};
use datafusion::datasource::TableProvider;

/// Attempt to handle a search request for a DeQL inspection stream.
///
/// If the target stream is an inspection table (deql_ins_* or deql_brn_*),
/// resolves it from the InspectionOrgState and returns the response.
/// Returns `None` if the stream is not an inspection stream (caller proceeds normally).
pub async fn try_search_inspection(
    trace_id: &str,
    org_id: &str,
    stream_type: config::meta::stream::StreamType,
    user_id: Option<String>,
    in_req: &config::meta::search::Request,
) -> Option<Result<config::meta::search::Response, infra::errors::Error>> {
    let sql = &in_req.query.sql;

    // Quick check: extract stream name from SQL FROM clause
    let stream_name = match extract_stream_name(sql) {
        Some(name) => name,
        None => return None,
    };

    // Check if stream is an inspection output table
    let is_inspection = stream_name.starts_with("deql_ins_") || stream_name.starts_with("deql_brn_");
    if !is_inspection {
        return None;
    }

    tracing::info!(
        org_id = %org_id,
        extracted_stream = %stream_name,
        sql = %sql,
        "DeQL inspection stream check"
    );

    // Get the InspectionTableProvider for this stream
    let provider = get_inspection_table_provider(org_id, &stream_name).await?;
    
    tracing::debug!(
        org_id = %org_id,
        stream = %stream_name,
        provider_exists = true,
        trace_id = %trace_id,
        "DeQL inspection stream: found table provider"
    );

    // Execute the query using DataFusion with the inspection table registered
    execute_inspection_query(org_id, trace_id, &stream_name, provider, in_req, stream_type, user_id).await
}

/// Get the InspectionTableProvider for a given org_id and stream_name.
async fn get_inspection_table_provider(
    org_id: &str,
    stream_name: &str,
) -> Option<Arc<dyn TableProvider>> {
    use crate::handler::http::request::dereg::get_deql_state;

    let state = get_deql_state().await;
    
    // Get inspection state from the state's inspection_map
    let inspection_state = state.inspection_state(org_id).await;
    
    // Get the provider from inspection state and convert to dyn TableProvider
    inspection_state.get_table_provider(stream_name)
}

/// Execute a SQL query against an inspection table provider.
async fn execute_inspection_query(
    org_id: &str,
    trace_id: &str,
    stream_name: &str,
    provider: Arc<dyn TableProvider>,
    in_req: &config::meta::search::Request,
    _stream_type: config::meta::stream::StreamType,
    _user_id: Option<String>,
) -> Option<Result<config::meta::search::Response, infra::errors::Error>> {
    use datafusion::prelude::SessionContext;

    let end_time = now_micros();
    let _start_time = end_time - (365 * 24 * 60 * 60 * 1_000_000);
    
    // Create a SessionContext with the inspection table registered
    let ctx = SessionContext::new();
    
    // Register the inspection table with DataFusion using bare table reference
    ctx.register_table(stream_name, provider)
        .map_err(|e| {
            log::error!(
                "[trace_id {trace_id}] Failed to register inspection table {stream_name}: {e}"
            );
            e
        })
        .ok()?;

    // Extract the SQL query from the request
    let sql = &in_req.query.sql;
    
    tracing::debug!(
        org_id = %org_id,
        stream = %stream_name,
        sql = %sql,
        trace_id = %trace_id,
        "DeQL inspection stream: executing query"
    );

    // Execute the query using DataFusion
    let df_query = ctx.sql(sql).await.map_err(|e| {
        log::error!("[trace_id {trace_id}] DataFusion SQL error: {e}");
        e
    }).ok()?;

    // Collect results
    let batches = df_query.collect().await.map_err(|e| {
        log::error!("[trace_id {trace_id}] DataFusion collect error: {e}");
        e
    }).ok()?;

    // Convert RecordBatches to JSON format for response using the correct function
    let json_rows = record_batches_to_json_rows(&batches.iter().collect::<Vec<_>>()).ok()?;
    
    // Convert JsonMap to serde_json::Value
    let hits: Vec<serde_json::Value> = json_rows
        .into_iter()
        .map(|row| serde_json::to_value(row).ok())
        .collect::<Option<_>>()?;

    tracing::info!(
        org_id = %org_id,
        stream = %stream_name,
        hits = hits.len(),
        trace_id = %trace_id,
        "DeQL inspection stream: query complete"
    );

    // Build the response using the correct fields from config::meta::search::Response
    let response = config::meta::search::Response {
        took: 0,
        took_detail: Default::default(),
        columns: Vec::new(),
        hits: vec![serde_json::Value::Array(hits)],
        total: 0, // Will be set by caller
        from: in_req.query.from,
        size: in_req.query.size,
        cached_ratio: 0,
        scan_files: 0,
        scan_size: 0,
        idx_scan_size: 0,
        scan_records: 0,
        response_type: String::new(),
        trace_id: trace_id.to_string(),
        function_error: Vec::new(),
        is_partial: false,
        histogram_interval: None,
        new_start_time: None,
        new_end_time: None,
        result_cache_ratio: 0,
        work_group: None,
        order_by: None,
        order_by_metadata: Vec::new(),
        converted_histogram_query: None,
        histogram_breakdown_field: None,
        is_histogram_eligible: None,
        query_index: None,
        peak_memory_usage: None,
    };

    Some(Ok(response))
}

/// Extract the primary stream/table name from a SQL FROM clause.
/// Handles: `FROM stream_name`, `FROM "stream_name"`, `FROM stream_name WHERE ...`
fn extract_stream_name(sql: &str) -> Option<String> {
    let sql_upper = sql.to_uppercase();
    let from_pos = sql_upper.find("FROM ")?;
    let after_from = &sql[from_pos + 5..];
    let trimmed = after_from.trim_start();

    // Handle quoted identifiers — strip quotes
    let name = if trimmed.starts_with('"') {
        let end = trimmed[1..].find('"')?;
        &trimmed[1..end + 1]
    } else if trimmed.starts_with('\'') {
        let end = trimmed[1..].find('\'')?;
        &trimmed[1..end + 1]
    } else {
        // Unquoted: take until whitespace, comma, or end
        trimmed.split(|c: char| c.is_whitespace() || c == ',').next()?
    };

    Some(name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_stream_name_inspection() {
        assert_eq!(
            extract_stream_name("SELECT * FROM deql_ins_deposit_results"),
            Some("deql_ins_deposit_results".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_branching() {
        assert_eq!(
            extract_stream_name("SELECT * FROM deql_brn_deposit_results"),
            Some("deql_brn_deposit_results".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_with_where() {
        assert_eq!(
            extract_stream_name("SELECT * FROM deql_ins_results WHERE _status = 'accepted'"),
            Some("deql_ins_results".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_quoted() {
        assert_eq!(
            extract_stream_name("SELECT * FROM \"deql_ins_results\" WHERE x = 1"),
            Some("deql_ins_results".to_string())
        );
    }
}
