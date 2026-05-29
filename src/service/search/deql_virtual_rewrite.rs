//! DeQL virtual stream query rewrite.
//!
//! Detects when a search query targets a virtual `deql_*_agg` stream and
//! rewrites the SQL to a fold query against `deql_events`. This makes the
//! virtual stream queryable through the standard OO search path without
//! any physical storage.
//!
//! Gated behind `#[cfg(feature = "deql")]`.

use config::{meta::stream::StreamType, utils::time::now_micros};

/// Attempt to handle a search request for a DeQL virtual stream.
///
/// If the target stream is virtual, executes the fold query directly
/// (same logic as the /agg API) and returns the response.
/// Returns `None` if the stream is not virtual (caller proceeds normally).
pub async fn try_search_virtual(
    trace_id: &str,
    org_id: &str,
    _stream_type: config::meta::stream::StreamType,
    _user_id: Option<String>,
    in_req: &config::meta::search::Request,
) -> Option<Result<config::meta::search::Response, infra::errors::Error>> {
    let sql = &in_req.query.sql;

    // Quick check: extract stream name from SQL FROM clause
    let stream_name = match extract_stream_name(sql) {
        Some(name) => name,
        None => return None,
    };

    tracing::info!(
        org_id = %org_id,
        extracted_stream = %stream_name,
        sql = %sql,
        "DeQL virtual stream check"
    );

    // Check naming convention for virtual agg streams
    if !stream_name.starts_with("deql_") || !stream_name.ends_with("_agg") {
        return None;
    }

    // Derive aggregate name from stream name: "deql_employee_agg" → "employee"
    let agg_name_lower = stream_name
        .strip_prefix("deql_")?
        .strip_suffix("_agg")?;

    // Get payload fields from DeReg (also validates the aggregate exists)
    let (agg_name, field_names) = get_payload_fields_for_aggregate(org_id, agg_name_lower).await?;
    let field_refs: Vec<&str> = field_names.iter().map(|s| s.as_str()).collect();

    // Extract aggregate_id filter from user's SQL (if present)
    let aggregate_id_filter = extract_aggregate_id_filter(sql);

    // Build fold SQL using the shared builder
    let fold_sql = o2_deql::query::agg_sql::build_agg_sql(
        &agg_name,
        &field_refs,
        aggregate_id_filter.as_deref(),
        0,
        10000,
    );

    tracing::debug!(
        org_id = %org_id,
        stream = %stream_name,
        aggregate = %agg_name,
        rewritten_sql = %fold_sql,
        trace_id = %trace_id,
        "DeQL virtual stream: executing fold query"
    );

    // Build search request targeting deql_events (same as /agg API)
    let end_time = now_micros();
    let start_time = end_time - (365 * 24 * 60 * 60 * 1_000_000);
    let search_req = config::meta::search::Request {
        query: config::meta::search::Query {
            sql: fold_sql,
            start_time,
            end_time,
            from: 0,
            size: 10000,
            ..Default::default()
        },
        ..Default::default()
    };

    // Execute via the cluster search path directly (avoids async recursion).
    use super::cluster;
    use proto::cluster_rpc::SearchQuery;

    let trace_id_str = trace_id.to_string();
    let query: SearchQuery = search_req.query.clone().into();
    let mut request = config::datafusion::request::Request::new(
        trace_id_str.clone(),
        org_id.to_string(),
        StreamType::Logs,
        0, // timeout (use default)
        None, // user_id
        Some((search_req.query.start_time, search_req.query.end_time)),
        None, // search_event_type
        0,    // histogram_interval
        false, // overwrite_cache
    );
    request.set_use_cache(false);

    let result = cluster::http::search(request, query, vec![], vec![], true).await;
    match result {
        Ok(response) => Some(Ok(response)),
        Err(e) => Some(Err(e)),
    }
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

/// Extract `aggregate_id = 'value'` from user SQL WHERE clause.
pub fn extract_aggregate_id_filter(sql: &str) -> Option<String> {
    let sql_lower = sql.to_lowercase();
    // Look for: aggregate_id = 'value' or aggregate_id='value'
    let patterns = ["aggregate_id = '", "aggregate_id='"];
    for pattern in patterns {
        if let Some(pos) = sql_lower.find(pattern) {
            let start = pos + pattern.len();
            let rest = &sql[start..];
            if let Some(end) = rest.find('\'') {
                return Some(rest[..end].to_string());
            }
        }
    }
    None
}

/// Public wrapper for getting aggregate fields (used by search_stream handler).
pub async fn get_agg_fields(org_id: &str, agg_name: &str) -> Option<(String, Vec<String>)> {
    get_payload_fields_for_aggregate(org_id, agg_name).await
}

/// Get payload field names and canonical aggregate name from the DeReg registry.
/// Returns None if the aggregate doesn't exist.
async fn get_payload_fields_for_aggregate(org_id: &str, agg_name: &str) -> Option<(String, Vec<String>)> {
    use crate::handler::http::request::dereg::get_deql_state;

    let deql_state = get_deql_state().await;
    let org_dereg = deql_state.org_map.get_or_init(org_id).await;
    let dereg = org_dereg.read().await;

    // Verify aggregate exists (case-insensitive)
    let canonical = dereg.get_aggregate_ci(agg_name)?;
    let canonical_name = canonical.name.node.clone();

    let fields: Vec<String> = dereg
        .payload_fields_for_aggregate(&canonical_name)
        .iter()
        .map(|f| f.name.node.clone())
        .collect();

    Some((canonical_name, fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_stream_name_simple() {
        assert_eq!(
            extract_stream_name("SELECT * FROM deql_employee_agg"),
            Some("deql_employee_agg".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_with_where() {
        assert_eq!(
            extract_stream_name("SELECT * FROM deql_employee_agg WHERE aggregate_id = 'X'"),
            Some("deql_employee_agg".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_quoted() {
        assert_eq!(
            extract_stream_name("SELECT * FROM \"deql_employee_agg\" WHERE x = 1"),
            Some("deql_employee_agg".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_quoted_no_where() {
        assert_eq!(
            extract_stream_name("SELECT * FROM \"deql_employee_agg\""),
            Some("deql_employee_agg".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_not_virtual() {
        assert_eq!(
            extract_stream_name("SELECT * FROM regular_stream"),
            Some("regular_stream".to_string())
        );
    }

    #[test]
    fn test_extract_aggregate_id_filter() {
        assert_eq!(
            extract_aggregate_id_filter("SELECT * FROM x WHERE aggregate_id = 'EMP-001'"),
            Some("EMP-001".to_string())
        );
    }

    #[test]
    fn test_extract_aggregate_id_filter_no_filter() {
        assert_eq!(
            extract_aggregate_id_filter("SELECT * FROM x WHERE name = 'Alice'"),
            None
        );
    }

    #[test]
    fn test_extract_aggregate_id_filter_no_space() {
        assert_eq!(
            extract_aggregate_id_filter("SELECT * FROM x WHERE aggregate_id='EMP-002'"),
            Some("EMP-002".to_string())
        );
    }
}
