//! DeQL virtual stream query rewrite.
//!
//! Detects when a search query targets a virtual `deql_agg_*` or `deql_prj_*` stream
//! and executes the appropriate fold/projection SQL against `deql_events`.
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

    // Check naming convention for virtual streams
    if stream_name.starts_with("deql_agg_") {
        // $Agg virtual stream: deql_agg_{aggregate}
        let agg_name_lower = &stream_name[9..]; // "deql_agg_".len() = 9

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

        return execute_virtual_sql(org_id, trace_id, &fold_sql).await;
    } else if stream_name.starts_with("deql_prj_") {
        // Projection virtual stream: deql_prj_{projection_name}
        let proj_name = &stream_name[9..]; // "deql_prj_".len() = 9

        if let Some(proj_sql) = get_projection_sql(org_id, proj_name).await {
            tracing::debug!(
                org_id = %org_id,
                stream = %stream_name,
                projection = %proj_name,
                sql = %proj_sql,
                trace_id = %trace_id,
                "DeQL virtual stream: executing projection query"
            );

            return execute_virtual_sql(org_id, trace_id, &proj_sql).await;
        }
    }

    None
}

/// Execute a SQL query against deql_events via the cluster search path.
/// Used by both $Agg and projection virtual streams.
async fn execute_virtual_sql(
    org_id: &str,
    trace_id: &str,
    sql: &str,
) -> Option<Result<config::meta::search::Response, infra::errors::Error>> {
    use super::cluster;
    use proto::cluster_rpc::SearchQuery;

    let end_time = now_micros();
    let start_time = end_time - (365 * 24 * 60 * 60 * 1_000_000);
    let search_req = config::meta::search::Request {
        query: config::meta::search::Query {
            sql: sql.to_string(),
            start_time,
            end_time,
            from: 0,
            size: 10000,
            ..Default::default()
        },
        ..Default::default()
    };

    let trace_id_str = trace_id.to_string();
    let query: SearchQuery = search_req.query.clone().into();
    let mut request = config::datafusion::request::Request::new(
        trace_id_str.clone(),
        org_id.to_string(),
        StreamType::Logs,
        0,
        None,
        Some((search_req.query.start_time, search_req.query.end_time)),
        None,
        0,
        false,
    );
    request.set_use_cache(false);

    let result = cluster::http::search(request, query, vec![], vec![], true).await;
    match result {
        Ok(response) => Some(Ok(response)),
        Err(e) => Some(Err(e)),
    }
}

/// Get projection SQL from DeReg by name (case-insensitive).
pub async fn get_projection_sql(org_id: &str, proj_name: &str) -> Option<String> {
    use crate::handler::http::request::dereg::get_deql_state;

    let deql_state = get_deql_state().await;
    let org_dereg = deql_state.org_map.get_or_init(org_id).await;
    let dereg = org_dereg.read().await;

    // Look up projection by name (case-insensitive)
    let projection = dereg.get_projection_ci(proj_name)?;
    Some(projection.body.sql.clone())
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
            extract_stream_name("SELECT * FROM deql_agg_employee"),
            Some("deql_agg_employee".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_with_where() {
        assert_eq!(
            extract_stream_name("SELECT * FROM deql_agg_employee WHERE aggregate_id = 'X'"),
            Some("deql_agg_employee".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_quoted() {
        assert_eq!(
            extract_stream_name("SELECT * FROM \"deql_agg_employee\" WHERE x = 1"),
            Some("deql_agg_employee".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_quoted_no_where() {
        assert_eq!(
            extract_stream_name("SELECT * FROM \"deql_agg_employee\""),
            Some("deql_agg_employee".to_string())
        );
    }

    #[test]
    fn test_extract_stream_name_projection() {
        assert_eq!(
            extract_stream_name("SELECT * FROM deql_prj_newhirereport"),
            Some("deql_prj_newhirereport".to_string())
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
