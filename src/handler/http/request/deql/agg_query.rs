//! HTTP request handler for the DeQL aggregate state query endpoint.
//!
//! Provides a GET endpoint that folds event streams into current aggregate state
//! using OO's standard querier path.
//!
//! Gated behind `#[cfg(feature = "deql")]`.

use axum::{
    Json,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use config::utils::time::now_micros;
use serde::Deserialize;
use serde_json::json;

use super::super::dereg::get_deql_state;

/// Query parameters for the aggregate state endpoint.
#[derive(Debug, Deserialize)]
pub struct AggQueryParams {
    /// Optional aggregate instance ID to filter to a single entity.
    pub aggregate_id: Option<String>,
    /// Pagination offset (default 0).
    pub from: Option<usize>,
    /// Page size (default 100, max 10000).
    pub size: Option<usize>,
}

/// Build the fold SQL that computes current aggregate state via the OO querier.
///
/// Generates a GROUP BY query with LAST_VALUE ordered aggregates for each
/// payload field, plus MAX(_timestamp), MAX(_offset), MAX(_aggregate_version).
fn build_agg_sql(
    agg_name: &str,
    field_names: &[&str],
    aggregate_id: Option<&str>,
    from: usize,
    size: usize,
) -> String {
    let mut select_parts = vec![
        "_aggregate_id AS aggregate_id".to_string(),
        "MAX(_timestamp) AS _timestamp".to_string(),
        "MAX(_offset) AS _offset".to_string(),
        "MAX(_aggregate_version) AS _aggregate_version".to_string(),
    ];

    for &field in field_names {
        select_parts.push(format!(
            "LAST_VALUE({field} ORDER BY _offset ASC) IGNORE NULLS AS {field}"
        ));
    }

    let select_clause = select_parts.join(", ");

    let mut where_parts = vec![format!(
        "_aggregate_type = '{}'",
        agg_name.replace('\'', "''")
    )];
    if let Some(id) = aggregate_id {
        where_parts.push(format!("_aggregate_id = '{}'", id.replace('\'', "''")));
    }
    let where_clause = where_parts.join(" AND ");

    format!(
        "SELECT {select_clause} FROM deql_events WHERE {where_clause} GROUP BY _aggregate_id LIMIT {size} OFFSET {from}"
    )
}

/// Query aggregate state via the OO querier.
///
/// Returns the folded (current) state of aggregate instances by replaying
/// events through the OO querier.
#[utoipa::path(
    get,
    path = "/{org_id}/deql/aggregates/{agg}/agg",
    params(
        ("org_id" = String, Path, description = "Organization ID"),
        ("agg" = String, Path, description = "Aggregate name (case-insensitive)"),
        ("aggregate_id" = Option<String>, Query, description = "Filter to a single aggregate instance"),
        ("from" = Option<usize>, Query, description = "Pagination offset (default 0)"),
        ("size" = Option<usize>, Query, description = "Page size (default 100, max 10000)"),
    ),
    responses(
        (status = 200, description = "Aggregate state computed successfully", content_type = "application/json"),
        (status = 400, description = "Invalid pagination parameters"),
        (status = 404, description = "Aggregate not found"),
        (status = 500, description = "Query execution failed"),
        (status = 503, description = "DeQL registry not ready"),
    ),
    tag = "DeQL"
)]
pub async fn get_agg(
    Path((org_id, agg)): Path<(String, String)>,
    Query(params): Query<AggQueryParams>,
) -> Response {
    // Validate pagination
    let from = params.from.unwrap_or(0);
    let size = params.size.unwrap_or(100);
    if size == 0 || size > 10000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Invalid pagination: size must be 1-10000"
            })),
        )
            .into_response();
    }

    // Resolve org's DeReg
    let deql_state = get_deql_state().await;
    let org_dereg = deql_state.org_map.get_or_init(&org_id).await;
    let dereg = org_dereg.read().await;

    // Validate aggregate exists (case-insensitive)
    let agg_canonical = match dereg.get_aggregate_ci(&agg) {
        Some(a) => a.name.node.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "error": format!("Aggregate '{}' not found in organization '{}'", agg, org_id)
                })),
            )
                .into_response();
        }
    };

    // Get payload field names from DeReg
    let field_names: Vec<String> = dereg
        .payload_fields_for_aggregate(&agg_canonical)
        .iter()
        .map(|f| f.name.node.clone())
        .collect();

    // Release DeReg lock before I/O
    drop(dereg);

    // Build fold SQL
    let field_refs: Vec<&str> = field_names.iter().map(|s| s.as_str()).collect();
    let sql = build_agg_sql(
        &agg_canonical,
        &field_refs,
        params.aggregate_id.as_deref(),
        from,
        size,
    );

    tracing::debug!(
        org_id = %org_id,
        aggregate = %agg_canonical,
        sql = %sql,
        "DeQL $Agg query"
    );

    // Construct search request
    let trace_id = config::ider::generate_trace_id();
    // Use a wide time range: 1 year ago to now.
    // start_time=0 is rejected by OO's file_list as "invalid time range".
    let end_time = now_micros();
    let start_time = end_time - (365 * 24 * 60 * 60 * 1_000_000); // 1 year back
    let search_req = config::meta::search::Request {
        query: config::meta::search::Query {
            sql: sql.clone(),
            start_time,
            end_time,
            from: 0,      // pagination handled in SQL
            size: 10000,  // large enough to not truncate SQL results
            ..Default::default()
        },
        ..Default::default()
    };

    // Submit to OO querier
    let stream_type = config::meta::stream::StreamType::Logs;
    match crate::service::search::search(
        &trace_id,
        &org_id,
        stream_type,
        None, // user_id — inherit from auth context
        &search_req,
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(e) => {
            tracing::error!(
                org_id = %org_id,
                aggregate = %agg_canonical,
                error = ?e,
                "DeQL $Agg query failed"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to compute aggregate state: {}", e)
                })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_agg_sql_no_filter() {
        let sql = build_agg_sql("Employee", &["grade", "name", "new_grade"], None, 0, 100);
        assert!(sql.contains("_aggregate_type = 'Employee'"));
        assert!(sql.contains("MAX(_timestamp) AS _timestamp"));
        assert!(sql.contains("MAX(_offset) AS _offset"));
        assert!(sql.contains("MAX(_aggregate_version) AS _aggregate_version"));
        assert!(sql.contains("LAST_VALUE(name ORDER BY _offset ASC) IGNORE NULLS AS name"));
        assert!(sql.contains("LIMIT 100 OFFSET 0"));
        assert!(!sql.contains("_aggregate_id ="));
    }

    #[test]
    fn test_build_agg_sql_with_filter() {
        let sql = build_agg_sql(
            "Employee",
            &["grade", "name"],
            Some("EMP-001"),
            5,
            50,
        );
        assert!(sql.contains("_aggregate_id = 'EMP-001'"));
        assert!(sql.contains("LIMIT 50 OFFSET 5"));
    }

    #[test]
    fn test_build_agg_sql_escapes_quotes() {
        let sql = build_agg_sql("Test'Agg", &["field"], Some("id'inject"), 0, 10);
        assert!(sql.contains("_aggregate_type = 'Test''Agg'"));
        assert!(sql.contains("_aggregate_id = 'id''inject'"));
    }
}
