//! Shared SQL builder for the `$Agg` fold query.
//!
//! Used by both the `/agg` HTTP endpoint (R3.4) and the virtual stream
//! query rewrite (R3.5).

/// Build the fold SQL that computes current aggregate state.
///
/// Generates a GROUP BY query with `LAST_VALUE` ordered aggregates for each
/// payload field, plus `MAX(_timestamp)`, `MAX(_offset)`, `MAX(_aggregate_version)`.
///
/// # Arguments
/// - `agg_name` — canonical aggregate name (used in WHERE _aggregate_type filter)
/// - `field_names` — payload field names (alphabetically sorted)
/// - `aggregate_id` — optional filter for a single aggregate instance
/// - `from` — pagination offset
/// - `size` — pagination limit
pub fn build_agg_sql(
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
        let sql = build_agg_sql("Employee", &["grade", "name"], Some("EMP-001"), 5, 50);
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
