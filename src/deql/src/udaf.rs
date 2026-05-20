//! Custom User-Defined Aggregate Functions (UDAFs) for DeQL.
//!
//! Implements LAST_VALUE and FIRST_VALUE aggregates for computing current state
//! and historical snapshots of aggregates.
//!
//! Note: Full UDAF registration with DataFusion requires substantial boilerplate.
//! This module provides the type definitions and placeholder functions.
//! Actual registration happens in schema_provider at SessionContext initialization.

/// LAST_VALUE(expr) — returns the last non-null value in accumulation order.
///
/// Used to compute current state columns: `SELECT stream_id, LAST_VALUE(balance) FROM events GROUP
/// BY stream_id`
///
/// For now, we rely on DataFusion's built-in `last_value` aggregate function.
/// This is a placeholder for future custom implementations if needed.
pub fn last_value_udaf_name() -> &'static str {
    "last_value"
}

/// FIRST_VALUE(expr) — returns the first non-null value in accumulation order.
///
/// Used to extract initial state at aggregate creation time.
///
/// For now, we rely on DataFusion's built-in `first_value` aggregate function.
pub fn first_value_udaf_name() -> &'static str {
    "first_value"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udaf_names() {
        assert_eq!(last_value_udaf_name(), "last_value");
        assert_eq!(first_value_udaf_name(), "first_value");
    }
}
