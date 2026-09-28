//! Build projection expressions for referenced guard fields.
//! For missing fields we produce NULL-fillers so DataFusion projections
//! have consistent columns during guard evaluation.

use std::collections::HashSet;

use datafusion::{
    arrow::datatypes::Schema,
    logical_expr::{Expr, col},
    scalar::ScalarValue,
};

/// Build a projection `Vec<Expr>` for the given `referenced` field names.
/// If a field is present in `schema` we return `col(field).alias(field)`;
/// otherwise we return a `NULL` literal aliased to `field`.
pub fn build_projection_for_referenced_fields(
    referenced: &HashSet<String>,
    schema: &Schema,
) -> Vec<Expr> {
    referenced
        .iter()
        .map(|name| {
            if schema.field_with_name(name).is_ok() {
                col(name.as_str()).alias(name.as_str())
            } else {
                Expr::Literal(ScalarValue::Utf8(None), None).alias(name.as_str())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn projection_builds_for_present_and_missing() {
        let schema = Schema::new(vec![Field::new("present", DataType::Utf8, true)]);
        let mut refs = HashSet::new();
        refs.insert("present".to_string());
        refs.insert("missing".to_string());
        let proj = build_projection_for_referenced_fields(&refs, &schema);
        assert_eq!(proj.len(), 2);
    }
}
