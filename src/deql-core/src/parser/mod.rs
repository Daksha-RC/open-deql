use crate::{concept::ConceptKind, error::Result};

/// Parse a SQL-like event discovery string into a list of [`ConceptKind`] values.
///
/// This is a stub implementation that always returns an empty list.
pub async fn parse_events(_input: &str) -> Result<Vec<ConceptKind>> {
    Ok(Vec::new())
}
