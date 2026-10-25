/// A registered concept entry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegistryEntry {
    /// Unique identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// The concept kind.
    pub kind: crate::concept::ConceptKind,
    /// Semantic version string.
    pub version: String,
    /// Arbitrary JSON payload.
    pub payload: Option<serde_json::Value>,
}
