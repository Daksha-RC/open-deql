use async_trait::async_trait;

use crate::error::Result;

/// A single event record in the append-only event store.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Event {
    /// Unique event identifier.
    pub id: String,
    /// The aggregate/stream this event belongs to.
    pub stream_id: String,
    /// The concept kind this event relates to.
    pub concept_kind: crate::concept::ConceptKind,
    /// The event payload.
    pub payload: serde_json::Value,
}

/// Audit log entry.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuditEntry {
    /// Unique audit identifier.
    pub id: String,
    /// What action was performed.
    pub action: String,
    /// JSON-serialized details.
    pub details: serde_json::Value,
}

/// Append-only event persistence port.
#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    /// Append a domain event.
    async fn append_event(&self, event: Event) -> Result<()>;

    /// Append an audit log entry.
    async fn append_audit(&self, entry: AuditEntry) -> Result<()>;

    /// Retrieve all events for a given stream.
    async fn events_for(&self, stream_id: &str) -> Result<Vec<Event>>;

    /// Export all events for backup or migration.
    async fn export_events(&self) -> Result<Vec<Event>>;
}
