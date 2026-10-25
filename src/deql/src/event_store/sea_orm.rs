use async_trait::async_trait;
use openobserve_deql_core::{
    error::Result,
    event_store::{AuditEntry, Event, EventStore},
};

/// SeaORM-backed event store adapter.
pub struct SeaOrmEventStore;

#[async_trait]
impl EventStore for SeaOrmEventStore {
    async fn append_event(&self, _event: Event) -> Result<()> {
        // TODO: implement SeaORM append
        todo!()
    }

    async fn append_audit(&self, _entry: AuditEntry) -> Result<()> {
        // TODO: implement SeaORM audit append
        todo!()
    }

    async fn events_for(&self, _stream_id: &str) -> Result<Vec<Event>> {
        // TODO: implement SeaORM query
        Ok(Vec::new())
    }

    async fn export_events(&self) -> Result<Vec<Event>> {
        // TODO: implement SeaORM export
        Ok(Vec::new())
    }
}
