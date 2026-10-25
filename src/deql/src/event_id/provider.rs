use async_trait::async_trait;
use openobserve_deql_core::{error::Result, event_id::EventIdProvider};

/// UUID v4-based event ID provider.
pub struct UuidEventIdProvider;

#[async_trait]
impl EventIdProvider for UuidEventIdProvider {
    async fn next_id(&self) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        Ok(id)
    }
}
