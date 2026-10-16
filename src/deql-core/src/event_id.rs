use async_trait::async_trait;

use crate::error::Result;

/// Monotonic event ID generation port.
#[async_trait]
pub trait EventIdProvider: Send + Sync + 'static {
    /// Generate the next event ID.
    async fn next_id(&self) -> Result<String>;
}
