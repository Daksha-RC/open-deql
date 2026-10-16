use async_trait::async_trait;

use crate::error::Result;

/// Registry for domain concepts.
#[async_trait]
pub trait Registry: Send + Sync + 'static {
    /// Insert a new entry.
    async fn insert(&self, entry: RegistryEntry) -> Result<()>;

    /// Get an entry by ID.
    async fn get(&self, id: &str) -> Result<Option<RegistryEntry>>;

    /// List all entries.
    async fn list(&self) -> Result<Vec<RegistryEntry>>;

    /// Remove an entry by ID.
    async fn remove(&self, id: &str) -> Result<()>;

    /// Upsert many entries atomically.
    async fn upsert_many(&self, entries: Vec<RegistryEntry>) -> Result<()>;

    /// Export all entries for backup or migration.
    async fn export_all(&self) -> Result<Vec<RegistryEntry>>;
}

mod entry;
pub use entry::RegistryEntry;

mod in_memory;
pub use in_memory::InMemoryRegistry;
