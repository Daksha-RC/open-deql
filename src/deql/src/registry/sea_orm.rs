use async_trait::async_trait;
use openobserve_deql_core::{
    error::Result,
    registry::{Registry, RegistryEntry},
};

/// SeaORM-backed registry adapter.
pub struct SeaOrmRegistry;

#[async_trait]
impl Registry for SeaOrmRegistry {
    async fn insert(&self, _entry: RegistryEntry) -> Result<()> {
        // TODO: implement SeaORM insert
        todo!()
    }

    async fn get(&self, _id: &str) -> Result<Option<RegistryEntry>> {
        // TODO: implement SeaORM get
        todo!()
    }

    async fn list(&self) -> Result<Vec<RegistryEntry>> {
        // TODO: implement SeaORM list
        Ok(Vec::new())
    }

    async fn remove(&self, _id: &str) -> Result<()> {
        // TODO: implement SeaORM remove
        todo!()
    }

    async fn upsert_many(&self, _entries: Vec<RegistryEntry>) -> Result<()> {
        // TODO: implement SeaORM upsert_many
        todo!()
    }

    async fn export_all(&self) -> Result<Vec<RegistryEntry>> {
        // TODO: implement SeaORM export_all
        Ok(Vec::new())
    }
}
