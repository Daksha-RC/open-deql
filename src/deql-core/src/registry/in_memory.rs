use std::{collections::HashMap, sync::Arc};

use super::{Registry, RegistryEntry};
use crate::error::Result;

/// A `HashMap`-backed in-memory implementation of [`Registry`].
pub struct InMemoryRegistry {
    entries: Arc<std::sync::RwLock<HashMap<String, RegistryEntry>>>,
}

impl InMemoryRegistry {
    /// Create a new empty in-memory registry.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Registry for InMemoryRegistry {
    async fn insert(&self, entry: RegistryEntry) -> Result<()> {
        let mut map = self.entries.write().unwrap();
        if map.contains_key(&entry.id) {
            return Err(crate::DeqlError::Conflict(format!(
                "entry already exists: {}",
                entry.id
            )));
        }
        map.insert(entry.id.clone(), entry);
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<RegistryEntry>> {
        let map = self.entries.read().unwrap();
        Ok(map.get(id).cloned())
    }

    async fn list(&self) -> Result<Vec<RegistryEntry>> {
        let map = self.entries.read().unwrap();
        Ok(map.values().cloned().collect())
    }

    async fn remove(&self, id: &str) -> Result<()> {
        let mut map = self.entries.write().unwrap();
        if map.remove(id).is_none() {
            return Err(crate::DeqlError::NotFound(format!(
                "entry not found: {}",
                id
            )));
        }
        Ok(())
    }

    async fn upsert_many(&self, entries: Vec<RegistryEntry>) -> Result<()> {
        let mut map = self.entries.write().unwrap();
        for entry in entries {
            map.insert(entry.id.clone(), entry);
        }
        Ok(())
    }

    async fn export_all(&self) -> Result<Vec<RegistryEntry>> {
        self.list().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry() -> RegistryEntry {
        RegistryEntry {
            id: "test-1".to_string(),
            name: "Test Concept".to_string(),
            kind: crate::concept::ConceptKind::Unknown,
            version: "1.0.0".to_string(),
            payload: None,
        }
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let reg = InMemoryRegistry::new();
        let entry = sample_entry();
        reg.insert(entry.clone()).await.unwrap();
        let got = reg.get("test-1").await.unwrap();
        assert_eq!(got, Some(entry));
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let reg = InMemoryRegistry::new();
        let got = reg.get("no-such-id").await.unwrap();
        assert_eq!(got, None);
    }

    #[tokio::test]
    async fn test_list_empty() {
        let reg = InMemoryRegistry::new();
        let list = reg.list().await.unwrap();
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn test_list_after_insert() {
        let reg = InMemoryRegistry::new();
        let entry = sample_entry();
        reg.insert(entry.clone()).await.unwrap();
        let list = reg.list().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], entry);
    }

    #[tokio::test]
    async fn test_remove() {
        let reg = InMemoryRegistry::new();
        let entry = sample_entry();
        reg.insert(entry).await.unwrap();
        reg.remove("test-1").await.unwrap();
        let got = reg.get("test-1").await.unwrap();
        assert_eq!(got, None);
    }

    #[tokio::test]
    async fn test_remove_nonexistent() {
        let reg = InMemoryRegistry::new();
        let err = reg.remove("no-such-id").await.unwrap_err();
        assert!(matches!(err, crate::DeqlError::NotFound(_)));
    }

    #[tokio::test]
    async fn test_upsert_many() {
        let reg = InMemoryRegistry::new();
        let entries = vec![
            RegistryEntry {
                id: "a".to_string(),
                name: "A".to_string(),
                kind: crate::concept::ConceptKind::Unknown,
                version: "1.0.0".to_string(),
                payload: None,
            },
            RegistryEntry {
                id: "b".to_string(),
                name: "B".to_string(),
                kind: crate::concept::ConceptKind::Unknown,
                version: "1.0.0".to_string(),
                payload: None,
            },
        ];
        reg.upsert_many(entries).await.unwrap();
        let list = reg.list().await.unwrap();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_export_all() {
        let reg = InMemoryRegistry::new();
        let entry = sample_entry();
        reg.insert(entry).await.unwrap();
        let exported = reg.export_all().await.unwrap();
        assert_eq!(exported.len(), 1);
    }

    #[tokio::test]
    async fn test_insert_duplicate_fails() {
        let reg = InMemoryRegistry::new();
        let entry = sample_entry();
        reg.insert(entry.clone()).await.unwrap();
        let err = reg.insert(entry).await.unwrap_err();
        assert!(matches!(err, crate::DeqlError::Conflict(_)));
    }
}
