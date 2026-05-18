//! OrgDeRegMap — org-scoped in-memory DeReg state.
//!
//! [REQ-054] [REQ-055] Each org gets its own isolated Registry.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::dereg::DeReg;

pub type OrgId = String;

/// Per-org in-memory DeReg state map.
#[derive(Clone)]
pub struct OrgDeRegMap {
    inner: Arc<tokio::sync::RwLock<HashMap<OrgId, Arc<RwLock<DeReg>>>>>,
}

impl OrgDeRegMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Returns existing or initialises a new empty DeReg for the org.
    pub async fn get_or_init(&self, org_id: &str) -> Arc<RwLock<DeReg>> {
        {
            let map = self.inner.read().await;
            if let Some(dereg) = map.get(org_id) {
                return dereg.clone();
            }
        }
        let mut map = self.inner.write().await;
        map.entry(org_id.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(DeReg::new())))
            .clone()
    }

    /// Replace the DeReg for a given org (used during replay-refresh).
    pub async fn replace(&self, org_id: &str, dereg: DeReg) {
        let mut map = self.inner.write().await;
        map.insert(org_id.to_string(), Arc::new(RwLock::new(dereg)));
    }

    /// Remove org — used during replay-refresh lock phase.
    pub async fn remove(&self, org_id: &str) {
        let mut map = self.inner.write().await;
        map.remove(org_id);
    }

    /// List all registered org ids.
    pub async fn org_ids(&self) -> Vec<OrgId> {
        let map = self.inner.read().await;
        map.keys().cloned().collect()
    }

    /// Get a new instance that can be Arc-wrapped for service injection.
    /// Since OrgDeRegMap is now Clone and wraps Arc<RwLock>, this just clones self.
    pub fn clone_for_service(&self) -> Self {
        self.clone()
    }
}

impl Default for OrgDeRegMap {
    fn default() -> Self {
        Self::new()
    }
}
