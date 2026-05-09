//! Worker registry — manages per-org background projection workers.
//!
//! [REQ-063-2] [REQ-063-9] [REQ-064] [REQ-065]

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::org_registry::OrgId;

/// Manages background projection worker tasks per org.
pub struct WorkerRegistry {
    workers: Mutex<HashMap<OrgId, JoinHandle<()>>>,
}

impl WorkerRegistry {
    pub fn new() -> Self {
        Self {
            workers: Mutex::new(HashMap::new()),
        }
    }

    /// Register a running worker handle for an org.
    pub async fn register(&self, org_id: &str, handle: JoinHandle<()>) {
        let mut workers = self.workers.lock().await;
        workers.insert(org_id.to_string(), handle);
    }

    /// Stop the worker for an org. [REQ-063-2]
    pub async fn stop_org(&self, org_id: &str) {
        let mut workers = self.workers.lock().await;
        if let Some(handle) = workers.remove(org_id) {
            handle.abort();
        }
    }

    /// Check if a worker is running for an org.
    pub async fn is_running(&self, org_id: &str) -> bool {
        let workers = self.workers.lock().await;
        workers
            .get(org_id)
            .map(|h| !h.is_finished())
            .unwrap_or(false)
    }

    /// Count active (non-finished) workers.
    pub async fn active_count(&self) -> usize {
        let workers = self.workers.lock().await;
        workers.values().filter(|h| !h.is_finished()).count()
    }

    /// Count active workers for a specific org.
    pub async fn active_count_for_org(&self, org_id: &str) -> usize {
        let workers = self.workers.lock().await;
        workers
            .get(org_id)
            .map(|h| if h.is_finished() { 0 } else { 1 })
            .unwrap_or(0)
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-org lock map for replay-refresh blocking.
/// [REQ-063-1] [REQ-064]
pub struct OrgLockMap {
    locks: RwLock<HashMap<OrgId, Arc<RwLock<()>>>>,
}

impl OrgLockMap {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// Check if an org is currently locked (replay-refresh in progress).
    /// [REQ-064]
    pub async fn is_locked(&self, org_id: &str) -> bool {
        let locks = self.locks.read().await;
        if let Some(lock) = locks.get(org_id) {
            lock.try_read().is_err()
        } else {
            false
        }
    }

    /// Acquire write lock for an org. Returns the lock guard.
    /// [REQ-063-1]
    pub async fn acquire_write(&self, org_id: &str) -> OrgLockGuard {
        let lock = {
            let mut locks = self.locks.write().await;
            locks
                .entry(org_id.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(())))
                .clone()
        };
        // Acquire the write lock
        let guard = lock.write_owned().await;
        OrgLockGuard { _guard: guard }
    }
}

impl Default for OrgLockMap {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for org-level write lock.
pub struct OrgLockGuard {
    _guard: tokio::sync::OwnedRwLockWriteGuard<()>,
}
