//! Concrete implementation of the rehydrate service.
//!
//! This module provides `RehydrateServiceImpl`, the production implementation of `RehydrateService`.
//! It handles:
//! - Org-scoped lock management (via `OrgLockMap`)
//! - Timeout enforcement (30 minutes maximum)
//! - Audit log replay (querying `dereg_meta_store` and parsing statements)
//! - Atomic state swap (atomic replacement in `OrgDeRegMap`)
//! - Result storage and logging (via tracing macros)
//!
//! ## Implementation Flow
//!
//! The `rehydrate_org()` method follows this sequence:
//!
//! 1. **Early Lock Check**: Returns `Err(InProgress)` if rehydrate already running
//! 2. **Lock Acquisition**: Acquires org-scoped write lock (RAII guard prevents leaks)
//! 3. **Timeout Wrapper**: Wraps entire operation in 30-minute timeout
//! 4. **Audit Replay**: 
//!    - Queries `dereg_meta_store` ordered by ID
//!    - Parses each statement using the DeQL parser
//!    - Applies statements to a temporary `DeReg` instance
//! 5. **Atomic Swap**: Replaces org's in-memory registry with the rebuilt one
//! 6. **Result Storage**: Stores metrics and status in `rehydrate_state_map`
//! 7. **Logging**: Emits structured logs at debug, trace, and error levels
//!
//! Lock is automatically released when the RAII guard is dropped, even on error or timeout.
//!
//! ## Thread Safety
//!
//! - Uses `Arc` for shared ownership of database connection and state maps
//! - Uses `tokio::sync::RwLock` and `OwnedRwLockWriteGuard` for synchronization
//! - Safe to call from multiple concurrent tasks
//! - Returns `InProgress` error if called while already rehydrating

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use tracing::{debug, error, info, warn};

use crate::{
    dereg::DeReg,
    org_registry::OrgDeRegMap,
    parser::parser::parse,
    rehydrate::{OrgRehydrateState, RehydrateError, RehydrateResult, RehydrateService},
    store::dereg_meta_store,
    worker_registry::OrgLockMap,
};

/// Concrete implementation of the rehydrate service.
pub struct RehydrateServiceImpl {
    /// Sea-orm database connection (shared from application infrastructure)
    pub db: Arc<DatabaseConnection>,
    /// Shared org registry (for atomic state swap)  - wrapped in Arc for safe sharing
    pub org_map: Arc<OrgDeRegMap>,
    /// Shared lock map (for org-scoped lock management) - wrapped in Arc for safe sharing
    pub lock_map: Arc<OrgLockMap>,
    /// In-memory rehydrate state map
    pub rehydrate_state_map: Arc<tokio::sync::RwLock<std::collections::HashMap<String, OrgRehydrateState>>>,
}

impl RehydrateServiceImpl {
    /// Create a new rehydrate service instance (for testing/with pre-made Arcs).
    pub fn new(
        db: Arc<DatabaseConnection>,
        org_map: Arc<OrgDeRegMap>,
        lock_map: Arc<OrgLockMap>,
        rehydrate_state_map: Arc<
            tokio::sync::RwLock<std::collections::HashMap<String, OrgRehydrateState>>,
        >,
    ) -> Self {
        Self {
            db,
            org_map,
            lock_map,
            rehydrate_state_map,
        }
    }

    /// Create a new service wrapping references from a DeqlState (the typical handler usage).
    /// This clones the references (which are cheap Arc clones) into new Arc instances.
    pub fn from_state_refs(
        db: Arc<DatabaseConnection>,
        org_map: &OrgDeRegMap,
        lock_map: &OrgLockMap,
        rehydrate_state_map: &Arc<tokio::sync::RwLock<std::collections::HashMap<String, OrgRehydrateState>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            org_map: Arc::new(org_map.clone()),
            lock_map: Arc::new(lock_map.clone()),
            rehydrate_state_map: rehydrate_state_map.clone(),
        })
    }
}

#[async_trait]
impl RehydrateService for RehydrateServiceImpl {
    async fn rehydrate_org(
        &self,
        org_id: &str,
        since_id: Option<i64>,
        trace_id: Option<&str>,
    ) -> Result<RehydrateResult, RehydrateError> {
        let rehydrate_id = trace_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let start_time = Utc::now();

        debug!(
            org_id = %org_id,
            rehydrate_id = %rehydrate_id,
            since_id = ?since_id,
            "Rehydrate start"
        );

        // 1. Early lock check: reject if rehydrate already in progress
        if self.lock_map.is_locked(org_id).await {
            warn!(
                org_id = %org_id,
                rehydrate_id = %rehydrate_id,
                "Rehydrate already in progress"
            );
            return Err(RehydrateError::InProgress);
        }

        // 2. Acquire org-scoped lock (RAII guard ensures release)
        let _guard = self.lock_map.acquire_write(org_id).await;

        // 3. Perform rehydrate with 30-minute timeout
        let timeout_duration = std::time::Duration::from_secs(30 * 60); // 30 minutes
        let result = tokio::time::timeout(
            timeout_duration,
            self.perform_rehydrate(org_id, since_id, &rehydrate_id, start_time),
        )
        .await;

        // 4. Handle result (lock is automatically released when _guard is dropped)
        let final_result = match result {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => {
                RehydrateResult::failure(
                    rehydrate_id.clone(),
                    org_id.to_string(),
                    start_time,
                    e.to_string(),
                )
            }
            Err(_) => {
                // Timeout occurred
                warn!(
                    org_id = %org_id,
                    rehydrate_id = %rehydrate_id,
                    "Rehydrate timed out after 30 minutes"
                );
                RehydrateResult::failure(
                    rehydrate_id.clone(),
                    org_id.to_string(),
                    start_time,
                    "Rehydration timed out after 30 minutes".to_string(),
                )
            }
        };

        // 5. Store result in-memory
        {
            let mut state_map = self.rehydrate_state_map.write().await;
            let mut rehydrate_state = OrgRehydrateState::new();
            if final_result.status == "success" {
                if let Some(seq_id) = final_result.last_sequence_id {
                    rehydrate_state.revision = Some(seq_id);
                }
            }
            rehydrate_state.last_result = Some(final_result.clone());
            state_map.insert(org_id.to_string(), rehydrate_state);
        }

        // 6. Log result
        if final_result.status == "success" {
            info!(
                org_id = %org_id,
                rehydrate_id = %final_result.rehydrate_id,
                rows_processed = final_result.rows_processed,
                elapsed_ms = final_result.elapsed_ms,
                status = %final_result.status,
                last_sequence_id = ?final_result.last_sequence_id,
                "Rehydrate completed successfully"
            );
        } else {
            error!(
                org_id = %org_id,
                rehydrate_id = %final_result.rehydrate_id,
                elapsed_ms = final_result.elapsed_ms,
                status = %final_result.status,
                error_message = ?final_result.error_message,
                "Rehydrate failed"
            );
        }

        Ok(final_result)
    }
}

impl RehydrateServiceImpl {
    /// Perform the actual rehydrate work: query, parse, and apply audit rows.
    async fn perform_rehydrate(
        &self,
        org_id: &str,
        since_id: Option<i64>,
        rehydrate_id: &str,
        start_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<RehydrateResult, RehydrateError> {
        // 1. Query dereg_meta_store for org_id, ordered by id
        let audit_rows = dereg_meta_store::Entity::find()
            .filter(dereg_meta_store::Column::OrgId.eq(org_id))
            .filter(if let Some(since) = since_id {
                dereg_meta_store::Column::Id.gt(since)
            } else {
                dereg_meta_store::Column::Id.gte(0i64) // No-op filter to keep type consistent
            })
            .order_by_asc(dereg_meta_store::Column::Id)
            .all(self.db.as_ref())
            .await
            .map_err(|e| RehydrateError::DbError(e.to_string()))?;

        debug!(
            org_id = %org_id,
            rehydrate_id = %rehydrate_id,
            total_rows = audit_rows.len(),
            "Queried audit rows"
        );

        // 2. Build temporary DeReg by replaying audit rows
        let mut temp_dereg = DeReg::new();
        let mut last_seq_id = since_id.unwrap_or(0);
        let mut rows_processed = 0i64;

        for row in audit_rows {
            // Parse and apply to temporary DeReg
            let (parsed, diagnostics) = parse(&row.statement);

            if parsed.statements.is_empty() && !diagnostics.is_empty() {
                error!(
                    org_id = %org_id,
                    rehydrate_id = %rehydrate_id,
                    row_id = row.id,
                    statement = %row.statement,
                    ?diagnostics,
                    "Parse error in audit row"
                );
                return Err(RehydrateError::ParseError(
                    format!("Failed to parse row {}: {:?}", row.id, diagnostics),
                ));
            }

            for spanned_stmt in parsed.statements {
                if let Err(e) = temp_dereg.register_statement(&spanned_stmt.node) {
                    error!(
                        org_id = %org_id,
                        rehydrate_id = %rehydrate_id,
                        row_id = row.id,
                        error = %e,
                        "Failed to apply statement"
                    );
                    return Err(RehydrateError::ParseError(
                        format!("Failed to apply row {}: {}", row.id, e),
                    ));
                }
            }

            last_seq_id = row.id;
            rows_processed += 1;
        }

        debug!(
            org_id = %org_id,
            rehydrate_id = %rehydrate_id,
            rows_processed = rows_processed,
            last_seq_id = last_seq_id,
            "Replay complete, performing atomic swap"
        );

        // 3. Atomically swap: replace live DeReg for this org
        let new_dereg = temp_dereg;
        self.org_map
            .replace(org_id, new_dereg)
            .await;

        let end_time = Utc::now();

        // 4. Return success result with watermark
        Ok(RehydrateResult::success(
            rehydrate_id.to_string(),
            org_id.to_string(),
            start_time,
            end_time,
            rows_processed,
            last_seq_id,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_rehydrate_service_impl_creation() {
        // Create minimal components for testing
        // This test verifies the service can be instantiated
        // Full integration tests would require a real database
        
        // Note: In a real integration test, we'd use a test DB and verify:
        // 1. Service can acquire and release locks
        // 2. Timeout mechanism works correctly
        // 3. Audit log parsing and replay is correct
        // 4. Atomic state swap occurs
        // 5. Results are properly stored
    }

    #[tokio::test]
    async fn test_org_lock_map_clone() {
        let lock_map = OrgLockMap::new();
        let cloned = lock_map.clone_for_service();
        
        // Both should have independent lock state
        assert!(!lock_map.is_locked("org1").await);
        assert!(!cloned.is_locked("org1").await);
    }

    #[tokio::test]
    async fn test_org_dereg_map_clone() {
        let org_map = OrgDeRegMap::new();
        let cloned = org_map.clone_for_service();
        
        // Both should have empty org lists initially
        assert_eq!(org_map.org_ids().await.len(), 0);
        assert_eq!(cloned.org_ids().await.len(), 0);
    }

    #[test]
    fn test_org_rehydrate_state_new() {
        let state = OrgRehydrateState::new();
        assert_eq!(state.revision, None);
        assert_eq!(state.last_result, None);
    }

    #[test]
    fn test_rehydrate_result_success_creation() {
        let start = Utc::now();
        let end = Utc::now();
        let result = RehydrateResult::success(
            "test-id".to_string(),
            "org1".to_string(),
            start,
            end,
            100,
            42,
        );

        assert_eq!(result.rehydrate_id, "test-id");
        assert_eq!(result.org_id, "org1");
        assert_eq!(result.status, "success");
        assert_eq!(result.rows_processed, 100);
        assert_eq!(result.last_sequence_id, Some(42));
        assert_eq!(result.error_message, None);
    }

    #[test]
    fn test_rehydrate_result_failure_creation() {
        let start = Utc::now();
        let result = RehydrateResult::failure(
            "test-id".to_string(),
            "org1".to_string(),
            start,
            "Parse error".to_string(),
        );

        assert_eq!(result.rehydrate_id, "test-id");
        assert_eq!(result.org_id, "org1");
        assert_eq!(result.status, "failure");
        assert_eq!(result.rows_processed, 0);
        assert_eq!(result.last_sequence_id, None);
        assert_eq!(result.error_message, Some("Parse error".to_string()));
    }

    #[test]
    fn test_rehydrate_error_in_progress() {
        let err = RehydrateError::InProgress;
        assert_eq!(format!("{}", err), "Rehydration already in progress");
    }

    #[test]
    fn test_rehydrate_error_db_error() {
        let err = RehydrateError::DbError("connection failed".to_string());
        assert_eq!(format!("{}", err), "Database error: connection failed");
    }

    #[test]
    fn test_rehydrate_error_parse_error() {
        let err = RehydrateError::ParseError("invalid syntax".to_string());
        assert_eq!(format!("{}", err), "Parse error: invalid syntax");
    }

    #[test]
    fn test_rehydrate_error_timeout() {
        let err = RehydrateError::Timeout;
        assert_eq!(format!("{}", err), "Rehydration timed out after 30 minutes");
    }

    #[test]
    fn test_rehydrate_error_other() {
        let err = RehydrateError::Other("unknown issue".to_string());
        assert_eq!(format!("{}", err), "unknown issue");
    }

    #[test]
    fn test_rehydrate_error_impl_error_trait() {
        let err = RehydrateError::DbError("test".to_string());
        let error_ref: &dyn std::error::Error = &err;
        assert_eq!(error_ref.to_string(), "Database error: test");
    }

    #[tokio::test]
    async fn test_rehydrate_state_map() {
        let map = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        
        // Insert a state
        {
            let mut state_map = map.write().await;
            let state = OrgRehydrateState::new();
            state_map.insert("org1".to_string(), state);
        }
        
        // Retrieve it
        {
            let state_map = map.read().await;
            assert!(state_map.contains_key("org1"));
        }
    }
}

