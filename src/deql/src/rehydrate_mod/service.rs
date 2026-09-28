//! Rehydrate module: types and trait for reconstructing in-memory DeReg state from audit logs.
//!
//! This module provides the core abstractions for the rehydrate feature, which allows
//! reconstructing the in-memory DeReg (registry) state from the canonical `dereg_meta_store`
//! audit log. Key components:
//!
//! - `RehydrateError`: Enum for rehydrate-specific errors
//! - `RehydrateResult`: Struct capturing the outcome of a rehydrate operation
//! - `RehydrateService`: Async trait for triggering rehydrate operations from jobs
//!
//! ## Design Overview
//!
//! The rehydrate system ensures that DeQL definitions can be restored from the audit log
//! when the in-memory registry becomes out of sync or needs to be reconstructed:
//!
//! 1. **Org-scoped locks**: Each organization gets an exclusive lock during rehydrate to prevent
//!    concurrent mutations while replay is in progress.
//! 2. **Timeout enforcement**: Rehydrate operations are bounded by a 30-minute timeout to prevent
//!    indefinite hangs.
//! 3. **Atomic state swap**: The rebuilt registry is atomically swapped into place once replay
//!    succeeds, ensuring no partial states are visible.
//! 4. **Result tracking**: Success and failure metrics are stored in-memory for observability via
//!    the `/info` endpoint.
//!
//! ## Integration Points
//!
//! - **Handlers**: `src/handler/http/request/dereg/rehydrate.rs` exposes POST
//!   /{org}/dereg/rehydrate
//! - **Service**: `rehydrate_impl.rs` implements the trait for production use
//! - **Storage**: Queries `dereg_meta_store` for audit rows
//! - **Observability**: GET /{org}/deql/info shows last result and rehydrate revision

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Error type for rehydrate operations.
#[derive(Debug, Clone)]
pub enum RehydrateError {
    /// Rehydration already in progress for this organization.
    InProgress,
    /// Database error during replay or metadata fetch.
    DbError(String),
    /// Error parsing or validating audited statements.
    ParseError(String),
    /// Rehydration operation timed out (30 minutes).
    Timeout,
    /// Other operational errors.
    Other(String),
}

impl fmt::Display for RehydrateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InProgress => write!(f, "Rehydration already in progress"),
            Self::DbError(e) => write!(f, "Database error: {}", e),
            Self::ParseError(e) => write!(f, "Parse error: {}", e),
            Self::Timeout => write!(f, "Rehydration timed out after 30 minutes"),
            Self::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for RehydrateError {}

/// Outcome of a single rehydrate operation for an organization.
///
/// This struct captures all relevant metrics and status information from a rehydrate run,
/// including timing, rows processed, watermark, and any error details.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RehydrateResult {
    /// Unique trace/rehydrate identifier (UUID)
    pub rehydrate_id: String,

    /// Start timestamp (UTC)
    pub start_time: DateTime<Utc>,

    /// End timestamp (UTC)
    pub end_time: DateTime<Utc>,

    /// Elapsed time in milliseconds
    pub elapsed_ms: u64,

    /// Number of rows processed from `dereg_meta_store`
    pub rows_processed: i64,

    /// Last sequence id processed; becomes `rehydrate_revision` on success
    pub last_sequence_id: Option<i64>,

    /// Final status: "success" or "failure"
    pub status: String,

    /// Error message if status is "failure", None on success
    pub error_message: Option<String>,

    /// Organization id being rehydrated
    pub org_id: String,
}

impl RehydrateResult {
    /// Create a successful rehydrate result.
    ///
    /// Calculates `elapsed_ms` from start and end times.
    /// Sets status to "success" and clears error_message.
    pub fn success(
        rehydrate_id: String,
        org_id: String,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        rows_processed: i64,
        last_sequence_id: i64,
    ) -> Self {
        let elapsed_ms = (end_time - start_time).num_milliseconds().max(0) as u64;

        Self {
            rehydrate_id,
            start_time,
            end_time,
            elapsed_ms,
            rows_processed,
            last_sequence_id: Some(last_sequence_id),
            status: "success".to_string(),
            error_message: None,
            org_id,
        }
    }

    /// Create a failed rehydrate result.
    ///
    /// Automatically sets end_time to now if called immediately after failure.
    /// Calculates `elapsed_ms` and sets status to "failure" with provided error message.
    pub fn failure(
        rehydrate_id: String,
        org_id: String,
        start_time: DateTime<Utc>,
        error_message: String,
    ) -> Self {
        let end_time = Utc::now();
        let elapsed_ms = (end_time - start_time).num_milliseconds().max(0) as u64;

        Self {
            rehydrate_id,
            start_time,
            end_time,
            elapsed_ms,
            rows_processed: 0,
            last_sequence_id: None,
            status: "failure".to_string(),
            error_message: Some(error_message),
            org_id,
        }
    }
}

/// Per-organization rehydrate watermark and last result (in-memory).
///
/// Tracks the most recent successful sequence id (watermark) and the outcome
/// of the last rehydrate attempt for visibility and debugging.
#[derive(Debug, Clone)]
pub struct OrgRehydrateState {
    /// Latest sequence id successfully rehydrated (watermark)
    pub revision: Option<i64>,
    /// Most recent rehydrate result (success or failure)
    pub last_result: Option<RehydrateResult>,
}

impl OrgRehydrateState {
    /// Create a new, empty rehydrate state for an organization.
    pub fn new() -> Self {
        Self {
            revision: None,
            last_result: None,
        }
    }
}

impl Default for OrgRehydrateState {
    fn default() -> Self {
        Self::new()
    }
}

/// Type alias for thread-safe per-org rehydrate state map.
pub type OrgRehydrateStateMap =
    std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, OrgRehydrateState>>>;

/// Trait for rehydrating in-memory `DeReg` from the audit log.
///
/// This trait provides an async interface for rehydrating the in-memory registry state
/// from the canonical `dereg_meta_store` audit log. Implementations must:
///
/// - Enforce org-scoped locks to prevent concurrent rehydrates for the same org
/// - Return early with `InProgress` error if a rehydrate is already running
/// - Support an optional `since_id` for incremental rehydrate
/// - Enforce a 30-minute timeout
/// - Atomically swap the rebuilt state into the live registry
/// - Store and log the final result
///
/// The result is NOT returned via HTTP; instead, it's stored in-memory and queryable
/// via the info API. Callers should poll the info endpoint to check rehydrate status.
#[async_trait::async_trait]
pub trait RehydrateService: Send + Sync {
    /// Rehydrate the in-memory DeReg for a given organization.
    ///
    /// # Parameters
    /// - `org_id`: Organization identifier (org-scoped lock will be held during rehydrate)
    /// - `since_id`: Optional starting sequence id for incremental rehydrate; if None, starts from
    ///   beginning
    /// - `trace_id`: Optional trace/correlation id for logging and observability
    ///
    /// # Behavior
    /// - Returns `Err(RehydrateError::InProgress)` immediately if rehydrate already in progress
    /// - Acquires org-scoped lock and holds it throughout replay and atomic swap
    /// - Enforces 30-minute timeout; on timeout aborts, releases lock, returns `Timeout` error
    /// - On success: sets in-memory `rehydrate_revision`, releases lock, logs result
    /// - On failure: releases lock, logs error; result available via info API
    ///
    /// # Return
    /// Returns a `RehydrateResult` wrapped in the result type, or a `RehydrateError` if
    /// preconditions fail. The result includes timing, rows processed, final status, and any
    /// error message.
    async fn rehydrate_org(
        &self,
        org_id: &str,
        since_id: Option<i64>,
        trace_id: Option<&str>,
    ) -> Result<RehydrateResult, RehydrateError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rehydrate_result_success() {
        let start = Utc::now();
        let end = start + chrono::Duration::seconds(5);

        let result = RehydrateResult::success(
            "test-id".to_string(),
            "org1".to_string(),
            start,
            end,
            100,
            42,
        );

        assert_eq!(result.status, "success");
        assert_eq!(result.rows_processed, 100);
        assert_eq!(result.last_sequence_id, Some(42));
        assert_eq!(result.error_message, None);
        assert_eq!(result.elapsed_ms, 5000);
    }

    #[test]
    fn test_rehydrate_result_failure() {
        let start = Utc::now();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let result = RehydrateResult::failure(
            "test-id".to_string(),
            "org1".to_string(),
            start,
            "DB error".to_string(),
        );

        assert_eq!(result.status, "failure");
        assert_eq!(result.rows_processed, 0);
        assert_eq!(result.last_sequence_id, None);
        assert_eq!(result.error_message, Some("DB error".to_string()));
        assert!(result.elapsed_ms >= 0);
    }

    #[test]
    fn test_rehydrate_error_display() {
        assert_eq!(
            format!("{}", RehydrateError::InProgress),
            "Rehydration already in progress"
        );
        assert_eq!(
            format!("{}", RehydrateError::Timeout),
            "Rehydration timed out after 30 minutes"
        );
    }

    #[test]
    fn test_org_rehydrate_state_default() {
        let state = OrgRehydrateState::default();
        assert_eq!(state.revision, None);
        assert_eq!(state.last_result, None);
    }
}
