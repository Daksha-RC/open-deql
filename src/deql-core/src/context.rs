use async_trait::async_trait;

use crate::error::Result;

/// Result of a SQL execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SqlResult {
    /// Column names.
    pub columns: Vec<String>,
    /// Row data as vectors of strings.
    pub rows: Vec<Vec<String>>,
}

/// SQL execution port — implemented by DataFusion in the adapter crate.
#[async_trait]
pub trait ExecutionContext: Send + Sync + 'static {
    /// Execute a SQL query string.
    async fn execute_sql(&self, sql: &str) -> Result<SqlResult>;

    /// Prepare a statement for repeated execution.
    async fn prepare_statement(&self, sql: &str) -> Result<PreparedStatement>;
}

/// A prepared SQL statement handle.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// The SQL text.
    pub sql: String,
}
