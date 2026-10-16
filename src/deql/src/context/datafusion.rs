use async_trait::async_trait;
use openobserve_deql_core::{
    context::{ExecutionContext, SqlResult},
    error::Result,
};

/// DataFusion-backed execution context.
pub struct DataFusionContext;

#[async_trait]
impl ExecutionContext for DataFusionContext {
    async fn execute_sql(&self, _sql: &str) -> Result<SqlResult> {
        // TODO: implement DataFusion execution
        todo!()
    }

    async fn prepare_statement(
        &self,
        _sql: &str,
    ) -> Result<openobserve_deql_core::context::PreparedStatement> {
        // TODO: implement DataFusion prepare
        todo!()
    }
}
