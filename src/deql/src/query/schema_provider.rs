//! DataFusion SchemaProvider for DeQL runtime integration with OpenObserve.
//!
//! Implements the `SchemaProvider` trait to expose virtual tables for:
//! - Aggregate state views (`{Aggregate}$Agg`) — backed by `AggProvider` (IMPL-04)
//! - Event streams (`{Aggregate}$Events`) — backed by `EventTableProvider` (IMPL-03)
//! - User-defined projections
//!
//! The `DeQlSchemaProvider` holds an optional `DeqlSearchBackend` injected by
//! the main crate. When no backend is available, `table()` returns `Ok(None)` so
//! queries fail gracefully rather than panic.

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::SchemaProvider, datasource::TableProvider, error::Result as DFResult,
    prelude::SessionContext,
};
use tokio::sync::RwLock as TokioRwLock;

use crate::{
    agg_provider::AggProvider,
    dereg::DeReg,
    event_table_provider::{DeqlSearchBackend, EventTableProvider},
};

/// OO-adapted SchemaProvider for DeQL.
///
/// Resolves table names to `TableProvider` implementations.
///
/// # Lock Strategy
///
/// The schema provider wraps `Arc<tokio::sync::RwLock<DeReg>>` from the executor.
/// Synchronous methods use `try_read()` so they never block; async `table()` uses
/// a regular `read().await`.
#[derive(Clone)]
pub struct DeQlSchemaProvider {
    /// Shared registry; accessed under read lock
    dereg: Arc<TokioRwLock<DeReg>>,
    /// Organization ID for multi-tenant isolation
    org_id: String,
    /// OO search backend for EventTableProvider / AggProvider (`IMPL-03`, `IMPL-04`).
    /// `None` when running without the main openobserve binary (e.g., unit tests).
    search_backend: Option<Arc<dyn DeqlSearchBackend>>,
}

impl std::fmt::Debug for DeQlSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeQlSchemaProvider")
            .field("org_id", &self.org_id)
            .field("has_search_backend", &self.search_backend.is_some())
            .finish()
    }
}

impl DeQlSchemaProvider {
    /// Create a new schema provider without a search backend.
    ///
    /// `table()` will return `Ok(None)` for `$Events`/`$Agg` tables. Use
    /// [`Self::with_search_backend`] to enable data-backed providers.
    pub fn new(dereg: Arc<TokioRwLock<DeReg>>, org_id: String) -> Self {
        Self {
            dereg,
            org_id,
            search_backend: None,
        }
    }

    /// Create a schema provider with a concrete OO search backend.
    ///
    /// Called by `execute_command` in the main binary crate so that STATE AS
    /// queries can resolve `$Events` and `$Agg` tables (`IMPL-03`, `IMPL-04`).
    pub fn with_search_backend(
        dereg: Arc<TokioRwLock<DeReg>>,
        org_id: String,
        backend: Arc<dyn DeqlSearchBackend>,
    ) -> Self {
        Self {
            dereg,
            org_id,
            search_backend: Some(backend),
        }
    }

    /// Build a StateContext for STATE AS query execution.
    ///
    /// Registers `DeQlSchemaProvider` (self) under schema `"dereg"` in the
    /// default DataFusion catalog so that `dereg.{Aggregate}$Events` and
    /// `dereg.{Aggregate}$Agg` table references resolve correctly (IMPL-01,
    /// NS-04). Also registers the `LAST` and `FIRST` UDAFs (IMPL-02).
    pub async fn build_state_context(&self) -> DFResult<SessionContext> {
        use crate::udaf::{create_first_udaf, create_last_udaf};

        let ctx = SessionContext::new();

        // IMPL-01: register this provider under schema "dereg" so that
        // queries like `SELECT ... FROM dereg.Employee$Events` resolve.
        if let Some(catalog) = ctx.catalog("datafusion") {
            catalog.register_schema("dereg", Arc::new(self.clone()))?;
        }

        // IMPL-02: register LAST and FIRST UDAFs.
        ctx.register_udaf(create_last_udaf());
        ctx.register_udaf(create_first_udaf());

        Ok(ctx)
    }
}

#[async_trait]
impl SchemaProvider for DeQlSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_exist(&self, name: &str) -> bool {
        let name_lower = name.to_lowercase();

        // Try to read without blocking; if we can't, assume table doesn't exist
        let Ok(dereg) = self.dereg.try_read() else {
            return false;
        };

        let registry = &dereg.registry;

        // Check $Agg suffix
        if let Some(agg_name) = name_lower.strip_suffix("$agg") {
            if registry
                .aggregates
                .values()
                .any(|a| a.name.node.to_lowercase() == agg_name)
            {
                return true;
            }
        }

        // Check $Events suffix
        if let Some(agg_name) = name_lower.strip_suffix("$events") {
            if registry
                .aggregates
                .values()
                .any(|a| a.name.node.to_lowercase() == agg_name)
            {
                return true;
            }
        }

        // Check projections
        if registry
            .projections
            .values()
            .any(|p| p.name.node.to_lowercase() == name_lower)
        {
            return true;
        }

        // Check metadata tables
        matches!(
            name_lower.as_str(),
            "meta_aggregates"
                | "meta_commands"
                | "meta_events"
                | "meta_decisions"
                | "meta_command_fields"
                | "meta_event_fields"
                | "meta_aggregate_fields"
        )
    }

    fn table_names(&self) -> Vec<String> {
        let Ok(dereg) = self.dereg.try_read() else {
            return vec![];
        };

        let registry = &dereg.registry;
        let mut names = Vec::new();

        // $Agg and $Events tables for each aggregate
        for agg in registry.aggregates.values() {
            let agg_name = &agg.name.node;
            names.push(format!("{agg_name}$Agg"));
            names.push(format!("{agg_name}$Events"));
        }

        // Projection names
        for proj in registry.projections.values() {
            names.push(proj.name.node.clone());
        }

        // Metadata introspection tables
        for meta_table in &[
            "meta_aggregates",
            "meta_commands",
            "meta_events",
            "meta_decisions",
            "meta_command_fields",
            "meta_event_fields",
            "meta_aggregate_fields",
        ] {
            names.push(meta_table.to_string());
        }

        names
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Case-insensitive matching (DataFusion lowercases names).
        let name_lower = name.to_lowercase();

        let dereg = self.dereg.read().await;
        let registry = &dereg.registry;

        // ── {AggregateName}$Agg ────────────────────────────────────────────
        // IMPL-04: return AggProvider wrapping EventTableProvider.
        if let Some(agg_name) = name_lower.strip_suffix("$agg") {
            if let Some(agg) = registry
                .aggregates
                .values()
                .find(|a| a.name.node.to_lowercase() == agg_name)
            {
                let canonical = agg.name.node.clone();

                let Some(backend) = self.search_backend.clone() else {
                    tracing::warn!(
                        agg = %canonical,
                        "$Agg table requested but no search backend available (IMPL-04)"
                    );
                    return Ok(None);
                };

                // Build schemas using DeReg helpers (IMPL-05, IMPL-15).
                let event_schema = dereg.stream_schema_for_org(&self.org_id, &canonical);
                let payload_fields = dereg.payload_fields_for_aggregate(&canonical);
                let field_names: Vec<String> =
                    payload_fields.iter().map(|f| f.name.node.clone()).collect();
                // Phase 7: SENSITIVE field list for null-masking in scan().
                let sensitive_fields = dereg.sensitive_fields_for_aggregate(&canonical);

                // $Agg schema: aggregate_id first, then sorted payload fields (IMPL-05).
                let mut agg_fields = vec![datafusion::arrow::datatypes::Field::new(
                    "aggregate_id",
                    datafusion::arrow::datatypes::DataType::Utf8,
                    false,
                )];
                for pf in &payload_fields {
                    agg_fields.push(datafusion::arrow::datatypes::Field::new(
                        &pf.name.node,
                        crate::stream_schema::deql_type_to_arrow(&pf.data_type.node),
                        true, // all payload columns nullable
                    ));
                }
                let agg_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(agg_fields));

                let event_provider: Arc<dyn TableProvider> = Arc::new(
                    EventTableProvider::new(
                        self.org_id.clone(),
                        canonical.clone(),
                        event_schema,
                        backend,
                    )
                    .with_sensitive_fields(sensitive_fields),
                );

                return Ok(Some(Arc::new(AggProvider::new(
                    canonical,
                    agg_schema,
                    event_provider,
                    field_names,
                ))));
            }
        }

        // ── {AggregateName}$Events ─────────────────────────────────────────
        // IMPL-03: return EventTableProvider backed by OO search API.
        if let Some(agg_name) = name_lower.strip_suffix("$events") {
            if let Some(agg) = registry
                .aggregates
                .values()
                .find(|a| a.name.node.to_lowercase() == agg_name)
            {
                let canonical = agg.name.node.clone();

                let Some(backend) = self.search_backend.clone() else {
                    tracing::warn!(
                        agg = %canonical,
                        "$Events table requested but no search backend available (IMPL-03)"
                    );
                    return Ok(None);
                };

                let schema = dereg.stream_schema_for_org(&self.org_id, &canonical);
                // Phase 7: SENSITIVE field list for null-masking in scan().
                let sensitive_fields = dereg.sensitive_fields_for_aggregate(&canonical);

                return Ok(Some(Arc::new(
                    EventTableProvider::new(self.org_id.clone(), canonical, schema, backend)
                        .with_sensitive_fields(sensitive_fields),
                )));
            }
        }

        // Projections and metadata tables are not yet data-backed.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::RwLock as TokioRwLock;

    use super::*;

    fn make_provider() -> DeQlSchemaProvider {
        let (parsed, _diags) = crate::parse(r#"CREATE AGGREGATE Employee (id UUID KEY);"#);
        let mut dereg = crate::dereg::DeReg::default();
        for stmt in &parsed.statements {
            let _ = dereg.register_statement(&stmt.node);
        }
        DeQlSchemaProvider::new(Arc::new(TokioRwLock::new(dereg)), "test-org".to_string())
    }

    /// Checkpoint 3 — task 3.4: build_state_context() registers "dereg" schema
    /// and LAST/FIRST UDAFs (IMPL-01, IMPL-02, NS-04).
    #[tokio::test]
    async fn test_build_state_context_registers_dereg_schema() {
        let provider = make_provider();
        let ctx = provider
            .build_state_context()
            .await
            .expect("build_state_context failed");

        // Schema "dereg" must be present in the "datafusion" catalog.
        let catalog = ctx
            .catalog("datafusion")
            .expect("datafusion catalog missing");
        assert!(
            catalog.schema("dereg").is_some(),
            "expected schema 'dereg' to be registered"
        );
    }

    /// LAST and FIRST UDAFs are resolvable after build_state_context().
    #[tokio::test]
    async fn test_build_state_context_registers_last_first_udafs() {
        let provider = make_provider();
        let ctx = provider
            .build_state_context()
            .await
            .expect("build_state_context failed");

        // If LAST/FIRST are registered, SQL planning succeeds.
        // We use a trivial query that exercises the UDAF resolver.
        ctx.sql("SELECT last(1) FROM (VALUES (1)) t(x)")
            .await
            .expect("LAST UDAF should be registered and resolvable");
        ctx.sql("SELECT first(1) FROM (VALUES (1)) t(x)")
            .await
            .expect("FIRST UDAF should be registered and resolvable");
    }
}
