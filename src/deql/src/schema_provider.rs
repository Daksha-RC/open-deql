//! DataFusion SchemaProvider for DeQL runtime integration with OpenObserve.
//!
//! Implements the `SchemaProvider` trait to expose virtual tables for:
//! - Aggregate state views (`{Aggregate}$Agg`)
//! - Event streams (`{Aggregate}$Events`)
//! - Metadata introspection tables
//! - User-defined projections
//!
//! Note: This is a partial implementation that provides table resolution logic.
//! Full data-backed providers (AggProvider, EventTableProvider) require integration
//! with OpenObserve's storage and search layers.

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::SchemaProvider, datasource::TableProvider, error::Result as DFResult,
    prelude::SessionContext,
};
use tokio::sync::RwLock as TokioRwLock;

use crate::dereg::DeReg;

/// OO-adapted SchemaProvider for DeQL.
///
/// Resolves table names to `TableProvider` implementations.
/// Manages both virtual schema-only tables and data-backed providers.
///
/// # Lock Strategy
///
/// The schema provider wraps `Arc<tokio::sync::RwLock<DeReg>>` from the executor.
/// For synchronous trait methods (table_exist, table_names), we use try_read()
/// to avoid blocking. This is safe because these methods only query schema metadata,
/// not actual data, and failures are safe to handle.
#[derive(Clone)]
pub struct DeQlSchemaProvider {
    /// Shared registry; accessed under read lock
    dereg: Arc<TokioRwLock<DeReg>>,
    /// Organization ID for multi-tenant isolation
    org_id: String,
}

impl std::fmt::Debug for DeQlSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeQlSchemaProvider")
            .field("org_id", &self.org_id)
            .finish()
    }
}

impl DeQlSchemaProvider {
    /// Create a new schema provider for an organization.
    pub fn new(dereg: Arc<TokioRwLock<DeReg>>, org_id: String) -> Self {
        Self { dereg, org_id }
    }

    /// Build a StateContext for STATE AS query execution.
    ///
    /// This function:
    /// 1. Prepares the schema provider for use in DataFusion queries
    /// 2. Registers necessary components
    /// 3. Returns a configured `SessionContext` ready for execution
    ///
    /// Called by the executor to create the query context.
    pub async fn build_state_context(&self) -> DFResult<SessionContext> {
        let ctx = SessionContext::new();
        // TODO: Register schema provider and UDAFs
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
        // Case-insensitive matching (DataFusion lowercases names)
        let name_lower = name.to_lowercase();

        // Try resolving within the read lock, then drop it before any async work
        {
            let dereg = self.dereg.read().await;
            let registry = &dereg.registry;

            // $Agg suffix: {AggregateName}$Agg
            if let Some(agg_name) = name_lower.strip_suffix("$agg") {
                if let Some(_agg) = registry
                    .aggregates
                    .values()
                    .find(|a| a.name.node.to_lowercase() == agg_name)
                {
                    // TODO: Return AggProvider wrapping aggregate state computation
                    // For now, return None (placeholder)
                    return Ok(None);
                }
            }

            // $Events suffix: {AggregateName}$Events
            if let Some(agg_name) = name_lower.strip_suffix("$events") {
                if let Some(_agg) = registry
                    .aggregates
                    .values()
                    .find(|a| a.name.node.to_lowercase() == agg_name)
                {
                    // TODO: Return EventTableProvider backed by OO search API
                    // For now, return None (placeholder)
                    return Ok(None);
                }
            }

            // Projection tables
            if let Some(_proj) = registry
                .projections
                .values()
                .find(|p| p.name.node.to_lowercase() == name_lower)
            {
                // TODO: Extract SQL body, plan, and wrap in ProjectionProvider
                // For now, return None (placeholder)
                return Ok(None);
            }
        }

        // Metadata tables are not yet implemented
        Ok(None)
    }
}
