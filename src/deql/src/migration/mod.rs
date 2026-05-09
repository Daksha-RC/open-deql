//! DeQL-owned migration set.
//!
//! Maintains Phase 2 DeReg table migrations independently from the parent
//! infra migration module. Uses the shared `ORM_CLIENT_DDL` connection.
//! [REQ-028a]

pub use sea_orm_migration::prelude::*;

mod m20260508_000001_create_dereg_meta_store;
mod m20260508_000002_create_projection_watermark;
mod m20260508_000003_create_meta_concepts;
mod m20260508_000004_create_meta_aggregates;
mod m20260508_000005_create_meta_decisions;
mod m20260508_000006_create_meta_inspections;
mod m20260508_000007_create_meta_commands;
mod m20260508_000008_create_meta_events;
mod m20260508_000009_create_meta_templates;
mod m20260508_000010_create_meta_templates_instances;

pub struct DeqlMigrator;

#[async_trait::async_trait]
impl MigratorTrait for DeqlMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260508_000001_create_dereg_meta_store::Migration),
            Box::new(m20260508_000002_create_projection_watermark::Migration),
            Box::new(m20260508_000003_create_meta_concepts::Migration),
            Box::new(m20260508_000004_create_meta_aggregates::Migration),
            Box::new(m20260508_000005_create_meta_decisions::Migration),
            Box::new(m20260508_000006_create_meta_inspections::Migration),
            Box::new(m20260508_000007_create_meta_commands::Migration),
            Box::new(m20260508_000008_create_meta_events::Migration),
            Box::new(m20260508_000009_create_meta_templates::Migration),
            Box::new(m20260508_000010_create_meta_templates_instances::Migration),
        ]
    }
}
