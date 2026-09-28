//! SeaORM entities for DeReg projection tables.
//!
//! Each concept type gets a dedicated projection table that the projection
//! worker populates from `dereg_meta_store` rows.

pub mod meta_aggregates;
pub mod meta_commands;
pub mod meta_concepts;
pub mod meta_decisions;
pub mod meta_events;
pub mod meta_inspections;
pub mod meta_templates;
pub mod meta_templates_instances;
