// Sub-modules (organized by domain)
pub mod command;
pub mod projection;
pub mod query;
pub mod registry;
pub mod rehydrate_mod;

// Standalone modules
pub mod batch_filter;
pub mod core;
pub mod error;
pub mod guard_injector;
pub mod metrics;
#[cfg(test)]
mod metrics_tests;
pub mod migration;
pub mod parser;
pub mod store;
pub mod validator;
#[cfg(test)]
mod write_path_tests;

// ============================================================================
// Public re-exports — maintain identical surface for downstream consumers
// ============================================================================

pub use core::{ConfigPair, ConfigValue, DeqlType, FieldDef};

// command/
pub use command::executor::{
    EmittedEvent, ExecutionError, ExecutionRejection, ExecutionResult, ExecutionSuccess,
    execute_command,
};
pub use command::state::get_deql_state;

// query/
pub use query::event_table_provider::DeqlSearchBackend;
pub use query::schema_provider::DeQlSchemaProvider;

// registry/
pub use registry::dereg::{DeReg, DropResult, RegistrationResult};
pub use registry::org_registry::{OrgDeRegMap, OrgId};
pub use registry::registry_impl::Registry;

// rehydrate/
pub use rehydrate_mod::service::{
    OrgRehydrateState, OrgRehydrateStateMap, RehydrateError, RehydrateResult, RehydrateService,
};
pub use rehydrate_mod::service_impl::RehydrateServiceImpl;

// error
pub use error::{ApiError, ApiErrorBody, ConceptKind, DeRegError, ServiceError};

// parser
pub use parser::{
    ast::{
        ApplyTemplate, Assignment, CreateAggregate, CreateCommand, CreateDecision, CreateEvent,
        CreateEventStore, CreateProjection, CreateTemplate, DeqlStatement, Describe, Execute,
        ExportDeReg, ExportMetadata, FieldAnnotation, InspectDecision, InspectProjection,
        ParsedSource, Spanned, SqlFragment,
    },
    dispatch::{StatementKind, classify_statement},
    parser::parse,
    pretty::{pretty_print, pretty_print_statement},
    token::Span,
};

// validator
pub use validator::validate_command_payload;

// ============================================================================
// Internal path aliases — allow sub-modules to use `crate::dereg`, etc.
// without updating every internal import. These are NOT part of the public API.
// ============================================================================

// registry/ aliases
pub use registry::allocator;
pub use registry::dereg;
pub use registry::meta_json;
pub use registry::org_registry;
pub use registry::registry_impl as registry_mod;
pub use registry::worker_registry;

// command/ aliases
pub use command::executor;
pub use command::guard_translator;
pub use command::state as deql_state;

// query/ aliases
pub use query::agg_provider;
pub use query::event_table_provider;
pub use query::schema_provider;
pub use query::stream_schema;
pub use query::udaf;

// rehydrate/ aliases
pub use rehydrate_mod::replay;
pub use rehydrate_mod::service as rehydrate;
pub use rehydrate_mod::service_impl as rehydrate_impl;

// projection/ aliases
pub use projection::worker as projection_worker;
