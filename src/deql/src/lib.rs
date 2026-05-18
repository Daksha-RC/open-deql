pub mod allocator;
pub mod core;
pub mod dereg;
pub mod error;
pub mod meta_json;
pub mod metrics;
#[cfg(test)]
mod metrics_tests;
pub mod migration;
pub mod org_registry;
pub mod parser;
#[cfg(test)]
mod projection_tests;
pub mod projection_worker;
pub mod registry;
pub mod rehydrate;
pub mod rehydrate_impl;
pub mod replay;
pub mod store;
pub mod validator;
pub mod worker_registry;
#[cfg(test)]
mod write_path_tests;

pub use core::{ConfigPair, ConfigValue, DeqlType, FieldDef};

pub use dereg::{DeReg, DropResult, RegistrationResult};
pub use error::{ApiError, ApiErrorBody, ConceptKind, DeRegError, ServiceError};
pub use org_registry::{OrgDeRegMap, OrgId};
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
pub use registry::Registry;
pub use rehydrate::{
    OrgRehydrateState, OrgRehydrateStateMap, RehydrateError, RehydrateResult, RehydrateService,
};
pub use rehydrate_impl::RehydrateServiceImpl;
