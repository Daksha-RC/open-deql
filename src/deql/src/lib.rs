pub mod core;
pub mod error;
pub mod parser;

pub use core::{ConfigPair, ConfigValue, DeqlType, FieldDef};
pub use error::{ApiError, ApiErrorBody, ConceptKind, DeRegError, ServiceError};
pub use parser::ast::{
	ApplyTemplate, Assignment, CreateAggregate, CreateCommand, CreateDecision, CreateEvent,
	CreateEventStore, CreateProjection, CreateTemplate, DeqlStatement, Describe, Execute,
	ExportDeReg, ExportMetadata, FieldAnnotation, InspectDecision, InspectProjection,
	ParsedSource, Spanned, SqlFragment,
};
pub use parser::dispatch::{StatementKind, classify_statement};
pub use parser::parser::parse;
pub use parser::pretty::{pretty_print, pretty_print_statement};
pub use parser::token::Span;
