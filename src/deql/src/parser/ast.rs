// AST types for parsed DeQL statements.

use crate::parser::token::Span;

/// Generic wrapper attaching a source Span to any AST node.
#[derive(Debug, Clone, PartialEq)]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

/// A parsed DeQL source file or REPL input.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSource {
    pub statements: Vec<Spanned<DeqlStatement>>,
}

/// Top-level statement variants.
#[derive(Debug, Clone, PartialEq)]
pub enum DeqlStatement {
    CreateAggregate(CreateAggregate),
    CreateCommand(CreateCommand),
    CreateEvent(CreateEvent),
    CreateDecision(CreateDecision),
    CreateProjection(CreateProjection),
    CreateEventStore(CreateEventStore),
    CreateTemplate(CreateTemplate),
    Execute(Execute),
    InspectDecision(InspectDecision),
    InspectProjection(InspectProjection),
    Describe(Describe),
    ApplyTemplate(ApplyTemplate),
    ExportDeReg(ExportDeReg),
    ExportMetadata(ExportMetadata),
    ValidateDeReg,
}

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

/// A typed field definition used in aggregates, commands, and events.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: Spanned<String>,
    pub data_type: Spanned<DeqlType>,
    pub is_key: bool,
    pub annotation: Option<FieldAnnotation>,
}

/// DeQL data types for field definitions.
#[derive(Debug, Clone, PartialEq)]
pub enum DeqlType {
    Uuid,
    String,
    Int,
    Decimal { precision: u8, scale: u8 },
    Timestamp,
    Boolean,
}

/// Raw SQL text preserved verbatim for DataFusion.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlFragment {
    pub sql: String,
    pub span: Span,
}

/// A `field := value` assignment used in EMIT AS and EXECUTE statements.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub field: Spanned<String>,
    pub value: Spanned<String>,
}

/// A `dotted.key = value` configuration pair inside EVENTSTORE WITH blocks.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigPair {
    pub key: Spanned<String>,
    pub value: Spanned<ConfigValue>,
}

/// Configuration value variants for EVENTSTORE WITH blocks.
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    StringLit(String),
    IntLit(i64),
    DecimalLit(f64),
    BoolLit(bool),
    List(Vec<String>),
}

// ---------------------------------------------------------------------------
// Statement-specific nodes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAggregate {
    pub or_replace: bool,
    pub name: Spanned<String>,
    pub fields: Option<Vec<FieldDef>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateCommand {
    pub or_replace: bool,
    pub name: Spanned<String>,
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateEvent {
    pub or_replace: bool,
    pub name: Spanned<String>,
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDecision {
    pub or_replace: bool,
    pub name: Spanned<String>,
    pub aggregate: Spanned<String>,
    pub command: Spanned<String>,
    pub state_as: Option<SqlFragment>,
    pub branches: Vec<DecisionBranch>,
}

impl CreateDecision {
    /// Convenience: iterate all emit items across all branches.
    pub fn all_emit_items(&self) -> impl Iterator<Item = &EmitItem> {
        self.branches.iter().flat_map(|b| &b.emit_items)
    }

    /// True when any branch has a guard.
    pub fn has_guards(&self) -> bool {
        self.branches.iter().any(|b| b.guard.is_some())
    }

    /// Return the single decision-level guard when there is exactly one branch.
    /// Useful for backward-compatible code paths.
    pub fn single_guard(&self) -> Option<&SqlFragment> {
        if self.branches.len() == 1 {
            self.branches[0].guard.as_ref()
        } else {
            None
        }
    }
}

/// A single branch in EMIT AS, separated by UNION ALL from other branches.
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionBranch {
    pub branch_index: usize,
    pub rule_name: Option<Spanned<String>>,
    pub guard: Option<SqlFragment>,
    pub emit_items: Vec<EmitItem>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EmitItem {
    pub event_type: Spanned<String>,
    pub assignments: Vec<Assignment>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateProjection {
    pub or_replace: bool,
    pub name: Spanned<String>,
    pub body: SqlFragment,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateEventStore {
    pub or_replace: bool,
    pub name: Spanned<String>,
    pub config: Vec<ConfigPair>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTemplate {
    pub or_replace: bool,
    pub name: Spanned<String>,
    pub params: Vec<TemplateParam>,
    pub body: Vec<Spanned<DeqlStatement>>,
    /// Raw body text for Phase 1 (template placeholders make full parsing complex).
    pub raw_body: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TemplateParam {
    pub name: Spanned<String>,
    pub data_type: Option<Spanned<DeqlType>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Execute {
    pub command: Spanned<String>,
    pub assignments: Vec<Assignment>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InspectDecision {
    pub name: Spanned<String>,
    pub from: Spanned<String>,
    pub into: Spanned<String>,
    pub offset: Option<Spanned<i64>>,
    pub guard: Option<SqlFragment>,
    pub limit: Option<Spanned<i64>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InspectProjection {
    pub name: Spanned<String>,
    pub from: Spanned<String>,
    pub into: Spanned<String>,
    pub offset: Option<Spanned<i64>>,
    pub guard: Option<SqlFragment>,
    pub limit: Option<Spanned<i64>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Describe {
    pub target: DescribeTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DescribeTarget {
    Single {
        concept: Spanned<ConceptType>,
        name: Spanned<String>,
    },
    ListAll {
        concept: Spanned<PluralConceptType>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConceptType {
    Aggregate,
    Command,
    Event,
    Decision,
    Projection,
    Inspection,
    EventStore,
    Template,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluralConceptType {
    Aggregates,
    Commands,
    Events,
    Decisions,
    Projections,
    Inspections,
    EventStores,
    Templates,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApplyTemplate {
    pub name: Spanned<String>,
    pub params: Vec<TemplateArg>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TemplateArg {
    pub name: Spanned<String>,
    pub value: Spanned<TemplateArgValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TemplateArgValue {
    StringLit(String),
    IntLit(i64),
    DecimalLit(f64),
    BoolLit(bool),
    FieldList(Vec<FieldDef>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExportDeReg {
    pub path: Option<Spanned<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExportMetadata {
    pub path: Option<Spanned<String>>,
}

// ---------------------------------------------------------------------------
// deql-core trait implementations
// ---------------------------------------------------------------------------

use crate::core::{
    AggregateDef, CommandDef, DecisionDef, EventDef, EventStoreDef, ProjectionDef, TemplateDef,
};

/// Helper: convert parser FieldDef to core FieldDef
fn to_core_field(f: &FieldDef) -> crate::core::FieldDef {
    crate::core::FieldDef {
        name: f.name.node.clone(),
        data_type: to_core_type(&f.data_type.node),
        is_key: f.is_key,
    }
}

/// Helper: convert parser DeqlType to core DeqlType
fn to_core_type(t: &DeqlType) -> crate::core::DeqlType {
    match t {
        DeqlType::Uuid => crate::core::DeqlType::Uuid,
        DeqlType::String => crate::core::DeqlType::String,
        DeqlType::Int => crate::core::DeqlType::Int,
        DeqlType::Decimal { precision, scale } => crate::core::DeqlType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DeqlType::Timestamp => crate::core::DeqlType::Timestamp,
        DeqlType::Boolean => crate::core::DeqlType::Boolean,
    }
}

/// Helper: convert parser ConfigPair to core ConfigPair
fn to_core_config_pair(p: &ConfigPair) -> crate::core::ConfigPair {
    crate::core::ConfigPair {
        key: p.key.node.clone(),
        value: to_core_config_value(&p.value.node),
    }
}

/// Helper: convert parser ConfigValue to core ConfigValue
fn to_core_config_value(v: &ConfigValue) -> crate::core::ConfigValue {
    match v {
        ConfigValue::StringLit(s) => crate::core::ConfigValue::StringLit(s.clone()),
        ConfigValue::IntLit(i) => crate::core::ConfigValue::IntLit(*i),
        ConfigValue::DecimalLit(d) => crate::core::ConfigValue::DecimalLit(*d),
        ConfigValue::BoolLit(b) => crate::core::ConfigValue::BoolLit(*b),
        ConfigValue::List(l) => crate::core::ConfigValue::List(l.clone()),
    }
}

impl AggregateDef for CreateAggregate {
    fn name(&self) -> &str {
        &self.name.node
    }
    fn fields(&self) -> Option<Vec<crate::core::FieldDef>> {
        self.fields
            .as_ref()
            .map(|fs| fs.iter().map(to_core_field).collect())
    }
}

impl CommandDef for CreateCommand {
    fn name(&self) -> &str {
        &self.name.node
    }
    fn fields(&self) -> Vec<crate::core::FieldDef> {
        self.fields.iter().map(to_core_field).collect()
    }
}

impl EventDef for CreateEvent {
    fn name(&self) -> &str {
        &self.name.node
    }
    fn fields(&self) -> Vec<crate::core::FieldDef> {
        self.fields.iter().map(to_core_field).collect()
    }
}

impl DecisionDef for CreateDecision {
    fn name(&self) -> &str {
        &self.name.node
    }
    fn aggregate_name(&self) -> &str {
        &self.aggregate.node
    }
    fn command_name(&self) -> &str {
        &self.command.node
    }
    fn emit_event_types(&self) -> Vec<&str> {
        self.all_emit_items()
            .map(|e| e.event_type.node.as_str())
            .collect()
    }
    fn has_guard(&self) -> bool {
        self.has_guards()
    }
    fn state_sql(&self) -> Option<&str> {
        self.state_as.as_ref().map(|s| s.sql.as_str())
    }
}

impl ProjectionDef for CreateProjection {
    fn name(&self) -> &str {
        &self.name.node
    }
    fn body_sql(&self) -> &str {
        &self.body.sql
    }
}

impl EventStoreDef for CreateEventStore {
    fn name(&self) -> &str {
        &self.name.node
    }
    fn config(&self) -> Vec<crate::core::ConfigPair> {
        self.config.iter().map(to_core_config_pair).collect()
    }
}

impl TemplateDef for CreateTemplate {
    fn name(&self) -> &str {
        &self.name.node
    }
    fn param_count(&self) -> usize {
        self.params.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldAnnotation {
    Sensitive,
    Volatile,
}
