use crate::core::types::{ConfigPair, FieldDef};

/// Trait for aggregate definitions.
pub trait AggregateDef {
    fn name(&self) -> &str;
    fn fields(&self) -> Option<Vec<FieldDef>>;
}

/// Trait for command definitions.
pub trait CommandDef {
    fn name(&self) -> &str;
    fn fields(&self) -> Vec<FieldDef>;
}

/// Trait for event definitions.
pub trait EventDef {
    fn name(&self) -> &str;
    fn fields(&self) -> Vec<FieldDef>;
}

/// Trait for decision definitions.
pub trait DecisionDef {
    fn name(&self) -> &str;
    fn aggregate_name(&self) -> &str;
    fn command_name(&self) -> &str;
    fn emit_event_types(&self) -> Vec<&str>;
    fn has_guard(&self) -> bool;
    fn state_sql(&self) -> Option<&str>;
}

/// Trait for projection definitions.
pub trait ProjectionDef {
    fn name(&self) -> &str;
    fn body_sql(&self) -> &str;
}

/// Trait for event store definitions.
pub trait EventStoreDef {
    fn name(&self) -> &str;
    fn config(&self) -> Vec<ConfigPair>;
}

/// Trait for template definitions.
pub trait TemplateDef {
    fn name(&self) -> &str;
    fn param_count(&self) -> usize;
}
