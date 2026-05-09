//! Registry — HashMap-based in-memory concept storage.
//!
//! Ported from `deql-cli/deql-dereg/src/registry.rs`.
//! Provides insert/get/remove/list operations for all DeQL concept types.

use std::collections::HashMap;

use crate::{
    error::{ConceptKind, DeRegError},
    parser::ast::*,
};

/// Internal storage for all concept definitions.
pub struct Registry {
    pub(crate) aggregates: HashMap<String, CreateAggregate>,
    pub(crate) commands: HashMap<String, CreateCommand>,
    pub(crate) events: HashMap<String, CreateEvent>,
    pub(crate) decisions: HashMap<String, CreateDecision>,
    pub(crate) projections: HashMap<String, CreateProjection>,
    pub(crate) eventstores: HashMap<String, CreateEventStore>,
    pub(crate) templates: HashMap<String, CreateTemplate>,
}

/// Generates insert, get, remove, contains, and list_names methods for a concept type.
macro_rules! impl_registry_methods {
    (
        $field:ident,
        $kind:expr,
        $ast_type:ty,
        $insert:ident,
        $get:ident,
        $remove:ident,
        $contains:ident,
        $list:ident
    ) => {
        pub fn $insert(&mut self, def: $ast_type, or_replace: bool) -> Result<(), DeRegError> {
            let name = def.name.node.clone();
            if !or_replace && self.$field.contains_key(&name) {
                return Err(DeRegError::DuplicateName {
                    concept_kind: $kind,
                    name,
                });
            }
            self.$field.insert(name, def);
            Ok(())
        }

        pub fn $get(&self, name: &str) -> Option<&$ast_type> {
            self.$field.get(name)
        }

        pub fn $remove(&mut self, name: &str) -> Option<$ast_type> {
            self.$field.remove(name)
        }

        pub fn $contains(&self, name: &str) -> bool {
            self.$field.contains_key(name)
        }

        pub fn $list(&self) -> Vec<&str> {
            let mut names: Vec<&str> = self.$field.keys().map(|s| s.as_str()).collect();
            names.sort_unstable();
            names
        }
    };
}

impl Registry {
    pub fn new() -> Self {
        Self {
            aggregates: HashMap::new(),
            commands: HashMap::new(),
            events: HashMap::new(),
            decisions: HashMap::new(),
            projections: HashMap::new(),
            eventstores: HashMap::new(),
            templates: HashMap::new(),
        }
    }

    impl_registry_methods!(
        aggregates,
        ConceptKind::Aggregate,
        CreateAggregate,
        insert_aggregate,
        get_aggregate,
        remove_aggregate,
        contains_aggregate,
        list_aggregate_names
    );
    impl_registry_methods!(
        commands,
        ConceptKind::Command,
        CreateCommand,
        insert_command,
        get_command,
        remove_command,
        contains_command,
        list_command_names
    );
    impl_registry_methods!(
        events,
        ConceptKind::Event,
        CreateEvent,
        insert_event,
        get_event,
        remove_event,
        contains_event,
        list_event_names
    );
    impl_registry_methods!(
        decisions,
        ConceptKind::Decision,
        CreateDecision,
        insert_decision,
        get_decision,
        remove_decision,
        contains_decision,
        list_decision_names
    );
    impl_registry_methods!(
        projections,
        ConceptKind::Projection,
        CreateProjection,
        insert_projection,
        get_projection,
        remove_projection,
        contains_projection,
        list_projection_names
    );
    impl_registry_methods!(
        eventstores,
        ConceptKind::EventStore,
        CreateEventStore,
        insert_eventstore,
        get_eventstore,
        remove_eventstore,
        contains_eventstore,
        list_eventstore_names
    );
    impl_registry_methods!(
        templates,
        ConceptKind::Template,
        CreateTemplate,
        insert_template,
        get_template,
        remove_template,
        contains_template,
        list_template_names
    );
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}
