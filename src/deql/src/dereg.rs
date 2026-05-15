//! DeReg — The Decision Registry central catalog.
//!
//! Ported from `deql-cli/deql-dereg/src/lib.rs` (DeReg struct).
//! Simplified for production: no WAL, no DataFusion, no event store instances.
//! Phase 2 persistence uses SeaORM-backed `dereg_meta_store`.

use std::collections::HashMap;

use crate::error::{ConceptKind, DeRegError};
use crate::parser::ast::*;
use crate::registry::Registry;
use crate::validator;

/// Registration outcome for the write handler.
#[derive(Debug)]
pub struct RegistrationResult {
    pub event_type: &'static str,
    pub concept_type: ConceptKind,
    pub concept_name: String,
    pub or_replace: bool,
}

/// DROP outcome for the write handler.
#[derive(Debug)]
pub struct DropResult {
    pub concept_type: ConceptKind,
    pub concept_name: String,
}

/// The Decision Registry — central catalog for all DeQL definitions.
pub struct DeReg {
    pub(crate) registry: Registry,
    pub(crate) command_map: HashMap<String, String>,
}

impl DeReg {
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
            command_map: HashMap::new(),
        }
    }

    /// Register a parsed DeQL statement. Performs cross-reference validation
    /// for decisions and projections.
    pub fn register_statement(
        &mut self,
        stmt: &DeqlStatement,
    ) -> Result<RegistrationResult, DeRegError> {
        match stmt {
            DeqlStatement::CreateAggregate(a) => {
                self.registry.insert_aggregate(a.clone(), a.or_replace)?;
                Ok(RegistrationResult {
                    event_type: "AggregateCreated",
                    concept_type: ConceptKind::Aggregate,
                    concept_name: a.name.node.clone(),
                    or_replace: a.or_replace,
                })
            }
            DeqlStatement::CreateCommand(c) => {
                self.registry.insert_command(c.clone(), c.or_replace)?;
                Ok(RegistrationResult {
                    event_type: "CommandCreated",
                    concept_type: ConceptKind::Command,
                    concept_name: c.name.node.clone(),
                    or_replace: c.or_replace,
                })
            }
            DeqlStatement::CreateEvent(e) => {
                self.registry.insert_event(e.clone(), e.or_replace)?;
                Ok(RegistrationResult {
                    event_type: "EventCreated",
                    concept_type: ConceptKind::Event,
                    concept_name: e.name.node.clone(),
                    or_replace: e.or_replace,
                })
            }
            DeqlStatement::CreateDecision(d) => {
                validator::validate_decision(d, &self.registry)?;

                let cmd_name = d.command.node.clone();
                let dec_name = d.name.node.clone();
                if let Some(existing) = self.command_map.get(&cmd_name) {
                    if existing != &dec_name && !d.or_replace {
                        return Err(DeRegError::DuplicateCommandBinding {
                            command_name: cmd_name,
                            existing_decision: existing.clone(),
                            new_decision: dec_name,
                        });
                    }
                }

                self.registry.insert_decision(d.clone(), d.or_replace)?;
                self.command_map.insert(cmd_name, dec_name.clone());
                Ok(RegistrationResult {
                    event_type: "DecisionCreated",
                    concept_type: ConceptKind::Decision,
                    concept_name: dec_name,
                    or_replace: d.or_replace,
                })
            }
            DeqlStatement::CreateProjection(p) => {
                validator::validate_projection(p, &self.registry)?;
                self.registry.insert_projection(p.clone(), p.or_replace)?;
                Ok(RegistrationResult {
                    event_type: "ProjectionCreated",
                    concept_type: ConceptKind::Projection,
                    concept_name: p.name.node.clone(),
                    or_replace: p.or_replace,
                })
            }
            DeqlStatement::CreateEventStore(es) => {
                self.registry.eventstores.clear();
                self.registry.insert_eventstore(es.clone(), true)?;
                Ok(RegistrationResult {
                    event_type: "EventStoreCreated",
                    concept_type: ConceptKind::EventStore,
                    concept_name: es.name.node.clone(),
                    or_replace: true,
                })
            }
            DeqlStatement::CreateTemplate(t) => {
                self.registry.insert_template(t.clone(), t.or_replace)?;
                Ok(RegistrationResult {
                    event_type: "TemplateCreated",
                    concept_type: ConceptKind::Template,
                    concept_name: t.name.node.clone(),
                    or_replace: t.or_replace,
                })
            }
            _ => Err(DeRegError::NotFound {
                concept_kind: ConceptKind::Validate,
                name: "unsupported statement type".to_string(),
            }),
        }
    }

    /// Drop a concept from the in-memory registry.
    /// Returns `NotFound` if the concept does not exist (no tombstone should be written).
    /// [REQ-021] [REQ-023]
    pub fn drop_concept(
        &mut self,
        concept_kind: ConceptKind,
        name: &str,
    ) -> Result<DropResult, DeRegError> {
        let removed = match concept_kind {
            ConceptKind::Aggregate => self.registry.remove_aggregate(name).is_some(),
            ConceptKind::Command => self.registry.remove_command(name).is_some(),
            ConceptKind::Event => self.registry.remove_event(name).is_some(),
            ConceptKind::Decision => {
                if self.registry.remove_decision(name).is_some() {
                    // Also remove from command_map
                    self.command_map.retain(|_, v| v != name);
                    true
                } else {
                    false
                }
            }
            ConceptKind::Projection => self.registry.remove_projection(name).is_some(),
            ConceptKind::EventStore => self.registry.remove_eventstore(name).is_some(),
            ConceptKind::Template => self.registry.remove_template(name).is_some(),
            _ => false,
        };

        if removed {
            Ok(DropResult {
                concept_type: concept_kind,
                concept_name: name.to_string(),
            })
        } else {
            Err(DeRegError::NotFound {
                concept_kind,
                name: name.to_string(),
            })
        }
    }

    // --- Lookup ---

    pub fn get_aggregate(&self, name: &str) -> Option<&CreateAggregate> {
        self.registry.get_aggregate(name)
    }

    pub fn get_command(&self, name: &str) -> Option<&CreateCommand> {
        self.registry.get_command(name)
    }

    pub fn get_event(&self, name: &str) -> Option<&CreateEvent> {
        self.registry.get_event(name)
    }

    pub fn get_decision(&self, name: &str) -> Option<&CreateDecision> {
        self.registry.get_decision(name)
    }

    pub fn get_template(&self, name: &str) -> Option<&CreateTemplate> {
        self.registry.get_template(name)
    }

    pub fn get_projection(&self, name: &str) -> Option<&CreateProjection> {
        self.registry.get_projection(name)
    }

    // --- List ---

    pub fn list_aggregate_names(&self) -> Vec<&str> {
        self.registry.list_aggregate_names()
    }

    pub fn list_command_names(&self) -> Vec<&str> {
        self.registry.list_command_names()
    }

    pub fn list_event_names(&self) -> Vec<&str> {
        self.registry.list_event_names()
    }

    pub fn list_decision_names(&self) -> Vec<&str> {
        self.registry.list_decision_names()
    }

    pub fn list_projection_names(&self) -> Vec<&str> {
        self.registry.list_projection_names()
    }

    pub fn list_template_names(&self) -> Vec<&str> {
        self.registry.list_template_names()
    }

    // --- Contains ---

    pub fn contains_aggregate(&self, name: &str) -> bool {
        self.registry.contains_aggregate(name)
    }

    pub fn contains_command(&self, name: &str) -> bool {
        self.registry.contains_command(name)
    }

    pub fn contains_event(&self, name: &str) -> bool {
        self.registry.contains_event(name)
    }

    pub fn contains_decision(&self, name: &str) -> bool {
        self.registry.contains_decision(name)
    }

    pub fn contains_projection(&self, name: &str) -> bool {
        self.registry.contains_projection(name)
    }

    pub fn contains_template(&self, name: &str) -> bool {
        self.registry.contains_template(name)
    }

    // --- Counts ---

    pub fn aggregate_count(&self) -> usize {
        self.registry.aggregates.len()
    }

    pub fn command_count(&self) -> usize {
        self.registry.commands.len()
    }

    pub fn event_count(&self) -> usize {
        self.registry.events.len()
    }

    pub fn decision_count(&self) -> usize {
        self.registry.decisions.len()
    }

    pub fn projection_count(&self) -> usize {
        self.registry.projections.len()
    }

    pub fn template_count(&self) -> usize {
        self.registry.templates.len()
    }
}

impl Default for DeReg {
    fn default() -> Self {
        Self::new()
    }
}
