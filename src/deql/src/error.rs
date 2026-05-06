use std::fmt;

use http::StatusCode;
use serde::Serialize;

use crate::parser::error::Diagnostic;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConceptKind {
    Aggregate,
    Command,
    Event,
    Decision,
    Projection,
    Inspection,
    EventStore,
    Template,
    Validate,
}

impl fmt::Display for ConceptKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConceptKind::Aggregate => write!(f, "AGGREGATE"),
            ConceptKind::Command => write!(f, "COMMAND"),
            ConceptKind::Event => write!(f, "EVENT"),
            ConceptKind::Decision => write!(f, "DECISION"),
            ConceptKind::Projection => write!(f, "PROJECTION"),
            ConceptKind::Inspection => write!(f, "INSPECTION"),
            ConceptKind::EventStore => write!(f, "EVENTSTORE"),
            ConceptKind::Template => write!(f, "TEMPLATE"),
            ConceptKind::Validate => write!(f, "VALIDATE"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DeRegError {
    DuplicateName {
        concept_kind: ConceptKind,
        name: String,
    },
    MissingReferences {
        source_kind: ConceptKind,
        source_name: String,
        missing: Vec<(ConceptKind, String)>,
    },
    DuplicateCommandBinding {
        command_name: String,
        existing_decision: String,
        new_decision: String,
    },
    NotFound {
        concept_kind: ConceptKind,
        name: String,
    },
    ExportIo {
        path: String,
        source: String,
    },
    ProjectionSqlInvalid {
        projection_name: String,
        source: String,
    },
    TemplateExpansion {
        template_name: String,
        detail: String,
    },
    TemplateParamInvalid {
        template_name: String,
        param_name: String,
        value: String,
        reason: String,
    },
}

impl fmt::Display for DeRegError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeRegError::DuplicateName { concept_kind, name } => {
                write!(
                    f,
                    "Duplicate name '{}' - a {} with this name already exists. Use CREATE OR REPLACE to overwrite.",
                    name, concept_kind
                )
            }
            DeRegError::MissingReferences {
                source_kind,
                source_name,
                missing,
            } => {
                write!(
                    f,
                    "{} '{}' references missing concepts:",
                    source_kind, source_name
                )?;
                for (kind, name) in missing {
                    write!(f, "\n  - {} '{}' not found", kind, name)?;
                }
                Ok(())
            }
            DeRegError::DuplicateCommandBinding {
                command_name,
                existing_decision,
                new_decision,
            } => {
                write!(
                    f,
                    "Duplicate command binding - command '{}' is already handled by decision '{}'. Cannot also bind to decision '{}'.",
                    command_name, existing_decision, new_decision
                )
            }
            DeRegError::NotFound { concept_kind, name } => {
                write!(f, "{} '{}' not found", concept_kind, name)
            }
            DeRegError::ExportIo { path, source } => {
                write!(f, "Failed to export to '{}': {}", path, source)
            }
            DeRegError::ProjectionSqlInvalid {
                projection_name,
                source,
            } => {
                write!(
                    f,
                    "Projection '{}' has invalid SQL: {}",
                    projection_name, source
                )
            }
            DeRegError::TemplateExpansion {
                template_name,
                detail,
            } => {
                write!(
                    f,
                    "Template '{}' expansion failed: {}",
                    template_name, detail
                )
            }
            DeRegError::TemplateParamInvalid {
                template_name,
                param_name,
                value,
                reason,
            } => {
                write!(
                    f,
                    "Template '{}': parameter '{}' value '{}' {}",
                    template_name, param_name, value, reason
                )
            }
        }
    }
}

impl std::error::Error for DeRegError {}

#[derive(Debug, Clone)]
pub enum ServiceError {
    Parse(Vec<Diagnostic>),
    DeReg(DeRegError),
    Internal(String),
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceError::Parse(diagnostics) => {
                if diagnostics.is_empty() {
                    write!(f, "Parse error")
                } else {
                    write!(f, "{}", diagnostics[0].message)
                }
            }
            ServiceError::DeReg(err) => write!(f, "{}", err),
            ServiceError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl From<DeRegError> for ServiceError {
    fn from(value: DeRegError) -> Self {
        ServiceError::DeReg(value)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ApiErrorBody,
}

impl ApiError {
    pub fn status_code(&self) -> StatusCode {
        self.status
    }

    pub fn json_body(&self) -> &ApiErrorBody {
        &self.body
    }
}

impl From<ServiceError> for ApiError {
    fn from(value: ServiceError) -> Self {
        match value {
            ServiceError::Parse(diags) => ApiError {
                status: StatusCode::BAD_REQUEST,
                body: ApiErrorBody {
                    code: "parse_error".to_string(),
                    message: if diags.is_empty() {
                        "Parse error".to_string()
                    } else {
                        diags[0].message.clone()
                    },
                },
            },
            ServiceError::DeReg(err) => {
                let (status, code) = match &err {
                    DeRegError::DuplicateName { .. } => {
                        (StatusCode::CONFLICT, "dereg_duplicate_name")
                    }
                    DeRegError::NotFound { .. } => (StatusCode::NOT_FOUND, "dereg_not_found"),
                    DeRegError::MissingReferences { .. } => {
                        (StatusCode::UNPROCESSABLE_ENTITY, "dereg_missing_references")
                    }
                    DeRegError::DuplicateCommandBinding { .. } => {
                        (StatusCode::CONFLICT, "dereg_duplicate_command_binding")
                    }
                    DeRegError::ProjectionSqlInvalid { .. } => {
                        (StatusCode::BAD_REQUEST, "dereg_projection_sql_invalid")
                    }
                    DeRegError::TemplateExpansion { .. } => {
                        (StatusCode::UNPROCESSABLE_ENTITY, "dereg_template_expansion")
                    }
                    DeRegError::TemplateParamInvalid { .. } => {
                        (StatusCode::BAD_REQUEST, "dereg_template_param_invalid")
                    }
                    DeRegError::ExportIo { .. } => {
                        (StatusCode::INTERNAL_SERVER_ERROR, "dereg_export_io")
                    }
                };
                ApiError {
                    status,
                    body: ApiErrorBody {
                        code: code.to_string(),
                        message: err.to_string(),
                    },
                }
            }
            ServiceError::Internal(msg) => ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: ApiErrorBody {
                    code: "internal_error".to_string(),
                    message: msg,
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_name_message_contains_name_and_kind() {
        let msg = format!(
            "{}",
            DeRegError::DuplicateName {
                concept_kind: ConceptKind::Aggregate,
                name: "X".to_string(),
            }
        );
        assert!(msg.contains("Duplicate name 'X'"));
        assert!(msg.contains("AGGREGATE"));
    }

    #[test]
    fn missing_references_renders_multiline() {
        let msg = format!(
            "{}",
            DeRegError::MissingReferences {
                source_kind: ConceptKind::Decision,
                source_name: "D1".to_string(),
                missing: vec![
                    (ConceptKind::Aggregate, "A1".to_string()),
                    (ConceptKind::Command, "C1".to_string()),
                ],
            }
        );
        assert!(msg.contains("\n  - AGGREGATE 'A1' not found"));
        assert!(msg.contains("\n  - COMMAND 'C1' not found"));
    }

    #[test]
    fn service_error_from_dereg_renders_inner_message() {
        let msg = format!(
            "{}",
            ServiceError::from(DeRegError::NotFound {
                concept_kind: ConceptKind::Aggregate,
                name: "X".to_string(),
            })
        );
        assert!(msg.contains("AGGREGATE 'X' not found"));
    }

    #[test]
    fn api_error_maps_not_found_to_404() {
        let api_err = ApiError::from(ServiceError::DeReg(DeRegError::NotFound {
            concept_kind: ConceptKind::Aggregate,
            name: "X".to_string(),
        }));
        assert_eq!(api_err.status_code(), StatusCode::NOT_FOUND);
    }
}
