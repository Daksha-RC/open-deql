//! HTTP request handlers for DeQL introspection endpoints (Phase 4).
//!
//! Provides read-only endpoints for runtime introspection, registry inspection,
//! and concept metadata discovery.
//!
//! All endpoints are behind `#[cfg(feature = "deql")]`.

use axum::{
    Json,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use infra::db::ORM_CLIENT;
use o2_deql::store::projections::{
    meta_aggregates, meta_commands, meta_decisions, meta_events, meta_templates,
};
use sea_orm::EntityTrait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;

use super::super::dereg::get_deql_state;

/// Pagination query parameters for list endpoints.
#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

fn default_limit() -> usize {
    100
}

/// DeQL info response.
#[derive(Debug, Serialize, ToSchema)]
pub struct DeqlInfoResponse {
    pub version: String,
    pub readonly: bool,
    pub counts: ConceptCounts,
    pub last_stream_seq: Option<i64>,
    /// In-memory rehydrate watermark (sequence id of last successful rehydrate)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rehydrate_revision: Option<i64>,
    /// Most recent rehydrate result (success or failure with timestamp & message)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rehydrate_result: Option<LastRehydrateResult>,
}

/// Last rehydrate result information.
#[derive(Debug, Serialize, ToSchema, Clone)]
pub struct LastRehydrateResult {
    pub status: String,              // "success" | "failure"
    pub timestamp: String,           // ISO 8601 UTC
    pub elapsed_ms: u64,
    pub rows_processed: i64,
    pub last_sequence_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Counts of registered concepts.
#[derive(Debug, Serialize, ToSchema)]
pub struct ConceptCounts {
    pub aggregates: usize,
    pub commands: usize,
    pub events: usize,
    pub decisions: usize,
    pub projections: usize,
    pub templates: usize,
}

/// Paginated list response.
#[derive(Debug, Serialize, ToSchema)]
pub struct PaginatedListResponse<T> {
    pub items: Vec<T>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

/// Registry item summary (for list endpoints).
#[derive(Debug, Serialize, ToSchema)]
pub struct RegistryItemSummary {
    pub name: String,
    pub concept_type: String,
}

/// Registry item detail (for detail endpoints).
#[derive(Debug, Serialize, ToSchema)]
pub struct RegistryItemDetail {
    pub name: String,
    pub concept_type: String,
    pub source: Option<String>,
    pub fields: Vec<FieldInfo>,
    pub meta: Value,
}

/// Field information for schema endpoints.
#[derive(Debug, Serialize, ToSchema)]
pub struct FieldInfo {
    pub name: String,
    pub field_type: String,
    pub nullable: bool,
}

/// Route info for routes endpoint.
#[derive(Debug, Serialize, ToSchema)]
pub struct RouteInfo {
    pub method: String,
    pub path: String,
    pub concept_type: String,
    pub concept_name: String,
}

/// GET /{org}/deql/info — server version, concept counts, readonly status
#[utoipa::path(
    get,
    path = "/{org_id}/deql/info",
    context_path = "/api",
    tag = "DeQL Inspection",
    operation_id = "DeqlInfo",
    summary = "Get DeQL server info",
    description = "Returns server version, counts of registered concepts, readonly status, and last-applied stream_seq for DeReg projections.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
    ),
    responses(
        (status = StatusCode::OK, description = "DeQL info", body = DeqlInfoResponse),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Server Error"),
    ),
    extensions(
        ("x-o2-mcp" = json!({"description": "Get DeQL server info and concept counts", "category": "deql"}))
    )
)]
pub async fn info(Path(org_id): Path<String>) -> Response {
    let state = get_deql_state().await;
    let dereg_arc = state.org_map.get_or_init(&org_id).await;
    let dereg = dereg_arc.read().await;

    let counts = ConceptCounts {
        aggregates: dereg.aggregate_count(),
        commands: dereg.command_count(),
        events: dereg.event_count(),
        decisions: dereg.decision_count(),
        projections: dereg.projection_count(),
        templates: dereg.template_count(),
    };

    // Fetch in-memory rehydrate state
    let rehydrate_state_map = state.rehydrate_state_map.read().await;
    let rehydrate_state = rehydrate_state_map.get(&org_id);
    let rehydrate_revision = rehydrate_state.and_then(|rs| rs.revision);
    let last_result = rehydrate_state.and_then(|rs| rs.last_result.as_ref()).map(|r| {
        LastRehydrateResult {
            status: r.status.clone(),
            timestamp: r.end_time.to_rfc3339(),
            elapsed_ms: r.elapsed_ms,
            rows_processed: r.rows_processed,
            last_sequence_id: r.last_sequence_id,
            error_message: r.error_message.clone(),
        }
    });

    let response = DeqlInfoResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        readonly: false,
        counts,
        last_stream_seq: None, // TODO: fetch from dereg_meta_store
        rehydrate_revision,
        last_rehydrate_result: last_result,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// GET /{org}/deql/registry/{type} — list registered concepts (paginated)
#[utoipa::path(
    get,
    path = "/{org_id}/deql/registry/{concept_type}",
    context_path = "/api",
    tag = "DeQL Inspection",
    operation_id = "ListDeqlConcepts",
    summary = "List registered concepts",
    description = "Returns a paginated list of registered concepts of the specified type.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("concept_type" = String, Path, description = "Concept type: aggregates, commands, events, decisions, projections, templates"),
        ("limit" = Option<usize>, Query, description = "Page size (default 100)"),
        ("offset" = Option<usize>, Query, description = "Offset for pagination (default 0)"),
    ),
    responses(
        (status = StatusCode::OK, description = "List of concepts", body = PaginatedListResponse<RegistryItemSummary>),
        (status = StatusCode::BAD_REQUEST, description = "Invalid concept type"),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Server Error"),
    ),
    extensions(
        ("x-o2-mcp" = json!({"description": "List registered DeQL concepts by type", "category": "deql"}))
    )
)]
pub async fn list_concepts(
    Path((org_id, concept_type)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
) -> Response {
    let state = get_deql_state().await;
    let dereg_arc = state.org_map.get_or_init(&org_id).await;
    let dereg = dereg_arc.read().await;

    let names: Vec<&str> = match concept_type.as_str() {
        "aggregates" => dereg.list_aggregate_names(),
        "commands" => dereg.list_command_names(),
        "events" => dereg.list_event_names(),
        "decisions" => dereg.list_decision_names(),
        "projections" => dereg.list_projection_names(),
        "templates" => dereg.list_template_names(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Invalid concept type: {}. Valid types: aggregates, commands, events, decisions, projections, templates", concept_type)
                })),
            )
                .into_response();
        }
    };

    let total = names.len();
    let items: Vec<RegistryItemSummary> = names
        .into_iter()
        .skip(pagination.offset)
        .take(pagination.limit)
        .map(|name: &str| RegistryItemSummary {
            name: name.to_string(),
            concept_type: concept_type.clone(),
        })
        .collect();

    let response = PaginatedListResponse {
        items,
        total,
        limit: pagination.limit,
        offset: pagination.offset,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// GET /{org}/deql/registry/{type}/{name} — concept detail
#[utoipa::path(
    get,
    path = "/{org_id}/deql/registry/{concept_type}/{name}",
    context_path = "/api",
    tag = "DeQL Inspection",
    operation_id = "GetDeqlConcept",
    summary = "Get concept detail",
    description = "Returns detailed information about a registered concept, including source DeQL text and fields.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("concept_type" = String, Path, description = "Concept type: aggregates, commands, events, decisions, projections, templates"),
        ("name" = String, Path, description = "Concept name"),
    ),
    responses(
        (status = StatusCode::OK, description = "Concept detail", body = RegistryItemDetail),
        (status = StatusCode::NOT_FOUND, description = "Concept not found"),
        (status = StatusCode::BAD_REQUEST, description = "Invalid concept type"),
    ),
    extensions(
        ("x-o2-mcp" = json!({"description": "Get detailed information about a DeQL concept", "category": "deql"}))
    )
)]
pub async fn get_concept(
    Path((org_id, concept_type, name)): Path<(String, String, String)>,
) -> Response {
    let state = get_deql_state().await;
    let dereg_arc = state.org_map.get_or_init(&org_id).await;
    let dereg = dereg_arc.read().await;
    // Prefer projection tables for authoritative metadata when available.
    // If the DB row is missing (projection worker hasn't run yet) or the ORM
    // client is not initialized (unit tests), fall back to the in-memory DeReg
    // state.

    // Validate concept_type early to avoid duplicating validation in both branches.
    match concept_type.as_str() {
        "aggregates" | "commands" | "events" | "decisions" | "projections" | "templates" => {}
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Invalid concept type: {}. Valid types: aggregates, commands, events, decisions, projections, templates", concept_type)
                })),
            )
            .into_response();
        }
    }

    // --- Try projection tables first ---
    let db_meta: Option<Value> = if let Some(db) = ORM_CLIENT.get() {
        match concept_type.as_str() {
            "aggregates" => {
                match meta_aggregates::Entity::find_by_id((org_id.clone(), name.clone()))
                    .one(db)
                    .await
                {
                    Ok(Some(m)) => Some(json!({
                        "org_id": m.org_id,
                        "name": m.name,
                        "fields_json": m.fields_json,
                        "last_applied_id": m.last_applied_id,
                        "is_dropped": m.is_dropped,
                    })),
                    Ok(None) => None,
                    Err(e) => {
                        log::warn!("DB error querying meta_aggregates for {name}: {e}");
                        None
                    }
                }
            }
            "commands" => match meta_commands::Entity::find_by_id((org_id.clone(), name.clone()))
                .one(db)
                .await
            {
                Ok(Some(m)) => Some(json!({
                    "org_id": m.org_id,
                    "name": m.name,
                    "aggregate": m.aggregate,
                    "attributes_json": m.attributes_json,
                    "full_sql": m.full_sql,
                    "last_applied_id": m.last_applied_id,
                    "is_dropped": m.is_dropped,
                })),
                Ok(None) => None,
                Err(e) => {
                    log::warn!("DB error querying meta_commands for {name}: {e}");
                    None
                }
            },
            "events" => match meta_events::Entity::find_by_id((org_id.clone(), name.clone()))
                .one(db)
                .await
            {
                Ok(Some(m)) => Some(json!({
                    "org_id": m.org_id,
                    "name": m.name,
                    "aggregate": m.aggregate,
                    "attributes_json": m.attributes_json,
                    "full_sql": m.full_sql,
                    "last_applied_id": m.last_applied_id,
                    "is_dropped": m.is_dropped,
                })),
                Ok(None) => None,
                Err(e) => {
                    log::warn!("DB error querying meta_events for {name}: {e}");
                    None
                }
            },
            "decisions" => match meta_decisions::Entity::find_by_id((org_id.clone(), name.clone()))
                .one(db)
                .await
            {
                Ok(Some(m)) => Some(json!({
                    "org_id": m.org_id,
                    "name": m.name,
                    "aggregate": m.aggregate,
                    "command": m.command,
                    "emits_json": m.emits_json,
                    "has_guard": m.has_guard,
                    "guard_sql": m.guard_sql,
                    "state_sql": m.state_sql,
                    "full_sql": m.full_sql,
                    "last_applied_id": m.last_applied_id,
                    "is_dropped": m.is_dropped,
                })),
                Ok(None) => None,
                Err(e) => {
                    log::warn!("DB error querying meta_decisions for {name}: {e}");
                    None
                }
            },
            "projections" | "templates" => {
                match meta_templates::Entity::find_by_id((org_id.clone(), name.clone()))
                    .one(db)
                    .await
                {
                    Ok(Some(m)) => Some(json!({
                        "org_id": m.org_id,
                        "name": m.name,
                        "parameters_json": m.parameters_json,
                        "full_sql": m.full_sql,
                        "last_applied_id": m.last_applied_id,
                        "is_dropped": m.is_dropped,
                    })),
                    Ok(None) => None,
                    Err(e) => {
                        log::warn!("DB error querying meta_templates for {name}: {e}");
                        None
                    }
                }
            }
            _ => None, // unreachable — validated above
        }
    } else {
        None
    };

    // --- Fall back to in-memory DeReg if DB had no row ---
    let meta_json: Option<Value> = db_meta.or_else(|| {
        match concept_type.as_str() {
            "aggregates" => dereg.get_aggregate(&name).map(|agg| {
                let fields_out: Vec<FieldInfo> = agg
                    .fields
                    .as_ref()
                    .map(|fs| {
                        fs.iter()
                            .map(|f| FieldInfo {
                                name: f.name.node.clone(),
                                field_type: format!("{:?}", f.data_type.node),
                                nullable: !f.is_key,
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                json!({
                    "org_id": org_id.clone(),
                    "name": name.clone(),
                    "fields": fields_out,
                })
            }),
            "commands" => dereg.get_command(&name).map(|cmd| {
                let fields_out: Vec<FieldInfo> = cmd
                    .fields
                    .iter()
                    .map(|f| FieldInfo {
                        name: f.name.node.clone(),
                        field_type: format!("{:?}", f.data_type.node),
                        nullable: !f.is_key,
                    })
                    .collect();
                json!({
                    "org_id": org_id.clone(),
                    "name": name.clone(),
                    "fields": fields_out,
                })
            }),
            "events" => dereg.get_event(&name).map(|evt| {
                let fields_out: Vec<FieldInfo> = evt
                    .fields
                    .iter()
                    .map(|f| FieldInfo {
                        name: f.name.node.clone(),
                        field_type: format!("{:?}", f.data_type.node),
                        nullable: !f.is_key,
                    })
                    .collect();
                json!({
                    "org_id": org_id.clone(),
                    "name": name.clone(),
                    "fields": fields_out,
                })
            }),
            "decisions" => {
                if dereg.contains_decision(&name) {
                    Some(json!({"org_id": org_id.clone(), "name": name.clone()}))
                } else {
                    None
                }
            }
            "projections" => {
                if dereg.contains_projection(&name) {
                    Some(json!({"org_id": org_id.clone(), "name": name.clone()}))
                } else {
                    None
                }
            }
            "templates" => {
                if dereg.contains_template(&name) {
                    Some(json!({"org_id": org_id.clone(), "name": name.clone()}))
                } else {
                    None
                }
            }
            _ => None, // unreachable — validated above
        }
    });

    if meta_json.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("{} '{}' not found", concept_type, name)})),
        )
            .into_response();
    }

    let response = RegistryItemDetail {
        name: name.clone(),
        concept_type: concept_type.clone(),
        source: None,   // TODO: fetch from dereg_meta_store if needed
        fields: vec![], // schema can be fetched via schema endpoint if required
        meta: meta_json.unwrap(),
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// GET /{org}/deql/registry/{type}/{name}/schema — concept schema
#[utoipa::path(
    get,
    path = "/{org_id}/deql/registry/{concept_type}/{name}/schema",
    context_path = "/api",
    tag = "DeQL Inspection",
    operation_id = "GetDeqlConceptSchema",
    summary = "Get concept schema",
    description = "Returns the canonical schema (field names and types) for a registered concept.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("concept_type" = String, Path, description = "Concept type: aggregates, commands, events"),
        ("name" = String, Path, description = "Concept name"),
    ),
    responses(
        (status = StatusCode::OK, description = "Concept schema", body = Vec<FieldInfo>),
        (status = StatusCode::NOT_FOUND, description = "Concept not found"),
        (status = StatusCode::BAD_REQUEST, description = "Invalid concept type or schema not available"),
    ),
    extensions(
        ("x-o2-mcp" = json!({"description": "Get schema (field names and types) for a DeQL concept", "category": "deql"}))
    )
)]
pub async fn get_concept_schema(
    Path((org_id, concept_type, name)): Path<(String, String, String)>,
) -> Response {
    let state = get_deql_state().await;
    let dereg_arc = state.org_map.get_or_init(&org_id).await;
    let dereg = dereg_arc.read().await;

    let fields: Option<Vec<FieldInfo>> = match concept_type.as_str() {
        "aggregates" => dereg.get_aggregate(&name).map(|agg| {
            agg.fields
                .as_ref()
                .map(|fs| {
                    fs.iter()
                        .map(|f| FieldInfo {
                            name: f.name.node.clone(),
                            field_type: format!("{:?}", f.data_type.node),
                            nullable: !f.is_key,
                        })
                        .collect()
                })
                .unwrap_or_default()
        }),
        "commands" => dereg.get_command(&name).map(|cmd| {
            cmd.fields
                .iter()
                .map(|f| FieldInfo {
                    name: f.name.node.clone(),
                    field_type: format!("{:?}", f.data_type.node),
                    nullable: !f.is_key,
                })
                .collect()
        }),
        "events" => dereg.get_event(&name).map(|evt| {
            evt.fields
                .iter()
                .map(|f| FieldInfo {
                    name: f.name.node.clone(),
                    field_type: format!("{:?}", f.data_type.node),
                    nullable: !f.is_key,
                })
                .collect()
        }),
        "decisions" | "projections" | "templates" => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Schema not available for concept type: {}", concept_type)
                })),
            )
                .into_response();
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Invalid concept type: {}. Valid types for schema: aggregates, commands, events", concept_type)
                })),
            )
                .into_response();
        }
    };

    match fields {
        Some(f) => (StatusCode::OK, Json(f)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": format!("{} '{}' not found", concept_type, name)
            })),
        )
            .into_response(),
    }
}

/// GET /{org}/deql/registry/routes — list REST routes exported by DeQL-managed concepts
#[utoipa::path(
    get,
    path = "/{org_id}/deql/registry/routes",
    context_path = "/api",
    tag = "DeQL Inspection",
    operation_id = "ListDeqlRoutes",
    summary = "List DeQL-managed routes",
    description = "Returns a list of REST routes exported by DeQL-managed concepts for console wiring.",
    params(
        ("org_id" = String, Path, description = "Organization name"),
    ),
    responses(
        (status = StatusCode::OK, description = "List of routes", body = Vec<RouteInfo>),
        (status = StatusCode::INTERNAL_SERVER_ERROR, description = "Internal Server Error"),
    ),
    extensions(
        ("x-o2-mcp" = json!({"description": "List REST routes exported by DeQL-managed concepts", "category": "deql"}))
    )
)]
pub async fn list_routes(Path(org_id): Path<String>) -> Response {
    let state = get_deql_state().await;
    let dereg_arc = state.org_map.get_or_init(&org_id).await;
    let dereg = dereg_arc.read().await;

    let mut routes: Vec<RouteInfo> = Vec::new();

    // Generate routes for aggregates (command endpoint)
    for name in dereg.list_aggregate_names() {
        routes.push(RouteInfo {
            method: "POST".to_string(),
            path: format!("/api/{}/deql/{}/command", org_id, name),
            concept_type: "aggregate".to_string(),
            concept_name: name.to_string(),
        });
    }

    (StatusCode::OK, Json(routes)).into_response()
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn info_returns_200_with_counts() {
        let app = Router::new().route("/{org_id}/deql/info", get(info));

        let req = Request::builder()
            .method("GET")
            .uri("/test_org/deql/info")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_concepts_returns_400_for_invalid_type() {
        let app = Router::new().route("/{org_id}/deql/registry/{concept_type}", get(list_concepts));

        let req = Request::builder()
            .method("GET")
            .uri("/test_org/deql/registry/invalid_type")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn list_concepts_returns_200_for_valid_type() {
        let app = Router::new().route("/{org_id}/deql/registry/{concept_type}", get(list_concepts));

        let req = Request::builder()
            .method("GET")
            .uri("/test_org/deql/registry/aggregates")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_concept_returns_404_for_missing() {
        let app = Router::new().route(
            "/{org_id}/deql/registry/{concept_type}/{name}",
            get(get_concept),
        );

        let req = Request::builder()
            .method("GET")
            .uri("/test_org/deql/registry/aggregates/NonExistent")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn list_routes_returns_200() {
        let app = Router::new().route("/{org_id}/deql/registry/routes", get(list_routes));

        let req = Request::builder()
            .method("GET")
            .uri("/test_org/deql/registry/routes")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
