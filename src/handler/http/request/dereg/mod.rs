use axum::{
    Json,
    extract::{Path, Query},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct Pagination {
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_size")]
    pub size: u32,
}

fn default_page() -> u32 {
    1
}

fn default_size() -> u32 {
    20
}

#[derive(Debug, Serialize)]
pub struct DeregIndexResponse {
    pub commands: &'static str,
    pub aggregates: &'static str,
    pub events: &'static str,
    pub projections: &'static str,
    pub inspections: &'static str,
}

#[derive(Debug, Serialize)]
pub struct CommandListResponse {
    pub page: u32,
    pub size: u32,
    pub total: u32,
    pub items: Vec<serde_json::Value>,
}

pub async fn index(Path(_org_id): Path<String>) -> Json<DeregIndexResponse> {
    Json(DeregIndexResponse {
        commands: "dereg/commands",
        aggregates: "dereg/aggregates",
        events: "dereg/events",
        projections: "dereg/projections",
        inspections: "dereg/inspections",
    })
}

pub async fn list_commands(
    Path(_org_id): Path<String>,
    Query(pagination): Query<Pagination>,
) -> Json<CommandListResponse> {
    Json(CommandListResponse {
        page: pagination.page,
        size: pagination.size,
        total: 0,
        items: Vec::new(),
    })
}

pub async fn not_implemented() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({"error": "not implemented in Phase 1"})),
    )
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use serde_json::Value;
    use tower::ServiceExt;

    use super::list_commands;

    #[tokio::test]
    async fn command_list_has_paginated_shape() {
        let app = Router::new().route("/{org_id}/dereg/commands", get(list_commands));
        let req = Request::builder()
            .method("GET")
            .uri("/o1/dereg/commands?page=2&size=10")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(body.get("page").is_some());
        assert!(body.get("size").is_some());
        assert!(body.get("total").is_some());
        assert!(body.get("items").is_some());
    }
}
