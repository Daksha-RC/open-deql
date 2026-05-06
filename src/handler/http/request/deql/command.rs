use axum::{
    Json,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
pub struct ExecuteCommandRequest {
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub payload: Option<Value>,
}

pub async fn execute(
    Path((_org_id, aggregate)): Path<(String, String)>,
    Json(req): Json<ExecuteCommandRequest>,
) -> Response {
    if aggregate.trim().is_empty() || req.command.as_deref().is_none_or(str::is_empty) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "aggregate and command are required"
            })),
        )
            .into_response();
    }

    if req.mode.as_deref() == Some("async") {
        return (
            StatusCode::ACCEPTED,
            Json(json!({
                "status": "accepted",
                "aggregate": aggregate,
                "command": req.command,
            })),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "aggregate": aggregate,
            "command": req.command,
            "result": {
                "accepted": true,
                "payload": req.payload,
            }
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::post,
    };
    use tower::ServiceExt;

    use super::execute;

    #[tokio::test]
    async fn command_execute_returns_400_for_missing_command() {
        let app = Router::new().route("/{org_id}/deql/{aggregate}/command", post(execute));

        let req = Request::builder()
            .method("POST")
            .uri("/o1/deql/bank_account/command")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn command_execute_returns_202_for_async_mode() {
        let app = Router::new().route("/{org_id}/deql/{aggregate}/command", post(execute));

        let req = Request::builder()
            .method("POST")
            .uri("/o1/deql/bank_account/command")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"mode":"async","command":"Deposit"}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn command_execute_returns_200_for_sync_mode() {
        let app = Router::new().route("/{org_id}/deql/{aggregate}/command", post(execute));

        let req = Request::builder()
            .method("POST")
            .uri("/o1/deql/bank_account/command")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"mode":"sync","command":"Deposit","payload":{"amount":100}}"#,
            ))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
