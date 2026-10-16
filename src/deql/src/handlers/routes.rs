use crate::DeqlConfig;

/// Stub router returning an empty Axum router under `/api/deql`.
pub fn deql_routes(_cfg: &DeqlConfig) -> axum::Router {
    axum::Router::new()
}
