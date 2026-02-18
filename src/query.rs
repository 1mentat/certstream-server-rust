use crate::config::QueryApiConfig;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use std::sync::Arc;

pub struct QueryApiState {
    pub config: QueryApiConfig,
}

pub fn query_api_router(state: Arc<QueryApiState>) -> Router {
    Router::new()
        .route("/api/query/certs", get(handle_query_certs))
        .with_state(state)
}

async fn handle_query_certs(
    State(_state): State<Arc<QueryApiState>>,
) -> impl IntoResponse {
    (StatusCode::NOT_IMPLEMENTED, "Query API not yet implemented")
}
