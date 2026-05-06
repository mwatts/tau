use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::{Router, http};
use rust_embed::Embed;
use std::sync::Arc;

#[derive(Embed)]
#[folder = "assets/"]
struct Assets;

#[derive(Clone)]
pub struct AppState {
    pub token: String,
}

#[derive(serde::Deserialize)]
pub struct TokenQuery {
    token: Option<String>,
}

pub fn build_router(token: String) -> Router {
    let state = Arc::new(AppState { token });
    Router::new()
        .route("/health", axum::routing::get(health))
        .route("/ws", axum::routing::get(ws_handler))
        .fallback(static_handler)
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(query): Query<TokenQuery>,
) -> impl IntoResponse {
    if query.token.as_deref() != Some(&state.token) {
        return (http::StatusCode::UNAUTHORIZED, "invalid token").into_response();
    }
    ws.on_upgrade(crate::ws_bridge::handle_ws).into_response()
}

async fn static_handler(uri: http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Assets::get(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                [(http::header::CONTENT_TYPE, mime.as_ref())],
                file.data,
            )
                .into_response()
        }
        None => match Assets::get("index.html") {
            Some(file) => (
                [(http::header::CONTENT_TYPE, "text/html")],
                file.data,
            )
                .into_response(),
            None => (http::StatusCode::NOT_FOUND, "not found").into_response(),
        },
    }
}
