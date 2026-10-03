//! Shared authentication and Origin validation for all MCP transports.
use crate::server::AppState;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

pub async fn auth_layer(State(state): State<AppState>, req: Request, next: Next) -> Response {
    if req.uri().path() == "/health" {
        return next.run(req).await;
    }
    if req.uri().path().starts_with("/mcp") {
        if let Some(origin) = req.headers().get("origin") {
            let allowed = origin.to_str().ok().is_some_and(|origin| {
                state
                    .config
                    .gateway
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed == origin)
            });
            if !allowed {
                return StatusCode::FORBIDDEN.into_response();
            }
        }
    }
    let config = &state.config.auth;
    if config.enabled {
        let key = req
            .headers()
            .get(&config.api_key_header)
            .and_then(|v| v.to_str().ok())
            .or_else(|| {
                req.headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("Bearer "))
            });
        if !key.is_some_and(|key| config.api_keys.iter().any(|valid| valid == key)) {
            return (
                StatusCode::UNAUTHORIZED,
                [("www-authenticate", "Bearer realm=\"mcp-gateway\"")],
            )
                .into_response();
        }
    }
    next.run(req).await
}
