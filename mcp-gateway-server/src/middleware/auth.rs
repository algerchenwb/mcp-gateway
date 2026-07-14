//! Auth middleware — tower Layer for API key authentication.
//!
//! Phase 1: simple pass-through. Auth enforcement happens in the JSON-RPC handler
//! which has access to AppState. The middleware here just logs auth headers.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

/// Auth middleware — logs auth headers, enforcement in handler.
pub async fn auth_layer(
    req: Request,
    next: Next,
) -> Response {
    // Log auth headers for observability
    if let Some(auth) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth.to_str() {
            tracing::debug!(auth_header = auth_str, "auth header present");
        }
    }
    if let Some(key) = req.headers().get("x-api-key") {
        if let Ok(key_str) = key.to_str() {
            tracing::debug!(api_key = key_str, "api key header present");
        }
    }

    next.run(req).await
}