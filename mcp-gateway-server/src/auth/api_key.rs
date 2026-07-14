//! API Key authentication — Phase 1 simple key-based auth.
//!
//! Per the document: "Phase 1: API Key / JWT"
//!
//! Checks the `Authorization: Bearer <key>` or `x-api-key` header
//! against a configured list of valid API keys.

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use crate::config::AuthConfig;

/// Extract the API key from the request headers.
///
/// Checks both `Authorization: Bearer <key>` and `x-api-key` headers.
fn extract_api_key(req: &Request, config: &AuthConfig) -> Option<String> {
    // Check x-api-key header first
    if let Some(key) = req.headers().get(&config.api_key_header) {
        if let Ok(key_str) = key.to_str() {
            return Some(key_str.to_string());
        }
    }

    // Check Authorization: Bearer header
    if let Some(auth) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth.to_str() {
            if let Some(key) = auth_str.strip_prefix("Bearer ") {
                return Some(key.to_string());
            }
        }
    }

    None
}

/// Tower-compatible middleware for API key authentication.
pub async fn auth_middleware(
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // The auth config is injected via axum state.
    // For middleware, we check if the state has auth enabled.
    // If auth is disabled, we pass through.
    // If enabled, we validate the API key.

    // Note: axum::middleware::from_fn can't access State directly.
    // We use a different approach: check for auth config in extensions.
    if let Some(config) = req.extensions().get::<AuthConfig>() {
        if config.enabled {
            if let Some(key) = extract_api_key(&req, config) {
                if config.api_keys.iter().any(|k| k == &key) {
                    return Ok(next.run(req).await);
                }
            }
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    // Auth not configured or disabled → pass through
    Ok(next.run(req).await)
}