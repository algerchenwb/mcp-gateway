//! Shared authentication and Origin validation for all MCP transports.
use crate::server::AppState;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

pub async fn auth_layer(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    if matches!(
        req.uri().path(),
        "/health"
            | "/ready"
            | "/.well-known/oauth-protected-resource"
            | "/.well-known/oauth-protected-resource/mcp"
    ) {
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
            let bearer = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));
            match (&state.oauth, bearer) {
                (Some(verifier), Some(token)) => match verifier.verify(token).await {
                    Ok(principal) => {
                        req.extensions_mut().insert(principal);
                    }
                    Err(crate::auth::oauth::AuthError::Unavailable) => {
                        return StatusCode::SERVICE_UNAVAILABLE.into_response()
                    }
                    Err(crate::auth::oauth::AuthError::InsufficientScope) => {
                        return challenge(&state, StatusCode::FORBIDDEN, "insufficient_scope")
                    }
                    Err(crate::auth::oauth::AuthError::InvalidToken) => {
                        return challenge(&state, StatusCode::UNAUTHORIZED, "invalid_token")
                    }
                },
                _ => return challenge(&state, StatusCode::UNAUTHORIZED, "invalid_token"),
            }
        }
    }
    next.run(req).await
}

fn challenge(state: &AppState, status: StatusCode, error: &str) -> Response {
    let value = if let Some(oauth) = &state.config.auth.oauth {
        let origin = reqwest::Url::parse(&oauth.resource_url)
            .map(|url| url.origin().ascii_serialization())
            .unwrap_or_default();
        format!("Bearer resource_metadata=\"{origin}/.well-known/oauth-protected-resource\", error=\"{error}\", scope=\"{}\"",oauth.required_scopes.join(" "))
    } else {
        "Bearer realm=\"mcp-gateway\"".into()
    };
    match axum::http::HeaderValue::from_str(&value) {
        Ok(value) => (status, [(axum::http::header::WWW_AUTHENTICATE, value)]).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Clone)]
pub struct AdmissionPermit(pub std::sync::Arc<tokio::sync::OwnedSemaphorePermit>);
pub async fn admission_layer(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    if req.method() == axum::http::Method::POST && req.uri().path().starts_with("/mcp") {
        let Ok(permit) = state.inflight.clone().try_acquire_owned() else {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        };
        req.extensions_mut()
            .insert(AdmissionPermit(std::sync::Arc::new(permit)));
    }
    next.run(req).await
}
