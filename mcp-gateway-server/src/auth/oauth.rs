//! OAuth resource-server JWT verification against an explicitly configured issuer/JWKS.
use crate::config::OAuthConfig;
use jsonwebtoken::{decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, Validation};
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
#[derive(Debug, Clone)]
pub struct Principal {
    pub issuer: String,
    pub subject: String,
    pub scopes: HashSet<String>,
}
#[derive(Debug)]
pub enum AuthError {
    InvalidToken,
    InsufficientScope,
    Unavailable,
}
#[derive(Default)]
struct Keys {
    set: Option<(Instant, JwkSet)>,
    last_attempt: Option<Instant>,
}
pub struct OAuthVerifier {
    config: OAuthConfig,
    client: reqwest::Client,
    keys: Mutex<Keys>,
}
impl OAuthVerifier {
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("HTTP client configuration"),
            keys: Mutex::default(),
        }
    }
    pub async fn verify(&self, token: &str) -> Result<Principal, AuthError> {
        if token.len() > 16384 {
            return Err(AuthError::InvalidToken);
        }
        let header = decode_header(token).map_err(|_| AuthError::InvalidToken)?;
        if header.alg != Algorithm::RS256 {
            return Err(AuthError::InvalidToken);
        }
        let kid = header
            .kid
            .filter(|kid| !kid.is_empty() && kid.len() <= 128)
            .ok_or(AuthError::InvalidToken)?;
        let key = {
            let mut keys = self.keys.lock().await;
            let known = keys
                .set
                .as_ref()
                .is_some_and(|(_, set)| set.find(&kid).is_some());
            let fresh = keys
                .set
                .as_ref()
                .is_some_and(|(created, _)| created.elapsed() < Duration::from_secs(300));
            if !fresh || !known {
                if keys
                    .last_attempt
                    .is_some_and(|attempt| attempt.elapsed() < Duration::from_secs(5))
                {
                    if !fresh {
                        return Err(AuthError::Unavailable);
                    }
                } else {
                    keys.last_attempt = Some(Instant::now());
                    let mut response = self
                        .client
                        .get(&self.config.jwks_url)
                        .send()
                        .await
                        .map_err(|_| AuthError::Unavailable)?;
                    if !response.status().is_success() {
                        return Err(AuthError::Unavailable);
                    }
                    let mut body = Vec::new();
                    while let Some(chunk) =
                        response.chunk().await.map_err(|_| AuthError::Unavailable)?
                    {
                        if body.len() + chunk.len() > 1024 * 1024 {
                            return Err(AuthError::Unavailable);
                        }
                        body.extend_from_slice(&chunk);
                    }
                    let set: JwkSet =
                        serde_json::from_slice(&body).map_err(|_| AuthError::Unavailable)?;
                    if set.keys.len() > 100 {
                        return Err(AuthError::Unavailable);
                    }
                    keys.set = Some((Instant::now(), set));
                }
            }
            let key = keys
                .set
                .as_ref()
                .and_then(|(_, set)| set.find(&kid))
                .ok_or(AuthError::InvalidToken)?;
            if key
                .common
                .key_algorithm
                .as_ref()
                .is_some_and(|alg| *alg != jsonwebtoken::jwk::KeyAlgorithm::RS256)
                || key
                    .common
                    .public_key_use
                    .as_ref()
                    .is_some_and(|usage| *usage != jsonwebtoken::jwk::PublicKeyUse::Signature)
            {
                return Err(AuthError::InvalidToken);
            }
            DecodingKey::from_jwk(key).map_err(|_| AuthError::InvalidToken)?
        };
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = 5;
        let data = decode::<serde_json::Value>(token, &key, &validation)
            .map_err(|_| AuthError::InvalidToken)?;
        let subject = data
            .claims
            .get("sub")
            .and_then(serde_json::Value::as_str)
            .filter(|sub| !sub.is_empty())
            .ok_or(AuthError::InvalidToken)?
            .to_owned();
        let scopes: HashSet<String> = data
            .claims
            .get("scope")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        if !self
            .config
            .required_scopes
            .iter()
            .all(|scope| scopes.contains(scope))
        {
            return Err(AuthError::InsufficientScope);
        }
        Ok(Principal {
            issuer: self.config.issuer.clone(),
            subject,
            scopes,
        })
    }
}
pub fn allows_tool(config: &OAuthConfig, principal: &Principal, name: &str) -> bool {
    config.tool_scopes.get(name).is_none_or(|required| {
        required
            .iter()
            .all(|scope| principal.scopes.contains(scope))
    })
}
pub async fn metadata(
    axum::extract::State(state): axum::extract::State<crate::server::AppState>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(config) = &state.config.auth.oauth else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let mut scopes: std::collections::BTreeSet<String> =
        config.required_scopes.iter().cloned().collect();
    scopes.extend(config.tool_scopes.values().flatten().cloned());
    axum::Json(serde_json::json!({"resource":config.resource_url,"authorization_servers":[config.issuer],"scopes_supported":scopes,"bearer_methods_supported":["header"]})).into_response()
}
