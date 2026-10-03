use std::net::SocketAddr;
use std::sync::Arc;

use axum::middleware;
use axum::Router;

use crate::cache::l1::L1Cache;
use crate::config::GatewayConfig;
use crate::middleware::{auth, logging, metrics};

/// Shared application state accessible from all handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<GatewayConfig>,
    pub cache: Arc<L1Cache>,
    pub metrics: Arc<metrics::Metrics>,
    pub backends: Arc<crate::proxy::registry::BackendRegistry>,
    pub sse: Arc<crate::handlers::sse::SseSessions>,
    pub inflight: Arc<tokio::sync::Semaphore>,
    pub oauth: Option<Arc<crate::auth::oauth::OAuthVerifier>>,
}

impl AppState {
    pub fn new(config: GatewayConfig) -> Self {
        let cache = Arc::new(L1Cache::with_limits(
            config.cache.max_capacity,
            config.cache.max_bytes,
            config.cache.max_result_bytes,
            std::time::Duration::from_secs(config.cache.ttl_seconds),
        ));
        let backends = Arc::new(crate::proxy::registry::BackendRegistry::new(&config));
        Self {
            oauth: config
                .auth
                .oauth
                .clone()
                .map(|oauth| Arc::new(crate::auth::oauth::OAuthVerifier::new(oauth))),
            inflight: Arc::new(tokio::sync::Semaphore::new(
                config.gateway.max_inflight_requests,
            )),
            sse: Arc::new(crate::handlers::sse::SseSessions::new(
                config.gateway.max_sse_sessions,
                config.gateway.sse_ttl_seconds,
                config.gateway.sse_queue_capacity,
            )),
            config: Arc::new(config),
            cache,
            backends,
            metrics: Arc::default(),
        }
    }
}

/// Build the axum router with all routes and middleware.
///
/// Per the document's transport layer diagram:
/// - POST /mcp          → Streamable HTTP (JSON-RPC)
/// - GET  /mcp/sse      → SSE transport
/// - POST /mcp/sse/:id  → SSE message endpoint
/// - GET  /health       → Health check
/// - GET  /metrics      → Metrics endpoint
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route(
            "/.well-known/oauth-protected-resource",
            axum::routing::get(crate::auth::oauth::metadata),
        )
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            axum::routing::get(crate::auth::oauth::metadata),
        )
        // Streamable HTTP — the primary JSON-RPC endpoint
        .route(
            "/mcp",
            axum::routing::post(crate::handlers::streamable_http::handle),
        )
        // SSE transport — persistent connection
        .route("/mcp/sse", axum::routing::get(crate::handlers::sse::handle))
        // SSE message endpoint
        .route(
            "/mcp/sse/{session_id}",
            axum::routing::post(crate::handlers::sse::handle_message),
        )
        // Health check
        .route("/health", axum::routing::get(health_check))
        .route("/ready", axum::routing::get(readiness))
        // Metrics
        .route("/metrics", axum::routing::get(metrics_handler))
        .route("/metrics/prometheus", axum::routing::get(prometheus))
        // Middleware layers (applied from bottom to top)
        .layer(middleware::from_fn(logging::logging_layer))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::admission_layer,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::auth_layer,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            metrics::metrics_layer,
        ))
        .with_state(state)
}

/// Start the gateway server.
pub async fn run(config: GatewayConfig) -> mcp_gateway_core::error::McpResult<()> {
    let listen_addr = config.gateway.listen_addr.clone();

    let state = AppState::new(config);

    let app = build_router(state.clone());

    let addr: SocketAddr = listen_addr
        .parse()
        .map_err(|_| mcp_gateway_core::error::McpError::Config("invalid listen address".into()))?;

    tracing::info!("MCP Gateway listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    use std::future::IntoFuture;
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result=&mut server => { result?; },
        _=shutdown_signal() => {
            tracing::info!("draining gateway requests");
            state.sse.close_all();
            let _=shutdown_tx.send(());
            match tokio::time::timeout(std::time::Duration::from_secs(30),&mut server).await {
                Ok(result)=>result?,
                Err(_)=>tracing::warn!("shutdown grace period exceeded"),
            }
        }
    }
    state.backends.shutdown().await;
    Ok(())
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}};
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
async fn readiness(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::http::StatusCode {
    match tokio::time::timeout(std::time::Duration::from_secs(5), state.backends.tools()).await {
        Ok(Ok(_)) => axum::http::StatusCode::OK,
        _ => axum::http::StatusCode::SERVICE_UNAVAILABLE,
    }
}
async fn prometheus(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> impl axum::response::IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4")],
        state.metrics.prometheus(),
    )
}

/// Health check endpoint.
async fn health_check() -> &'static str {
    "OK"
}

/// Metrics endpoint — returns basic metrics in JSON format.
async fn metrics_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::Json<metrics::MetricsSnapshot> {
    axum::Json(state.metrics.snapshot())
}
#[cfg(test)]
mod security_tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    fn app() -> Router {
        let mut config = GatewayConfig::default();
        config.auth.enabled = true;
        config.auth.api_keys = vec!["test-secret".into()];
        config.gateway.allowed_origins = vec!["https://trusted.example".into()];
        build_router(AppState::new(config))
    }
    #[tokio::test]
    async fn every_mcp_entrypoint_requires_authentication() {
        for (method, path) in [
            ("POST", "/mcp"),
            ("GET", "/mcp/sse"),
            ("POST", "/mcp/sse/unknown"),
            ("GET", "/metrics"),
        ] {
            let response = app()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        }
        assert_eq!(
            app()
                .oneshot(
                    Request::builder()
                        .uri("/health")
                        .body(Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
    #[tokio::test]
    async fn origin_is_checked_even_with_valid_key() {
        let response = app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("origin", "https://evil.example")
                    .header("x-api-key", "test-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}

#[cfg(test)]
mod operations_tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    #[tokio::test]
    async fn metrics_count_http_and_rpc_errors_and_inflight_is_bounded() {
        let state = AppState::new(GatewayConfig::default());
        let app = build_router(state.clone());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"unknown"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.metrics.snapshot().total_rpc_errors, 1);
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(state.metrics.snapshot().total_requests, 2);
        assert_eq!(state.metrics.snapshot().total_errors, 1);
        assert!(state
            .metrics
            .prometheus()
            .contains("mcp_gateway_rpc_errors_total 1"));
        let _permits = state
            .inflight
            .clone()
            .acquire_many_owned(state.config.gateway.max_inflight_requests as u32)
            .await
            .unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
