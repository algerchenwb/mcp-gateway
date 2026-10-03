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
        // Streamable HTTP — the primary JSON-RPC endpoint
        .route(
            "/mcp",
            axum::routing::post(crate::handlers::json_rpc::handle),
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
        // Metrics
        .route("/metrics", axum::routing::get(metrics_handler))
        // Middleware layers (applied from bottom to top)
        .layer(middleware::from_fn(logging::logging_layer))
        .layer(middleware::from_fn(metrics::metrics_layer))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::auth_layer,
        ))
        .with_state(state)
}

/// Start the gateway server.
pub async fn run(config: GatewayConfig) {
    let listen_addr = config.gateway.listen_addr.clone();

    // Build L1 cache
    let cache = Arc::new(L1Cache::with_limits(
        config.cache.max_capacity,
        config.cache.max_bytes,
        config.cache.max_result_bytes,
        std::time::Duration::from_secs(config.cache.ttl_seconds),
    ));

    let state = AppState {
        config: Arc::new(config),
        cache,
        metrics: Arc::new(metrics::Metrics::default()),
    };

    let app = build_router(state);

    let addr: SocketAddr = listen_addr.parse().expect("Invalid listen address");

    tracing::info!("MCP Gateway listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind to address");

    axum::serve(listener, app).await.expect("Server error");
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
        build_router(AppState {
            config: Arc::new(config),
            cache: Arc::new(L1Cache::new(10, std::time::Duration::from_secs(10))),
            metrics: Arc::default(),
        })
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
