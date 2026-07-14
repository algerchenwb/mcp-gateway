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
        .route("/mcp", axum::routing::post(crate::handlers::json_rpc::handle))
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
        .layer(middleware::from_fn(auth::auth_layer))
        .with_state(state)
}

/// Start the gateway server.
pub async fn run(config: GatewayConfig) {
    let listen_addr = config.gateway.listen_addr.clone();

    // Build L1 cache
    let cache = Arc::new(L1Cache::new(
        config.cache.max_capacity,
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

    axum::serve(listener, app)
        .await
        .expect("Server error");
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