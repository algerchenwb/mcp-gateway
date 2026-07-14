//! Logging middleware — logs each request with method, latency, and status.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use std::time::Instant;

/// Logging middleware that records request method, path, latency, and status.
pub async fn logging_layer(
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let start = Instant::now();

    let response = next.run(req).await;

    let latency = start.elapsed();
    let status = response.status();

    tracing::info!(
        method = %method,
        uri = %uri,
        status = status.as_u16(),
        latency_ms = latency.as_millis(),
        "request completed"
    );

    response
}