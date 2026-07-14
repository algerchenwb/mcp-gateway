//! Metrics middleware — collects basic request metrics.
//!
//! Phase 1: simple counter-based metrics via tracing spans.
//! Phase 4+: full OpenTelemetry integration with Prometheus endpoint.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Basic metrics counters.
#[derive(Default)]
pub struct Metrics {
    pub total_requests: AtomicU64,
    pub total_errors: AtomicU64,
    pub total_tool_calls: AtomicU64,
}

impl Metrics {
    pub fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_tool_call(&self) {
        self.total_tool_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            total_tool_calls: self.total_tool_calls.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    pub total_requests: u64,
    pub total_errors: u64,
    pub total_tool_calls: u64,
}

/// Metrics middleware layer.
pub async fn metrics_layer(
    req: Request,
    next: Next,
) -> Response {
    // Extract metrics from extensions if available
    let metrics = req
        .extensions()
        .get::<Arc<Metrics>>()
        .cloned();

    let response = next.run(req).await;

    if let Some(m) = metrics {
        m.record_request();
        if !response.status().is_success() {
            m.record_error();
        }
    }

    response
}