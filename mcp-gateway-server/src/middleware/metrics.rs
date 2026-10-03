//! HTTP and JSON-RPC metrics. Protocol errors can occur inside HTTP 200 responses.
use crate::server::AppState;
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
#[derive(Default)]
pub struct Metrics {
    pub total_requests: AtomicU64,
    pub total_errors: AtomicU64,
    pub total_rpc_errors: AtomicU64,
    pub total_tool_calls: AtomicU64,
    pub total_tool_errors: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    latency_us: AtomicU64,
    latency_buckets: [AtomicU64; 6],
}
const BUCKETS: [u64; 6] = [1000, 10000, 100000, 1000000, 10000000, u64::MAX];
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
    pub fn record_rpc_error(&self) {
        self.total_rpc_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_tool_error(&self) {
        self.total_tool_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_cache(&self, hit: bool) {
        if hit {
            &self.cache_hits
        } else {
            &self.cache_misses
        }
        .fetch_add(1, Ordering::Relaxed);
    }
    fn record_latency(&self, us: u64) {
        self.latency_us.fetch_add(us, Ordering::Relaxed);
        for (i, bound) in BUCKETS.iter().enumerate() {
            if us <= *bound {
                self.latency_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            total_rpc_errors: self.total_rpc_errors.load(Ordering::Relaxed),
            total_tool_calls: self.total_tool_calls.load(Ordering::Relaxed),
            total_tool_errors: self.total_tool_errors.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            request_latency_seconds_sum: self.latency_us.load(Ordering::Relaxed) as f64 / 1000000.0,
        }
    }
    pub fn prometheus(&self) -> String {
        let s = self.snapshot();
        let mut out = String::new();
        for (name, value) in [
            ("requests", s.total_requests),
            ("http_errors", s.total_errors),
            ("rpc_errors", s.total_rpc_errors),
            ("tool_calls", s.total_tool_calls),
            ("tool_errors", s.total_tool_errors),
            ("cache_hits", s.cache_hits),
            ("cache_misses", s.cache_misses),
        ] {
            out.push_str(&format!(
                "# TYPE mcp_gateway_{name}_total counter\nmcp_gateway_{name}_total {value}\n"
            ));
        }
        out.push_str("# TYPE mcp_gateway_request_duration_seconds histogram\n");
        for (i, bound) in BUCKETS.iter().enumerate() {
            let le = if *bound == u64::MAX {
                "+Inf".into()
            } else {
                format!("{}", *bound as f64 / 1000000.0)
            };
            out.push_str(&format!(
                "mcp_gateway_request_duration_seconds_bucket{{le=\"{le}\"}} {}\n",
                self.latency_buckets[i].load(Ordering::Relaxed)
            ));
        }
        out.push_str(&format!("mcp_gateway_request_duration_seconds_sum {}\nmcp_gateway_request_duration_seconds_count {}\n",s.request_latency_seconds_sum,self.latency_buckets[5].load(Ordering::Relaxed)));
        out
    }
}
#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    pub total_requests: u64,
    pub total_errors: u64,
    pub total_rpc_errors: u64,
    pub total_tool_calls: u64,
    pub total_tool_errors: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub request_latency_seconds_sum: f64,
}
pub async fn metrics_layer(State(state): State<AppState>, req: Request, next: Next) -> Response {
    state.metrics.record_request();
    let start = Instant::now();
    let response = next.run(req).await;
    if !response.status().is_success() {
        state.metrics.record_error();
    }
    state
        .metrics
        .record_latency(start.elapsed().as_micros().min(u64::MAX as u128) as u64);
    response
}
