//! Static route configuration — loads routes from the gateway config file.
//!
//! Phase 1: routes are defined statically in `gateway.toml` under `[[backends]]`.
//! Each backend specifies which tools it handles via the `tools` field.

use std::sync::Arc;

use crate::config::{BackendConfig, GatewayConfig};
use super::engine::{RouteEngine, RouteStrategy};

/// Build a RouteEngine from the gateway configuration.
pub fn build_engine(config: &GatewayConfig) -> RouteEngine {
    let backends: Vec<Arc<BackendConfig>> = config
        .backends
        .iter()
        .map(|b| Arc::new(b.clone()))
        .collect();

    // Phase 1: use RuleBased strategy with no custom rules
    // (falls back to static tool-name matching)
    let strategy = RouteStrategy::RuleBased { rules: vec![] };

    RouteEngine::new(strategy, backends)
}