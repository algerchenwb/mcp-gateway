//! Route engine — determines which backend handles a given tool call.
//!
//! Per the document, the routing engine supports 5 strategies:
//! - SemanticMatch: route based on tool description embedding similarity
//! - RuleBased: route based on user-defined rules (Phase 1 default)
//! - LoadBalanced: distribute across multiple instances
//! - MultiModel: distribute to multiple models
//! - Cascade: try backends in order (fallback chain)

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::BackendConfig;

/// Route target for a tool call.
#[derive(Debug, Clone)]
pub struct RouteTarget {
    pub backend: Arc<BackendConfig>,
    pub tool_name: String,
}

/// Load balancing strategy (Phase 2+).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LBStrategy {
    RoundRobin,
    LeastConnections,
    Weighted,
    Random,
}

/// Model configuration for MultiModel strategy (Phase 3+).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub name: String,
    pub endpoint: String,
    pub cost_weight: f64,
}

/// A routing rule for the RuleBased strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteRule {
    pub tool_pattern: String,
    pub backend: String,
    pub priority: i32,
}

/// Route strategy — from the document section 3 (智能路由).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RouteStrategy {
    /// Route based on tool description embedding similarity.
    SemanticMatch {
        /// Name of the embedding model to use.
        embedding_model: String,
    },
    /// Route based on user-defined rules (Phase 1 default).
    RuleBased {
        rules: Vec<RouteRule>,
    },
    /// Distribute across multiple instances.
    LoadBalanced {
        strategy: LBStrategy,
    },
    /// Distribute to multiple models.
    MultiModel {
        models: Vec<ModelConfig>,
    },
    /// Try backends in order, falling back on failure.
    Cascade {
        /// Backend names in fallback order.
        chain: Vec<String>,
    },
}

impl Default for RouteStrategy {
    fn default() -> Self {
        RouteStrategy::RuleBased { rules: Vec::new() }
    }
}

/// The route engine resolves tool calls to backend targets.
pub struct RouteEngine {
    strategy: RouteStrategy,
    backends: Vec<Arc<BackendConfig>>,
}

impl RouteEngine {
    /// Create a new RouteEngine with the given strategy and backends.
    pub fn new(strategy: RouteStrategy, backends: Vec<Arc<BackendConfig>>) -> Self {
        Self {
            strategy,
            backends,
        }
    }

    /// Find the backend that handles the given tool.
    ///
    /// Phase 1: uses static rule-based matching (first backend that lists the tool).
    pub fn resolve(&self, tool_name: &str) -> Option<RouteTarget> {
        match &self.strategy {
            RouteStrategy::RuleBased { rules } => {
                // If rules are defined, use them; otherwise fall back to static matching
                if !rules.is_empty() {
                    for rule in rules {
                        if self.match_pattern(tool_name, &rule.tool_pattern) {
                            if let Some(backend) = self
                                .backends
                                .iter()
                                .find(|b| b.name == rule.backend)
                            {
                                return Some(RouteTarget {
                                    backend: Arc::clone(backend),
                                    tool_name: tool_name.to_string(),
                                });
                            }
                        }
                    }
                    None
                } else {
                    self.static_match(tool_name)
                }
            }
            RouteStrategy::Cascade { chain: _ } => {
                // Phase 2: cascade logic
                self.static_match(tool_name)
            }
            _ => {
                // Phase 1: all other strategies fall back to static matching
                self.static_match(tool_name)
            }
        }
    }

    /// Static matching: find the first backend that lists this tool.
    fn static_match(&self, tool_name: &str) -> Option<RouteTarget> {
        for backend in &self.backends {
            // If backend lists no tools, it accepts all tools
            if backend.tools.is_empty() || backend.tools.iter().any(|t| t == tool_name) {
                return Some(RouteTarget {
                    backend: Arc::clone(backend),
                    tool_name: tool_name.to_string(),
                });
            }
        }
        None
    }

    /// Simple glob-style pattern matching for tool names.
    fn match_pattern(&self, tool_name: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if pattern.contains('*') {
            let prefix = pattern.trim_end_matches('*');
            tool_name.starts_with(prefix)
        } else {
            tool_name == pattern
        }
    }

    /// Get all available tools from configured backends.
    pub fn all_tools(&self) -> Vec<(String, String)> {
        let mut tools = Vec::new();
        for backend in &self.backends {
            for tool in &backend.tools {
                tools.push((tool.clone(), backend.name.clone()));
            }
        }
        tools
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackendConfig;

    fn make_backend(name: &str, tools: Vec<&str>) -> Arc<BackendConfig> {
        Arc::new(BackendConfig {
            name: name.to_string(),
            tools: tools.into_iter().map(String::from).collect(),
            ..Default::default()
        })
    }

    #[test]
    fn test_static_match_exact() {
        let engine = RouteEngine::new(
            RouteStrategy::RuleBased { rules: vec![] },
            vec![make_backend("b1", vec!["echo", "ping"])],
        );
        let result = engine.resolve("echo");
        assert!(result.is_some());
        assert_eq!(result.unwrap().tool_name, "echo");
    }

    #[test]
    fn test_static_match_not_found() {
        let engine = RouteEngine::new(
            RouteStrategy::RuleBased { rules: vec![] },
            vec![make_backend("b1", vec!["echo"])],
        );
        assert!(engine.resolve("unknown").is_none());
    }

    #[test]
    fn test_static_match_catch_all() {
        let engine = RouteEngine::new(
            RouteStrategy::RuleBased { rules: vec![] },
            vec![make_backend("b1", vec![])],
        );
        assert!(engine.resolve("anything").is_some());
    }

    #[test]
    fn test_rule_based_routing() {
        let engine = RouteEngine::new(
            RouteStrategy::RuleBased {
                rules: vec![RouteRule {
                    tool_pattern: "weather_*".to_string(),
                    backend: "weather".to_string(),
                    priority: 1,
                }],
            },
            vec![
                make_backend("weather", vec!["weather_city", "weather_forecast"]),
                make_backend("default", vec!["echo"]),
            ],
        );
        let result = engine.resolve("weather_city");
        assert!(result.is_some());
        assert_eq!(result.unwrap().backend.name, "weather");
    }

    #[test]
    fn test_pattern_match() {
        let engine = RouteEngine::new(
            RouteStrategy::RuleBased { rules: vec![] },
            vec![],
        );
        assert!(engine.match_pattern("weather_city", "weather_*"));
        assert!(engine.match_pattern("anything", "*"));
        assert!(!engine.match_pattern("weather_city", "echo_*"));
    }
}