use serde::{Deserialize, Serialize};
use std::path::Path;

use mcp_gateway_core::transport::TransportType;

/// Root configuration for the MCP Gateway.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GatewayConfig {
    pub gateway: GatewaySettings,
    #[serde(default)]
    pub backends: Vec<BackendConfig>,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewaySettings {
    /// Human-readable name for this gateway instance.
    #[serde(default = "default_gateway_name")]
    pub name: String,
    /// Address to listen on (e.g., "127.0.0.1:8080").
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    /// Browser origins explicitly allowed to access MCP endpoints.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default = "default_inflight")]
    pub max_inflight_requests: usize,
    #[serde(default = "default_sse_sessions")]
    pub max_sse_sessions: usize,
    #[serde(default = "default_sse_ttl")]
    pub sse_ttl_seconds: u64,
    #[serde(default = "default_sse_queue")]
    pub sse_queue_capacity: usize,
}

impl Default for GatewaySettings {
    fn default() -> Self {
        Self {
            name: default_gateway_name(),
            listen_addr: default_listen_addr(),
            allowed_origins: Vec::new(),
            max_inflight_requests: default_inflight(),
            max_sse_sessions: default_sse_sessions(),
            sse_ttl_seconds: default_sse_ttl(),
            sse_queue_capacity: default_sse_queue(),
        }
    }
}

fn default_inflight() -> usize {
    256
}
fn default_sse_sessions() -> usize {
    1024
}
fn default_sse_ttl() -> u64 {
    1800
}
fn default_sse_queue() -> usize {
    32
}

fn default_gateway_name() -> String {
    "mcp-gateway".to_string()
}

fn default_listen_addr() -> String {
    "127.0.0.1:8080".to_string()
}

/// Configuration for a backend MCP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendConfig {
    /// Unique name for this backend.
    pub name: String,
    /// Transport type: "stdio", "sse", "streamable-http", "websocket".
    #[serde(default = "default_transport")]
    pub transport: String,
    /// Endpoint URL (for HTTP/SSE/WebSocket transports).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Command to run (for stdio transport).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Arguments for the command (for stdio transport).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Environment variables for the command (for stdio transport).
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub env: std::collections::HashMap<String, String>,
    /// Fixed backend credentials; inbound credentials are never forwarded.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// Tools this backend provides. The gateway routes tool calls to the
    /// first backend that lists the requested tool. If empty, all tools
    /// from this backend are available.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<String>,
    /// Explicit allowlist of read-only tools whose successful results may be cached.
    #[serde(default)]
    pub cache_tools: Vec<String>,
    /// Request timeout in milliseconds.
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    /// Maximum number of concurrent connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Weight for load balancing (Phase 2+).
    #[serde(default = "default_weight")]
    pub weight: u32,
}

fn default_transport() -> String {
    "streamable-http".to_string()
}

fn default_timeout() -> u64 {
    30000
}

fn default_max_connections() -> usize {
    10
}

fn default_weight() -> u32 {
    1
}

impl BackendConfig {
    pub fn transport_type(&self) -> TransportType {
        self.transport
            .parse()
            .unwrap_or(TransportType::StreamableHttp)
    }
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            transport: default_transport(),
            endpoint: None,
            command: None,
            args: Vec::new(),
            env: std::collections::HashMap::new(),
            headers: std::collections::HashMap::new(),
            tools: Vec::new(),
            cache_tools: Vec::new(),
            timeout_ms: default_timeout(),
            max_connections: default_max_connections(),
            weight: default_weight(),
        }
    }
}

/// Authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Whether authentication is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// List of valid API keys.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub api_keys: Vec<String>,
    #[serde(default)]
    pub api_key_env: Vec<String>,
    #[serde(default)]
    pub oauth: Option<OAuthConfig>,
    /// Header name for the API key.
    #[serde(default = "default_api_key_header")]
    pub api_key_header: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
    pub resource_url: String,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub tool_scopes: std::collections::HashMap<String, Vec<String>>,
}

fn default_api_key_header() -> String {
    "x-api-key".to_string()
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_keys: Vec::new(),
            api_key_env: Vec::new(),
            oauth: None,
            api_key_header: default_api_key_header(),
        }
    }
}

/// Cache configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Whether caching is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Maximum number of entries in the cache.
    #[serde(default = "default_cache_max_capacity")]
    pub max_capacity: u64,
    /// Time-to-live in seconds.
    #[serde(default = "default_cache_ttl")]
    pub ttl_seconds: u64,
    #[serde(default = "default_cache_max_bytes")]
    pub max_bytes: u64,
    #[serde(default = "default_cache_result_bytes")]
    pub max_result_bytes: usize,
}

fn default_cache_max_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_cache_result_bytes() -> usize {
    1024 * 1024
}

fn default_cache_max_capacity() -> u64 {
    10000
}

fn default_cache_ttl() -> u64 {
    300
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_capacity: default_cache_max_capacity(),
            ttl_seconds: default_cache_ttl(),
            max_bytes: default_cache_max_bytes(),
            max_result_bytes: default_cache_result_bytes(),
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level: "trace", "debug", "info", "warn", "error".
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Log format: "json" or "pretty".
    #[serde(default = "default_log_format")]
    pub format: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "json".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

impl GatewayConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let mut config: GatewayConfig = toml::from_str(&content)?;
        for name in &config.auth.api_key_env {
            let key = std::env::var(name)
                .map_err(|_| format!("missing API key environment variable '{name}'"))?;
            if key.is_empty() {
                return Err(format!("empty API key environment variable '{name}'").into());
            }
            config.auth.api_keys.push(key);
        }
        // ${ENV_NAME} placeholders allow credentials to stay out of committed TOML files.
        for backend in &mut config.backends {
            for value in backend.headers.values_mut().chain(backend.env.values_mut()) {
                if let Some(name) = value.strip_prefix("${").and_then(|v| v.strip_suffix('}')) {
                    *value = std::env::var(name)
                        .map_err(|_| format!("missing backend environment variable '{name}'"))?;
                }
            }
        }
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.gateway.name.is_empty() {
            errors.push("gateway.name must not be empty".to_string());
        }

        if self.gateway.max_inflight_requests == 0
            || self.gateway.max_sse_sessions == 0
            || self.gateway.sse_ttl_seconds == 0
            || self.gateway.sse_queue_capacity == 0
        {
            errors.push("gateway concurrency, SSE limits and TTL must be positive".into());
        }
        if self.backends.is_empty() {
            errors.push("at least one backend must be configured".to_string());
        }

        if self
            .gateway
            .listen_addr
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            errors.push("gateway.listen_addr must be an IP socket address".into());
        }
        if self.cache.enabled
            && (self.cache.max_capacity == 0
                || self.cache.max_bytes == 0
                || self.cache.max_result_bytes == 0
                || self.cache.ttl_seconds == 0)
        {
            errors.push("enabled cache limits and TTL must be positive".into());
        }
        if self.auth.enabled && self.auth.api_keys.is_empty() && self.auth.oauth.is_none() {
            errors.push("auth.enabled requires at least one API key".into());
        }
        if self
            .auth
            .api_key_header
            .parse::<axum::http::HeaderName>()
            .is_err()
        {
            errors.push("auth.api_key_header is invalid".into());
        }
        if let Some(oauth) = &self.auth.oauth {
            if !self.auth.enabled {
                errors.push("auth.oauth requires auth.enabled=true".into());
            }
            for (name, value) in [
                ("issuer", &oauth.issuer),
                ("jwks_url", &oauth.jwks_url),
                ("resource_url", &oauth.resource_url),
            ] {
                if !reqwest::Url::parse(value).is_ok_and(|url| {
                    url.username().is_empty()
                        && url.password().is_none()
                        && (url.scheme() == "https"
                            || (url.scheme() == "http"
                                && matches!(
                                    url.host_str(),
                                    Some("localhost" | "127.0.0.1" | "[::1]")
                                )))
                }) {
                    errors.push(format!(
                        "auth.oauth.{name} must use HTTPS (HTTP is allowed only for localhost)"
                    ));
                }
            }
            if oauth.audience.is_empty() {
                errors.push("auth.oauth.audience must not be empty".into());
            }
            if oauth
                .required_scopes
                .iter()
                .chain(oauth.tool_scopes.values().flatten())
                .any(|scope| {
                    scope.is_empty()
                        || scope
                            .chars()
                            .any(|c| !('!'..='~').contains(&c) || c == '"' || c == '\\')
                })
            {
                errors.push("OAuth scopes must be nonempty printable tokens".into());
            }
        }
        let mut backend_names = std::collections::HashSet::new();
        let mut tool_names = std::collections::HashSet::new();
        for (i, backend) in self.backends.iter().enumerate() {
            if backend.name.is_empty() || !backend_names.insert(&backend.name) {
                errors.push(format!(
                    "backends[{i}]: backend name must be nonempty and unique"
                ));
            }
            if backend.transport.parse::<TransportType>().is_err()
                || backend.transport == "websocket"
            {
                errors.push(format!(
                    "backends[{i}]: unsupported transport '{}'",
                    backend.transport
                ));
            }
            if backend.timeout_ms == 0 || backend.max_connections == 0 {
                errors.push(format!(
                    "backends[{i}]: timeout_ms and max_connections must be positive"
                ));
            }
            if let Some(endpoint) = &backend.endpoint {
                if !reqwest::Url::parse(endpoint).is_ok_and(|url| {
                    matches!(url.scheme(), "http" | "https")
                        && url.host_str().is_some()
                        && url.username().is_empty()
                        && url.password().is_none()
                }) {
                    errors.push(format!(
                        "backends[{i}]: endpoint must be an HTTP(S) URL without userinfo"
                    ));
                }
            }
            for (name, value) in &backend.headers {
                if name.parse::<axum::http::HeaderName>().is_err()
                    || value.parse::<axum::http::HeaderValue>().is_err()
                {
                    errors.push(format!("backends[{i}]: invalid backend header"));
                }
            }
            for tool in &backend.tools {
                if tool.is_empty() || !tool_names.insert(tool) {
                    errors.push(format!(
                        "backends[{i}]: tool names must be nonempty and unique"
                    ));
                }
            }
            match backend.transport_type() {
                TransportType::Stdio => {
                    if backend.command.is_none() {
                        errors.push(format!(
                            "backends[{i}] ({name}): stdio transport requires 'command'",
                            name = backend.name
                        ));
                    }
                }
                TransportType::Sse | TransportType::StreamableHttp | TransportType::WebSocket => {
                    if backend.endpoint.is_none() {
                        errors.push(format!(
                            "backends[{i}] ({name}): {t} transport requires 'endpoint'",
                            name = backend.name,
                            t = backend.transport_type().as_str()
                        ));
                    }
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let toml_str = r#"
[gateway]
name = "test-gateway"

[[backends]]
name = "echo"
endpoint = "http://localhost:9000/mcp"
tools = ["echo"]
"#;
        let config: GatewayConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.gateway.name, "test-gateway");
        assert_eq!(config.backends.len(), 1);
        assert_eq!(config.backends[0].name, "echo");
        assert!(!config.auth.enabled);
        assert!(config.cache.enabled);
    }

    #[test]
    fn test_validate_missing_command_for_stdio() {
        let toml_str = r#"
[gateway]
name = "test"

[[backends]]
name = "bad-stdio"
transport = "stdio"
"#;
        let config: GatewayConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
    }
}
#[cfg(test)]
mod validation_tests {
    #![allow(clippy::field_reassign_with_default)]
    use super::*;
    #[test]
    fn invalid_transport_and_duplicate_names_are_rejected() {
        let mut config = GatewayConfig::default();
        config.backends = vec![
            BackendConfig {
                name: "same".into(),
                transport: "typo".into(),
                endpoint: Some("ftp://example".into()),
                tools: vec!["echo".into()],
                ..Default::default()
            };
            2
        ];
        config.auth.enabled = true;
        assert!(config.validate().unwrap_err().len() >= 4);
    }
}
