use crate::types::error_codes;
use serde_json::Value;
use thiserror::Error;

/// Unified error type for the MCP Gateway.
#[derive(Error, Debug)]
pub enum McpError {
    #[error("JSON-RPC error: {0:?}")]
    Rpc(crate::types::ErrorDetail),
    // ── JSON-RPC 2.0 Protocol Errors ──
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Method not found: {0}")]
    MethodNotFound(String),

    #[error("Invalid params: {0}")]
    InvalidParams(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    // ── MCP Protocol Errors ──
    #[error("Server not initialized")]
    ServerNotInitialized,

    #[error("Unknown capability: {0}")]
    UnknownCapability(String),

    // ── Gateway Errors ──
    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Backend connection failed: {0}")]
    BackendConnection(String),

    #[error("Backend timeout: {0}")]
    BackendTimeout(String),

    #[error("Route not found for tool: {0}")]
    RouteNotFound(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Circuit breaker open for backend: {0}")]
    CircuitBreakerOpen(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("{0}")]
    Other(String),
}

impl McpError {
    /// Convert to a JSON-RPC 2.0 error code.
    pub fn to_error_code(&self) -> i32 {
        match self {
            McpError::Rpc(detail) => detail.code,
            McpError::ParseError(_) => error_codes::PARSE_ERROR,
            McpError::InvalidRequest(_) => error_codes::INVALID_REQUEST,
            McpError::MethodNotFound(_) => error_codes::METHOD_NOT_FOUND,
            McpError::InvalidParams(_) => error_codes::INVALID_PARAMS,
            McpError::InternalError(_) => error_codes::INTERNAL_ERROR,
            McpError::ServerNotInitialized => error_codes::SERVER_NOT_INITIALIZED,
            McpError::UnknownCapability(_) => error_codes::UNKNOWN_CAPABILITY,
            _ => error_codes::INTERNAL_ERROR,
        }
    }

    /// Convert to a JSON-RPC error detail value.
    pub fn to_error_value(&self) -> Value {
        serde_json::json!({
            "code": self.to_error_code(),
            "message": self.to_string(),
        })
    }
}

impl From<serde_json::Error> for McpError {
    fn from(e: serde_json::Error) -> Self {
        McpError::Serialization(e.to_string())
    }
}

impl From<std::io::Error> for McpError {
    fn from(e: std::io::Error) -> Self {
        McpError::Transport(e.to_string())
    }
}

/// Result type alias for MCP Gateway operations.
pub type McpResult<T> = Result<T, McpError>;
