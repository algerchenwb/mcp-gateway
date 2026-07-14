use async_trait::async_trait;

use crate::error::McpResult;
use crate::types::JsonRpcMessage;

/// Transport layer types supported by the MCP Gateway.
///
/// Corresponds to the transport layer diagram in the architecture document:
/// - stdio: local process communication
/// - SSE: Server-Sent Events
/// - Streamable HTTP: bidirectional HTTP streaming
/// - WebSocket: persistent connections (Phase 2)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportType {
    Stdio,
    Sse,
    StreamableHttp,
    WebSocket,
}

impl TransportType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransportType::Stdio => "stdio",
            TransportType::Sse => "sse",
            TransportType::StreamableHttp => "streamable-http",
            TransportType::WebSocket => "websocket",
        }
    }
}

impl std::str::FromStr for TransportType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "stdio" => Ok(TransportType::Stdio),
            "sse" => Ok(TransportType::Sse),
            "streamable-http" | "streamable_http" | "http" => Ok(TransportType::StreamableHttp),
            "websocket" | "ws" => Ok(TransportType::WebSocket),
            other => Err(format!("unknown transport type: {other}")),
        }
    }
}

/// Abstraction over MCP transport layers.
///
/// Each transport implementation handles:
/// - Sending JSON-RPC messages (requests, responses, notifications, errors)
/// - Receiving JSON-RPC messages
/// - Connection lifecycle (close, heartbeat, reconnection)
#[async_trait]
pub trait McpTransport: Send + Sync {
    /// Send a JSON-RPC message over this transport.
    async fn send(&self, message: JsonRpcMessage) -> McpResult<()>;

    /// Receive the next JSON-RPC message from this transport.
    async fn receive(&self) -> McpResult<JsonRpcMessage>;

    /// Close the transport connection gracefully.
    async fn close(&self) -> McpResult<()>;

    /// Whether this transport is still connected.
    fn is_connected(&self) -> bool;

    /// The transport type for diagnostics.
    fn transport_type(&self) -> TransportType;
}