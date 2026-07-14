//! Transport abstractions for the client SDK.
//!
//! Phase 1: HTTP transport. Phase 2+: SSE, stdio, WebSocket transports.

use async_trait::async_trait;
use serde_json::Value;
use mcp_gateway_core::error::McpResult;

/// Transport trait for the client SDK.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Send a JSON-RPC request and return the raw response value.
    async fn send(&self, method: &str, params: Option<Value>) -> McpResult<Value>;
}

/// HTTP transport implementation.
pub struct HttpTransport {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
}

impl HttpTransport {
    /// Create a new HTTP transport.
    pub fn new(base_url: impl Into<String>, api_key: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
            api_key,
        }
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, method: &str, params: Option<Value>) -> McpResult<Value> {
        let id = uuid::Uuid::new_v4().to_string();
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        let mut req = self
            .client
            .post(&self.base_url)
            .header("Content-Type", "application/json")
            .json(&request);

        if let Some(ref key) = self.api_key {
            req = req.header("x-api-key", key);
        }

        let response = req.send().await.map_err(|e| {
            mcp_gateway_core::error::McpError::BackendConnection(format!(
                "HTTP request failed: {e}"
            ))
        })?;

        let body: Value = response.json().await.map_err(|e| {
            mcp_gateway_core::error::McpError::Serialization(format!(
                "failed to parse response: {e}"
            ))
        })?;

        body.get("result")
            .cloned()
            .ok_or_else(|| {
                let err_msg = body
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error");
                mcp_gateway_core::error::McpError::InternalError(err_msg.to_string())
            })
    }
}