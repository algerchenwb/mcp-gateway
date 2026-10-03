//! Gateway client — high-level async API for the MCP Gateway.
//!
//! Per the document: "客户端 SDK（Rust）"

use serde_json::Value;

use mcp_gateway_core::error::McpResult;
use mcp_gateway_core::tool::{Tool, ToolCallResult};
use mcp_gateway_core::types::{
    Implementation, InitializeRequest, InitializeResult, JsonRpcMessage,
};

/// Client for communicating with the MCP Gateway.
pub struct GatewayClient {
    transport: crate::transport::HttpTransport,
    base_url: String,
    api_key: Option<String>,
}

impl GatewayClient {
    /// Create a new GatewayClient.
    ///
    /// # Arguments
    /// * `base_url` — URL of the gateway's MCP endpoint (e.g., `http://127.0.0.1:8080/mcp`)
    /// * `api_key` — optional API key for authentication
    pub fn new(base_url: impl Into<String>, api_key: Option<String>) -> Self {
        Self {
            transport: crate::transport::HttpTransport::new(base_url.into(), api_key.clone()),
            base_url: String::new(),
            api_key,
        }
        .with_base_url()
    }
    fn with_base_url(mut self) -> Self {
        self.base_url = self.transport.base_url().to_owned();
        self
    }
    async fn send_request(&self, method: &str, params: Option<Value>) -> McpResult<Value> {
        use crate::transport::Transport;
        self.transport.send(method, params).await
    }

    /// Initialize the connection with the gateway.
    ///
    /// Performs the MCP capability negotiation handshake.
    pub async fn initialize(&self) -> McpResult<InitializeResult> {
        let params = serde_json::to_value(InitializeRequest {
            protocol_version: "2025-06-18".to_string(),
            capabilities: Default::default(),
            client_info: Implementation {
                name: "mcp-gateway-sdk".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        })?;

        let result = self.send_request("initialize", Some(params)).await?;
        let init_result: InitializeResult = serde_json::from_value(result)?;

        self.transport
            .exchange(JsonRpcMessage::Notification(
                mcp_gateway_core::types::JsonRpcNotification {
                    jsonrpc: "2.0".into(),
                    method: "notifications/initialized".into(),
                    params: None,
                },
            ))
            .await?;

        Ok(init_result)
    }

    /// List all available tools from the gateway.
    pub async fn list_tools(&self) -> McpResult<Vec<Tool>> {
        let result = self.send_request("tools/list", None).await?;
        let tools: Vec<Tool> = serde_json::from_value(
            result
                .get("tools")
                .cloned()
                .unwrap_or(serde_json::json!([])),
        )?;
        Ok(tools)
    }

    /// Call a tool with the given arguments.
    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Option<Value>,
    ) -> McpResult<ToolCallResult> {
        let params = serde_json::json!({
            "name": name,
            "arguments": arguments.unwrap_or_else(||serde_json::json!({})),
        });

        let result = self.send_request("tools/call", Some(params)).await?;
        let tool_result: ToolCallResult = serde_json::from_value(result)?;
        Ok(tool_result)
    }

    /// Ping the gateway to check connectivity.
    pub async fn ping(&self) -> McpResult<()> {
        self.send_request("ping", None).await?;
        Ok(())
    }

    /// Get the gateway base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Set the API key.
    pub fn set_api_key(&mut self, key: impl Into<String>) {
        self.api_key = Some(key.into());
        self.transport =
            crate::transport::HttpTransport::new(self.base_url.clone(), self.api_key.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = GatewayClient::new("http://localhost:8080/mcp", None);
        assert_eq!(client.base_url(), "http://localhost:8080/mcp");
    }

    #[test]
    fn test_client_with_api_key() {
        let client = GatewayClient::new("http://localhost:8080/mcp", Some("sk-test".into()));
        assert_eq!(client.api_key.as_deref(), Some("sk-test"));
    }
}
