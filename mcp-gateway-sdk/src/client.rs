//! Gateway client — high-level async API for the MCP Gateway.
//!
//! Per the document: "客户端 SDK（Rust）"

use std::time::Duration;

use reqwest::Client as HttpClient;
use serde_json::Value;

use mcp_gateway_core::error::{McpError, McpResult};
use mcp_gateway_core::tool::{Tool, ToolCallResult};
use mcp_gateway_core::types::{
    InitializeRequest, InitializeResult, Implementation, JsonRpcMessage, JsonRpcRequest,
    RequestId,
};

/// Client for communicating with the MCP Gateway.
pub struct GatewayClient {
    http_client: HttpClient,
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
        let http_client = HttpClient::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            http_client,
            base_url: base_url.into(),
            api_key,
        }
    }

    /// Send a JSON-RPC request and return the response value.
    async fn send_request(
        &self,
        method: &str,
        params: Option<Value>,
    ) -> McpResult<Value> {
        let id = RequestId::String(uuid::Uuid::new_v4().to_string());
        let request = JsonRpcRequest::new(method, params, id.clone());

        let mut req = self
            .http_client
            .post(&self.base_url)
            .header("Content-Type", "application/json")
            .json(&request);

        if let Some(ref key) = self.api_key {
            req = req.header("x-api-key", key);
        }

        let response = req.send().await.map_err(|e| {
            McpError::BackendConnection(format!("HTTP request failed: {e}"))
        })?;

        let status = response.status();
        let body: Value = response.json().await.map_err(|e| {
            McpError::Serialization(format!("failed to parse response: {e}"))
        })?;

        if !status.is_success() {
            let err_msg = body
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error");
            return Err(McpError::BackendConnection(format!(
                "gateway returned {status}: {err_msg}"
            )));
        }

        // Extract result from JSON-RPC response
        let message: JsonRpcMessage = serde_json::from_value(body).map_err(|e| {
            McpError::Serialization(format!("failed to parse JSON-RPC response: {e}"))
        })?;

        match message {
            JsonRpcMessage::Response(resp) => {
                resp.result.ok_or_else(|| McpError::InternalError("empty response".into()))
            }
            JsonRpcMessage::Error(err) => {
                Err(McpError::InternalError(format!("{}: {}", err.error.code, err.error.message)))
            }
            _ => Err(McpError::InternalError("unexpected response type".into())),
        }
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

        // Send initialized notification
        let _ = self
            .send_request(
                "notifications/initialized",
                Some(serde_json::json!({"protocolVersion": "2025-06-18"})),
            )
            .await;

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
            "arguments": arguments.unwrap_or(serde_json::Value::Null),
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