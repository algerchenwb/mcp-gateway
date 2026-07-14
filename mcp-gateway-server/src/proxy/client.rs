//! MCP client for connecting to backend MCP servers.
//!
//! Supports multiple transport types:
//! - Streamable HTTP: POST JSON-RPC to an HTTP endpoint
//! - stdio: spawn a child process and communicate via stdin/stdout
//! - SSE: connect via Server-Sent Events (Phase 2)

use std::sync::Arc;
use std::time::Duration;

use mcp_gateway_core::error::McpResult;
use mcp_gateway_core::types::JsonRpcMessage;
use mcp_gateway_core::transport::TransportType;

use crate::config::BackendConfig;

/// Client for communicating with a backend MCP server.
pub struct BackendClient {
    config: Arc<BackendConfig>,
    http_client: reqwest::Client,
}

impl BackendClient {
    /// Create a new backend client.
    pub fn new(config: Arc<BackendConfig>) -> Self {
        let timeout = Duration::from_millis(config.timeout_ms);
        let http_client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            config,
            http_client,
        }
    }

    /// Send a JSON-RPC message to the backend and return the response.
    pub async fn send(&self, message: JsonRpcMessage) -> McpResult<JsonRpcMessage> {
        match self.config.transport_type() {
            TransportType::StreamableHttp | TransportType::Sse => {
                self.send_http(message).await
            }
            TransportType::Stdio => {
                self.send_stdio(message).await
            }
            TransportType::WebSocket => {
                // Phase 2: WebSocket transport
                Err(mcp_gateway_core::error::McpError::Transport(
                    "WebSocket transport not yet implemented".to_string(),
                ))
            }
        }
    }

    /// Send via HTTP (Streamable HTTP or SSE).
    async fn send_http(&self, message: JsonRpcMessage) -> McpResult<JsonRpcMessage> {
        let endpoint = self.config.endpoint.as_ref().ok_or_else(|| {
            mcp_gateway_core::error::McpError::BackendConnection(
                "no endpoint configured".to_string(),
            )
        })?;

        let body = serde_json::to_vec(&message)?;

        let response = self
            .http_client
            .post(endpoint)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| {
                mcp_gateway_core::error::McpError::BackendConnection(format!(
                    "HTTP request failed for backend '{}': {e}",
                    self.config.name
                ))
            })?;

        let status = response.status();
        let response_body = response.text().await.map_err(|e| {
            mcp_gateway_core::error::McpError::BackendConnection(format!(
                "failed to read response from backend '{}': {e}",
                self.config.name
            ))
        })?;

        if !status.is_success() {
            return Err(mcp_gateway_core::error::McpError::BackendConnection(
                format!(
                    "backend '{}' returned HTTP {status}: {response_body}",
                    self.config.name
                ),
            ));
        }

        let response_msg: JsonRpcMessage = serde_json::from_str(&response_body).map_err(|e| {
            mcp_gateway_core::error::McpError::Serialization(format!(
                "failed to parse backend response: {e}"
            ))
        })?;

        Ok(response_msg)
    }

    /// Send via stdio (spawn process, write stdin, read stdout).
    async fn send_stdio(&self, message: JsonRpcMessage) -> McpResult<JsonRpcMessage> {
        let command = self.config.command.as_ref().ok_or_else(|| {
            mcp_gateway_core::error::McpError::BackendConnection(
                "no command configured for stdio transport".to_string(),
            )
        })?;

        use tokio::process::Command;
        use tokio::io::AsyncWriteExt;

        let mut child = Command::new(command)
            .args(&self.config.args)
            .envs(&self.config.env)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                mcp_gateway_core::error::McpError::BackendConnection(format!(
                    "failed to spawn stdio command '{command}': {e}"
                ))
            })?;

        // Write the JSON-RPC message to stdin
        let mut stdin = child.stdin.take().expect("failed to open stdin");
        let body = serde_json::to_vec(&message)?;
        stdin.write_all(&body).await?;
        stdin.write_all(b"\n").await?;
        drop(stdin); // Close stdin to signal EOF

        // Read the response from stdout
        let output = child.wait_with_output().await.map_err(|e| {
            mcp_gateway_core::error::McpError::BackendConnection(format!(
                "stdio process failed: {e}"
            ))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(mcp_gateway_core::error::McpError::BackendConnection(
                format!(
                    "stdio command '{}' exited with {}: {stderr}",
                    command, output.status
                ),
            ));
        }

        let response_msg: JsonRpcMessage = serde_json::from_slice(&output.stdout).map_err(|e| {
            mcp_gateway_core::error::McpError::Serialization(format!(
                "failed to parse stdio response: {e}"
            ))
        })?;

        Ok(response_msg)
    }

    /// Get the backend name.
    pub fn name(&self) -> &str {
        &self.config.name
    }
}