//! MCP Gateway SDK — Rust client library for the MCP Gateway.
//!
//! Provides a high-level async API for communicating with the gateway:
//! - `initialize` — establish a connection
//! - `list_tools` — discover available tools
//! - `call_tool` — invoke a tool
//! - `ping` — keepalive
//!
//! # Example
//!
//! ```no_run
//! use mcp_gateway_sdk::GatewayClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = GatewayClient::new("http://127.0.0.1:8080/mcp", None);
//!     let result = client.initialize().await?;
//!     println!("Server: {} v{}", result.server_info.name, result.server_info.version);
//!
//!     let tools = client.list_tools().await?;
//!     println!("Available tools: {}", tools.len());
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod transport;

pub use client::GatewayClient;
pub use mcp_gateway_core::tool::Tool;
pub use mcp_gateway_core::types::{InitializeResult, ServerCapabilities};
pub use mcp_gateway_core::tool::ToolCallResult;