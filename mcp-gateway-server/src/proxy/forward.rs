//! Request forwarding — proxy tool calls to backend MCP servers.
//!
//! The forward layer:
//! 1. Checks the L1 cache for a matching result
//! 2. Forwards the request to the backend via the appropriate transport
//! 3. Stores the result in the cache

use std::sync::Arc;
use std::time::Duration;

use mcp_gateway_core::error::{McpError, McpResult};
use mcp_gateway_core::tool::ToolCallResult;
use mcp_gateway_core::types::{JsonRpcMessage, JsonRpcRequest, RequestId};

use crate::cache::l1::L1Cache;
use crate::proxy::client::BackendClient;
use crate::router::engine::RouteTarget;

/// Build a deterministic cache key from tool name and arguments.
///
/// Per the document: "相同参数 + 相同工具 → 直接返回缓存结果"
pub fn cache_key(tool_name: &str, arguments: &serde_json::Value) -> String {
    let args_str = serde_json::to_string(arguments).unwrap_or_default();
    format!("tool:{tool_name}:args:{args_str}")
}

/// Forward a tool call request to the appropriate backend.
///
/// Returns the tool call result, checking the cache first.
pub async fn forward_tool_call(
    target: &RouteTarget,
    tool_name: &str,
    arguments: Option<serde_json::Value>,
    cache: Option<&L1Cache>,
) -> McpResult<ToolCallResult> {
    let args = arguments.unwrap_or(serde_json::Value::Null);

    // 1. Check cache
    if let Some(cache) = cache {
        let key = cache_key(tool_name, &args);
        if let Some(cached) = cache.get(&key).await {
            tracing::debug!(
                tool = tool_name,
                backend = target.backend.name,
                "cache hit"
            );
            return Ok(cached);
        }
    }

    // 2. Build the JSON-RPC request
    let request = JsonRpcRequest::new(
        "tools/call",
        Some(serde_json::json!({
            "name": tool_name,
            "arguments": args,
        })),
        RequestId::String(uuid::Uuid::new_v4().to_string()),
    );

    let message = JsonRpcMessage::Request(request);

    // 3. Send to backend
    let client = BackendClient::new(Arc::clone(&target.backend));

    tracing::info!(
        tool = tool_name,
        backend = target.backend.name,
        transport = target.backend.transport_type().as_str(),
        "forwarding tool call"
    );

    let start = std::time::Instant::now();

    let response = client.send(message).await.map_err(|e| {
        tracing::error!(
            tool = tool_name,
            backend = target.backend.name,
            error = %e,
            "backend call failed"
        );
        e
    })?;

    let elapsed = start.elapsed();
    tracing::debug!(
        tool = tool_name,
        backend = target.backend.name,
        latency_ms = elapsed.as_millis(),
        "backend responded"
    );

    // 4. Parse the response
    let result: ToolCallResult = match response {
        JsonRpcMessage::Response(resp) => {
            let value = resp.result.ok_or_else(|| {
                McpError::BackendConnection("empty response from backend".to_string())
            })?;
            serde_json::from_value(value).map_err(|e| {
                McpError::Serialization(format!("failed to parse tool call result: {e}"))
            })?
        }
        JsonRpcMessage::Error(err) => {
            return Err(McpError::BackendConnection(format!(
                "backend returned error: {} (code: {})",
                err.error.message, err.error.code
            )));
        }
        _ => {
            return Err(McpError::BackendConnection(
                "unexpected response type from backend".to_string(),
            ));
        }
    };

    // 5. Store in cache
    if let Some(cache) = cache {
        let key = cache_key(tool_name, &args);
        cache.set(&key, result.clone(), Duration::from_secs(300)).await;
        tracing::debug!(tool = tool_name, "cached result");
    }

    Ok(result)
}