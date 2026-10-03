//! Request forwarding — proxy tool calls to backend MCP servers.
//!
//! The forward layer:
//! 1. Checks the L1 cache for a matching result
//! 2. Forwards the request to the backend via the appropriate transport
//! 3. Stores the result in the cache

use mcp_gateway_core::error::{McpError, McpResult};
use mcp_gateway_core::tool::ToolCallResult;
use mcp_gateway_core::types::{JsonRpcMessage, JsonRpcRequest, RequestId};

use crate::cache::l1::L1Cache;
use crate::proxy::registry::ToolEntry;

/// Build a deterministic cache key from tool name and arguments.
///
/// Per the document: "相同参数 + 相同工具 → 直接返回缓存结果"
pub fn cache_key(
    backend: &crate::config::BackendConfig,
    scope: &str,
    tool_name: &str,
    arguments: &serde_json::Value,
) -> String {
    use sha2::{Digest, Sha256};
    let material = serde_json::json!([
        backend.name,
        backend.endpoint,
        backend.command,
        backend.args,
        backend.env,
        scope,
        tool_name,
        canonical(arguments)
    ]);
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&material).expect("JSON value serialization"))
    )
}
fn canonical(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let ordered: std::collections::BTreeMap<_, _> =
                map.iter().map(|(k, v)| (k.clone(), canonical(v))).collect();
            serde_json::to_value(ordered).expect("JSON object serialization")
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical).collect())
        }
        _ => value.clone(),
    }
}

/// Forward a tool call request to the appropriate backend.
///
/// Returns the tool call result, checking the cache first.
pub async fn forward_tool_call(
    target: &ToolEntry,
    tool_name: &str,
    arguments: Option<serde_json::Value>,
    cache: Option<&L1Cache>,
    scope: &str,
) -> McpResult<ToolCallResult> {
    let args = arguments.unwrap_or_else(|| serde_json::json!({}));
    let scope = format!(
        "{scope}:{}",
        serde_json::to_string(&target.tool).unwrap_or_default()
    );

    let cache = cache.filter(|_| {
        target
            .backend
            .cache_tools
            .iter()
            .any(|name| name == tool_name)
    });

    // 1. Check cache
    if let Some(cache) = cache {
        let key = cache_key(&target.backend, &scope, tool_name, &args);
        if let Some(cached) = cache.get(&key).await {
            tracing::debug!(tool = tool_name, backend = target.backend.name, "cache hit");
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
    let client = &target.client;

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
            return Err(McpError::Rpc(err.error));
        }
        _ => {
            return Err(McpError::BackendConnection(
                "unexpected response type from backend".to_string(),
            ));
        }
    };

    // 5. Store in cache
    if let Some(cache) = cache {
        let key = cache_key(&target.backend, &scope, tool_name, &args);
        cache.set(&key, result.clone()).await;
        tracing::debug!(tool = tool_name, "cached result");
    }

    Ok(result)
}
#[cfg(test)]
mod cache_tests {
    use super::*;
    #[test]
    fn cache_is_scoped_and_arguments_are_canonical() {
        let backend = crate::config::BackendConfig::default();
        let a = cache_key(
            &backend,
            "a",
            "echo",
            &serde_json::json!({"x":1,"y":{"b":2,"a":1}}),
        );
        let b = cache_key(
            &backend,
            "a",
            "echo",
            &serde_json::json!({"y":{"a":1,"b":2},"x":1}),
        );
        assert_eq!(a, b);
        assert_ne!(
            a,
            cache_key(
                &backend,
                "b",
                "echo",
                &serde_json::json!({"x":1,"y":{"b":2,"a":1}})
            )
        );
    }
}
