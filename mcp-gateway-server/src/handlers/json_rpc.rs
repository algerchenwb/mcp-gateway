//! JSON-RPC handler — the main POST /mcp endpoint.
//!
//! This implements the Streamable HTTP transport from the MCP specification:
//! clients POST JSON-RPC messages and receive JSON-RPC responses.

use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use serde_json::Value;

use mcp_gateway_core::tool::Tool;
use mcp_gateway_core::types::{
    error_codes, Implementation, InitializeRequest, InitializeResult, JsonRpcErrorResponse,
    JsonRpcMessage, JsonRpcRequest, JsonRpcResponse, RequestId, ServerCapabilities,
    ToolsCapability,
};

use crate::proxy::forward;
use crate::router::static_route::build_engine;
use crate::server::AppState;

/// Handle a JSON-RPC request at POST /mcp.
pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, Json<Value>> {
    // Auth check
    check_auth(&state, &headers)?;

    // Parse the incoming JSON-RPC message
    let message: JsonRpcMessage = serde_json::from_value(body)
        .map_err(|e| json_error(RequestId::Null, error_codes::PARSE_ERROR, e.to_string()))?;

    process_message(state, message).await
}

/// Check API key authentication.
fn check_auth(state: &AppState, headers: &HeaderMap) -> Result<(), Json<Value>> {
    if !state.config.auth.enabled {
        return Ok(());
    }

    // Extract API key from headers
    let key = headers
        .get(&state.config.auth.api_key_header)
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
        });

    match key {
        Some(k) if state.config.auth.api_keys.iter().any(|valid| valid == k) => Ok(()),
        _ => Err(json_error(
            RequestId::Null,
            error_codes::INTERNAL_ERROR,
            "authentication required",
        )),
    }
}

/// Core message processing logic, shared across transport handlers.
pub async fn process_message(
    state: AppState,
    message: JsonRpcMessage,
) -> Result<Json<Value>, Json<Value>> {
    match message {
        JsonRpcMessage::Request(req) => {
            let resp = handle_request(state, req).await;
            let json = serde_json::to_value(&resp).unwrap_or_else(|_| {
                json_error_value(
                    RequestId::Null,
                    error_codes::INTERNAL_ERROR,
                    "serialization error",
                )
            });
            Ok(Json(json))
        }
        JsonRpcMessage::Notification(notif) => {
            handle_notification(state, notif).await;
            Ok(Json(serde_json::json!({})))
        }
        JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_) => Err(Json(json_error_value(
            RequestId::Null,
            error_codes::INVALID_REQUEST,
            "gateway does not accept responses from clients",
        ))),
    }
}

/// Route a JSON-RPC request to the appropriate handler.
async fn handle_request(state: AppState, req: JsonRpcRequest) -> JsonRpcMessage {
    let method = req.method.as_str();

    match method {
        "initialize" => handle_initialize(req.id, req.params),
        "tools/list" => handle_tools_list(state, req.id),
        "tools/call" => handle_tools_call(state, req.id, req.params).await,
        "ping" => handle_ping(req.id),
        _ => JsonRpcMessage::Error(JsonRpcErrorResponse::new(
            req.id,
            error_codes::METHOD_NOT_FOUND,
            format!("method not found: {method}"),
        )),
    }
}

/// Handle a JSON-RPC notification.
async fn handle_notification(
    _state: AppState,
    notif: mcp_gateway_core::types::JsonRpcNotification,
) {
    let method = notif.method.as_str();
    tracing::debug!(method = method, "received notification");

    match method {
        "notifications/initialized" => {
            tracing::info!("client initialized successfully");
        }
        "notifications/cancelled" => {
            tracing::debug!("client cancelled request");
        }
        _ => {
            tracing::debug!(method = method, "unknown notification");
        }
    }
}

// ── Method Handlers ──

/// Handle `initialize` — MCP capability negotiation.
fn handle_initialize(id: RequestId, params: Option<Value>) -> JsonRpcMessage {
    let _init_req: InitializeRequest = match params {
        Some(p) => match serde_json::from_value(p) {
            Ok(r) => r,
            Err(e) => {
                return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                    id,
                    error_codes::INVALID_PARAMS,
                    format!("invalid initialize params: {e}"),
                ));
            }
        },
        None => {
            return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                id,
                error_codes::INVALID_PARAMS,
                "missing initialize params",
            ));
        }
    };

    let result = InitializeResult {
        protocol_version: "2025-06-18".to_string(),
        capabilities: ServerCapabilities {
            tools: Some(ToolsCapability {
                list_changed: Some(false),
            }),
            resources: None,
            prompts: None,
            logging: None,
            experimental: None,
        },
        server_info: Implementation {
            name: "mcp-gateway".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        instructions: Some("MCP Gateway — route tool calls to configured backends".to_string()),
    };

    JsonRpcMessage::Response(JsonRpcResponse::new(
        id,
        serde_json::to_value(result).unwrap_or_default(),
    ))
}

/// Handle `tools/list` — list all tools from all configured backends.
fn handle_tools_list(state: AppState, id: RequestId) -> JsonRpcMessage {
    let engine = build_engine(&state.config);
    let tools: Vec<Tool> = engine
        .all_tools()
        .into_iter()
        .map(|(name, backend)| Tool {
            name,
            description: Some(format!("Tool from backend '{backend}'")),
            extra: Default::default(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        })
        .collect();

    JsonRpcMessage::Response(JsonRpcResponse::new(
        id,
        serde_json::json!({"tools": tools}),
    ))
}

/// Handle `tools/call` — invoke a tool on a backend via the proxy layer.
async fn handle_tools_call(
    state: AppState,
    id: RequestId,
    params: Option<Value>,
) -> JsonRpcMessage {
    let params = match params {
        Some(p) => p,
        None => {
            return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                id,
                error_codes::INVALID_PARAMS,
                "missing params for tools/call",
            ));
        }
    };

    let tool_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let arguments = params.get("arguments").cloned();

    // Resolve the backend via route engine
    let engine = build_engine(&state.config);
    let target = match engine.resolve(tool_name) {
        Some(t) => t,
        None => {
            tracing::warn!(tool = tool_name, "no backend found for tool");
            return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                id,
                error_codes::METHOD_NOT_FOUND,
                format!("no backend configured for tool: {tool_name}"),
            ));
        }
    };

    // Record the tool call metric
    state.metrics.record_tool_call();

    // Forward to backend via proxy layer
    let cache = if state.config.cache.enabled {
        Some(state.cache.as_ref())
    } else {
        None
    };

    match forward::forward_tool_call(&target, tool_name, arguments, cache).await {
        Ok(result) => JsonRpcMessage::Response(JsonRpcResponse::new(
            id,
            serde_json::to_value(result).unwrap_or_default(),
        )),
        Err(e) => {
            tracing::error!(
                tool = tool_name,
                error = %e,
                "tool call failed"
            );
            JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                id,
                e.to_error_code(),
                e.to_string(),
            ))
        }
    }
}

/// Handle `ping` — keepalive.
fn handle_ping(id: RequestId) -> JsonRpcMessage {
    JsonRpcMessage::Response(JsonRpcResponse::new(id, serde_json::json!({})))
}

// ── Helpers ──

fn json_error(id: RequestId, code: i32, message: impl Into<String>) -> Json<Value> {
    Json(json_error_value(id, code, message))
}

fn json_error_value(id: RequestId, code: i32, message: impl Into<String>) -> Value {
    serde_json::to_value(JsonRpcErrorResponse::new(id, code, message)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::l1::L1Cache;
    use crate::config::{AuthConfig, BackendConfig, CacheConfig, GatewayConfig, GatewaySettings};
    use std::sync::Arc;

    fn test_state() -> AppState {
        AppState {
            config: Arc::new(GatewayConfig {
                gateway: GatewaySettings {
                    name: "test".into(),
                    listen_addr: "127.0.0.1:0".into(),
                },
                backends: vec![BackendConfig {
                    name: "test-backend".into(),
                    transport: "streamable-http".into(),
                    endpoint: Some("http://localhost:9001/mcp".into()),
                    tools: vec!["echo".into()],
                    ..Default::default()
                }],
                auth: AuthConfig::default(),
                cache: CacheConfig::default(),
                ..Default::default()
            }),
            cache: Arc::new(L1Cache::new(100, std::time::Duration::from_secs(60))),
            metrics: Arc::new(crate::middleware::metrics::Metrics::default()),
        }
    }

    #[test]
    fn test_initialize() {
        let result = handle_initialize(
            RequestId::Number(1),
            Some(serde_json::json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1.0" }
            })),
        );

        match result {
            JsonRpcMessage::Response(resp) => {
                let init_result: InitializeResult =
                    serde_json::from_value(resp.result.unwrap()).unwrap();
                assert_eq!(init_result.protocol_version, "2025-06-18");
                assert!(init_result.capabilities.tools.is_some());
            }
            _ => panic!("expected response"),
        }
    }

    #[test]
    fn test_tools_list() {
        let state = test_state();
        let result = handle_tools_list(state, RequestId::Number(1));

        match result {
            JsonRpcMessage::Response(resp) => {
                let obj = resp.result.unwrap();
                let tools = obj["tools"].as_array().unwrap();
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0]["name"], "echo");
            }
            _ => panic!("expected response"),
        }
    }

    #[test]
    fn test_ping() {
        let result = handle_ping(RequestId::Number(1));
        match result {
            JsonRpcMessage::Response(_) => {}
            _ => panic!("expected response"),
        }
    }
}
