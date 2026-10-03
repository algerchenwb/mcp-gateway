//! JSON-RPC handler — the main POST /mcp endpoint.
//!
//! This implements the Streamable HTTP transport from the MCP specification:
//! clients POST JSON-RPC messages and receive JSON-RPC responses.

use axum::extract::State;
use axum::Json;
use serde_json::Value;

use mcp_gateway_core::types::{
    error_codes, Implementation, InitializeRequest, InitializeResult, JsonRpcErrorResponse,
    JsonRpcMessage, JsonRpcRequest, JsonRpcResponse, RequestId, ServerCapabilities,
    ToolsCapability,
};

use crate::proxy::forward;
use crate::server::AppState;

/// Handle a JSON-RPC request at POST /mcp.
pub async fn handle(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, Json<Value>> {
    // Parse the incoming JSON-RPC message
    let message: JsonRpcMessage = serde_json::from_value(body)
        .map_err(|e| json_error(RequestId::Null, error_codes::PARSE_ERROR, e.to_string()))?;

    let scope = credential_scope(&state, &headers);
    process_message_scoped(state, message, &scope).await
}

pub fn credential_scope(state: &AppState, headers: &axum::http::HeaderMap) -> String {
    use sha2::{Digest, Sha256};
    let credential = headers
        .get(&state.config.auth.api_key_header)
        .or_else(|| headers.get("authorization"));
    credential.map_or_else(
        || "public".to_string(),
        |value| format!("{:x}", Sha256::digest(value.as_bytes())),
    )
}

/// Core message processing logic, shared across transport handlers.
pub async fn process_message(
    state: AppState,
    message: JsonRpcMessage,
) -> Result<Json<Value>, Json<Value>> {
    process_message_scoped(state, message, "public").await
}

pub async fn process_message_scoped(
    state: AppState,
    message: JsonRpcMessage,
    scope: &str,
) -> Result<Json<Value>, Json<Value>> {
    process_message_authorized(state, message, scope, None).await
}
pub async fn process_message_authorized(
    state: AppState,
    message: JsonRpcMessage,
    scope: &str,
    principal: Option<crate::auth::oauth::Principal>,
) -> Result<Json<Value>, Json<Value>> {
    match message {
        JsonRpcMessage::Request(req) => {
            let id = req.id.clone();
            let resp = if req.method == "initialize" {
                handle_request(state.clone(), req, scope, principal.as_ref()).await
            } else if let Some((_guard, registration)) = state.requests.register(scope, id.clone())
            {
                futures_util::future::Abortable::new(
                    handle_request(state.clone(), req, scope, principal.as_ref()),
                    registration,
                )
                .await
                .unwrap_or_else(|_| {
                    JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                        id,
                        -32800,
                        "request cancelled",
                    ))
                })
            } else {
                JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                    id,
                    error_codes::INVALID_REQUEST,
                    "request ID already in flight for this identity",
                ))
            };
            if matches!(resp, JsonRpcMessage::Error(_)) {
                state.metrics.record_rpc_error();
            }
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
            handle_notification(state, notif, scope).await;
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
async fn handle_request(
    state: AppState,
    req: JsonRpcRequest,
    scope: &str,
    principal: Option<&crate::auth::oauth::Principal>,
) -> JsonRpcMessage {
    let method = req.method.as_str();

    match method {
        "initialize" => handle_initialize(req.id, req.params),
        "tools/list" => handle_tools_list(state, req.id, principal).await,
        "tools/call" => {
            if let (Some(oauth), Some(principal)) = (&state.config.auth.oauth, principal) {
                if let Some(name) = req
                    .params
                    .as_ref()
                    .and_then(|p| p.get("name"))
                    .and_then(Value::as_str)
                {
                    if !crate::auth::oauth::allows_tool(oauth, principal, name) {
                        return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                            req.id,
                            error_codes::INVALID_PARAMS,
                            "tool access denied",
                        ));
                    }
                }
            }
            handle_tools_call(state, req.id, req.params, scope).await
        }
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
    state: AppState,
    notif: mcp_gateway_core::types::JsonRpcNotification,
    scope: &str,
) {
    let method = notif.method.as_str();
    tracing::debug!(method = method, "received notification");

    match method {
        "notifications/initialized" => {
            tracing::info!("client initialized successfully");
        }
        "notifications/cancelled" => {
            if let Some(id) = notif.params.as_ref().and_then(|p| p.get("requestId")) {
                if let Ok(id) = serde_json::from_value::<RequestId>(id.clone()) {
                    if id != RequestId::Null {
                        state.requests.cancel(scope, id);
                    }
                }
            }
        }
        _ => {
            tracing::debug!(method = method, "unknown notification");
        }
    }
}

// ── Method Handlers ──

/// Handle `initialize` — MCP capability negotiation.
fn handle_initialize(id: RequestId, params: Option<Value>) -> JsonRpcMessage {
    let init_req: InitializeRequest = match params {
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
        protocol_version: if mcp_gateway_sdk::transport::SUPPORTED_VERSIONS
            .contains(&init_req.protocol_version.as_str())
        {
            init_req.protocol_version
        } else {
            mcp_gateway_sdk::transport::PROTOCOL_VERSION.into()
        },
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
async fn handle_tools_list(
    state: AppState,
    id: RequestId,
    principal: Option<&crate::auth::oauth::Principal>,
) -> JsonRpcMessage {
    match state.backends.tools().await {
        Ok(mut entries) => {
            if let (Some(oauth), Some(principal)) = (&state.config.auth.oauth, principal) {
                entries.retain(|entry| {
                    crate::auth::oauth::allows_tool(oauth, principal, &entry.tool.name)
                });
            }
            entries.sort_by(|a, b| a.tool.name.cmp(&b.tool.name));
            let tools: Vec<_> = entries.into_iter().map(|entry| entry.tool).collect();
            JsonRpcMessage::Response(JsonRpcResponse::new(id, serde_json::json!({"tools":tools})))
        }
        Err(error) => JsonRpcMessage::Error(JsonRpcErrorResponse::new(
            id,
            error.to_error_code(),
            error.to_string(),
        )),
    }
}

/// Handle `tools/call` — invoke a tool on a backend via the proxy layer.
async fn handle_tools_call(
    state: AppState,
    id: RequestId,
    params: Option<Value>,
    scope: &str,
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

    let Some(tool_name) = params
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
    else {
        return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
            id,
            error_codes::INVALID_PARAMS,
            "tools/call requires a nonempty name",
        ));
    };
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    if !arguments.is_object() {
        return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
            id,
            error_codes::INVALID_PARAMS,
            "arguments must be an object",
        ));
    }
    let target = match state.backends.resolve(tool_name).await {
        Ok(Some(target)) => target,
        Ok(None) => {
            return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                id,
                error_codes::INVALID_PARAMS,
                format!("unknown tool: {tool_name}"),
            ))
        }
        Err(error) => {
            return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                id,
                error.to_error_code(),
                error.to_string(),
            ))
        }
    };
    if !target.validator.is_valid(&arguments) {
        return JsonRpcMessage::Error(JsonRpcErrorResponse::new(
            id,
            error_codes::INVALID_PARAMS,
            "arguments do not match the tool inputSchema",
        ));
    }

    // Record the tool call metric
    state.metrics.record_tool_call();

    // Forward to backend via proxy layer
    let cache = if state.config.cache.enabled {
        Some(state.cache.as_ref())
    } else {
        None
    };

    match forward::forward_tool_call(
        &target,
        tool_name,
        Some(arguments),
        cache,
        scope,
        &state.metrics,
    )
    .await
    {
        Ok(result) => JsonRpcMessage::Response(JsonRpcResponse::new(
            id,
            serde_json::to_value(result).unwrap_or_default(),
        )),
        Err(e) => {
            state.metrics.record_tool_error();
            tracing::error!(
                tool = tool_name,
                error = %e,
                "tool call failed"
            );
            if let mcp_gateway_core::error::McpError::Rpc(detail) = e {
                JsonRpcMessage::Error(JsonRpcErrorResponse {
                    jsonrpc: "2.0".into(),
                    id,
                    error: detail,
                })
            } else {
                JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                    id,
                    e.to_error_code(),
                    e.to_string(),
                ))
            }
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

pub fn authorized_scope(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    principal: Option<&crate::auth::oauth::Principal>,
) -> String {
    use sha2::{Digest, Sha256};
    if let Some(principal) = principal {
        let scopes: std::collections::BTreeSet<_> = principal.scopes.iter().collect();
        let material = serde_json::json!([principal.issuer, principal.subject, scopes]);
        format!("{:x}", Sha256::digest(material.to_string().as_bytes()))
    } else {
        credential_scope(state, headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_ping() {
        let result = handle_ping(RequestId::Number(1));
        match result {
            JsonRpcMessage::Response(_) => {}
            _ => panic!("expected response"),
        }
    }
}
