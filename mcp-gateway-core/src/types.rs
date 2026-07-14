use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// JSON-RPC 2.0 request ID: string, number, or null (for notifications).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum RequestId {
    String(String),
    Number(i64),
    Null,
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::String(s) => write!(f, "{s}"),
            RequestId::Number(n) => write!(f, "{n}"),
            RequestId::Null => write!(f, "null"),
        }
    }
}

// ── JSON-RPC 2.0 Core Types ──

/// A JSON-RPC 2.0 request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: RequestId,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// A JSON-RPC 2.0 success response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
}

/// JSON-RPC 2.0 error detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetail {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// A JSON-RPC 2.0 error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcErrorResponse {
    pub jsonrpc: String,
    pub id: RequestId,
    pub error: ErrorDetail,
}

/// A JSON-RPC 2.0 notification (no id field).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// All four JSON-RPC 2.0 message types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcMessage {
    Request(JsonRpcRequest),
    Response(JsonRpcResponse),
    Error(JsonRpcErrorResponse),
    Notification(JsonRpcNotification),
}

impl JsonRpcMessage {
    /// Returns the method name if this message is a request or notification.
    pub fn method(&self) -> Option<&str> {
        match self {
            JsonRpcMessage::Request(r) => Some(&r.method),
            JsonRpcMessage::Notification(n) => Some(&n.method),
            _ => None,
        }
    }

    /// Returns the request ID if present.
    pub fn id(&self) -> Option<&RequestId> {
        match self {
            JsonRpcMessage::Request(r) => Some(&r.id),
            JsonRpcMessage::Response(r) => Some(&r.id),
            JsonRpcMessage::Error(e) => Some(&e.id),
            JsonRpcMessage::Notification(_) => None,
        }
    }
}

// ── MCP Protocol Types ──

/// Implementation info exchanged during initialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Implementation {
    pub name: String,
    pub version: String,
}

/// Client capabilities announced during initialize.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClientCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roots: Option<RootsCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experimental: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootsCapability {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_changed: Option<bool>,
}

/// Server capabilities returned during initialize.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolsCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompts: Option<PromptsCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logging: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experimental: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolsCapability {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_changed: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcesCapability {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscribe: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_changed: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptsCapability {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_changed: Option<bool>,
}

/// MCP initialize request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeRequest {
    #[serde(rename = "protocolVersion")]
    pub protocol_version: String,
    pub capabilities: ClientCapabilities,
    #[serde(rename = "clientInfo")]
    pub client_info: Implementation,
}

/// MCP initialize result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializeResult {
    #[serde(rename = "protocolVersion")]
    pub protocol_version: String,
    pub capabilities: ServerCapabilities,
    #[serde(rename = "serverInfo")]
    pub server_info: Implementation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

/// MCP initialized notification (sent by client after receiving initialize result).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitializedNotification {
    #[serde(rename = "protocolVersion", skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,
}

// ── JSON-RPC 2.0 Error Codes ──

/// Standard JSON-RPC 2.0 error codes.
pub mod error_codes {
    // Standard JSON-RPC errors
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;

    // MCP-specific errors
    pub const SERVER_NOT_INITIALIZED: i32 = -32000;
    pub const UNKNOWN_CAPABILITY: i32 = -32001;
}

impl JsonRpcRequest {
    pub fn new(method: impl Into<String>, params: Option<Value>, id: RequestId) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.into(),
            params,
        }
    }
}

impl JsonRpcResponse {
    pub fn new(id: RequestId, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
        }
    }
}

impl JsonRpcErrorResponse {
    pub fn new(id: RequestId, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            error: ErrorDetail {
                code,
                message: message.into(),
                data: None,
            },
        }
    }

    pub fn with_data(id: RequestId, code: i32, message: impl Into<String>, data: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            error: ErrorDetail {
                code,
                message: message.into(),
                data: Some(data),
            },
        }
    }
}

impl JsonRpcNotification {
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            method: method.into(),
            params,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_request() {
        let req = JsonRpcRequest::new(
            "tools/list",
            None,
            RequestId::Number(1),
        );
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"method\":\"tools/list\""));
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
    }

    #[test]
    fn test_deserialize_response() {
        let json = r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#;
        let msg: JsonRpcMessage = serde_json::from_str(json).unwrap();
        assert!(matches!(msg, JsonRpcMessage::Response(_)));
    }

    #[test]
    fn test_deserialize_notification() {
        let json = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        let msg: JsonRpcMessage = serde_json::from_str(json).unwrap();
        assert!(matches!(msg, JsonRpcMessage::Notification(_)));
    }
}