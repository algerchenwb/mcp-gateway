//! Streamable HTTP transport handler.
//!
//! Streamable HTTP is the primary MCP transport for bidirectional streaming over HTTP.
//! It extends the basic JSON-RPC endpoint with:
//! - Streaming responses (chunked transfer encoding)
//! - Session management via Mcp-Session-Id header
//! - Support for server-to-client notifications

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::Value;
use uuid::Uuid;

use mcp_gateway_core::types::{error_codes, JsonRpcErrorResponse, JsonRpcMessage, RequestId};

use crate::server::AppState;

/// Handle incoming requests with Streamable HTTP transport semantics.
pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let session_id = headers
        .get("Mcp-Session-Id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Parse the message
    let message: JsonRpcMessage = match serde_json::from_value(body) {
        Ok(m) => m,
        Err(e) => {
            return streamable_error(
                session_id,
                RequestId::Null,
                error_codes::PARSE_ERROR,
                e.to_string(),
            );
        }
    };

    // Process through the shared JSON-RPC handler
    let response = super::json_rpc::process_message(state, message).await;

    let response_json = match response {
        Ok(json) => json.0,
        Err(json) => json.0,
    };

    // Build response headers
    let mut response_headers = HeaderMap::new();
    let new_session_id = session_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    response_headers.insert("Mcp-Session-Id", new_session_id.parse().unwrap());

    (StatusCode::OK, response_headers, Json(response_json)).into_response()
}

fn streamable_error(
    session_id: Option<String>,
    id: RequestId,
    code: i32,
    message: impl Into<String>,
) -> Response {
    let err = JsonRpcErrorResponse::new(id, code, message);
    let mut headers = HeaderMap::new();
    if let Some(sid) = session_id {
        headers.insert("Mcp-Session-Id", sid.parse().unwrap());
    }
    (
        StatusCode::OK,
        headers,
        Json(serde_json::to_value(err).unwrap_or_default()),
    )
        .into_response()
}