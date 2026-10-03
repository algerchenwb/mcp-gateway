//! Stateless Streamable HTTP endpoint. Stateful upstream sessions live in backend clients.
use crate::server::AppState;
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use mcp_gateway_core::types::{error_codes, JsonRpcErrorResponse, JsonRpcMessage, RequestId};

pub async fn handle(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(version) = headers.get("MCP-Protocol-Version") {
        if !version
            .to_str()
            .ok()
            .is_some_and(|v| mcp_gateway_sdk::transport::SUPPORTED_VERSIONS.contains(&v))
        {
            return StatusCode::BAD_REQUEST.into_response();
        }
    }
    let accepts = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !accepts.contains("application/json") || !accepts.contains("text/event-stream") {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    }
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if content_type != "application/json" {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return protocol_error(error_codes::PARSE_ERROR, "invalid JSON"),
    };
    let message: JsonRpcMessage = match serde_json::from_value(value) {
        Ok(message) => message,
        Err(_) => return protocol_error(error_codes::INVALID_REQUEST, "invalid JSON-RPC message"),
    };
    if matches!(
        message,
        JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_)
    ) {
        return protocol_error(error_codes::INVALID_REQUEST, "unsolicited client response");
    }
    let notification = matches!(message, JsonRpcMessage::Notification(_));
    let scope = super::json_rpc::credential_scope(&state, &headers);
    let response = super::json_rpc::process_message_scoped(state, message, &scope).await;
    if notification {
        return StatusCode::ACCEPTED.into_response();
    }
    match response {
        Ok(json) | Err(json) => json.into_response(),
    }
}
fn protocol_error(code: i32, message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(JsonRpcErrorResponse::new(RequestId::Null, code, message)),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::GatewayConfig, server::build_router};
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    async fn post(body: &str) -> Response {
        let state = AppState::new(GatewayConfig::default());
        build_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .body(Body::from(body.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn notifications_return_empty_202_and_invalid_envelopes_return_400() {
        let response = post(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(post("bad json").await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            post(r#"{"jsonrpc":"1.0","id":1,"method":"ping"}"#)
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            post(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#)
                .await
                .status(),
            StatusCode::OK
        );
    }
}
