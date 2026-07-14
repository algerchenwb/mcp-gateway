//! SSE (Server-Sent Events) transport handler.
//!
//! Per the MCP specification, SSE transport works as follows:
//! 1. Client connects to GET /mcp/sse
//! 2. Server sends an `endpoint` event with a unique session-specific message URL
//! 3. Client POSTs JSON-RPC messages to that session URL
//! 4. Server streams responses back as SSE events

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::Sse as SseResponse;
use futures_util::stream::Stream;
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::server::AppState;

/// SSE event types for MCP.
#[derive(Debug, Clone)]
pub enum McpSseEvent {
    /// Sent when the SSE connection is established, containing the message endpoint URL.
    Endpoint { session_id: String, message_url: String },
    /// A JSON-RPC response or notification to be sent to the client.
    Message(String),
    /// Connection is being closed.
    Close,
}

// SSE endpoint handler — establishes a persistent SSE connection.
// Phase 1: basic SSE with keepalive. Full bidirectional streaming in Phase 2.
pub async fn handle(
    State(state): State<AppState>,
) -> SseResponse<impl Stream<Item = Result<Event, Infallible>>> {
    let session_id = Uuid::new_v4().to_string();
    let message_url = format!(
        "http://{}/mcp/sse/{}",
        state.config.gateway.listen_addr,
        session_id
    );

    tracing::info!(session_id = %session_id, "SSE connection established");

    // Create a channel for sending messages to this SSE client
    let (_tx, rx) = broadcast::channel::<String>(64);

    // Store the sender for later use by the JSON-RPC handler
    // Phase 1: sender stored for future use in session registry

    let stream = BroadcastStream::new(rx).filter_map(|result| {
        match result {
            Ok(msg) => Some(Ok(Event::default().data(msg).event("message"))),
            Err(_) => None,
        }
    });

    // Send the endpoint event first, then the message stream
    let endpoint_event = Event::default()
        .data(message_url)
        .event("endpoint");

    let initial = tokio_stream::once(Ok::<Event, Infallible>(endpoint_event));
    let combined = initial.chain(stream);

    Sse::new(combined)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keepalive"),
        )
}

/// Handle POST /mcp/sse/:session_id — receive JSON-RPC messages for an SSE session.
pub async fn handle_message(
    State(_state): State<AppState>,
    axum::extract::Path(session_id): axum::extract::Path<String>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    tracing::debug!(
        session_id = %session_id,
        "received SSE message"
    );

    // Phase 1: basic acknowledgment
    // Full SSE message handling will be implemented with the session registry in Phase 2
    match serde_json::from_value::<mcp_gateway_core::types::JsonRpcMessage>(body) {
        Ok(msg) => {
            tracing::debug!(
                method = ?msg.method(),
                "SSE message received"
            );
            axum::Json(serde_json::json!({"status": "accepted", "session": session_id}))
        }
        Err(e) => {
            axum::Json(serde_json::json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32700,
                    "message": format!("Parse error: {}", e)
                },
                "id": null
            }))
        }
    }
}