use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json, Router,
};
use mcp_gateway_sdk::{
    transport::{HttpTransport, Transport},
    GatewayClient,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
async fn backend(
    State(count): State<Arc<AtomicUsize>>,
    headers: HeaderMap,
    Json(value): Json<serde_json::Value>,
) -> axum::response::Response {
    assert!(headers["accept"]
        .to_str()
        .unwrap()
        .contains("text/event-stream"));
    let method = value["method"].as_str().unwrap();
    if method == "initialize" {
        return ([("Mcp-Session-Id", "test-session")], Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"mock","version":"1"}}}))).into_response();
    }
    assert_eq!(headers["Mcp-Session-Id"], "test-session");
    assert_eq!(headers["MCP-Protocol-Version"], "2025-06-18");
    if method == "notifications/initialized" {
        assert!(value.get("id").is_none());
        count.fetch_add(1, Ordering::SeqCst);
        return StatusCode::ACCEPTED.into_response();
    }
    let payload = serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":{}});
    (
        [("content-type", "text/event-stream")],
        format!("event: message\ndata: {payload}\n\n"),
    )
        .into_response()
}
#[tokio::test]
async fn initializes_notifies_and_reads_sse_with_session_headers() {
    let count = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/mcp", axum::routing::post(backend))
        .with_state(count.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = GatewayClient::new(url, None);
    client.initialize().await.unwrap();
    client.ping().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    task.abort();
}
#[tokio::test]
async fn rejects_response_id_mismatch_and_retains_rpc_error_data() {
    async fn bad(Json(value): Json<serde_json::Value>) -> Json<serde_json::Value> {
        if value["method"] == "error" {
            Json(
                serde_json::json!({"jsonrpc":"2.0","id":value["id"],"error":{"code":-32602,"message":"bad","data":{"field":"city"}}}),
            )
        } else {
            Json(serde_json::json!({"jsonrpc":"2.0","id":"wrong","result":{}}))
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/mcp", axum::routing::post(bad)),
        )
        .await
        .unwrap();
    });
    let transport = HttpTransport::new(url, None);
    assert!(transport.send("ping", None).await.is_err());
    let mcp_gateway_core::error::McpError::Rpc(error) =
        transport.send("error", None).await.unwrap_err()
    else {
        panic!("expected RPC error")
    };
    assert_eq!(error.data.unwrap()["field"], "city");
    task.abort();
}
