use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json, Router,
};
use mcp_gateway_sdk::GatewayClient;
use mcp_gateway_server::{
    config::{BackendConfig, GatewayConfig},
    proxy::client::BackendClient,
    server::{build_router, AppState},
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
fn stdio_config() -> BackendConfig {
    BackendConfig {
        name: "stdio".into(),
        transport: "stdio".into(),
        command: Some("python3".into()),
        args: vec![
            "-u".into(),
            format!("{}/tests/fixtures/backend.py", env!("CARGO_MANIFEST_DIR")),
        ],
        timeout_ms: 2000,
        ..Default::default()
    }
}
#[tokio::test]
async fn stdio_initializes_once_and_reuses_process() {
    let client = BackendClient::new(Arc::new(stdio_config()));
    let a = client
        .request(
            "tools/call",
            Some(serde_json::json!({"name":"echo","arguments":{"value":"one"}})),
        )
        .await
        .unwrap();
    let b = client
        .request(
            "tools/call",
            Some(serde_json::json!({"name":"echo","arguments":{"value":"two"}})),
        )
        .await
        .unwrap();
    assert_eq!(a["structuredContent"]["pid"], b["structuredContent"]["pid"]);
    assert_eq!(b["content"][0]["text"], "two");
    client.shutdown().await;
}
#[tokio::test]
async fn stdio_timeout_is_bounded_and_next_call_reinitializes() {
    let mut config = stdio_config();
    config.timeout_ms = 300;
    let client = BackendClient::new(Arc::new(config));
    let start = std::time::Instant::now();
    assert!(client
        .request(
            "tools/call",
            Some(serde_json::json!({"name":"echo","arguments":{"value":"slow"}}))
        )
        .await
        .is_err());
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(client
        .request(
            "tools/call",
            Some(serde_json::json!({"name":"echo","arguments":{"value":"ok"}}))
        )
        .await
        .is_ok());
    client.shutdown().await;
}
async fn mock(
    State(count): State<Arc<AtomicUsize>>,
    headers: HeaderMap,
    Json(value): Json<serde_json::Value>,
) -> axum::response::Response {
    assert_eq!(headers["x-backend-key"], "backend-secret");
    let method = value["method"].as_str().unwrap();
    let result = match method {
        "initialize" => {
            count.fetch_add(1, Ordering::SeqCst);
            return ([("Mcp-Session-Id","backend-session")],Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"mock","version":"1"}}}))).into_response();
        }
        "notifications/initialized" => {
            assert!(value.get("id").is_none());
            return StatusCode::ACCEPTED.into_response();
        }
        "tools/list" => {
            serde_json::json!({"tools":[{"name":"echo","description":"Real echo description","inputSchema":{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]},"annotations":{"readOnlyHint":true}}]})
        }
        "tools/call" => {
            count.fetch_add(10, Ordering::SeqCst);
            serde_json::json!({"content":[],"structuredContent":{"value":value["params"]["arguments"]["value"]}})
        }
        _ => panic!("unexpected method"),
    };
    assert_eq!(headers["Mcp-Session-Id"], "backend-session");
    Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":result})).into_response()
}
#[tokio::test]
async fn gateway_discovers_real_schema_validates_arguments_and_caches_opted_in_tools() {
    let count = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/mcp", axum::routing::post(mock))
        .with_state(count.clone());
    let backend = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut config = GatewayConfig::default();
    config.cache.enabled = true;
    config.backends = vec![BackendConfig {
        name: "mock".into(),
        endpoint: Some(endpoint),
        cache_tools: vec!["echo".into()],
        headers: std::collections::HashMap::from([(
            "x-backend-key".into(),
            "backend-secret".into(),
        )]),
        ..Default::default()
    }];
    let state = AppState::new(config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let app = build_router(state.clone());
    let gateway = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = GatewayClient::new(url, None);
    client.initialize().await.unwrap();
    let tools = client.list_tools().await.unwrap();
    assert_eq!(
        tools[0].description.as_deref(),
        Some("Real echo description")
    );
    assert_eq!(tools[0].input_schema["required"][0], "value");
    assert!(client
        .call_tool("echo", Some(serde_json::json!({})))
        .await
        .is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    for _ in 0..2 {
        let result = client
            .call_tool("echo", Some(serde_json::json!({"value":"hello"})))
            .await
            .unwrap();
        assert_eq!(result.structured_content.unwrap()["value"], "hello");
    }
    assert_eq!(count.load(Ordering::SeqCst), 11);
    state.backends.shutdown().await;
    gateway.abort();
    backend.abort();
}
