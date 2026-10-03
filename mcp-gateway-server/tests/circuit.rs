use axum::{extract::State, http::StatusCode, response::IntoResponse, Json, Router};
use mcp_gateway_core::error::McpError;
use mcp_gateway_server::{config::BackendConfig, proxy::client::BackendClient};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
#[derive(Clone)]
struct Mock {
    fail: Arc<AtomicBool>,
    hold: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
    started: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Notify>,
}
async fn backend(
    State(state): State<Mock>,
    Json(value): Json<serde_json::Value>,
) -> axum::response::Response {
    let result = match value["method"].as_str().unwrap() {
        "initialize" => {
            serde_json::json!({"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"circuit","version":"1"}})
        }
        "notifications/initialized" | "notifications/cancelled" => {
            return StatusCode::ACCEPTED.into_response()
        }
        "tools/call" => {
            state.calls.fetch_add(1, Ordering::SeqCst);
            if state.fail.load(Ordering::SeqCst) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            if state.hold.load(Ordering::SeqCst) {
                let notified = state.release.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                state.started.send(()).unwrap();
                notified.await;
            }
            if value["params"]["rpcError"] == true {
                return Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"error":{"code":-32602,"message":"invalid tool argument"}})).into_response();
            }
            serde_json::json!({"content":[]})
        }
        _ => panic!(),
    };
    Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":result})).into_response()
}
#[tokio::test]
async fn circuit_limits_recovery_to_one_probe_and_ignores_rpc_errors() {
    let (started, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let mock = Mock {
        fail: Arc::new(AtomicBool::new(true)),
        hold: Arc::new(AtomicBool::new(false)),
        calls: Arc::new(AtomicUsize::new(0)),
        started,
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let upstream_mock = mock.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/mcp", axum::routing::post(backend))
                .with_state(upstream_mock),
        )
        .await
        .unwrap()
    });
    let client = Arc::new(BackendClient::new(Arc::new(BackendConfig {
        name: "circuit".into(),
        endpoint: Some(endpoint),
        failure_threshold: 2,
        cooldown_ms: 200,
        timeout_ms: 1000,
        ..Default::default()
    })));
    for _ in 0..2 {
        assert!(client.request("tools/call", None).await.is_err());
    }
    assert!(matches!(
        client.request("tools/call", None).await,
        Err(McpError::CircuitBreakerOpen(_))
    ));
    assert_eq!(mock.calls.load(Ordering::SeqCst), 2);
    mock.fail.store(false, Ordering::SeqCst);
    mock.hold.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let probe_client = client.clone();
    let probe = tokio::spawn(async move { probe_client.request("tools/call", None).await });
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(!client.available());
    assert!(matches!(
        client.request("tools/call", None).await,
        Err(McpError::CircuitBreakerOpen(_))
    ));
    assert_eq!(mock.calls.load(Ordering::SeqCst), 3);
    mock.release.notify_waiters();
    assert!(probe.await.unwrap().is_ok());
    mock.hold.store(false, Ordering::SeqCst);
    for _ in 0..4 {
        assert!(matches!(
            client
                .request("tools/call", Some(serde_json::json!({"rpcError":true})))
                .await,
            Err(McpError::Rpc(_))
        ));
    }
    assert!(client.available());
    assert!(client.request("tools/call", None).await.is_ok());
    client.shutdown().await;
    server.abort();
}
