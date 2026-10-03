use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::IntoResponse,
    Json, Router,
};
use mcp_gateway_server::{
    config::{BackendConfig, GatewayConfig},
    server::{build_router, AppState},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tower::ServiceExt;

async fn backend(
    State(events): State<mpsc::UnboundedSender<serde_json::Value>>,
    Json(value): Json<serde_json::Value>,
) -> axum::response::Response {
    let result = match value["method"].as_str().unwrap() {
        "initialize" => {
            serde_json::json!({"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"mock","version":"1"}})
        }
        "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
        "notifications/cancelled" => {
            events.send(value).unwrap();
            return StatusCode::ACCEPTED.into_response();
        }
        "tools/list" => {
            serde_json::json!({"tools":[{"name":"slow","inputSchema":{"type":"object"}}]})
        }
        "tools/call" => {
            events.send(value.clone()).unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
            serde_json::json!({"content":[]})
        }
        _ => panic!(),
    };
    Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":result})).into_response()
}
fn request(key: &str, value: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("x-api-key", key)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .body(Body::from(value.to_string()))
        .unwrap()
}
#[tokio::test]
async fn cancellation_is_identity_scoped_translates_ids_and_works_when_saturated() {
    let (events, mut receiver) = mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let upstream = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/mcp", axum::routing::post(backend))
                .with_state(events),
        )
        .await
        .unwrap()
    });
    let mut config = GatewayConfig::default();
    config.auth.enabled = true;
    config.auth.api_keys = vec!["owner".into(), "other".into()];
    config.backends = vec![BackendConfig {
        endpoint: Some(endpoint),
        timeout_ms: 10000,
        ..Default::default()
    }];
    let state = AppState::new(config);
    let app = build_router(state.clone());
    let call = serde_json::json!({"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"slow","arguments":{}}});
    let task = tokio::spawn(app.clone().oneshot(request("owner", call.clone())));
    let sent = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(sent["id"], 42);
    let duplicate = app.clone().oneshot(request("owner", call)).await.unwrap();
    let bytes = axum::body::to_bytes(duplicate.into_body(), 10000)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["error"]["code"],
        -32600
    );
    let cancellation = serde_json::json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":42}});
    assert_eq!(
        app.clone()
            .oneshot(request("other", cancellation.clone()))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), receiver.recv())
            .await
            .is_err()
    );
    let permits = state
        .inflight
        .clone()
        .acquire_many_owned((state.config.gateway.max_inflight_requests - 1) as u32)
        .await
        .unwrap();
    assert_eq!(
        app.oneshot(request("owner", cancellation))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    let response = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 10000)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["id"], 42);
    assert_eq!(value["error"]["code"], -32800);
    let notification = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(notification["params"]["requestId"], sent["id"]);
    assert!(notification.get("id").is_none());
    drop(permits);
    assert_eq!(
        state.inflight.available_permits(),
        state.config.gateway.max_inflight_requests
    );
    state.backends.shutdown().await;
    upstream.abort();
}
#[tokio::test]
async fn request_registration_cleans_up_and_is_identity_scoped() {
    use mcp_gateway_core::types::RequestId;
    use mcp_gateway_server::handlers::cancellation::Requests;
    let requests = Arc::new(Requests::default());
    let (guard, registration) = requests.register("a", RequestId::Number(1)).unwrap();
    let future = futures_util::future::Abortable::new(std::future::pending::<()>(), registration);
    requests.cancel("b", RequestId::Number(1));
    assert!(!future.is_aborted());
    requests.cancel("a", RequestId::Number(1));
    assert!(future.is_aborted());
    drop(guard);
    assert!(requests.register("a", RequestId::Number(1)).is_some());
}

#[tokio::test]
async fn deadline_sends_cancellation_to_the_original_backend_connection() {
    let (events, mut receiver) = mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let upstream = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/mcp", axum::routing::post(backend))
                .with_state(events),
        )
        .await
        .unwrap()
    });
    let client = mcp_gateway_server::proxy::client::BackendClient::new(Arc::new(BackendConfig {
        endpoint: Some(endpoint),
        timeout_ms: 500,
        ..Default::default()
    }));
    let result = client
        .request(
            "tools/call",
            Some(serde_json::json!({"name":"slow","arguments":{}})),
        )
        .await;
    assert!(matches!(
        result,
        Err(mcp_gateway_core::error::McpError::BackendTimeout(_))
    ));
    let sent = receiver.recv().await.unwrap();
    let notification = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(notification["params"]["requestId"], sent["id"]);
    client.shutdown().await;
    upstream.abort();
}
