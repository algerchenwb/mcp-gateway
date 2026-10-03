use axum::{
    body::{Body, BodyDataStream},
    http::{Request, StatusCode},
    response::Response,
    Router,
};
use futures_util::StreamExt;
use mcp_gateway_server::{
    config::{BackendConfig, GatewayConfig},
    server::{build_router, AppState},
};
use std::time::Duration;
use tower::ServiceExt;
fn fixture(ttl: u64, capacity: usize) -> (AppState, Router) {
    let mut config = GatewayConfig::default();
    config.auth.enabled = true;
    config.auth.api_keys = vec!["owner".into(), "other".into()];
    config.gateway.sse_ttl_seconds = ttl;
    config.gateway.sse_queue_capacity = capacity;
    config.gateway.max_sse_sessions = 1;
    config.cache.enabled = false;
    config.backends = vec![BackendConfig {
        name: "stdio".into(),
        transport: "stdio".into(),
        command: Some("python3".into()),
        args: vec![
            "-u".into(),
            format!("{}/tests/fixtures/backend.py", env!("CARGO_MANIFEST_DIR")),
        ],
        ..Default::default()
    }];
    let state = AppState::new(config);
    let app = build_router(state.clone());
    (state, app)
}
async fn connect(app: &Router, key: &str, cursor: Option<&str>) -> Response {
    let mut request = Request::builder().uri("/mcp/sse").header("x-api-key", key);
    if let Some(cursor) = cursor {
        request = request.header("last-event-id", cursor);
    }
    app.clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}
async fn event(stream: &mut BodyDataStream) -> (String, String, String) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let bytes = stream.next().await.unwrap().unwrap();
            let text = std::str::from_utf8(&bytes).unwrap();
            let id = text
                .lines()
                .find_map(|line| line.strip_prefix("id: "))
                .unwrap_or_default()
                .to_owned();
            let mut decoder = mcp_gateway_core::sse::SseDecoder::default();
            if let Some((kind, data)) = decoder.push(&bytes, 100000).unwrap().pop() {
                return (id, kind, data);
            }
        }
    })
    .await
    .unwrap()
}
async fn post(app: &Router, endpoint: &str, value: serde_json::Value) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(endpoint)
                .header("x-api-key", "owner")
                .header("content-type", "application/json")
                .body(Body::from(value.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}
fn call(id: i64, value: &str) -> serde_json::Value {
    serde_json::json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"echo","arguments":{"value":value}}})
}
#[tokio::test]
async fn reconnect_replays_offline_responses_without_executing_again_and_checks_identity() {
    let (state, app) = fixture(30, 4);
    let response = connect(&app, "owner", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let (zero, kind, endpoint) = event(&mut stream).await;
    assert_eq!(kind, "endpoint");
    assert!(zero.ends_with(":0"));
    assert_eq!(
        post(&app, &endpoint, call(1, "one")).await,
        StatusCode::ACCEPTED
    );
    let (one, _, data) = event(&mut stream).await;
    let first: serde_json::Value = serde_json::from_str(&data).unwrap();
    assert_eq!(first["id"], 1);
    drop(stream);
    assert_eq!(
        post(&app, &endpoint, call(2, "two")).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        connect(&app, "other", Some(&one)).await.status(),
        StatusCode::NOT_FOUND
    );
    let response = connect(&app, "owner", Some(&one)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut resumed = response.into_body().into_data_stream();
    assert_eq!(event(&mut resumed).await.2, endpoint);
    let (two, _, data) = event(&mut resumed).await;
    assert!(two.ends_with(":2"));
    let second: serde_json::Value = serde_json::from_str(&data).unwrap();
    assert_eq!(second["id"], 2);
    assert_eq!(second["result"]["content"][0]["text"], "two");
    assert_eq!(
        first["result"]["structuredContent"]["pid"],
        second["result"]["structuredContent"]["pid"]
    );
    // A new attachment supersedes an old stream. Dropping the old stream must not detach the new one.
    let replacement = connect(&app, "owner", Some(&zero)).await;
    assert_eq!(replacement.status(), StatusCode::OK);
    drop(resumed);
    let mut replay = replacement.into_body().into_data_stream();
    event(&mut replay).await;
    assert_eq!(event(&mut replay).await.0, one);
    assert_eq!(event(&mut replay).await.0, two);
    assert_eq!(
        post(&app, &endpoint, call(3, "three")).await,
        StatusCode::ACCEPTED
    );
    assert!(event(&mut replay).await.0.ends_with(":3"));
    assert_eq!(state.metrics.snapshot().total_tool_calls, 3);
    state.sse.close_all();
    state.backends.shutdown().await;
}
#[tokio::test]
async fn replay_gaps_bad_cursors_and_expired_sessions_are_explicit() {
    let (state, app) = fixture(1, 2);
    let mut stream = connect(&app, "owner", None)
        .await
        .into_body()
        .into_data_stream();
    let (zero, _, endpoint) = event(&mut stream).await;
    let mut last = String::new();
    for id in 1..=3 {
        assert_eq!(
            post(
                &app,
                &endpoint,
                serde_json::json!({"jsonrpc":"2.0","id":id,"method":"ping"})
            )
            .await,
            StatusCode::ACCEPTED
        );
        last = event(&mut stream).await.0;
    }
    drop(stream);
    assert_eq!(
        connect(&app, "owner", Some(&zero)).await.status(),
        StatusCode::GONE
    );
    let future = format!("{}:99", zero.rsplit_once(':').unwrap().0);
    assert_eq!(
        connect(&app, "owner", Some(&future)).await.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        connect(&app, "owner", Some("broken")).await.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        connect(&app, "owner", None).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        connect(&app, "owner", Some(&last)).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(&app, &endpoint, call(4, "expired")).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(connect(&app, "owner", None).await.status(), StatusCode::OK);
    state.sse.close_all();
    state.backends.shutdown().await;
}

#[tokio::test]
async fn expiry_releases_session_and_request_capacity_even_if_old_stream_is_not_read() {
    let (state, app) = fixture(1, 2);
    let mut stream = connect(&app, "owner", None)
        .await
        .into_body()
        .into_data_stream();
    let (_, _, endpoint) = event(&mut stream).await;
    assert_eq!(
        post(&app, &endpoint, call(1, "slow")).await,
        StatusCode::ACCEPTED
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while state.metrics.snapshot().total_tool_calls == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        state.inflight.available_permits(),
        state.config.gateway.max_inflight_requests
    );
    assert_eq!(connect(&app, "owner", None).await.status(), StatusCode::OK);
    drop(stream);
    state.sse.close_all();
    state.backends.shutdown().await;
}
