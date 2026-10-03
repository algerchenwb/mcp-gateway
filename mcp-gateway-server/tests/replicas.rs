use axum::{extract::State, http::StatusCode, response::IntoResponse, Json, Router};
use mcp_gateway_server::{
    config::{BackendConfig, GatewayConfig},
    server::AppState,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone)]
struct Mock {
    node: &'static str,
    count: Arc<AtomicUsize>,
    incompatible: bool,
    fail: bool,
}
async fn backend(
    State(mock): State<Mock>,
    Json(value): Json<serde_json::Value>,
) -> axum::response::Response {
    let result = match value["method"].as_str().unwrap() {
        "initialize" => {
            serde_json::json!({"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"replica","version":"1"}})
        }
        "notifications/initialized" | "notifications/cancelled" => {
            return StatusCode::ACCEPTED.into_response()
        }
        "tools/list" => {
            serde_json::json!({"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"value":{"type":if mock.incompatible {"integer"} else {"string"}}}}}]})
        }
        "tools/call" => {
            mock.count.fetch_add(1, Ordering::SeqCst);
            if mock.fail {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            serde_json::json!({"content":[],"structuredContent":{"node":mock.node}})
        }
        _ => panic!(),
    };
    Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":result})).into_response()
}
async fn fixture(
    incompatible: bool,
    group: bool,
    fail: bool,
) -> (
    AppState,
    Vec<tokio::task::JoinHandle<()>>,
    Vec<Arc<AtomicUsize>>,
) {
    let mut config = GatewayConfig::default();
    let mut tasks = Vec::new();
    let mut counts = Vec::new();
    for (node, weight) in [("a", 1), ("b", 3)] {
        let count = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let mock = Mock {
            node,
            count: count.clone(),
            incompatible: incompatible && node == "b",
            fail: fail && node == "a",
        };
        tasks.push(tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/mcp", axum::routing::post(backend))
                    .with_state(mock),
            )
            .await
            .unwrap()
        }));
        counts.push(count);
        config.backends.push(BackendConfig {
            name: node.into(),
            endpoint: Some(endpoint),
            tools: vec!["echo".into()],
            replica_group: group.then(|| "echo-pool".into()),
            weight,
            ..Default::default()
        });
    }
    if group {
        assert!(config.validate().is_ok());
    }
    (AppState::new(config), tasks, counts)
}
#[tokio::test]
async fn matching_replicas_list_once_and_route_in_weighted_proportions() {
    let (state, tasks, counts) = fixture(false, true, false).await;
    assert_eq!(state.backends.tools().await.unwrap().len(), 1);
    for id in 0..8 {
        let result = mcp_gateway_server::handlers::json_rpc::process_message(state.clone(), serde_json::from_value(serde_json::json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"echo","arguments":{"value":"ok"}}})).unwrap()).await.unwrap().0;
        assert!(result.get("result").is_some(), "{result}");
    }
    assert_eq!(counts[0].load(Ordering::SeqCst), 2);
    assert_eq!(counts[1].load(Ordering::SeqCst), 6);
    state.backends.shutdown().await;
    for task in tasks {
        task.abort();
    }
}
#[tokio::test]
async fn ambiguous_or_incompatible_tools_are_rejected() {
    for (incompatible, group) in [(false, false), (true, true)] {
        let (state, tasks, _) = fixture(incompatible, group, false).await;
        assert!(state.backends.tools().await.is_err());
        state.backends.shutdown().await;
        for task in tasks {
            task.abort();
        }
    }
}
#[tokio::test]
async fn failed_call_is_not_replayed_on_another_replica() {
    let (state, tasks, counts) = fixture(false, true, true).await;
    let result = mcp_gateway_server::handlers::json_rpc::process_message(state.clone(), serde_json::from_value(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{}}})).unwrap()).await.unwrap().0;
    assert!(result.get("error").is_some());
    assert_eq!(counts[0].load(Ordering::SeqCst), 1);
    assert_eq!(counts[1].load(Ordering::SeqCst), 0);
    state.backends.shutdown().await;
    for task in tasks {
        task.abort();
    }
}
#[test]
fn invalid_replica_configuration_is_rejected() {
    let config = GatewayConfig {
        backends: vec![BackendConfig {
            name: "a".into(),
            endpoint: Some("http://localhost/mcp".into()),
            weight: 0,
            replica_group: Some(" ".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(config.validate().is_err());
}
