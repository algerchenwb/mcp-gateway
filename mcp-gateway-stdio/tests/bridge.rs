use axum::{http::StatusCode, response::IntoResponse, Json, Router};
use tokio::io::AsyncWriteExt;
#[tokio::test]
async fn bridge_suppresses_notification_output_and_preserves_numeric_ids() {
    async fn backend(Json(value): Json<serde_json::Value>) -> axum::response::Response {
        match value["method"].as_str().unwrap() {
            "initialize"=>Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"mock","version":"1"}}})).into_response(),
            "notifications/initialized"=>{assert!(value.get("id").is_none());StatusCode::ACCEPTED.into_response()},
            "fail"=>StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            _=>Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":{}})).into_response(),
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let app = Router::new().route("/mcp", axum::routing::post(backend));
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_mcp-gateway-stdio"))
        .args(["--gateway", &url])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    for value in [
        serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}),
        serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        serde_json::json!({"jsonrpc":"2.0","id":7,"method":"ping"}),
        serde_json::json!({"jsonrpc":"2.0","id":8,"method":"fail"}),
    ] {
        input
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
    }
    drop(input);
    let output = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(output.status.success());
    let lines: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0]["id"], 1);
    assert!(lines
        .iter()
        .any(|line| line["id"] == 7 && line.get("result").is_some()));
    assert!(lines
        .iter()
        .any(|line| line["id"] == 8 && line.get("error").is_some()));
    task.abort();
}

#[derive(Clone)]
struct BlockingGateway {
    started: tokio::sync::mpsc::UnboundedSender<i64>,
    cancelled: std::sync::Arc<tokio::sync::Notify>,
}
async fn blocking_gateway(
    axum::extract::State(state): axum::extract::State<BlockingGateway>,
    Json(value): Json<serde_json::Value>,
) -> axum::response::Response {
    match value["method"].as_str().unwrap() {
        "slow" => {
            let cancelled = state.cancelled.notified();
            tokio::pin!(cancelled);
            cancelled.as_mut().enable();
            state.started.send(value["id"].as_i64().unwrap()).unwrap();
            cancelled.await;
            Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"error":{"code":-32800,"message":"cancelled"}})).into_response()
        }
        "notifications/cancelled" => {
            assert!(value.get("id").is_none());
            assert_eq!(value["params"]["requestId"], 1);
            state.cancelled.notify_waiters();
            StatusCode::ACCEPTED.into_response()
        }
        "ping" => {
            Json(serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":{}})).into_response()
        }
        _ => panic!("unexpected method"),
    }
}
async fn blocking_fixture() -> (
    tokio::process::Child,
    tokio::sync::mpsc::UnboundedReceiver<i64>,
    tokio::task::JoinHandle<()>,
) {
    let (started, receiver) = tokio::sync::mpsc::unbounded_channel();
    let state = BlockingGateway {
        started,
        cancelled: std::sync::Arc::new(tokio::sync::Notify::new()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/mcp", axum::routing::post(blocking_gateway))
                .with_state(state),
        )
        .await
        .unwrap()
    });
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_mcp-gateway-stdio"))
        .args(["--gateway", &url])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    (child, receiver, task)
}
#[tokio::test]
async fn bridge_forwards_ping_and_cancellation_while_a_request_is_waiting() {
    use tokio::io::AsyncBufReadExt;
    let (mut child, mut started, task) = blocking_fixture().await;
    let mut input = child.stdin.take().unwrap();
    let mut output = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
    input
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"slow\"}\n")
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), started.recv())
            .await
            .unwrap(),
        Some(1)
    );
    input
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n")
        .await
        .unwrap();
    let line = tokio::time::timeout(std::time::Duration::from_secs(2), output.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["id"],
        2
    );
    input.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":1}}\n").await.unwrap();
    drop(input);
    let line = tokio::time::timeout(std::time::Duration::from_secs(2), output.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(value["id"], 1);
    assert_eq!(value["error"]["code"], -32800);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(output.next_line().await.unwrap().is_none());
    task.abort();
}
#[tokio::test]
async fn bridge_rejects_excess_requests_without_blocking_control_messages() {
    use tokio::io::AsyncBufReadExt;
    let (mut child, mut started, task) = blocking_fixture().await;
    let mut input = child.stdin.take().unwrap();
    let mut output = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
    for id in 1..=64 {
        input
            .write_all(
                format!("{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":\"slow\"}}\n").as_bytes(),
            )
            .await
            .unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for _ in 1..=64 {
            started.recv().await.unwrap();
        }
    })
    .await
    .unwrap();
    input
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":65,\"method\":\"slow\"}\n")
        .await
        .unwrap();
    let line = tokio::time::timeout(std::time::Duration::from_secs(2), output.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(value["id"], 65);
    assert_eq!(value["error"]["code"], -32000);
    assert!(started.try_recv().is_err());
    input.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":1}}\n").await.unwrap();
    drop(input);
    let mut ids = std::collections::HashSet::new();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(line) = output.next_line().await.unwrap() {
            let value: serde_json::Value = serde_json::from_str(&line).unwrap();
            ids.insert(value["id"].as_i64().unwrap());
        }
    })
    .await
    .unwrap();
    assert_eq!(ids.len(), 64);
    assert!(child.wait().await.unwrap().success());
    task.abort();
}
