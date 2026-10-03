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
    assert_eq!(lines[1]["id"], 7);
    assert_eq!(lines[2]["id"], 8);
    assert!(lines[2].get("error").is_some());
    task.abort();
}
