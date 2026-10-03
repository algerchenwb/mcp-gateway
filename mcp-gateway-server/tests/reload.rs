use mcp_gateway_server::{
    config::{BackendConfig, GatewayConfig},
    server::AppState,
};
use std::time::Duration;
fn config() -> GatewayConfig {
    GatewayConfig {
        backends: vec![BackendConfig {
            name: "stdio".into(),
            transport: "stdio".into(),
            command: Some("python3".into()),
            args: vec![
                "-u".into(),
                format!("{}/tests/fixtures/backend.py", env!("CARGO_MANIFEST_DIR")),
            ],
            timeout_ms: 15000,
            ..Default::default()
        }],
        ..Default::default()
    }
}
async fn call(
    entry: &mcp_gateway_server::proxy::registry::ToolEntry,
    value: &str,
) -> serde_json::Value {
    entry
        .client
        .request(
            "tools/call",
            Some(serde_json::json!({"name":"echo","arguments":{"value":value}})),
        )
        .await
        .unwrap()
}
#[tokio::test]
async fn reload_publishes_new_catalog_and_retains_inflight_connection() {
    let original = config();
    let state = AppState::new(original.clone());
    let old = state.backends.resolve("echo").await.unwrap().unwrap();
    let before = call(&old, "before").await;
    let in_flight = old.clone();
    let task = tokio::spawn(async move { call(&in_flight, "slow").await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut candidate = original;
    candidate.backends[0]
        .env
        .insert("RELOAD_REVISION".into(), "new".into());
    state.reload_config(candidate).await.unwrap();
    let new = state.backends.resolve("echo").await.unwrap().unwrap();
    let after = call(&new, "after").await;
    assert_ne!(
        before["structuredContent"]["pid"],
        after["structuredContent"]["pid"]
    );
    let completed = tokio::time::timeout(Duration::from_secs(12), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        completed["structuredContent"]["pid"],
        before["structuredContent"]["pid"]
    );
    assert_eq!(completed["content"][0]["text"], "slow");
    assert_eq!(
        call(&old, "still usable").await["structuredContent"]["pid"],
        before["structuredContent"]["pid"]
    );
    drop(old);
    state.backends.shutdown().await;
}
#[tokio::test]
async fn failed_reload_or_security_changes_preserve_current_backend() {
    let original = config();
    let state = AppState::new(original.clone());
    let old = state.backends.resolve("echo").await.unwrap().unwrap();
    let before = call(&old, "before").await;
    let mut invalid = original.clone();
    invalid.backends[0].command = Some("/does/not/exist".into());
    assert!(state.reload_config(invalid).await.is_err());
    let mut security = original;
    security.auth.enabled = true;
    security.auth.api_keys = vec!["new-key".into()];
    assert!(state.reload_config(security).await.is_err());
    let current = state.backends.resolve("echo").await.unwrap().unwrap();
    assert_eq!(
        call(&current, "after").await["structuredContent"]["pid"],
        before["structuredContent"]["pid"]
    );
    state.backends.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn running_process_reloads_on_sighup_and_survives_invalid_file() {
    use std::process::{Command, Stdio};
    struct ChildGuard(std::process::Child, std::path::PathBuf);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
            let _ = std::fs::remove_file(&self.1);
        }
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let mut settings = config();
    settings.gateway.listen_addr = addr.to_string();
    let path = std::env::temp_dir().join(format!("mcp-reload-{}.toml", uuid::Uuid::new_v4()));
    std::fs::write(&path, toml::to_string(&settings).unwrap()).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_mcp-gateway-server"))
        .args(["run", "--config"])
        .arg(&path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child, path.clone());
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if http
                .get(format!("http://{addr}/health"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    async fn pid(http: &reqwest::Client, addr: std::net::SocketAddr) -> serde_json::Value {
        http.post(format!("http://{addr}/mcp")).header("accept","application/json, text/event-stream")
            .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"value":"process"}}}))
            .send().await.unwrap().json::<serde_json::Value>().await.unwrap()["result"]["structuredContent"]["pid"].clone()
    }
    let before = pid(&http, addr).await;
    assert!(before.is_number());
    settings.backends[0]
        .env
        .insert("RELOAD_REVISION".into(), "signal".into());
    std::fs::write(&path, toml::to_string(&settings).unwrap()).unwrap();
    assert!(Command::new("kill")
        .args(["-HUP", &child.0.id().to_string()])
        .status()
        .unwrap()
        .success());
    let after = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = pid(&http, addr).await;
            if current != before {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(after.is_number());
    std::fs::write(&path, "invalid = [").unwrap();
    assert!(Command::new("kill")
        .args(["-HUP", &child.0.id().to_string()])
        .status()
        .unwrap()
        .success());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(pid(&http, addr).await, after);
    assert!(Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .unwrap()
        .success());
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(status.success());
}
