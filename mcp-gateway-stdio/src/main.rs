//! Newline-delimited stdio bridge using the same HTTP transport as the Rust SDK.
use clap::Parser;
use mcp_gateway_core::types::{JsonRpcErrorResponse, JsonRpcMessage, RequestId};
use mcp_gateway_sdk::transport::HttpTransport;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
const MAX_INFLIGHT: usize = 64;
#[derive(Parser)]
#[command(
    name = "mcp-gateway-stdio",
    about = "stdio-to-HTTP bridge for MCP Gateway"
)]
struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:8080/mcp")]
    gateway: String,
    /// Prefer MCP_GATEWAY_API_KEY to avoid process-list exposure.
    #[arg(short, long)]
    api_key: Option<String>,
    #[arg(short, long)]
    verbose: bool,
}
#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let key = cli
        .api_key
        .or_else(|| std::env::var("MCP_GATEWAY_API_KEY").ok());
    let transport = HttpTransport::new(cli.gateway, key);
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    let permits = Arc::new(Semaphore::new(MAX_INFLIGHT));
    let mut tasks = JoinSet::new();
    'input: loop {
        while let Some(result) = tasks.try_join_next() {
            if !matches!(result, Ok(Ok(()))) {
                return;
            }
        }
        // Bound a line without buffering unbounded stdin data.
        let mut line = Vec::new();
        loop {
            let available = match stdin.fill_buf().await {
                Ok(bytes) => bytes,
                Err(_) => return,
            };
            if available.is_empty() {
                if line.is_empty() {
                    break 'input;
                }
                break;
            }
            let count = available
                .iter()
                .position(|b| *b == b'\n')
                .map_or(available.len(), |p| p + 1);
            if line.len() + count > 2 * 1024 * 1024 {
                eprintln!("stdio message exceeds 2 MiB");
                return;
            }
            let newline = available[count - 1] == b'\n';
            line.extend_from_slice(&available[..count]);
            stdin.consume(count);
            if newline {
                break;
            }
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let message = match serde_json::from_slice::<JsonRpcMessage>(&line) {
            Ok(message) => message,
            Err(_) => {
                if write_response(
                    &stdout,
                    Some(JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                        RequestId::Null,
                        -32700,
                        "invalid JSON-RPC input",
                    ))),
                )
                .await
                .is_err()
                {
                    return;
                }
                continue;
            }
        };
        // Initialization and notifications remain ordered; ordinary requests can finish out of order.
        let ordered = matches!(&message, JsonRpcMessage::Notification(_))
            || matches!(&message, JsonRpcMessage::Request(request) if request.method == "initialize");
        if ordered {
            if matches!(&message, JsonRpcMessage::Request(_)) {
                while let Some(result) = tasks.join_next().await {
                    if !matches!(result, Ok(Ok(()))) {
                        return;
                    }
                }
            }
            let response = exchange(&transport, message, cli.verbose).await;
            if write_response(&stdout, response).await.is_err() {
                return;
            }
        } else {
            let permit = match permits.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    let response = JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                        message.id().cloned().unwrap_or(RequestId::Null),
                        -32000,
                        "stdio bridge request capacity exhausted",
                    ));
                    if write_response(&stdout, Some(response)).await.is_err() {
                        return;
                    }
                    continue;
                }
            };
            let transport = transport.clone();
            let stdout = stdout.clone();
            let verbose = cli.verbose;
            tasks.spawn(async move {
                let _permit = permit;
                let response = exchange(&transport, message, verbose).await;
                write_response(&stdout, response).await
            });
        }
    }
    // EOF ends input, but already accepted requests still produce their responses.
    while let Some(result) = tasks.join_next().await {
        if !matches!(result, Ok(Ok(()))) {
            return;
        }
    }
}
async fn exchange(
    transport: &HttpTransport,
    message: JsonRpcMessage,
    verbose: bool,
) -> Option<JsonRpcMessage> {
    let id = message.id().cloned();
    let notification = matches!(message, JsonRpcMessage::Notification(_));
    match transport.exchange(message).await {
        Ok(response) => response,
        Err(error) => {
            if verbose {
                eprintln!("gateway call failed: {error}");
            }
            if notification {
                None
            } else {
                Some(JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                    id.unwrap_or(RequestId::Null),
                    error.to_error_code(),
                    error.to_string(),
                )))
            }
        }
    }
}
async fn write_response(
    stdout: &Arc<Mutex<tokio::io::Stdout>>,
    response: Option<JsonRpcMessage>,
) -> std::io::Result<()> {
    if let Some(response) = response {
        let mut output = serde_json::to_vec(&response)?;
        output.push(b'\n');
        let mut stdout = stdout.lock().await;
        stdout.write_all(&output).await?;
        stdout.flush().await?;
    }
    Ok(())
}
