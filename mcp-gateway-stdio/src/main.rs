//! Newline-delimited stdio bridge using the same HTTP transport as the Rust SDK.
use clap::Parser;
use mcp_gateway_core::types::{JsonRpcErrorResponse, JsonRpcMessage, RequestId};
use mcp_gateway_sdk::transport::HttpTransport;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
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
    let mut stdout = tokio::io::stdout();
    loop {
        // Bound a line without buffering unbounded stdin data.
        let mut line = Vec::new();
        loop {
            let available = match stdin.fill_buf().await {
                Ok(bytes) => bytes,
                Err(_) => return,
            };
            if available.is_empty() {
                if line.is_empty() {
                    return;
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
        let result = match serde_json::from_slice::<JsonRpcMessage>(&line) {
            Ok(message) => {
                let id = message.id().cloned();
                let notification = matches!(message, JsonRpcMessage::Notification(_));
                match transport.exchange(message).await {
                    Ok(response) => response,
                    Err(error) => {
                        if cli.verbose {
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
            Err(_) => Some(JsonRpcMessage::Error(JsonRpcErrorResponse::new(
                RequestId::Null,
                -32700,
                "invalid JSON-RPC input",
            ))),
        };
        if let Some(response) = result {
            let Ok(mut output) = serde_json::to_vec(&response) else {
                continue;
            };
            output.push(b'\n');
            if stdout.write_all(&output).await.is_err() || stdout.flush().await.is_err() {
                return;
            }
        }
    }
}
