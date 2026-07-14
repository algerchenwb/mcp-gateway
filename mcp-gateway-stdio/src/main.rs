//! mcp-gateway-stdio — stdio-to-HTTP bridge.
//!
//! Reads JSON-RPC messages from stdin, forwards them to the MCP Gateway HTTP
//! endpoint, and writes responses to stdout.
//!
//! This enables stdio-based MCP clients to connect to the gateway.
//!
//! Usage:
//!   mcp-gateway-stdio --gateway http://localhost:8080/mcp
//!
//! Or as a bridge for a specific tool:
//!   mcp-gateway-stdio --gateway http://localhost:8080/mcp --tool get_weather

use clap::Parser;
use tokio::io::AsyncBufReadExt;

use mcp_gateway_core::types::JsonRpcMessage;

#[derive(Parser)]
#[command(name = "mcp-gateway-stdio")]
#[command(about = "stdio-to-HTTP bridge for MCP Gateway")]
struct Cli {
    /// Gateway HTTP endpoint URL.
    #[arg(short, long, default_value = "http://127.0.0.1:8080/mcp")]
    gateway: String,

    /// Optional API key for authentication.
    #[arg(short, long)]
    api_key: Option<String>,

    /// Verbose output.
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if cli.verbose {
        eprintln!("MCP Gateway stdio bridge");
        eprintln!("Gateway: {}", cli.gateway);
    }

    let http_client = reqwest::Client::new();
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        if cli.verbose {
            eprintln!("→ forwarding: {line}");
        }

        // Parse the incoming JSON-RPC message
        let message: JsonRpcMessage = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                let error = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {
                        "code": -32700,
                        "message": format!("Parse error: {}", e)
                    }
                });
                println!("{error}");
                continue;
            }
        };

        // Forward to the gateway
        let mut request = http_client
            .post(&cli.gateway)
            .header("Content-Type", "application/json");

        if let Some(ref key) = cli.api_key {
            request = request.header("x-api-key", key);
        }

        let response = match request.body(line).send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error forwarding to gateway: {e}");
                let error = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": message.id().map(|id| id.to_string()).unwrap_or_default(),
                    "error": {
                        "code": -32603,
                        "message": format!("Gateway connection error: {e}")
                    }
                });
                println!("{error}");
                continue;
            }
        };

        let response_text = match response.text().await {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Error reading gateway response: {e}");
                continue;
            }
        };

        // Write the response to stdout
        println!("{response_text}");

        if cli.verbose {
            eprintln!("← response: {response_text}");
        }
    }
}