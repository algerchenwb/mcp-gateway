use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

use mcp_gateway_server::config::GatewayConfig;
use mcp_gateway_server::server;

#[derive(Parser)]
#[command(name = "mcp-gateway")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "MCP Gateway - High-performance Model Context Protocol Gateway", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the gateway server.
    Run {
        /// Path to the configuration file.
        #[arg(short, long, default_value = "config/gateway.toml")]
        config: PathBuf,
    },
    /// Validate the configuration file.
    ValidateConfig {
        /// Path to the configuration file.
        #[arg(short, long, default_value = "config/gateway.toml")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { config } => {
            run_gateway(config).await;
        }
        Commands::ValidateConfig { config } => {
            validate_config(config);
        }
    }
}

fn setup_logging(config: &GatewayConfig) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.logging.level));

    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true);

    match config.logging.format.as_str() {
        "json" => {
            builder.json().init();
        }
        _ => {
            builder.pretty().init();
        }
    }
}

fn validate_config(config_path: PathBuf) {
    match GatewayConfig::from_file(&config_path) {
        Ok(config) => match config.validate() {
            Ok(()) => {
                println!("✅ Configuration is valid.");
                println!("   Gateway: {}", config.gateway.name);
                println!("   Listen: {}", config.gateway.listen_addr);
                println!("   Backends: {}", config.backends.len());
                for b in &config.backends {
                    println!(
                        "     - {} ({}) → tools: {:?}",
                        b.name,
                        b.transport_type().as_str(),
                        b.tools
                    );
                }
                println!(
                    "   Auth: {}",
                    if config.auth.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    }
                );
                println!(
                    "   Cache: {} (max={}, ttl={}s)",
                    if config.cache.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    config.cache.max_capacity,
                    config.cache.ttl_seconds
                );
            }
            Err(errors) => {
                eprintln!("❌ Configuration errors:");
                for e in errors {
                    eprintln!("   - {e}");
                }
                std::process::exit(1);
            }
        },
        Err(e) => {
            eprintln!("❌ Failed to load configuration: {e}");
            std::process::exit(1);
        }
    }
}

async fn run_gateway(config_path: PathBuf) {
    let config = match GatewayConfig::from_file(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "Failed to load configuration from {}: {e}",
                config_path.display()
            );
            std::process::exit(1);
        }
    };

    if let Err(errors) = config.validate() {
        eprintln!("Configuration validation failed:");
        for e in errors {
            eprintln!("  - {e}");
        }
        std::process::exit(1);
    }

    setup_logging(&config);

    tracing::info!(
        name = %config.gateway.name,
        listen_addr = %config.gateway.listen_addr,
        backends = config.backends.len(),
        "Starting MCP Gateway"
    );

    if let Err(error) = server::run(config).await {
        tracing::error!(error=%error,"gateway stopped with an error");
        std::process::exit(1);
    }
}
