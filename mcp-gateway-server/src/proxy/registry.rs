//! Discover real tool catalogs and reject ambiguous names instead of first-match routing.
use crate::{
    config::{BackendConfig, GatewayConfig},
    proxy::client::BackendClient,
};
use mcp_gateway_core::{
    error::{McpError, McpResult},
    tool::Tool,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
#[derive(Clone)]
pub struct ToolEntry {
    pub tool: Tool,
    pub backend: Arc<BackendConfig>,
    pub client: Arc<BackendClient>,
    pub validator: Arc<jsonschema::Validator>,
}
pub struct BackendRegistry {
    backends: Vec<(Arc<BackendConfig>, Arc<BackendClient>)>,
    catalog: Mutex<Option<(Instant, HashMap<String, ToolEntry>)>>,
}
struct NoNetwork;
impl jsonschema::Retrieve for NoNetwork {
    fn retrieve(
        &self,
        _uri: &jsonschema::Uri<String>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external JSON Schema references are disabled".into())
    }
}
impl BackendRegistry {
    pub fn new(config: &GatewayConfig) -> Self {
        Self {
            backends: config
                .backends
                .iter()
                .map(|config| {
                    let config = Arc::new(config.clone());
                    let client = Arc::new(BackendClient::new(config.clone()));
                    (config, client)
                })
                .collect(),
            catalog: Mutex::new(None),
        }
    }
    async fn refresh(&self) -> McpResult<()> {
        let mut catalog = self.catalog.lock().await;
        if let Some((created, entries)) = catalog.as_ref() {
            if created.elapsed() < Duration::from_secs(60) {
                let _ = entries;
                return Ok(());
            }
        }
        let lists = futures_util::future::join_all(self.backends.iter().map(
            |(config, client)| async move {
                let mut cursor = None;
                let mut tools = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for _ in 0..100 {
                    let params = cursor
                        .as_ref()
                        .map(|cursor| serde_json::json!({"cursor":cursor}));
                    let result = client.request("tools/list", params).await?;
                    let list: Vec<Tool> =
                        serde_json::from_value(result.get("tools").cloned().ok_or_else(|| {
                            McpError::Transport("backend tools/list omitted tools".into())
                        })?)?;
                    if tools.len() + list.len() > 10000 {
                        return Err(McpError::Transport(
                            "backend tool catalog exceeds limit".into(),
                        ));
                    }
                    tools.extend(list);
                    cursor = result
                        .get("nextCursor")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned);
                    if cursor.is_none() {
                        return Ok((config.clone(), client.clone(), tools));
                    }
                    if !seen.insert(cursor.clone()) {
                        return Err(McpError::Transport(
                            "backend repeated pagination cursor".into(),
                        ));
                    }
                }
                Err(McpError::Transport(
                    "backend pagination exceeded 100 pages".into(),
                ))
            },
        ))
        .await;
        let mut entries = HashMap::new();
        for list in lists {
            let (config, client, tools) = list?;
            let mut available = std::collections::HashSet::new();
            for tool in tools {
                available.insert(tool.name.clone());
                if !config.tools.is_empty() && !config.tools.contains(&tool.name) {
                    continue;
                }
                if entries.contains_key(&tool.name) {
                    return Err(McpError::Config(format!(
                        "duplicate discovered tool name '{}'",
                        tool.name
                    )));
                }
                let validator = jsonschema::options()
                    .with_retriever(NoNetwork)
                    .build(&tool.input_schema)
                    .map_err(|_| {
                        McpError::Config(format!("invalid inputSchema for '{}'", tool.name))
                    })?;
                entries.insert(
                    tool.name.clone(),
                    ToolEntry {
                        tool,
                        backend: config.clone(),
                        client: client.clone(),
                        validator: Arc::new(validator),
                    },
                );
            }
            for configured in &config.tools {
                if !available.contains(configured) {
                    return Err(McpError::Config(format!(
                        "backend '{}' does not advertise configured tool '{}'",
                        config.name, configured
                    )));
                }
            }
        }
        *catalog = Some((Instant::now(), entries));
        Ok(())
    }
    pub async fn tools(&self) -> McpResult<Vec<ToolEntry>> {
        self.refresh().await?;
        let catalog = self.catalog.lock().await;
        Ok(catalog
            .as_ref()
            .map(|(_, entries)| entries.values().cloned().collect())
            .unwrap_or_default())
    }
    pub async fn resolve(&self, name: &str) -> McpResult<Option<ToolEntry>> {
        self.refresh().await?;
        let catalog = self.catalog.lock().await;
        Ok(catalog
            .as_ref()
            .and_then(|(_, entries)| entries.get(name))
            .cloned())
    }
    pub async fn shutdown(&self) {
        for (_, client) in &self.backends {
            client.shutdown().await;
        }
    }
}
