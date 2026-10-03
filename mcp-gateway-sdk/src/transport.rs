//! Streamable HTTP transport with bounded JSON/SSE responses and session headers.
use async_trait::async_trait;
use mcp_gateway_core::{
    error::{McpError, McpResult},
    sse::SseDecoder,
    types::{JsonRpcMessage, JsonRpcRequest, RequestId},
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;

pub const PROTOCOL_VERSION: &str = "2025-06-18";
pub const SUPPORTED_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, method: &str, params: Option<Value>) -> McpResult<Value>;
}
#[derive(Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
    base_url: String,
    session: Arc<RwLock<Option<String>>>,
    version: Arc<RwLock<Option<String>>>,
}
impl HttpTransport {
    pub fn new(base_url: impl Into<String>, api_key: Option<String>) -> Self {
        let headers = api_key
            .map(|key| HashMap::from([("x-api-key".into(), key)]))
            .unwrap_or_default();
        Self::with_options(base_url, Duration::from_secs(30), headers)
            .expect("invalid HTTP client configuration")
    }
    pub fn with_options(
        base_url: impl Into<String>,
        timeout: Duration,
        headers: HashMap<String, String>,
    ) -> McpResult<Self> {
        let mut default_headers = reqwest::header::HeaderMap::new();
        for (name, value) in headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| McpError::Config("invalid backend header name".into()))?;
            let mut value = reqwest::header::HeaderValue::from_str(&value)
                .map_err(|_| McpError::Config("invalid backend header value".into()))?;
            value.set_sensitive(true);
            default_headers.insert(name, value);
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(10)))
            .redirect(reqwest::redirect::Policy::none())
            .default_headers(default_headers)
            .build()
            .map_err(|e| McpError::Config(e.to_string()))?;
        Ok(Self {
            client,
            base_url: base_url.into(),
            session: Arc::default(),
            version: Arc::default(),
        })
    }
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
    pub async fn reset(&self) {
        *self.session.write().await = None;
        *self.version.write().await = None;
    }
    pub async fn exchange(&self, message: JsonRpcMessage) -> McpResult<Option<JsonRpcMessage>> {
        let id = message.id().cloned();
        let initialize = message.method() == Some("initialize");
        let notification = matches!(message, JsonRpcMessage::Notification(_));
        let mut request = self
            .client
            .post(&self.base_url)
            .header("Accept", "application/json, text/event-stream")
            .json(&message);
        if !initialize {
            if let Some(session) = self.session.read().await.as_ref() {
                request = request.header("Mcp-Session-Id", session);
            }
            if let Some(version) = self.version.read().await.as_ref() {
                request = request.header("MCP-Protocol-Version", version);
            }
        }
        let mut response = request.send().await.map_err(|e| {
            if e.is_timeout() {
                McpError::BackendTimeout("HTTP request deadline exceeded".into())
            } else {
                McpError::BackendConnection("HTTP request failed".into())
            }
        })?;
        if response.status() == reqwest::StatusCode::NOT_FOUND
            && self.session.read().await.is_some()
        {
            self.reset().await;
            return Err(McpError::ServerNotInitialized);
        }
        if !response.status().is_success() {
            return Err(McpError::BackendConnection(format!(
                "backend returned HTTP {}",
                response.status()
            )));
        }
        if notification {
            if response.status() != reqwest::StatusCode::ACCEPTED {
                return Err(McpError::Transport(
                    "notification must receive HTTP 202".into(),
                ));
            }
            return Ok(None);
        }
        let session = response
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_owned();
        let response_message = if content_type == "text/event-stream" {
            let mut decoder = SseDecoder::default();
            'events: loop {
                let chunk = response
                    .chunk()
                    .await
                    .map_err(|_| McpError::Transport("failed to read SSE response".into()))?
                    .ok_or_else(|| {
                        McpError::Transport("SSE ended before matching response".into())
                    })?;
                for (_, data) in decoder.push(&chunk, MAX_RESPONSE_BYTES)? {
                    let parsed: JsonRpcMessage = serde_json::from_str(&data)?;
                    if matches!(
                        parsed,
                        JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_)
                    ) {
                        if parsed.id() != id.as_ref() {
                            return Err(McpError::Transport(
                                "response ID does not match request".into(),
                            ));
                        }
                        break 'events parsed;
                    }
                }
            }
        } else if content_type == "application/json" {
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| McpError::Transport("failed to read JSON response".into()))?
            {
                if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
                    return Err(McpError::Transport("response exceeds size limit".into()));
                }
                body.extend_from_slice(&chunk);
            }
            serde_json::from_slice(&body)?
        } else {
            return Err(McpError::Transport(
                "unsupported response Content-Type".into(),
            ));
        };
        if !matches!(
            response_message,
            JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_)
        ) || response_message.id() != id.as_ref()
        {
            return Err(McpError::Transport(
                "response ID/type does not match request".into(),
            ));
        }
        if initialize {
            if let JsonRpcMessage::Response(result) = &response_message {
                let version = result
                    .result
                    .as_ref()
                    .and_then(|v| v.get("protocolVersion"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        McpError::Transport("missing negotiated protocol version".into())
                    })?;
                if !SUPPORTED_VERSIONS.contains(&version) {
                    return Err(McpError::Transport(
                        "unsupported negotiated protocol version".into(),
                    ));
                }
                *self.version.write().await = Some(version.to_owned());
                *self.session.write().await = session;
            }
        }
        Ok(Some(response_message))
    }
}
#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, method: &str, params: Option<Value>) -> McpResult<Value> {
        let request = JsonRpcRequest::new(
            method,
            params,
            RequestId::String(uuid::Uuid::new_v4().to_string()),
        );
        match self.exchange(JsonRpcMessage::Request(request)).await? {
            Some(JsonRpcMessage::Response(response)) => response
                .result
                .ok_or_else(|| McpError::Transport("missing result".into())),
            Some(JsonRpcMessage::Error(error)) => Err(McpError::Rpc(error.error)),
            _ => Err(McpError::Transport("missing response".into())),
        }
    }
}
