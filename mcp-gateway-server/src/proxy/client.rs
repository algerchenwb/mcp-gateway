//! Persistent MCP backend connections with initialization, bounded I/O and concurrency.
use crate::config::BackendConfig;
use dashmap::DashMap;
use mcp_gateway_core::{
    error::{McpError, McpResult},
    sse::SseDecoder,
    transport::TransportType,
    types::{
        InitializeResult, JsonRpcErrorResponse, JsonRpcMessage, JsonRpcNotification,
        JsonRpcRequest, RequestId,
    },
};
use mcp_gateway_sdk::transport::{HttpTransport, PROTOCOL_VERSION, SUPPORTED_VERSIONS};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{oneshot, Mutex, RwLock, Semaphore},
    task::JoinHandle,
};
type Pending = Arc<DashMap<RequestId, oneshot::Sender<JsonRpcMessage>>>;
const MAX_MESSAGE: usize = 8 * 1024 * 1024;
struct PendingGuard {
    pending: Pending,
    id: RequestId,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.remove(&self.id);
    }
}

pub struct BackendClient {
    config: Arc<BackendConfig>,
    connection: RwLock<Option<Arc<Connection>>>,
    initialize: Mutex<()>,
    permits: Semaphore,
}
impl BackendClient {
    pub fn new(config: Arc<BackendConfig>) -> Self {
        Self {
            permits: Semaphore::new(config.max_connections),
            config,
            connection: RwLock::new(None),
            initialize: Mutex::new(()),
        }
    }
    pub fn name(&self) -> &str {
        &self.config.name
    }
    pub async fn request(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> McpResult<serde_json::Value> {
        let request = JsonRpcMessage::Request(JsonRpcRequest::new(
            method,
            params,
            RequestId::String(uuid::Uuid::new_v4().to_string()),
        ));
        match self.send(request).await? {
            JsonRpcMessage::Response(response) => response
                .result
                .ok_or_else(|| McpError::Transport("missing backend result".into())),
            JsonRpcMessage::Error(error) => Err(McpError::Rpc(error.error)),
            _ => Err(McpError::Transport("unexpected backend response".into())),
        }
    }
    pub async fn send(&self, message: JsonRpcMessage) -> McpResult<JsonRpcMessage> {
        let result = tokio::time::timeout(Duration::from_millis(self.config.timeout_ms), async {
            let _permit = self
                .permits
                .acquire()
                .await
                .map_err(|_| McpError::Transport("backend is shutting down".into()))?;
            let connection = self.ensure_connection().await?;
            let mut cancellation = BackendCancellation {
                connection: connection.clone(),
                id: match &message {
                    JsonRpcMessage::Request(request) if request.method == "tools/call" => {
                        Some(request.id.clone())
                    }
                    _ => None,
                },
            };
            let response = connection.exchange(message).await;
            cancellation.id = None;
            response?.ok_or_else(|| McpError::Transport("expected backend response".into()))
        })
        .await;
        match result {
            Ok(Ok(message)) => Ok(message),
            Ok(Err(error)) => {
                // Never automatically replay a tool call after a transport failure.
                if !matches!(error, McpError::Rpc(_)) {
                    self.invalidate().await;
                }
                Err(error)
            }
            Err(_) => {
                self.invalidate().await;
                Err(McpError::BackendTimeout(self.config.name.clone()))
            }
        }
    }
    async fn invalidate(&self) {
        self.connection.write().await.take();
    }
    async fn ensure_connection(&self) -> McpResult<Arc<Connection>> {
        if let Some(connection) = self
            .connection
            .read()
            .await
            .as_ref()
            .filter(|connection| connection.alive())
        {
            return Ok(connection.clone());
        }
        let _guard = self.initialize.lock().await;
        if let Some(connection) = self
            .connection
            .read()
            .await
            .as_ref()
            .filter(|connection| connection.alive())
        {
            return Ok(connection.clone());
        }
        let connection = Arc::new(Connection::connect(&self.config).await?);
        let initialize = JsonRpcMessage::Request(JsonRpcRequest::new(
            "initialize",
            Some(
                serde_json::json!({"protocolVersion":PROTOCOL_VERSION,"capabilities":{},"clientInfo":{"name":"mcp-gateway","version":env!("CARGO_PKG_VERSION")}}),
            ),
            RequestId::String(uuid::Uuid::new_v4().to_string()),
        ));
        let response = connection.exchange(initialize).await?;
        let result: InitializeResult = match response {
            Some(JsonRpcMessage::Response(response)) => serde_json::from_value(
                response
                    .result
                    .ok_or_else(|| McpError::Transport("missing initialize result".into()))?,
            )?,
            Some(JsonRpcMessage::Error(error)) => return Err(McpError::Rpc(error.error)),
            _ => return Err(McpError::Transport("invalid initialize response".into())),
        };
        if !SUPPORTED_VERSIONS.contains(&result.protocol_version.as_str()) {
            return Err(McpError::Transport(
                "unsupported backend protocol version".into(),
            ));
        }
        connection
            .exchange(JsonRpcMessage::Notification(JsonRpcNotification {
                jsonrpc: "2.0".into(),
                method: "notifications/initialized".into(),
                params: None,
            }))
            .await?;
        *self.connection.write().await = Some(connection.clone());
        Ok(connection)
    }
    pub async fn shutdown(&self) {
        self.permits.close();
        self.invalidate().await;
    }
}

// Keep the exact connection alive until a best-effort cancellation is sent.
// Dropping a client future (including deadline expiry) must not replay the call.
struct BackendCancellation {
    connection: Arc<Connection>,
    id: Option<RequestId>,
}
impl Drop for BackendCancellation {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let connection = self.connection.clone();
            tokio::spawn(async move {
                let notification = JsonRpcMessage::Notification(JsonRpcNotification {
                    jsonrpc: "2.0".into(),
                    method: "notifications/cancelled".into(),
                    params: Some(
                        serde_json::json!({"requestId":id,"reason":"gateway request cancelled or deadline expired"}),
                    ),
                });
                let _ =
                    tokio::time::timeout(Duration::from_secs(1), connection.exchange(notification))
                        .await;
            });
        }
    }
}

enum Connection {
    Http(HttpTransport),
    Stdio(StdioConnection),
    Sse(SseConnection),
}
impl Connection {
    async fn connect(config: &BackendConfig) -> McpResult<Self> {
        match config.transport_type() {
            TransportType::StreamableHttp => Ok(Self::Http(HttpTransport::with_options(
                config
                    .endpoint
                    .clone()
                    .ok_or_else(|| McpError::Config("missing endpoint".into()))?,
                Duration::from_millis(config.timeout_ms),
                config.headers.clone(),
            )?)),
            TransportType::Stdio => Ok(Self::Stdio(StdioConnection::connect(config).await?)),
            TransportType::Sse => Ok(Self::Sse(SseConnection::connect(config).await?)),
            TransportType::WebSocket => {
                Err(McpError::Config("WebSocket is not implemented".into()))
            }
        }
    }
    fn alive(&self) -> bool {
        match self {
            Self::Http(_) => true,
            Self::Stdio(c) => c.alive.load(Ordering::Acquire),
            Self::Sse(c) => c.alive.load(Ordering::Acquire),
        }
    }
    async fn exchange(&self, message: JsonRpcMessage) -> McpResult<Option<JsonRpcMessage>> {
        match self {
            Self::Http(c) => c.exchange(message).await,
            Self::Stdio(c) => c.exchange(message).await,
            Self::Sse(c) => c.exchange(message).await,
        }
    }
}
async fn bounded_line<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> McpResult<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let bytes = reader.fill_buf().await?;
        if bytes.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(line))
            };
        }
        let count = bytes
            .iter()
            .position(|b| *b == b'\n')
            .map_or(bytes.len(), |p| p + 1);
        if line.len() + count > MAX_MESSAGE {
            return Err(McpError::Transport("stdio message exceeds limit".into()));
        }
        let newline = bytes[count - 1] == b'\n';
        line.extend_from_slice(&bytes[..count]);
        reader.consume(count);
        if newline {
            return Ok(Some(line));
        }
    }
}
struct StdioConnection {
    stdin: Arc<Mutex<ChildStdin>>,
    pending: Pending,
    alive: Arc<AtomicBool>,
    child: Mutex<Child>,
    reader: JoinHandle<()>,
    stderr: JoinHandle<()>,
}
impl StdioConnection {
    async fn connect(config: &BackendConfig) -> McpResult<Self> {
        let mut child = Command::new(
            config
                .command
                .as_ref()
                .ok_or_else(|| McpError::Config("missing stdio command".into()))?,
        )
        .args(&config.args)
        .envs(&config.env)
        .kill_on_drop(true)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
        let stdin = Arc::new(Mutex::new(
            child
                .stdin
                .take()
                .ok_or_else(|| McpError::Transport("missing stdin".into()))?,
        ));
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| McpError::Transport("missing stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| McpError::Transport("missing stderr".into()))?;
        let pending: Pending = Arc::new(DashMap::new());
        let alive = Arc::new(AtomicBool::new(true));
        let p = pending.clone();
        let a = alive.clone();
        let input = stdin.clone();
        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            while let Ok(Some(line)) = bounded_line(&mut reader).await {
                let Ok(message) = serde_json::from_slice::<JsonRpcMessage>(&line) else {
                    break;
                };
                match message {
                    JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_) => {
                        if let Some(id) = message.id() {
                            if let Some((_, sender)) = p.remove(id) {
                                let _ = sender.send(message);
                            }
                        }
                    }
                    JsonRpcMessage::Request(request) => {
                        let response = JsonRpcErrorResponse::new(
                            request.id,
                            -32601,
                            "gateway does not advertise client request capabilities",
                        );
                        if let Ok(mut bytes) = serde_json::to_vec(&response) {
                            bytes.push(b'\n');
                            let _ = input.lock().await.write_all(&bytes).await;
                        }
                    }
                    JsonRpcMessage::Notification(_) => {}
                }
            }
            a.store(false, Ordering::Release);
            p.clear();
        });
        // Drain stderr without retaining or logging backend secrets.
        let stderr = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            while let Ok(bytes) = reader.fill_buf().await {
                if bytes.is_empty() {
                    break;
                }
                let len = bytes.len();
                reader.consume(len);
            }
        });
        Ok(Self {
            stdin,
            pending,
            alive,
            child: Mutex::new(child),
            reader,
            stderr,
        })
    }
    async fn exchange(&self, message: JsonRpcMessage) -> McpResult<Option<JsonRpcMessage>> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(McpError::Transport("stdio backend exited".into()));
        }
        let response = if let Some(id) = message.id() {
            let (sender, receiver) = oneshot::channel();
            self.pending.insert(id.clone(), sender);
            Some((
                PendingGuard {
                    pending: self.pending.clone(),
                    id: id.clone(),
                },
                receiver,
            ))
        } else {
            None
        };
        let mut bytes = serde_json::to_vec(&message)?;
        bytes.push(b'\n');
        self.stdin.lock().await.write_all(&bytes).await?;
        match response {
            Some((_guard, receiver)) => receiver
                .await
                .map(Some)
                .map_err(|_| McpError::Transport("stdio backend disconnected".into())),
            None => Ok(None),
        }
    }
}
impl Drop for StdioConnection {
    fn drop(&mut self) {
        self.reader.abort();
        self.stderr.abort();
        if let Ok(mut child) = self.child.try_lock() {
            let _ = child.start_kill();
        }
    }
}
struct SseConnection {
    client: reqwest::Client,
    endpoint: String,
    pending: Pending,
    alive: Arc<AtomicBool>,
    reader: JoinHandle<()>,
}
impl SseConnection {
    async fn connect(config: &BackendConfig) -> McpResult<Self> {
        let url = reqwest::Url::parse(
            config
                .endpoint
                .as_deref()
                .ok_or_else(|| McpError::Config("missing SSE endpoint".into()))?,
        )
        .map_err(|_| McpError::Config("invalid SSE URL".into()))?;
        let mut headers = reqwest::header::HeaderMap::new();
        for (key, value) in &config.headers {
            let key = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| McpError::Config("invalid header".into()))?;
            let mut value = reqwest::header::HeaderValue::from_str(value)
                .map_err(|_| McpError::Config("invalid header".into()))?;
            value.set_sensitive(true);
            headers.insert(key, value);
        }
        // A persistent event stream has no total lifetime timeout; each operation has a gateway deadline.
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_millis(config.timeout_ms))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| McpError::Config(e.to_string()))?;
        let mut response = client
            .get(url.clone())
            .header("accept", "text/event-stream")
            .send()
            .await
            .map_err(|_| McpError::BackendConnection("SSE connect failed".into()))?;
        if !response.status().is_success()
            || !response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("text/event-stream"))
        {
            return Err(McpError::Transport("invalid SSE handshake".into()));
        }
        let mut decoder = SseDecoder::default();
        let (sender, receiver) = oneshot::channel();
        let pending: Pending = Arc::new(DashMap::new());
        let p = pending.clone();
        let alive = Arc::new(AtomicBool::new(true));
        let a = alive.clone();
        let reader = tokio::spawn(async move {
            let mut endpoint_sender = Some(sender);
            while let Ok(Some(chunk)) = response.chunk().await {
                let Ok(events) = decoder.push(&chunk, MAX_MESSAGE) else {
                    break;
                };
                for (event, data) in events {
                    if event == "endpoint" {
                        if let Some(sender) = endpoint_sender.take() {
                            let _ = sender.send(data);
                        }
                    } else if event == "message" {
                        if let Ok(message) = serde_json::from_str::<JsonRpcMessage>(&data) {
                            if matches!(
                                message,
                                JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_)
                            ) {
                                if let Some(id) = message.id() {
                                    if let Some((_, sender)) = p.remove(id) {
                                        let _ = sender.send(message);
                                    }
                                }
                            }
                        } else {
                            a.store(false, Ordering::Release);
                            p.clear();
                            return;
                        }
                    }
                }
            }
            a.store(false, Ordering::Release);
            p.clear();
        });
        // Abort the stream if endpoint negotiation is cancelled or fails.
        struct AbortGuard(Option<JoinHandle<()>>);
        impl Drop for AbortGuard {
            fn drop(&mut self) {
                if let Some(task) = &self.0 {
                    task.abort();
                }
            }
        }
        let mut guard = AbortGuard(Some(reader));
        let endpoint = receiver
            .await
            .map_err(|_| McpError::Transport("SSE disconnected before endpoint".into()))?;
        let endpoint = url
            .join(&endpoint)
            .map_err(|_| McpError::Transport("invalid SSE message endpoint".into()))?;
        if endpoint.origin() != url.origin()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(McpError::Transport(
                "SSE message endpoint must have the same origin".into(),
            ));
        }
        Ok(Self {
            client,
            endpoint: endpoint.to_string(),
            pending,
            alive,
            reader: guard.0.take().expect("reader exists"),
        })
    }
    async fn exchange(&self, message: JsonRpcMessage) -> McpResult<Option<JsonRpcMessage>> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(McpError::Transport("SSE backend disconnected".into()));
        }
        let response = if let Some(id) = message.id() {
            let (sender, receiver) = oneshot::channel();
            self.pending.insert(id.clone(), sender);
            Some((
                PendingGuard {
                    pending: self.pending.clone(),
                    id: id.clone(),
                },
                receiver,
            ))
        } else {
            None
        };
        let status = self
            .client
            .post(&self.endpoint)
            .json(&message)
            .send()
            .await
            .map_err(|_| McpError::BackendConnection("SSE message POST failed".into()))?
            .status();
        if !status.is_success() {
            return Err(McpError::BackendConnection(format!(
                "SSE message POST returned {status}"
            )));
        }
        match response {
            Some((_guard, receiver)) => receiver
                .await
                .map(Some)
                .map_err(|_| McpError::Transport("SSE backend disconnected".into())),
            None => Ok(None),
        }
    }
}
impl Drop for SseConnection {
    fn drop(&mut self) {
        self.reader.abort();
    }
}
