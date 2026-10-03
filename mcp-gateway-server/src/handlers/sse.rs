//! Legacy HTTP+SSE with bounded queues, authenticated ownership and disconnect cleanup.
use crate::server::AppState;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
};
use dashmap::DashMap;
use futures_util::{Stream, StreamExt};
use mcp_gateway_core::types::JsonRpcMessage;
use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
struct Session {
    sender: mpsc::Sender<String>,
    scope: String,
    created: Instant,
    _permit: OwnedSemaphorePermit,
}
pub struct SseSessions {
    sessions: DashMap<String, Session>,
    slots: Arc<Semaphore>,
    ttl: Duration,
    queue_capacity: usize,
}
impl Default for SseSessions {
    fn default() -> Self {
        Self {
            sessions: DashMap::new(),
            slots: Arc::new(Semaphore::new(1024)),
            ttl: Duration::from_secs(1800),
            queue_capacity: 32,
        }
    }
}
impl SseSessions {
    pub fn close_all(&self) {
        self.slots.close();
        self.sessions.clear();
    }
}
struct SessionStream {
    inner: ReceiverStream<String>,
    sessions: Arc<SseSessions>,
    id: String,
}
impl Stream for SessionStream {
    type Item = Result<Event, Infallible>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|item| item.map(|data| Ok(Event::default().event("message").data(data))))
    }
}
impl Drop for SessionStream {
    fn drop(&mut self) {
        self.sessions.sessions.remove(&self.id);
    }
}
pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let permit = state
        .sse
        .slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    let id = uuid::Uuid::new_v4().to_string();
    let scope = super::json_rpc::credential_scope(&state, &headers);
    let (sender, receiver) = mpsc::channel(state.sse.queue_capacity);
    state.sse.sessions.insert(
        id.clone(),
        Session {
            sender,
            scope,
            created: Instant::now(),
            _permit: permit,
        },
    );
    let initial = tokio_stream::once(Ok(Event::default()
        .event("endpoint")
        .data(format!("/mcp/sse/{id}"))));
    let stream = SessionStream {
        inner: ReceiverStream::new(receiver),
        sessions: state.sse.clone(),
        id,
    };
    let combined = initial
        .chain(stream)
        .take_until(tokio::time::sleep(state.sse.ttl));
    Ok(Sse::new(combined).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}
pub async fn handle_message(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let scope = super::json_rpc::credential_scope(&state, &headers);
    let sender = {
        let Some(session) = state.sse.sessions.get(&id) else {
            return StatusCode::NOT_FOUND;
        };
        if session.scope != scope {
            return StatusCode::NOT_FOUND;
        }
        if session.created.elapsed() >= state.sse.ttl {
            drop(session);
            state.sse.sessions.remove(&id);
            return StatusCode::NOT_FOUND;
        }
        session.sender.clone()
    };
    if !headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(';').next() == Some("application/json"))
    {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE;
    }
    let Ok(message) = serde_json::from_slice::<JsonRpcMessage>(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    if matches!(
        message,
        JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_)
    ) {
        return StatusCode::BAD_REQUEST;
    }
    if matches!(message, JsonRpcMessage::Notification(_)) {
        let _ = super::json_rpc::process_message_scoped(state, message, &scope).await;
        return StatusCode::ACCEPTED;
    }
    // Reserve a bounded response slot before executing the tool, so saturation cannot trigger duplicate writes.
    let permit = match sender.try_reserve_owned() {
        Ok(permit) => permit,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS,
    };
    tokio::spawn(async move {
        let response = super::json_rpc::process_message_scoped(state, message, &scope).await;
        let value = match response {
            Ok(value) | Err(value) => value.0,
        };
        permit.send(value.to_string());
    });
    StatusCode::ACCEPTED
}
