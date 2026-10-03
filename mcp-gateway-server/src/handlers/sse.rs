//! Legacy HTTP+SSE with bounded replay, authenticated ownership and TTL cleanup.
use crate::{handlers::cancellation::Requests, server::AppState};
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
    collections::VecDeque,
    convert::Infallible,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
const PER_SESSION_REPLAY_BYTES: usize = 1024 * 1024;
const TOTAL_REPLAY_BYTES: usize = 64 * 1024 * 1024;
struct Record {
    sequence: u64,
    data: String,
}
impl Record {
    fn event(&self, id: &str) -> Event {
        Event::default()
            .event("message")
            .id(format!("{id}:{}", self.sequence))
            .data(&self.data)
    }
}
struct Queued {
    record: Arc<Record>,
    _permit: OwnedSemaphorePermit,
}
struct Retained {
    record: Arc<Record>,
    _bytes: OwnedSemaphorePermit,
}
struct SessionData {
    sender: Option<mpsc::Sender<Queued>>,
    replay: VecDeque<Retained>,
    replay_bytes: usize,
    floor: u64,
    next: u64,
    closed: bool,
    attachment: u64,
}
struct Session {
    data: Mutex<SessionData>,
    slots: Arc<Semaphore>,
    replay_budget: Arc<Semaphore>,
    scope: String,
    request_scope: String,
    requests: Arc<Requests>,
    shutdown: tokio::sync::watch::Sender<bool>,
    created: Instant,
    queue_capacity: usize,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
}
impl Session {
    fn close(&self) {
        let mut data = self.data.lock().expect("session lock poisoned");
        data.closed = true;
        self.shutdown.send_replace(true);
        data.sender.take();
        data.replay.clear();
        self.slots.close();
        self.permit
            .lock()
            .expect("session permit lock poisoned")
            .take();
        self.requests.cancel_scope(&self.request_scope);
    }
    fn complete(&self, permit: OwnedSemaphorePermit, value: String) {
        let mut data = self.data.lock().expect("session lock poisoned");
        if data.closed {
            return;
        }
        let record = Arc::new(Record {
            sequence: data.next,
            data: value,
        });
        data.next += 1;
        let size = record.data.len();
        while !data.replay.is_empty()
            && (data.replay.len() >= self.queue_capacity
                || data.replay_bytes + size > PER_SESSION_REPLAY_BYTES)
        {
            let removed = data.replay.pop_front().expect("nonempty replay");
            data.floor = removed.record.sequence;
            data.replay_bytes -= removed.record.data.len();
        }
        let bytes = (size <= PER_SESSION_REPLAY_BYTES)
            .then(|| {
                self.replay_budget
                    .clone()
                    .try_acquire_many_owned(size as u32)
                    .ok()
            })
            .flatten();
        if let Some(bytes) = bytes {
            data.replay_bytes += size;
            data.replay.push_back(Retained {
                record: record.clone(),
                _bytes: bytes,
            });
        } else {
            // A missing retained event creates a gap: reject old cursors instead of silently losing a response.
            data.replay.clear();
            data.replay_bytes = 0;
            data.floor = record.sequence;
        }
        if let Some(sender) = &data.sender {
            let _ = sender.try_send(Queued {
                record,
                _permit: permit,
            });
        }
    }
}
pub struct SseSessions {
    sessions: DashMap<String, Arc<Session>>,
    slots: Arc<Semaphore>,
    replay_budget: Arc<Semaphore>,
    ttl: Duration,
    queue_capacity: usize,
}
impl SseSessions {
    pub fn new(max_sessions: usize, ttl_seconds: u64, queue_capacity: usize) -> Self {
        Self {
            sessions: DashMap::new(),
            slots: Arc::new(Semaphore::new(max_sessions)),
            replay_budget: Arc::new(Semaphore::new(TOTAL_REPLAY_BYTES)),
            ttl: Duration::from_secs(ttl_seconds),
            queue_capacity,
        }
    }
    pub fn close_all(&self) {
        self.slots.close();
        for session in self.sessions.iter() {
            session.close();
        }
        self.sessions.clear();
    }
}
struct SessionStream {
    inner: ReceiverStream<Queued>,
    session: Arc<Session>,
    id: String,
    attachment: u64,
}
impl Stream for SessionStream {
    type Item = Result<Event, Infallible>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = Pin::new(&mut self.inner).poll_next(cx);
        item.map(|item| item.map(|queued| Ok(queued.record.event(&self.id))))
    }
}
impl Drop for SessionStream {
    fn drop(&mut self) {
        let mut data = self.session.data.lock().expect("session lock poisoned");
        if data.attachment == self.attachment {
            data.sender.take();
        }
    }
}
pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    principal: Option<axum::Extension<crate::auth::oauth::Principal>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let principal = principal.map(|p| p.0);
    let scope = super::json_rpc::authorized_scope(&state, &headers, principal.as_ref());
    let (id, cursor, session) = if let Some(cursor) = headers.get("last-event-id") {
        let cursor = cursor.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
        let (id, sequence) = cursor.rsplit_once(':').ok_or(StatusCode::BAD_REQUEST)?;
        let sequence = sequence
            .parse::<u64>()
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let session = state
            .sse
            .sessions
            .get(id)
            .filter(|session| session.scope == scope)
            .map(|session| session.clone())
            .ok_or(StatusCode::NOT_FOUND)?;
        (id.to_owned(), sequence, session)
    } else {
        let permit = state
            .sse
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
        let id = uuid::Uuid::new_v4().to_string();
        let session = Arc::new(Session {
            data: Mutex::new(SessionData {
                sender: None,
                replay: VecDeque::new(),
                replay_bytes: 0,
                floor: 0,
                next: 1,
                closed: false,
                attachment: 0,
            }),
            slots: Arc::new(Semaphore::new(state.sse.queue_capacity)),
            replay_budget: state.sse.replay_budget.clone(),
            scope: scope.clone(),
            request_scope: format!("{scope}:sse:{id}"),
            requests: state.requests.clone(),
            shutdown: tokio::sync::watch::channel(false).0,
            created: Instant::now(),
            queue_capacity: state.sse.queue_capacity,
            permit: Mutex::new(Some(permit)),
        });
        state.sse.sessions.insert(id.clone(), session.clone());
        let sessions = Arc::downgrade(&state.sse);
        let expired_id = id.clone();
        let ttl = state.sse.ttl;
        tokio::spawn(async move {
            tokio::time::sleep(ttl).await;
            if let Some(sessions) = sessions.upgrade() {
                if let Some((_, session)) = sessions.sessions.remove(&expired_id) {
                    session.close();
                }
            }
        });
        (id, 0, session)
    };
    let remaining = state
        .sse
        .ttl
        .checked_sub(session.created.elapsed())
        .ok_or(StatusCode::NOT_FOUND)?;
    let (sender, receiver) = mpsc::channel(state.sse.queue_capacity);
    let (replay, attachment) = {
        let mut data = session.data.lock().expect("session lock poisoned");
        if data.closed {
            return Err(StatusCode::NOT_FOUND);
        }
        if cursor < data.floor {
            return Err(StatusCode::GONE);
        }
        if cursor >= data.next {
            return Err(StatusCode::BAD_REQUEST);
        }
        let replay: Vec<_> = data
            .replay
            .iter()
            .filter(|event| event.record.sequence > cursor)
            .map(|event| Ok(event.record.event(&id)))
            .collect();
        data.attachment += 1;
        data.sender = Some(sender);
        (replay, data.attachment)
    };
    let initial = tokio_stream::once(Ok(Event::default()
        .event("endpoint")
        .id(format!("{id}:{cursor}"))
        .data(format!("/mcp/sse/{id}"))));
    let stream = SessionStream {
        inner: ReceiverStream::new(receiver),
        session,
        id,
        attachment,
    };
    let combined = initial
        .chain(tokio_stream::iter(replay))
        .chain(stream)
        .take_until(tokio::time::sleep(remaining));
    Ok(Sse::new(combined).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}
pub async fn handle_message(
    State(state): State<AppState>,
    Path(id): Path<String>,
    axum::Extension(admission): axum::Extension<crate::middleware::auth::AdmissionPermit>,
    headers: HeaderMap,
    principal: Option<axum::Extension<crate::auth::oauth::Principal>>,
    body: Bytes,
) -> StatusCode {
    let principal = principal.map(|p| p.0);
    let scope = super::json_rpc::authorized_scope(&state, &headers, principal.as_ref());
    let session = {
        let Some(session) = state.sse.sessions.get(&id) else {
            return StatusCode::NOT_FOUND;
        };
        if session.scope != scope || session.created.elapsed() >= state.sse.ttl {
            return StatusCode::NOT_FOUND;
        }
        session.clone()
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
        let _ = super::json_rpc::process_message_authorized(
            state,
            message,
            &session.request_scope,
            principal,
        )
        .await;
        return StatusCode::ACCEPTED;
    }
    // Bound pending work plus queued responses before executing any tool.
    let permit = match session.slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS,
    };
    tokio::spawn(async move {
        let _inflight = admission.0;
        let mut shutdown = session.shutdown.subscribe();
        if *shutdown.borrow() {
            return;
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => {},
            response = super::json_rpc::process_message_authorized(state, message, &session.request_scope, principal) => {
                let value = match response { Ok(value) | Err(value) => value.0 };
                session.complete(permit, value.to_string());
            }
        }
    });
    StatusCode::ACCEPTED
}

#[cfg(test)]
mod replay_limits {
    use super::*;
    fn session(budget: usize) -> Session {
        let slots = Arc::new(Semaphore::new(1));
        Session {
            data: Mutex::new(SessionData {
                sender: None,
                replay: VecDeque::new(),
                replay_bytes: 0,
                floor: 0,
                next: 1,
                closed: false,
                attachment: 0,
            }),
            slots: Arc::new(Semaphore::new(4)),
            replay_budget: Arc::new(Semaphore::new(budget)),
            scope: "owner".into(),
            request_scope: "owner:sse:test".into(),
            requests: Arc::default(),
            shutdown: tokio::sync::watch::channel(false).0,
            created: Instant::now(),
            queue_capacity: 4,
            permit: Mutex::new(Some(slots.try_acquire_owned().unwrap())),
        }
    }
    #[test]
    fn exhausted_global_budget_marks_a_replay_gap_and_releases_retained_bytes() {
        let session = session(16);
        for _ in 0..3 {
            session.complete(
                session.slots.clone().try_acquire_owned().unwrap(),
                "12345678".into(),
            );
        }
        let data = session.data.lock().unwrap();
        assert_eq!(data.floor, 3);
        assert!(data.replay.is_empty());
        assert_eq!(session.replay_budget.available_permits(), 16);
        drop(data);
        session.complete(
            session.slots.clone().try_acquire_owned().unwrap(),
            "next".into(),
        );
        assert_eq!(session.replay_budget.available_permits(), 12);
        session.close();
        assert_eq!(session.replay_budget.available_permits(), 16);
    }
    #[test]
    fn oversized_responses_are_not_retained() {
        let session = session(TOTAL_REPLAY_BYTES);
        session.complete(
            session.slots.clone().try_acquire_owned().unwrap(),
            "x".repeat(PER_SESSION_REPLAY_BYTES + 1),
        );
        let data = session.data.lock().unwrap();
        assert!(data.replay.is_empty());
        assert_eq!(data.floor, 1);
        assert_eq!(
            session.replay_budget.available_permits(),
            TOTAL_REPLAY_BYTES
        );
    }
}
