//! Track cancellable requests by authenticated identity and client request ID.
use dashmap::{mapref::entry::Entry, DashMap};
use futures_util::future::{AbortHandle, AbortRegistration};
use mcp_gateway_core::types::RequestId;
use std::sync::Arc;

type Key = (String, RequestId);
#[derive(Default)]
pub struct Requests(DashMap<Key, AbortHandle>);
pub struct RequestGuard {
    requests: Arc<Requests>,
    key: Key,
}
impl Requests {
    pub fn register(
        self: &Arc<Self>,
        scope: &str,
        id: RequestId,
    ) -> Option<(RequestGuard, AbortRegistration)> {
        let key = (scope.to_owned(), id);
        let (handle, registration) = AbortHandle::new_pair();
        match self.0.entry(key.clone()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(entry) => {
                entry.insert(handle);
                Some((
                    RequestGuard {
                        requests: self.clone(),
                        key,
                    },
                    registration,
                ))
            }
        }
    }
    pub fn cancel_scope(&self, scope: &str) {
        for request in self.0.iter() {
            if request.key().0 == scope {
                request.value().abort();
            }
        }
    }
    pub fn cancel(&self, scope: &str, id: RequestId) {
        if let Some(handle) = self.0.get(&(scope.to_owned(), id)) {
            handle.abort();
        }
    }
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.requests.0.remove(&self.key);
    }
}
