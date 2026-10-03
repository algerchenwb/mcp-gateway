//! Bounded, opt-in cache for successful read-only tool results.
use mcp_gateway_core::tool::ToolCallResult;
use std::time::Duration;

pub struct L1Cache {
    inner: moka::future::Cache<String, ToolCallResult>,
    max_entries: u64,
    max_result_bytes: usize,
    flights: std::sync::Mutex<
        std::collections::HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>,
    >,
}
impl L1Cache {
    pub fn new(max_capacity: u64, ttl: Duration) -> Self {
        Self::with_limits(max_capacity, 64 * 1024 * 1024, 1024 * 1024, ttl)
    }
    pub fn with_limits(
        max_entries: u64,
        max_bytes: u64,
        max_result_bytes: usize,
        ttl: Duration,
    ) -> Self {
        Self {
            inner: moka::future::Cache::builder()
                .max_capacity(max_bytes)
                .time_to_live(ttl)
                .weigher(|key: &String, value: &ToolCallResult| {
                    (key.len() + serde_json::to_vec(value).map_or(0, |v| v.len()))
                        .min(u32::MAX as usize) as u32
                })
                .build(),
            max_entries,
            max_result_bytes,
            flights: Default::default(),
        }
    }
    /// Keep only weak references so unique cache misses cannot grow a permanent lock table.
    pub async fn lock_key(&self, key: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let mutex = {
            let mut flights = self.flights.lock().expect("cache lock table poisoned");
            flights.retain(|_, value| value.strong_count() > 0);
            match flights.get(key).and_then(std::sync::Weak::upgrade) {
                Some(mutex) => mutex,
                None => {
                    let mutex = std::sync::Arc::new(tokio::sync::Mutex::new(()));
                    flights.insert(key.to_owned(), std::sync::Arc::downgrade(&mutex));
                    mutex
                }
            }
        };
        mutex.lock_owned().await
    }
    pub async fn get(&self, key: &str) -> Option<ToolCallResult> {
        self.inner.get(key).await
    }
    /// TTL is configured globally; only successful and bounded results are admitted.
    pub async fn set(&self, key: &str, value: ToolCallResult) {
        if value.is_error == Some(true) {
            return;
        }
        let Ok(encoded) = serde_json::to_vec(&value) else {
            return;
        };
        if encoded.len() > self.max_result_bytes {
            return;
        }
        self.inner.run_pending_tasks().await;
        if self.inner.entry_count() >= self.max_entries && !self.inner.contains_key(key) {
            return;
        }
        self.inner.insert(key.to_owned(), value).await;
    }
    pub async fn invalidate(&self, key: &str) {
        self.inner.invalidate(key).await;
    }
    pub async fn clear(&self) {
        self.inner.invalidate_all();
    }
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn rejects_error_and_oversized_results() {
        let cache = L1Cache::with_limits(10, 10000, 100, Duration::from_secs(60));
        cache.set("error", ToolCallResult::error("failed")).await;
        cache
            .set("large", ToolCallResult::text("x".repeat(1000)))
            .await;
        assert!(cache.get("error").await.is_none());
        assert!(cache.get("large").await.is_none());
        cache.set("ok", ToolCallResult::text("yes")).await;
        assert!(cache.get("ok").await.is_some());
        cache.invalidate("ok").await;
        assert!(cache.get("ok").await.is_none());
    }
    #[tokio::test]
    async fn respects_configured_ttl() {
        let cache = L1Cache::new(10, Duration::from_millis(20));
        cache.set("key", ToolCallResult::text("value")).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(cache.get("key").await.is_none());
    }
}
