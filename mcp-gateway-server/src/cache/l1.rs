//! L1 cache — in-process cache using moka.
//!
//! Per the document: "L1: 进程内缓存 (moka::Cache) ← 微秒级"
//!
//! Cache key strategy: tool_name + sorted JSON arguments
//! TTL: configurable, default 300s

use std::time::Duration;

use mcp_gateway_core::tool::ToolCallResult;

/// L1 in-process cache backed by moka.
pub struct L1Cache {
    inner: moka::future::Cache<String, ToolCallResult>,
}

impl L1Cache {
    /// Create a new L1 cache.
    pub fn new(max_capacity: u64, ttl: Duration) -> Self {
        Self {
            inner: moka::future::Cache::builder()
                .max_capacity(max_capacity)
                .time_to_live(ttl)
                .build(),
        }
    }

    /// Get a cached value.
    pub async fn get(&self, key: &str) -> Option<ToolCallResult> {
        self.inner.get(key).await
    }

    /// Insert a value with a custom TTL.
    pub async fn set(&self, key: &str, value: ToolCallResult, ttl: Duration) {
        // moka's policy is global; we use the global TTL set at construction.
        // For per-entry TTL, we'd need a different approach.
        let _ = ttl;
        self.inner.insert(key.to_string(), value).await;
    }

    /// Remove a value from the cache.
    pub async fn invalidate(&self, key: &str) {
        self.inner.invalidate(key).await;
    }

    /// Clear the entire cache.
    pub async fn clear(&self) {
        self.inner.invalidate_all();
    }

    /// Get the number of entries in the cache.
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcp_gateway_core::tool::ToolCallResult;

    #[tokio::test]
    async fn test_cache_hit() {
        let cache = L1Cache::new(100, Duration::from_secs(60));
        let result = ToolCallResult::text("cached value");
        cache.set("test_key", result.clone(), Duration::from_secs(60)).await;

        let cached = cache.get("test_key").await;
        assert!(cached.is_some());
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache = L1Cache::new(100, Duration::from_secs(60));
        let cached = cache.get("nonexistent").await;
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_cache_invalidate() {
        let cache = L1Cache::new(100, Duration::from_secs(60));
        cache.set("key", ToolCallResult::text("val"), Duration::from_secs(60)).await;
        cache.invalidate("key").await;
        assert!(cache.get("key").await.is_none());
    }
}