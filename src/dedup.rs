use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

const DEFAULT_CAPACITY: usize = 500_000;
const DEFAULT_TTL_SECS: u64 = 300;
const DEFAULT_CLEANUP_INTERVAL_SECS: u64 = 60;

pub struct DedupFilter {
    seen: DashMap<String, Instant>,
    capacity: usize,
    ttl: Duration,
}

impl DedupFilter {
    pub fn new() -> Self {
        Self {
            seen: DashMap::with_capacity(DEFAULT_CAPACITY / 4),
            capacity: DEFAULT_CAPACITY,
            ttl: Duration::from_secs(DEFAULT_TTL_SECS),
        }
    }

    /// Returns true if this SHA-256 hash has NOT been seen before (i.e., is new).
    pub fn is_new(&self, sha256: &str) -> bool {
        let now = Instant::now();

        if self.seen.len() >= self.capacity {
            self.cleanup();
            if self.seen.len() >= self.capacity {
                info!(size = self.seen.len(), "dedup cache full after cleanup, clearing");
                self.seen.clear();
                metrics::counter!("certstream_dedup_cache_clears").increment(1);
            }
        }

        match self.seen.entry(sha256.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let age = now.duration_since(*entry.get());
                if age > self.ttl {
                    entry.insert(now);
                    true
                } else {
                    metrics::counter!("certstream_duplicates_filtered").increment(1);
                    false
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(now);
                true
            }
        }
    }

    pub fn cleanup(&self) {
        let before = self.seen.len();
        let now = Instant::now();
        self.seen.retain(|_, v| now.duration_since(*v) < self.ttl);
        let removed = before.saturating_sub(self.seen.len());
        if removed > 0 {
            debug!(removed = removed, remaining = self.seen.len(), "dedup cleanup");
        }
        metrics::gauge!("certstream_dedup_cache_size").set(self.seen.len() as f64);
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn start_cleanup_task(self: Arc<Self>, cancel: CancellationToken) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(DEFAULT_CLEANUP_INTERVAL_SECS));
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        info!("dedup cleanup task stopping");
                        break;
                    }
                    _ = tick.tick() => {
                        self.cleanup();
                    }
                }
            }
        });
    }
}

impl Default for DedupFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_is_new_first_seen() {
        let filter = DedupFilter::new();
        assert!(filter.is_new("abc123"));
        assert!(filter.is_new("def456"));
    }

    #[test]
    fn test_is_new_duplicate() {
        let filter = DedupFilter::new();
        assert!(filter.is_new("abc123"));
        assert!(!filter.is_new("abc123")); // second time → duplicate
        assert!(!filter.is_new("abc123")); // third time → still duplicate
    }

    #[test]
    fn test_is_new_different_keys() {
        let filter = DedupFilter::new();
        assert!(filter.is_new("key1"));
        assert!(filter.is_new("key2"));
        assert!(filter.is_new("key3"));
        assert!(!filter.is_new("key1"));
        assert!(!filter.is_new("key2"));
    }

    #[test]
    fn test_is_new_ttl_expiry() {
        // Create a filter with a very short TTL for testing
        let filter = DedupFilter {
            seen: DashMap::with_capacity(100),
            capacity: DEFAULT_CAPACITY,
            ttl: Duration::from_millis(50),
        };

        assert!(filter.is_new("expired_key"));
        assert!(!filter.is_new("expired_key"));

        // Sleep past TTL
        thread::sleep(Duration::from_millis(60));

        // Should be treated as new again after TTL expiry
        assert!(filter.is_new("expired_key"));
    }

    #[test]
    fn test_is_new_capacity_overflow() {
        let filter = DedupFilter {
            seen: DashMap::with_capacity(4),
            capacity: 5, // Very small capacity
            ttl: Duration::from_secs(300),
        };

        // Fill to capacity
        for i in 0..5 {
            assert!(filter.is_new(&format!("key_{}", i)));
        }
        assert_eq!(filter.len(), 5);

        // Next insert should trigger clear
        assert!(filter.is_new("overflow_key"));
        // After clear + insert, only the new key should be present
        assert_eq!(filter.len(), 1);
    }

    #[test]
    fn test_cleanup_removes_expired() {
        let filter = DedupFilter {
            seen: DashMap::with_capacity(100),
            capacity: DEFAULT_CAPACITY,
            ttl: Duration::from_millis(50),
        };

        filter.is_new("key1");
        filter.is_new("key2");
        assert_eq!(filter.len(), 2);

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(60));

        // Add a fresh entry
        filter.is_new("key3");

        filter.cleanup();

        // key1 and key2 should be removed, key3 should remain
        assert_eq!(filter.len(), 1);
        assert!(filter.is_new("key1")); // key1 was cleaned up, so it's new again
    }

    #[test]
    fn test_cleanup_keeps_fresh_entries() {
        let filter = DedupFilter::new();
        filter.is_new("key1");
        filter.is_new("key2");
        filter.is_new("key3");

        filter.cleanup();

        // All entries are fresh, none removed
        assert_eq!(filter.len(), 3);
        assert!(!filter.is_new("key1")); // still a duplicate
    }

    #[test]
    fn test_len() {
        let filter = DedupFilter::new();
        assert_eq!(filter.len(), 0);

        filter.is_new("a");
        assert_eq!(filter.len(), 1);

        filter.is_new("b");
        assert_eq!(filter.len(), 2);

        filter.is_new("a"); // duplicate, no new entry
        assert_eq!(filter.len(), 2);
    }

    #[test]
    fn test_empty_string_key() {
        let filter = DedupFilter::new();
        assert!(filter.is_new(""));
        assert!(!filter.is_new(""));
    }

    #[tokio::test]
    async fn test_cleanup_task_stops_on_cancellation() {
        let filter = Arc::new(DedupFilter::new());
        let cancel = CancellationToken::new();

        filter.clone().start_cleanup_task(cancel.clone());

        // Let it run a tick
        tokio::time::sleep(Duration::from_millis(50)).await;

        cancel.cancel();

        // Give time for task to stop
        tokio::time::sleep(Duration::from_millis(50)).await;
        // If we get here without hanging, the task stopped correctly
    }
}
