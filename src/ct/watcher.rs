use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};

use super::fetch::{self, FetchError};
use super::{broadcast_cert, build_cached_cert, CtLog, WatcherContext};
use crate::models::Source;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

pub struct LogHealth {
    consecutive_failures: AtomicU32,
    consecutive_successes: AtomicU32,
    total_errors: AtomicU64,
    pub status: parking_lot::RwLock<HealthStatus>,
    circuit: parking_lot::RwLock<CircuitState>,
    circuit_opened_at: parking_lot::RwLock<Option<Instant>>,
    current_backoff_ms: AtomicU64,
}

impl LogHealth {
    const MIN_BACKOFF_MS: u64 = 1000;
    const MAX_BACKOFF_MS: u64 = 60000;
    const CIRCUIT_RESET_MS: u64 = 30000;
    const RATE_LIMIT_BACKOFF_MS: u64 = 30_000;

    pub fn new() -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            total_errors: AtomicU64::new(0),
            status: parking_lot::RwLock::new(HealthStatus::Healthy),
            circuit: parking_lot::RwLock::new(CircuitState::Closed),
            circuit_opened_at: parking_lot::RwLock::new(None),
            current_backoff_ms: AtomicU64::new(Self::MIN_BACKOFF_MS),
        }
    }

    pub fn record_success(&self, healthy_threshold: u32) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.current_backoff_ms.store(Self::MIN_BACKOFF_MS, Ordering::Relaxed);
        let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;

        let mut circuit = self.circuit.write();
        if *circuit == CircuitState::HalfOpen {
            *circuit = CircuitState::Closed;
            *self.circuit_opened_at.write() = None;
        }

        if successes >= healthy_threshold {
            let mut status = self.status.write();
            if *status != HealthStatus::Healthy {
                *status = HealthStatus::Healthy;
            }
        }
    }

    pub fn record_failure(&self, unhealthy_threshold: u32) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

        // Exponential backoff: double on each failure, cap at max
        let current = self.current_backoff_ms.load(Ordering::Relaxed);
        let next = (current * 2).min(Self::MAX_BACKOFF_MS);
        self.current_backoff_ms.store(next, Ordering::Relaxed);

        let mut status = self.status.write();
        if failures >= unhealthy_threshold {
            *status = HealthStatus::Unhealthy;
            let mut circuit = self.circuit.write();
            if *circuit != CircuitState::Open {
                *circuit = CircuitState::Open;
                *self.circuit_opened_at.write() = Some(Instant::now());
            }
        } else if failures >= unhealthy_threshold / 2 {
            *status = HealthStatus::Degraded;
        }
    }

    pub fn is_healthy(&self) -> bool {
        *self.status.read() != HealthStatus::Unhealthy
    }

    pub fn total_errors(&self) -> u64 {
        self.total_errors.load(Ordering::Relaxed)
    }

    pub fn record_rate_limit(&self, unhealthy_threshold: u32) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

        self.current_backoff_ms
            .store(Self::RATE_LIMIT_BACKOFF_MS, Ordering::Relaxed);

        let mut status = self.status.write();
        if failures >= unhealthy_threshold {
            *status = HealthStatus::Unhealthy;
            let mut circuit = self.circuit.write();
            if *circuit != CircuitState::Open {
                *circuit = CircuitState::Open;
                *self.circuit_opened_at.write() = Some(Instant::now());
            }
        } else if failures >= unhealthy_threshold / 2 {
            *status = HealthStatus::Degraded;
        }
    }

    pub fn get_backoff(&self) -> Duration {
        Duration::from_millis(self.current_backoff_ms.load(Ordering::Relaxed))
    }

    #[allow(dead_code)]
    pub fn circuit_state(&self) -> CircuitState {
        *self.circuit.read()
    }

    pub fn should_attempt(&self) -> bool {
        let mut circuit = self.circuit.write();
        match *circuit {
            CircuitState::Closed | CircuitState::HalfOpen => true,
            CircuitState::Open => {
                if let Some(opened_at) = *self.circuit_opened_at.read() {
                    if opened_at.elapsed() > Duration::from_millis(Self::CIRCUIT_RESET_MS) {
                        *circuit = CircuitState::HalfOpen;
                        return true;
                    }
                }
                false
            }
        }
    }
}

/// RFC 6962 CT log watcher — polls get-sth / get-entries in a loop.
#[allow(clippy::too_many_arguments)]
pub async fn run_watcher_with_cache(log: CtLog, ctx: WatcherContext) {
    let WatcherContext {
        client,
        tx,
        config,
        state_manager,
        cache,
        stats,
        tracker,
        shutdown,
        dedup,
        rate_limiter,
    } = ctx;
    use backon::{ExponentialBuilder, Retryable};
    use tokio::time::sleep;

    let base_url = log.normalized_url();
    let log_name = log.description.clone();
    let source = Arc::new(Source {
        name: Arc::from(log.description.as_str()),
        url: Arc::from(base_url.as_str()),
    });

    let health = Arc::new(LogHealth::new());
    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    let timeout = Duration::from_secs(config.request_timeout_secs);

    info!(log = %log_name, url = %base_url, "starting watcher");

    let mut current_index = if let Some(saved_index) = state_manager.get_index(&base_url) {
        info!(log = %log.description, saved_index = saved_index, "resuming from saved state");
        saved_index
    } else {
        let backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(config.retry_initial_delay_ms))
            .with_max_delay(Duration::from_millis(config.retry_max_delay_ms))
            .with_max_times(config.retry_max_attempts as usize);

        match (|| async {
            fetch::get_tree_size(&client, &base_url, timeout).await
        })
        .retry(backoff)
        .sleep(tokio::time::sleep)
        .await
        {
            Ok(size) => {
                let start = size.saturating_sub(1000);
                info!(log = %log.description, tree_size = size, starting_at = start, "starting fresh");
                start
            }
            Err(e) => {
                error!(log = %log.description, error = %e, "failed to get initial tree size");
                0
            }
        }
    };

    loop {
        if shutdown.is_cancelled() {
            info!(log = %log.description, "shutdown signal received");
            break;
        }

        if !health.should_attempt() {
            debug!(log = %log.description, "circuit breaker open, waiting");
            sleep(Duration::from_secs(config.health_check_interval_secs)).await;
            continue;
        }

        if !health.is_healthy() {
            warn!(log = %log.description, errors = health.total_errors(), "log is unhealthy, waiting for recovery check");
            sleep(Duration::from_secs(config.health_check_interval_secs)).await;

            let url = format!("{}/ct/v1/get-sth", base_url);
            match client.get(&url).timeout(timeout).send().await {
                Ok(_) => {
                    health.record_success(config.healthy_threshold);
                    info!(log = %log.description, "health check passed, resuming");
                }
                Err(e) => {
                    health.record_failure(config.unhealthy_threshold);
                    warn!(log = %log.description, error = %e, "health check failed, staying disabled");
                    metrics::counter!("certstream_log_health_checks_failed").increment(1);
                    continue;
                }
            }
        }

        let tree_size = match fetch::get_tree_size(&client, &base_url, timeout).await {
            Ok(size) => size,
            Err(e) => {
                match e {
                    FetchError::RateLimited(_) => {
                        health.record_rate_limit(config.unhealthy_threshold);
                        warn!(log = %log.description, "rate limited on get-sth, backing off");
                    }
                    FetchError::NotAvailable(_) => {
                        health.record_failure(config.unhealthy_threshold);
                        warn!(log = %log.description, error = %e, "get-sth returned error");
                    }
                    _ => {
                        health.record_failure(config.unhealthy_threshold);
                        warn!(log = %log.description, error = %e, "failed to get tree size");
                    }
                }
                sleep(health.get_backoff()).await;
                continue;
            }
        };

        if current_index >= tree_size {
            sleep(poll_interval).await;
            continue;
        }

        let end = (current_index + config.batch_size).min(tree_size - 1);

        // Respect per-operator rate limit before making request
        if let Some(ref limiter) = rate_limiter {
            limiter.lock().await.tick().await;
        }

        match fetch::fetch_entries(&client, &base_url, current_index, end, &source, timeout).await {
            Ok(messages) => {
                health.record_success(config.healthy_threshold);
                let count = messages.len();

                for msg in messages {
                    let cert_index = msg.data.cert_index;

                    if !dedup.is_new(&msg.data.leaf_cert.sha256) {
                        continue;
                    }

                    let cached = build_cached_cert(
                        &msg.data.leaf_cert,
                        msg.data.seen,
                        &log.description,
                        &base_url,
                        cert_index,
                    );
                    broadcast_cert(msg, &tx, &cache, cached, &stats, &log_name);
                }

                debug!(log = %log_name, count = count, "fetched entries");
                current_index = end + 1;
                state_manager.update_index(&base_url, current_index, tree_size);

                tracker.update(
                    &base_url,
                    *health.status.read(),
                    current_index,
                    tree_size,
                    health.total_errors(),
                );
            }
            Err(e) => {
                match e {
                    FetchError::RateLimited(_) => {
                        health.record_rate_limit(config.unhealthy_threshold);
                        warn!(log = %log.description, "rate limited by CT log, backing off 30s");
                    }
                    FetchError::NotAvailable(_) => {
                        // Entries not yet available — skip ahead to tree_size
                        debug!(log = %log.description, start = current_index, end = end,
                            "entries not available (400), skipping to tree head");
                        current_index = tree_size;
                        sleep(poll_interval).await;
                        continue;
                    }
                    _ => {
                        health.record_failure(config.unhealthy_threshold);
                        warn!(log = %log.description, error = %e, "failed to fetch entries");
                    }
                }
                sleep(health.get_backoff()).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_health_initial_state() {
        let health = LogHealth::new();
        assert!(health.is_healthy());
        assert_eq!(health.total_errors(), 0);
        assert_eq!(health.circuit_state(), CircuitState::Closed);
        assert!(health.should_attempt());
    }

    #[test]
    fn test_record_success_resets_failures() {
        let health = LogHealth::new();
        health.record_failure(5);
        health.record_failure(5);
        assert_eq!(health.total_errors(), 2);

        health.record_success(2);
        // consecutive failures reset, but total errors remain
        assert_eq!(health.total_errors(), 2);
        // Still healthy since we didn't hit unhealthy_threshold
        assert!(health.is_healthy());
    }

    #[test]
    fn test_record_failure_transitions_to_degraded() {
        let health = LogHealth::new();
        // unhealthy_threshold = 6, degraded at threshold/2 = 3
        health.record_failure(6);
        health.record_failure(6);
        assert_eq!(*health.status.read(), HealthStatus::Healthy);

        health.record_failure(6); // 3rd failure = degraded (6/2 = 3)
        assert_eq!(*health.status.read(), HealthStatus::Degraded);
    }

    #[test]
    fn test_record_failure_transitions_to_unhealthy() {
        let health = LogHealth::new();
        for _ in 0..5 {
            health.record_failure(5);
        }
        assert_eq!(*health.status.read(), HealthStatus::Unhealthy);
        assert!(!health.is_healthy());
    }

    #[test]
    fn test_circuit_opens_on_unhealthy() {
        let health = LogHealth::new();
        for _ in 0..5 {
            health.record_failure(5);
        }
        assert_eq!(health.circuit_state(), CircuitState::Open);
        assert!(!health.should_attempt());
    }

    #[test]
    fn test_success_recovers_from_degraded() {
        let health = LogHealth::new();
        // Go to degraded
        for _ in 0..3 {
            health.record_failure(6);
        }
        assert_eq!(*health.status.read(), HealthStatus::Degraded);

        // Recover with successes
        health.record_success(2);
        health.record_success(2);
        assert_eq!(*health.status.read(), HealthStatus::Healthy);
    }

    #[test]
    fn test_backoff_increases_exponentially() {
        let health = LogHealth::new();
        assert_eq!(health.get_backoff(), Duration::from_millis(1000));

        health.record_failure(100);
        assert_eq!(health.get_backoff(), Duration::from_millis(2000));

        health.record_failure(100);
        assert_eq!(health.get_backoff(), Duration::from_millis(4000));

        health.record_failure(100);
        assert_eq!(health.get_backoff(), Duration::from_millis(8000));
    }

    #[test]
    fn test_backoff_caps_at_max() {
        let health = LogHealth::new();
        // Trigger many failures to hit max backoff
        for _ in 0..20 {
            health.record_failure(100);
        }
        assert_eq!(health.get_backoff(), Duration::from_millis(60000));
    }

    #[test]
    fn test_success_resets_backoff() {
        let health = LogHealth::new();
        health.record_failure(100);
        health.record_failure(100);
        assert!(health.get_backoff() > Duration::from_millis(1000));

        health.record_success(1);
        assert_eq!(health.get_backoff(), Duration::from_millis(1000));
    }

    #[test]
    fn test_half_open_recovers_on_success() {
        let health = LogHealth::new();
        // Open the circuit
        for _ in 0..5 {
            health.record_failure(5);
        }
        assert_eq!(health.circuit_state(), CircuitState::Open);

        // Manually set to HalfOpen (simulates timeout)
        *health.circuit.write() = CircuitState::HalfOpen;
        assert!(health.should_attempt());

        // Success in HalfOpen → Closed
        health.record_success(1);
        assert_eq!(health.circuit_state(), CircuitState::Closed);
    }

    #[test]
    fn test_total_errors_accumulate() {
        let health = LogHealth::new();
        health.record_failure(100);
        health.record_success(1);
        health.record_failure(100);
        health.record_failure(100);
        assert_eq!(health.total_errors(), 3);
    }
}
