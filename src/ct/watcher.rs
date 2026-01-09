use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

pub struct LogHealth {
    consecutive_failures: AtomicU32,
    consecutive_successes: AtomicU32,
    total_errors: AtomicU64,
    pub status: parking_lot::RwLock<HealthStatus>,
}

impl LogHealth {
    pub fn new() -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            total_errors: AtomicU64::new(0),
            status: parking_lot::RwLock::new(HealthStatus::Healthy),
        }
    }

    pub fn record_success(&self, healthy_threshold: u32) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;

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

        let mut status = self.status.write();
        if failures >= unhealthy_threshold {
            *status = HealthStatus::Unhealthy;
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
}
