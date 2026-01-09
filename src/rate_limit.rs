use dashmap::DashMap;
use parking_lot::Mutex;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::RateLimitConfig;
use crate::hot_reload::HotReloadManager;

pub struct TokenBucket {
    tokens: f64,
    max_tokens: f64,
    refill_rate: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(max_tokens: f64, refill_rate: f64) -> Self {
        Self {
            tokens: max_tokens,
            max_tokens,
            refill_rate,
            last_refill: Instant::now(),
        }
    }

    fn try_consume(&mut self, tokens: f64) -> bool {
        self.refill();
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens);
        self.last_refill = now;
    }

    fn update_limits(&mut self, max_tokens: f64, refill_rate: f64) {
        self.max_tokens = max_tokens;
        self.refill_rate = refill_rate;
        self.tokens = self.tokens.min(max_tokens);
    }
}

pub struct SlidingWindow {
    window_size: Duration,
    max_requests: u32,
    timestamps: Vec<Instant>,
}

impl SlidingWindow {
    fn new(window_size: Duration, max_requests: u32) -> Self {
        Self {
            window_size,
            max_requests,
            timestamps: Vec::with_capacity(max_requests as usize),
        }
    }

    fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let cutoff = now - self.window_size;
        self.timestamps.retain(|&t| t > cutoff);
        if self.timestamps.len() < self.max_requests as usize {
            self.timestamps.push(now);
            true
        } else {
            false
        }
    }

    fn update_limits(&mut self, window_size: Duration, max_requests: u32) {
        self.window_size = window_size;
        self.max_requests = max_requests;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RateLimitTier {
    Free,
    Standard,
    Premium,
}

impl RateLimitTier {
    pub fn from_token(token: Option<&str>, tier_tokens: &TierTokens) -> Self {
        match token {
            Some(t) => {
                let clean = t.strip_prefix("Bearer ").unwrap_or(t);
                if tier_tokens.premium.iter().any(|p| p == clean) {
                    RateLimitTier::Premium
                } else if tier_tokens.standard.iter().any(|s| s == clean) {
                    RateLimitTier::Standard
                } else {
                    RateLimitTier::Free
                }
            }
            None => RateLimitTier::Free,
        }
    }

    fn limits(&self, config: &RateLimitConfig) -> TierLimits {
        match self {
            RateLimitTier::Free => TierLimits {
                max_tokens: config.free_max_tokens,
                refill_rate: config.free_refill_rate,
                burst_allowance: config.free_burst,
            },
            RateLimitTier::Standard => TierLimits {
                max_tokens: config.standard_max_tokens,
                refill_rate: config.standard_refill_rate,
                burst_allowance: config.standard_burst,
            },
            RateLimitTier::Premium => TierLimits {
                max_tokens: config.premium_max_tokens,
                refill_rate: config.premium_refill_rate,
                burst_allowance: config.premium_burst,
            },
        }
    }
}

struct TierLimits {
    max_tokens: f64,
    refill_rate: f64,
    burst_allowance: f64,
}

pub struct TierTokens {
    pub standard: Vec<String>,
    pub premium: Vec<String>,
}

struct ClientRateLimit {
    token_bucket: TokenBucket,
    sliding_window: SlidingWindow,
    tier: RateLimitTier,
    burst_used: f64,
    burst_reset: Instant,
}

pub struct RateLimiter {
    hot_reload: Option<Arc<HotReloadManager>>,
    fallback_config: RateLimitConfig,
    clients: DashMap<IpAddr, Mutex<ClientRateLimit>>,
    tier_tokens: TierTokens,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig, tier_tokens: TierTokens, hot_reload: Option<Arc<HotReloadManager>>) -> Arc<Self> {
        Arc::new(Self {
            hot_reload,
            fallback_config: config,
            clients: DashMap::new(),
            tier_tokens,
        })
    }

    fn get_config(&self) -> RateLimitConfig {
        self.hot_reload
            .as_ref()
            .map(|hr| hr.get().rate_limit.clone())
            .unwrap_or_else(|| self.fallback_config.clone())
    }

    pub fn check(&self, ip: IpAddr, token: Option<&str>) -> RateLimitResult {
        let config = self.get_config();

        if !config.enabled {
            return RateLimitResult::Allowed;
        }

        let tier = RateLimitTier::from_token(token, &self.tier_tokens);
        let limits = tier.limits(&config);

        let entry = self.clients.entry(ip).or_insert_with(|| {
            Mutex::new(ClientRateLimit {
                token_bucket: TokenBucket::new(limits.max_tokens, limits.refill_rate),
                sliding_window: SlidingWindow::new(
                    Duration::from_secs(config.window_seconds),
                    config.window_max_requests,
                ),
                tier,
                burst_used: 0.0,
                burst_reset: Instant::now() + Duration::from_secs(config.burst_window_seconds),
            })
        });

        let mut client = entry.lock();
        let current_limits = client.tier.limits(&config);
        client.token_bucket.update_limits(current_limits.max_tokens, current_limits.refill_rate);
        client.sliding_window.update_limits(
            Duration::from_secs(config.window_seconds),
            config.window_max_requests,
        );

        if client.tier != tier {
            let new_limits = tier.limits(&config);
            client.token_bucket = TokenBucket::new(new_limits.max_tokens, new_limits.refill_rate);
            client.tier = tier;
        }

        if !client.sliding_window.try_acquire() {
            metrics::counter!("certstream_rate_limit_rejected", "reason" => "sliding_window").increment(1);
            return RateLimitResult::Rejected {
                retry_after_ms: config.window_seconds * 1000 / 2,
            };
        }

        if client.token_bucket.try_consume(1.0) {
            return RateLimitResult::Allowed;
        }

        let now = Instant::now();
        if now >= client.burst_reset {
            client.burst_used = 0.0;
            client.burst_reset = now + Duration::from_secs(config.burst_window_seconds);
        }

        if client.burst_used < limits.burst_allowance {
            client.burst_used += 1.0;
            metrics::counter!("certstream_rate_limit_burst_used").increment(1);
            return RateLimitResult::Allowed;
        }

        metrics::counter!("certstream_rate_limit_rejected", "reason" => "token_bucket").increment(1);
        RateLimitResult::Rejected {
            retry_after_ms: (1000.0 / limits.refill_rate) as u64,
        }
    }

    pub fn cleanup_stale(&self, max_age: Duration) {
        let cutoff = Instant::now() - max_age;
        self.clients.retain(|_, v| {
            let client = v.lock();
            client.token_bucket.last_refill > cutoff
        });
    }
}

#[derive(Debug, Clone)]
pub enum RateLimitResult {
    Allowed,
    Rejected { retry_after_ms: u64 },
}
