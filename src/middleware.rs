use axum::{
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use dashmap::DashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use subtle::ConstantTimeEq;

use crate::config::{AuthConfig, ConnectionLimitConfig};
use crate::hot_reload::HotReloadManager;
use crate::rate_limit::{RateLimitResult, RateLimiter};

pub struct ConnectionLimiter {
    hot_reload: Option<Arc<HotReloadManager>>,
    fallback_config: ConnectionLimitConfig,
    total_connections: AtomicU32,
    per_ip_connections: DashMap<IpAddr, u32>,
}

impl ConnectionLimiter {
    pub fn new(config: ConnectionLimitConfig, hot_reload: Option<Arc<HotReloadManager>>) -> Arc<Self> {
        Arc::new(Self {
            hot_reload,
            fallback_config: config,
            total_connections: AtomicU32::new(0),
            per_ip_connections: DashMap::new(),
        })
    }

    fn get_config(&self) -> ConnectionLimitConfig {
        self.hot_reload
            .as_ref()
            .map(|hr| hr.get().connection_limit.clone())
            .unwrap_or_else(|| self.fallback_config.clone())
    }

    pub fn try_acquire(&self, ip: IpAddr) -> bool {
        let config = self.get_config();

        if !config.enabled {
            return true;
        }

        loop {
            let current_total = self.total_connections.load(Ordering::SeqCst);
            if current_total >= config.max_connections {
                metrics::counter!("certstream_connection_limit_rejected").increment(1);
                return false;
            }

            if self
                .total_connections
                .compare_exchange(current_total, current_total + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }

        if let Some(per_ip_limit) = config.per_ip_limit {
            let mut should_release = false;
            {
                let mut entry = self.per_ip_connections.entry(ip).or_insert(0);
                if *entry >= per_ip_limit {
                    should_release = true;
                } else {
                    *entry += 1;
                }
            }
            if should_release {
                self.total_connections.fetch_sub(1, Ordering::SeqCst);
                metrics::counter!("certstream_per_ip_limit_rejected").increment(1);
                return false;
            }
        } else {
            self.per_ip_connections
                .entry(ip)
                .and_modify(|v| *v += 1)
                .or_insert(1);
        }

        true
    }

    pub fn release(&self, ip: IpAddr) {
        let config = self.get_config();

        if !config.enabled {
            return;
        }

        self.total_connections.fetch_sub(1, Ordering::SeqCst);

        if let Some(mut entry) = self.per_ip_connections.get_mut(&ip) {
            *entry = entry.saturating_sub(1);
            if *entry == 0 {
                drop(entry);
                self.per_ip_connections.remove(&ip);
            }
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware {
    hot_reload: Option<Arc<HotReloadManager>>,
    fallback_config: AuthConfig,
}

impl AuthMiddleware {
    pub fn new(config: &AuthConfig, hot_reload: Option<Arc<HotReloadManager>>) -> Self {
        Self {
            hot_reload,
            fallback_config: config.clone(),
        }
    }

    fn get_config(&self) -> AuthConfig {
        self.hot_reload
            .as_ref()
            .map(|hr| hr.get().auth.clone())
            .unwrap_or_else(|| self.fallback_config.clone())
    }

    pub fn validate(&self, token: Option<&str>) -> bool {
        let config = self.get_config();

        if !config.enabled {
            return true;
        }

        match token {
            Some(t) => {
                let token_value = t.strip_prefix("Bearer ").unwrap_or(t);
                let token_bytes = token_value.as_bytes();
                config.tokens.iter().any(|stored| {
                    let stored_bytes = stored.as_bytes();
                    stored_bytes.len() == token_bytes.len()
                        && stored_bytes.ct_eq(token_bytes).into()
                })
            }
            None => false,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.get_config().enabled
    }

    pub fn header_name(&self) -> String {
        self.get_config().header_name
    }
}

pub async fn auth_middleware(
    State(auth): State<Arc<AuthMiddleware>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !auth.is_enabled() {
        return next.run(request).await;
    }

    let header_name = auth.header_name();
    let token = request
        .headers()
        .get(&header_name)
        .and_then(|v| v.to_str().ok());

    if auth.validate(token) {
        next.run(request).await
    } else {
        metrics::counter!("certstream_auth_rejected").increment(1);
        (StatusCode::UNAUTHORIZED, "Unauthorized").into_response()
    }
}

pub async fn rate_limit_middleware(
    State(limiter): State<Arc<RateLimiter>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok());

    match limiter.check(addr.ip(), token) {
        RateLimitResult::Allowed => next.run(request).await,
        RateLimitResult::Rejected { retry_after_ms } => {
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                format!("Rate limit exceeded. Retry after {}ms", retry_after_ms),
            )
                .into_response();
            response.headers_mut().insert(
                "Retry-After",
                ((retry_after_ms / 1000).max(1)).to_string().parse().unwrap(),
            );
            response
        }
    }
}
