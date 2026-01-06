use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use parking_lot::RwLock;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::ct::watcher::HealthStatus;

#[derive(Debug, Clone, Serialize)]
pub struct StatsResponse {
    pub uptime_seconds: u64,
    pub connections: ConnectionStats,
    pub throughput: ThroughputStats,
    pub memory: MemoryStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionStats {
    pub total: u64,
    pub websocket: u64,
    pub sse: u64,
    pub tcp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ThroughputStats {
    pub messages_sent: u64,
    pub certificates_processed: u64,
    pub bytes_sent: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryStats {
    pub cache_entries: usize,
    pub cache_capacity: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogStatus {
    pub name: String,
    pub url: String,
    pub operator: String,
    pub status: String,
    pub current_index: u64,
    pub tree_size: u64,
    pub total_errors: u64,
    pub last_success: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogsResponse {
    pub total_logs: usize,
    pub healthy: usize,
    pub degraded: usize,
    pub unhealthy: usize,
    pub logs: Vec<LogStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CertDetail {
    pub fingerprint: String,
    pub sha1: String,
    pub sha256: String,
    pub serial_number: String,
    pub subject: SubjectInfo,
    pub issuer: SubjectInfo,
    pub not_before: i64,
    pub not_after: i64,
    pub is_ca: bool,
    pub all_domains: Vec<String>,
    pub signature_algorithm: String,
    pub seen: f64,
    pub source: String,
    pub cert_index: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubjectInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub o: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

pub struct CachedCert {
    pub fingerprint: String,
    pub sha1: String,
    pub sha256: String,
    pub serial_number: String,
    pub subject_cn: Option<String>,
    pub subject_o: Option<String>,
    pub subject_c: Option<String>,
    pub issuer_cn: Option<String>,
    pub issuer_o: Option<String>,
    pub issuer_c: Option<String>,
    pub not_before: i64,
    pub not_after: i64,
    pub is_ca: bool,
    pub all_domains: Vec<String>,
    pub signature_algorithm: String,
    pub seen: f64,
    pub source_name: String,
    pub cert_index: u64,
}

pub struct CertificateCache {
    entries: RwLock<VecDeque<Arc<CachedCert>>>,
    capacity: usize,
}

impl CertificateCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    pub fn push(&self, cert: CachedCert) {
        let mut entries = self.entries.write();
        if entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(Arc::new(cert));
    }

    pub fn get_by_hash(&self, hash: &str) -> Option<Arc<CachedCert>> {
        let entries = self.entries.read();
        let hash_upper = hash.to_uppercase();
        let hash_clean = hash_upper.replace(':', "");
        entries.iter().find(|cert| {
            cert.sha256.replace(':', "").to_uppercase() == hash_clean
                || cert.sha1.replace(':', "").to_uppercase() == hash_clean
                || cert.fingerprint.replace(':', "").to_uppercase() == hash_clean
        }).cloned()
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct LogTracker {
    logs: RwLock<Vec<TrackedLog>>,
}

pub struct TrackedLog {
    pub name: String,
    pub url: String,
    pub operator: String,
    pub status: HealthStatus,
    pub current_index: u64,
    pub tree_size: u64,
    pub total_errors: u64,
    pub last_success: Option<i64>,
}

impl LogTracker {
    pub fn new() -> Self {
        Self {
            logs: RwLock::new(Vec::new()),
        }
    }

    pub fn register(&self, name: String, url: String, operator: String) {
        let mut logs = self.logs.write();
        logs.push(TrackedLog {
            name,
            url,
            operator,
            status: HealthStatus::Healthy,
            current_index: 0,
            tree_size: 0,
            total_errors: 0,
            last_success: None,
        });
    }

    pub fn update(&self, url: &str, status: HealthStatus, current_index: u64, tree_size: u64, total_errors: u64) {
        let mut logs = self.logs.write();
        if let Some(log) = logs.iter_mut().find(|l| l.url == url) {
            log.status = status;
            log.current_index = current_index;
            log.tree_size = tree_size;
            log.total_errors = total_errors;
            log.last_success = Some(chrono::Utc::now().timestamp());
        }
    }

    pub fn get_all(&self) -> Vec<LogStatus> {
        let logs = self.logs.read();
        logs.iter()
            .map(|l| LogStatus {
                name: l.name.clone(),
                url: l.url.clone(),
                operator: l.operator.clone(),
                status: match l.status {
                    HealthStatus::Healthy => "healthy".to_string(),
                    HealthStatus::Degraded => "degraded".to_string(),
                    HealthStatus::Unhealthy => "unhealthy".to_string(),
                },
                current_index: l.current_index,
                tree_size: l.tree_size,
                total_errors: l.total_errors,
                last_success: l.last_success,
            })
            .collect()
    }

    pub fn count_by_status(&self) -> (usize, usize, usize) {
        let logs = self.logs.read();
        let healthy = logs.iter().filter(|l| l.status == HealthStatus::Healthy).count();
        let degraded = logs.iter().filter(|l| l.status == HealthStatus::Degraded).count();
        let unhealthy = logs.iter().filter(|l| l.status == HealthStatus::Unhealthy).count();
        (healthy, degraded, unhealthy)
    }
}

pub struct ServerStats {
    pub start_time: Instant,
    pub messages_sent: AtomicU64,
    pub certificates_processed: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub ws_connections: AtomicU64,
    pub sse_connections: AtomicU64,
    pub tcp_connections: AtomicU64,
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            messages_sent: AtomicU64::new(0),
            certificates_processed: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            ws_connections: AtomicU64::new(0),
            sse_connections: AtomicU64::new(0),
            tcp_connections: AtomicU64::new(0),
        }
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

pub struct ApiState {
    pub stats: Arc<ServerStats>,
    pub cache: Arc<CertificateCache>,
    pub log_tracker: Arc<LogTracker>,
}

pub async fn handle_stats(State(state): State<Arc<ApiState>>) -> Json<StatsResponse> {
    Json(StatsResponse {
        uptime_seconds: state.stats.uptime_seconds(),
        connections: ConnectionStats {
            total: state.stats.ws_connections.load(Ordering::Relaxed)
                + state.stats.sse_connections.load(Ordering::Relaxed)
                + state.stats.tcp_connections.load(Ordering::Relaxed),
            websocket: state.stats.ws_connections.load(Ordering::Relaxed),
            sse: state.stats.sse_connections.load(Ordering::Relaxed),
            tcp: state.stats.tcp_connections.load(Ordering::Relaxed),
        },
        throughput: ThroughputStats {
            messages_sent: state.stats.messages_sent.load(Ordering::Relaxed),
            certificates_processed: state.stats.certificates_processed.load(Ordering::Relaxed),
            bytes_sent: state.stats.bytes_sent.load(Ordering::Relaxed),
        },
        memory: MemoryStats {
            cache_entries: state.cache.len(),
            cache_capacity: state.cache.capacity(),
        },
    })
}

pub async fn handle_logs(State(state): State<Arc<ApiState>>) -> Json<LogsResponse> {
    let logs = state.log_tracker.get_all();
    let (healthy, degraded, unhealthy) = state.log_tracker.count_by_status();
    Json(LogsResponse {
        total_logs: logs.len(),
        healthy,
        degraded,
        unhealthy,
        logs,
    })
}

pub async fn handle_cert(
    State(state): State<Arc<ApiState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    match state.cache.get_by_hash(&hash) {
        Some(cert) => Json(CertDetail {
            fingerprint: cert.fingerprint.clone(),
            sha1: cert.sha1.clone(),
            sha256: cert.sha256.clone(),
            serial_number: cert.serial_number.clone(),
            subject: SubjectInfo {
                cn: cert.subject_cn.clone(),
                o: cert.subject_o.clone(),
                c: cert.subject_c.clone(),
            },
            issuer: SubjectInfo {
                cn: cert.issuer_cn.clone(),
                o: cert.issuer_o.clone(),
                c: cert.issuer_c.clone(),
            },
            not_before: cert.not_before,
            not_after: cert.not_after,
            is_ca: cert.is_ca,
            all_domains: cert.all_domains.clone(),
            signature_algorithm: cert.signature_algorithm.clone(),
            seen: cert.seen,
            source: cert.source_name.clone(),
            cert_index: cert.cert_index,
        })
        .into_response(),
        None => (StatusCode::NOT_FOUND, "Certificate not found").into_response(),
    }
}
