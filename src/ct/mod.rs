mod log_list;
mod parser;
pub mod static_ct;
pub mod watcher;

pub use log_list::*;
pub use parser::*;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Per-operator rate limiter to avoid hitting CT log rate limits.
/// Wraps a tokio Interval so all watchers sharing it are serialized.
pub type OperatorRateLimiter = Arc<tokio::sync::Mutex<tokio::time::Interval>>;

use crate::api::{CachedCert, CertificateCache, LogTracker, ServerStats};
use crate::config::CtLogConfig;
use crate::dedup::DedupFilter;
use crate::models::{CertificateMessage, LeafCert, PreSerializedMessage};
use crate::state::StateManager;

/// Shared context for CT log watcher tasks.
#[derive(Clone)]
pub struct WatcherContext {
    pub client: reqwest::Client,
    pub tx: broadcast::Sender<Arc<PreSerializedMessage>>,
    pub config: Arc<CtLogConfig>,
    pub state_manager: Arc<StateManager>,
    pub cache: Arc<CertificateCache>,
    pub stats: Arc<ServerStats>,
    pub tracker: Arc<LogTracker>,
    pub shutdown: CancellationToken,
    pub dedup: Arc<DedupFilter>,
    pub rate_limiter: Option<OperatorRateLimiter>,
}

/// Build a CachedCert from a parsed leaf certificate.
pub fn build_cached_cert(
    leaf: &LeafCert,
    seen: f64,
    source_name: &str,
    source_url: &str,
    cert_index: u64,
) -> CachedCert {
    CachedCert {
        fingerprint: leaf.fingerprint.clone(),
        sha1: leaf.sha1.clone(),
        sha256: leaf.sha256.clone(),
        serial_number: leaf.serial_number.clone(),
        subject: leaf.subject.clone(),
        issuer: leaf.issuer.clone(),
        not_before: leaf.not_before,
        not_after: leaf.not_after,
        is_ca: leaf.is_ca,
        all_domains: leaf.all_domains.to_vec(),
        signature_algorithm: leaf.signature_algorithm.clone(),
        seen,
        source_name: source_name.to_string(),
        source_url: source_url.to_string(),
        cert_index,
    }
}

/// Serialize and broadcast a certificate message to all subscribers.
pub fn broadcast_cert(
    msg: CertificateMessage,
    tx: &broadcast::Sender<Arc<PreSerializedMessage>>,
    cache: &CertificateCache,
    cached: CachedCert,
    stats: &ServerStats,
    log_name: &str,
) {
    cache.push(cached);
    if let Some(serialized) = msg.pre_serialize() {
        let msg_size =
            serialized.full.len() + serialized.lite.len() + serialized.domains_only.len();
        let _ = tx.send(serialized);
        stats.messages_sent.fetch_add(1, Ordering::Relaxed);
        stats
            .certificates_processed
            .fetch_add(1, Ordering::Relaxed);
        stats
            .bytes_sent
            .fetch_add(msg_size as u64, Ordering::Relaxed);
        metrics::counter!("certstream_messages_sent", "log" => log_name.to_string()).increment(1);
    }
}
