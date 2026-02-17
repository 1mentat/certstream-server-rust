use bytes::Bytes;
use dashmap::DashMap;
use flate2::read::GzDecoder;
use reqwest::Client;
use std::borrow::Cow;
use std::fmt::Write;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::fetch::{self, FetchError};
use super::watcher::LogHealth;
use super::{broadcast_cert, build_cached_cert, parse_certificate, CtLog, WatcherContext};
use crate::models::{CertificateData, CertificateMessage, ChainCert, Source};

/// A parsed static CT checkpoint.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Checkpoint {
    pub origin: String,
    pub tree_size: u64,
    pub root_hash: String,
}

/// A single entry parsed from a tile/data tile.
#[derive(Debug)]
#[allow(dead_code)]
pub struct TileLeaf {
    pub timestamp: u64,
    pub entry_type: u16,
    pub cert_der: Vec<u8>,
    pub is_precert: bool,
    pub chain_fingerprints: Vec<[u8; 32]>,
}

const MAX_ISSUER_CACHE_SIZE: usize = 10_000;

pub struct IssuerCache {
    cache: DashMap<String, Bytes>,
}

impl IssuerCache {
    pub fn new() -> Self {
        Self {
            cache: DashMap::with_capacity(256),
        }
    }

    pub fn get(&self, fingerprint: &str) -> Option<Bytes> {
        self.cache.get(fingerprint).map(|v| v.value().clone())
    }

    pub fn insert(&self, fingerprint: String, data: Bytes) {
        if self.cache.len() < MAX_ISSUER_CACHE_SIZE {
            self.cache.insert(fingerprint, data);
        }
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }
}

impl Default for IssuerCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a checkpoint text response.
///
/// Format:
/// ```text
/// <origin>\n
/// <tree_size>\n
/// <base64_root_hash>\n
/// \n
/// — <signature line(s)>
/// ```
pub fn parse_checkpoint(text: &str) -> Option<Checkpoint> {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() < 3 {
        return None;
    }

    let origin = lines[0].to_string();
    let tree_size: u64 = lines[1].parse().ok()?;
    let root_hash = lines[2].to_string();

    Some(Checkpoint {
        origin,
        tree_size,
        root_hash,
    })
}

/// Encode a tile index into a hierarchical path (groups of 3 digits, "x" prefix for dirs).
///
/// Examples: 0→"000", 999→"999", 1234→"x001/234", 1234567→"x001/x234/567"
pub fn encode_tile_path(n: u64) -> String {
    if n < 1000 {
        return format!("{:03}", n);
    }

    // u64 max (~1.8e19) needs at most 7 groups of 3 digits — no heap alloc needed
    let mut groups = [0u16; 7];
    let mut count = 0;
    let mut remaining = n;
    while remaining > 0 {
        groups[count] = (remaining % 1000) as u16;
        count += 1;
        remaining /= 1000;
    }
    groups[..count].reverse();

    let mut result = String::with_capacity(count * 5);
    for (i, group) in groups.iter().enumerate().take(count) {
        if i > 0 {
            result.push('/');
        }
        if i < count - 1 {
            result.push('x');
        }
        let _ = write!(result, "{:03}", group);
    }

    result
}

/// Build the URL for a data tile fetch.
///
/// `tile_index` is the 0-based index of the 256-leaf tile.
/// `partial_width` is the number of leaves in a partial tile (0 means full 256-leaf tile).
///
/// Per the static-ct-api spec, data tile URLs do NOT include a level parameter:
/// `<prefix>/tile/data/<N>[.p/<W>]`
pub fn tile_url(base_url: &str, _level: u8, tile_index: u64, partial_width: u64) -> String {
    let base = base_url.trim_end_matches('/');
    let path = encode_tile_path(tile_index);
    if partial_width > 0 && partial_width < 256 {
        format!("{}/tile/data/{}.p/{}", base, path, partial_width)
    } else {
        format!("{}/tile/data/{}", base, path)
    }
}

/// Parse binary tile leaf entries. Each entry: 8B timestamp + 2B type (0=x509, 1=precert),
/// followed by type-specific cert data, extensions, and chain fingerprints.
pub fn parse_tile_leaves(data: &[u8]) -> Vec<TileLeaf> {
    let mut leaves = Vec::new();
    let mut offset = 0;

    while offset < data.len() {
        match parse_one_leaf(data, &mut offset) {
            Some(leaf) => leaves.push(leaf),
            None => break,
        }
    }

    leaves
}

fn parse_one_leaf(data: &[u8], offset: &mut usize) -> Option<TileLeaf> {
    // Need at least 10 bytes for timestamp + entry_type
    if *offset + 10 > data.len() {
        return None;
    }

    let timestamp = u64::from_be_bytes(data[*offset..*offset + 8].try_into().ok()?);
    *offset += 8;

    let entry_type = u16::from_be_bytes(data[*offset..*offset + 2].try_into().ok()?);
    *offset += 2;

    let is_precert = entry_type == 1;

    let cert_der;

    if entry_type == 0 {
        // x509 entry
        if *offset + 3 > data.len() {
            return None;
        }
        let cert_len = read_u24(data, *offset)?;
        *offset += 3;

        if *offset + cert_len > data.len() {
            return None;
        }
        cert_der = data[*offset..*offset + cert_len].to_vec();
        *offset += cert_len;
    } else if entry_type == 1 {
        if *offset + 32 > data.len() {
            return None;
        }
        *offset += 32; // issuer_key_hash

        if *offset + 3 > data.len() {
            return None;
        }
        let tbs_len = read_u24(data, *offset)?;
        *offset += 3;

        if *offset + tbs_len > data.len() {
            return None;
        }
        *offset += tbs_len;

        if *offset + 2 > data.len() {
            return None;
        }
        let ext_len = u16::from_be_bytes(data[*offset..*offset + 2].try_into().ok()?) as usize;
        *offset += 2;
        if *offset + ext_len > data.len() {
            return None;
        }
        *offset += ext_len;

        if *offset + 3 > data.len() {
            return None;
        }
        let precert_len = read_u24(data, *offset)?;
        *offset += 3;

        if *offset + precert_len > data.len() {
            return None;
        }
        cert_der = data[*offset..*offset + precert_len].to_vec();
        *offset += precert_len;

        if *offset + 2 > data.len() {
            return None;
        }
        let chain_count = u16::from_be_bytes(data[*offset..*offset + 2].try_into().ok()?) as usize;
        *offset += 2;

        let mut chain_fingerprints = Vec::with_capacity(chain_count);
        for _ in 0..chain_count {
            if *offset + 32 > data.len() {
                return None;
            }
            let mut fp = [0u8; 32];
            fp.copy_from_slice(&data[*offset..*offset + 32]);
            chain_fingerprints.push(fp);
            *offset += 32;
        }

        return Some(TileLeaf {
            timestamp,
            entry_type,
            cert_der,
            is_precert,
            chain_fingerprints,
        });
    } else {
        return None;
    }

    if *offset + 2 > data.len() {
        return None;
    }
    let ext_len = u16::from_be_bytes(data[*offset..*offset + 2].try_into().ok()?) as usize;
    *offset += 2;
    if *offset + ext_len > data.len() {
        return None;
    }
    *offset += ext_len;

    if *offset + 2 > data.len() {
        return None;
    }
    let chain_count = u16::from_be_bytes(data[*offset..*offset + 2].try_into().ok()?) as usize;
    *offset += 2;

    let mut chain_fingerprints = Vec::with_capacity(chain_count);
    for _ in 0..chain_count {
        if *offset + 32 > data.len() {
            return None;
        }
        let mut fp = [0u8; 32];
        fp.copy_from_slice(&data[*offset..*offset + 32]);
        chain_fingerprints.push(fp);
        *offset += 32;
    }

    Some(TileLeaf {
        timestamp,
        entry_type,
        cert_der,
        is_precert,
        chain_fingerprints,
    })
}

fn read_u24(data: &[u8], offset: usize) -> Option<usize> {
    if offset + 3 > data.len() {
        return None;
    }
    Some(u32::from_be_bytes([0, data[offset], data[offset + 1], data[offset + 2]]) as usize)
}

/// Format a 32-byte fingerprint as lowercase hex.
pub fn fingerprint_hex(fp: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in fp {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

pub async fn fetch_issuer(
    client: &Client,
    base_url: &str,
    fingerprint: &[u8; 32],
    cache: &IssuerCache,
    timeout: Duration,
) -> Option<Bytes> {
    let hex = fingerprint_hex(fingerprint);

    if let Some(cached) = cache.get(&hex) {
        metrics::counter!("certstream_issuer_cache_hits").increment(1);
        return Some(cached);
    }

    metrics::counter!("certstream_issuer_cache_misses").increment(1);

    let base = base_url.trim_end_matches('/');
    let url = format!("{}/issuer/{}", base, hex);

    match client.get(&url).timeout(timeout).send().await {
        Ok(resp) if resp.status().is_success() => match resp.bytes().await {
            Ok(bytes) => {
                cache.insert(hex, bytes.clone());
                metrics::gauge!("certstream_issuer_cache_size").set(cache.len() as f64);
                Some(bytes)
            }
            Err(e) => {
                warn!(url = %url, error = %e, "failed to read issuer response body");
                None
            }
        },
        Ok(resp) => {
            debug!(url = %url, status = %resp.status(), "issuer fetch returned non-success");
            None
        }
        Err(e) => {
            debug!(url = %url, error = %e, "failed to fetch issuer");
            None
        }
    }
}

/// Decompress gzipped tile data. Returns borrowed data if not gzipped.
pub fn decompress_tile(data: &[u8]) -> Cow<'_, [u8]> {
    if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        match decoder.read_to_end(&mut decompressed) {
            Ok(_) => Cow::Owned(decompressed),
            Err(_) => Cow::Borrowed(data),
        }
    } else {
        Cow::Borrowed(data)
    }
}

/// Static CT (checkpoint + tile protocol) watcher loop.
#[allow(clippy::too_many_arguments)]
pub async fn run_static_ct_watcher(log: CtLog, ctx: WatcherContext) {
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
    use tokio::time::sleep;

    let base_url = log.normalized_url();
    let log_name = log.description.clone();
    let source = Arc::new(Source {
        name: Arc::from(log.description.as_str()),
        url: Arc::from(base_url.as_str()),
    });

    let health = Arc::new(LogHealth::new());
    let issuer_cache = Arc::new(IssuerCache::new());
    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    let timeout = Duration::from_secs(config.request_timeout_secs);

    info!(log = %log_name, url = %base_url, "starting static CT watcher");

    let checkpoint_url = format!("{}/checkpoint", base_url);

    let mut current_index = if let Some(saved_index) = state_manager.get_index(&base_url) {
        info!(log = %log.description, saved_index = saved_index, "resuming from saved state");
        saved_index
    } else {
        match client.get(&checkpoint_url).timeout(timeout).send().await {
            Ok(resp) => match resp.text().await {
                Ok(text) => match parse_checkpoint(&text) {
                    Some(cp) => {
                        let start = cp.tree_size.saturating_sub(256);
                        info!(log = %log.description, tree_size = cp.tree_size, starting_at = start, "starting fresh (static CT)");
                        start
                    }
                    None => {
                        error!(log = %log.description, "failed to parse initial checkpoint");
                        metrics::counter!("certstream_static_ct_checkpoint_errors").increment(1);
                        0
                    }
                },
                Err(e) => {
                    error!(log = %log.description, error = %e, "failed to read checkpoint response");
                    metrics::counter!("certstream_static_ct_checkpoint_errors").increment(1);
                    0
                }
            },
            Err(e) => {
                error!(log = %log.description, error = %e, "failed to fetch initial checkpoint");
                metrics::counter!("certstream_static_ct_checkpoint_errors").increment(1);
                0
            }
        }
    };

    loop {
        if shutdown.is_cancelled() {
            info!(log = %log.description, "shutdown signal received (static CT)");
            break;
        }

        if !health.should_attempt() {
            debug!(log = %log.description, "circuit breaker open, waiting (static CT)");
            sleep(Duration::from_secs(config.health_check_interval_secs)).await;
            continue;
        }

        if !health.is_healthy() {
            warn!(log = %log.description, errors = health.total_errors(), "static CT log is unhealthy, waiting for recovery");
            sleep(Duration::from_secs(config.health_check_interval_secs)).await;

            match client.get(&checkpoint_url).timeout(timeout).send().await {
                Ok(resp) if resp.status().is_success() => {
                    health.record_success(config.healthy_threshold);
                    info!(log = %log.description, "health check passed (static CT), resuming");
                }
                _ => {
                    health.record_failure(config.unhealthy_threshold);
                    metrics::counter!("certstream_log_health_checks_failed").increment(1);
                    continue;
                }
            }
        }

        let tree_size = match fetch::get_checkpoint_tree_size(&client, &base_url, timeout).await {
            Ok(size) => size,
            Err(e) => {
                health.record_failure(config.unhealthy_threshold);
                warn!(log = %log.description, error = %e, "failed to fetch or parse checkpoint");
                metrics::counter!("certstream_static_ct_checkpoint_errors").increment(1);
                sleep(health.get_backoff()).await;
                continue;
            }
        };

        if current_index >= tree_size {
            sleep(poll_interval).await;
            continue;
        }

        let start_tile = current_index / 256;
        let end_tile = (tree_size.saturating_sub(1)) / 256;

        let tile_index = start_tile;
        if tile_index > end_tile {
            sleep(poll_interval).await;
            continue;
        }

        let is_last_tile = tile_index == end_tile;
        let entries_in_tile = if is_last_tile {
            let remainder = tree_size % 256;
            if remainder == 0 {
                256
            } else {
                remainder
            }
        } else {
            256
        };

        let partial_width = if is_last_tile && entries_in_tile < 256 {
            entries_in_tile
        } else {
            0
        };

        let tile_start_index = tile_index * 256;
        let offset_in_tile = if current_index > tile_start_index {
            (current_index - tile_start_index) as usize
        } else {
            0
        };

        // Respect per-operator rate limit before making request
        if let Some(ref limiter) = rate_limiter {
            limiter.lock().await.tick().await;
        }

        match fetch::fetch_tile_entries(&client, &base_url, tile_index, partial_width, offset_in_tile, &source, timeout, &issuer_cache).await {
            Ok(messages) => {
                health.record_success(config.healthy_threshold);
                metrics::counter!("certstream_static_ct_tiles_fetched", "log" => log_name.clone()).increment(1);

                metrics::counter!("certstream_static_ct_entries_parsed", "log" => log_name.clone()).increment(messages.len() as u64);

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

                let next_index = ((tile_index + 1) * 256).min(tree_size);
                current_index = next_index;
                state_manager.update_index(&base_url, current_index, tree_size);

                tracker.update(
                    &base_url,
                    *health.status.read(),
                    current_index,
                    tree_size,
                    health.total_errors(),
                );

                debug!(log = %log.description, tile = tile_index, "processed static CT tile");
            }
            Err(e) => {
                match e {
                    FetchError::RateLimited(_) => {
                        health.record_rate_limit(config.unhealthy_threshold);
                        warn!(log = %log.description, "rate limited by static CT log, backing off 30s");
                    }
                    _ => {
                        health.record_failure(config.unhealthy_threshold);
                        warn!(log = %log.description, error = %e, "failed to fetch tile");
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
    fn test_parse_checkpoint() {
        let text = "example.com/log\n12345\nABCDEF==\n\n— signature";
        let cp = parse_checkpoint(text).unwrap();
        assert_eq!(cp.origin, "example.com/log");
        assert_eq!(cp.tree_size, 12345);
        assert_eq!(cp.root_hash, "ABCDEF==");
    }

    #[test]
    fn test_parse_checkpoint_invalid() {
        assert!(parse_checkpoint("too\nshort").is_none());
        assert!(parse_checkpoint("origin\nnot_a_number\nhash").is_none());
    }

    #[test]
    fn test_encode_tile_path() {
        assert_eq!(encode_tile_path(0), "000");
        assert_eq!(encode_tile_path(5), "005");
        assert_eq!(encode_tile_path(999), "999");
        assert_eq!(encode_tile_path(1000), "x001/000");
        assert_eq!(encode_tile_path(1234), "x001/234");
        assert_eq!(encode_tile_path(123456), "x123/456");
        assert_eq!(encode_tile_path(1234567), "x001/x234/567");
    }

    #[test]
    fn test_tile_url_full() {
        let url = tile_url("https://example.com/log/", 0, 5, 0);
        assert_eq!(url, "https://example.com/log/tile/data/005");
    }

    #[test]
    fn test_tile_url_partial() {
        let url = tile_url("https://example.com/log", 0, 5, 100);
        assert_eq!(url, "https://example.com/log/tile/data/005.p/100");
    }

    #[test]
    fn test_fingerprint_hex() {
        let fp = [0xab; 32];
        let hex = fingerprint_hex(&fp);
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c == 'a' || c == 'b'));
    }

    #[test]
    fn test_decompress_tile_passthrough() {
        let data = b"not gzipped data";
        let result = decompress_tile(data);
        assert_eq!(&*result, &data[..]);
    }

    #[test]
    fn test_decompress_tile_gzip() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let original = b"hello, this is tile data that should be compressed";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        assert_eq!(compressed[0], 0x1f);
        assert_eq!(compressed[1], 0x8b);

        let decompressed = decompress_tile(&compressed);
        assert_eq!(&*decompressed, &original[..]);
    }

    #[test]
    fn test_decompress_tile_corrupt_gzip() {
        let data = vec![0x1f, 0x8b, 0x08, 0x00, 0xff, 0xff];
        let result = decompress_tile(&data);
        assert_eq!(&*result, &data[..]);
    }

    #[test]
    fn test_issuer_cache_new() {
        let cache = IssuerCache::new();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_issuer_cache_insert_and_get() {
        let cache = IssuerCache::new();
        let data = Bytes::from(vec![1, 2, 3, 4]);

        cache.insert("abc123".to_string(), data.clone());
        assert_eq!(cache.len(), 1);

        let retrieved = cache.get("abc123").unwrap();
        assert_eq!(&retrieved[..], &[1, 2, 3, 4]);
    }

    #[test]
    fn test_issuer_cache_get_missing() {
        let cache = IssuerCache::new();
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_issuer_cache_overwrite() {
        let cache = IssuerCache::new();
        cache.insert("key".to_string(), Bytes::from(vec![1, 2, 3]));
        cache.insert("key".to_string(), Bytes::from(vec![4, 5, 6]));
        assert_eq!(cache.len(), 1);
        assert_eq!(&cache.get("key").unwrap()[..], &[4, 5, 6]);
    }

    #[test]
    fn test_issuer_cache_multiple_entries() {
        let cache = IssuerCache::new();
        for i in 0..100 {
            cache.insert(format!("fp_{:02x}", i), Bytes::from(vec![i as u8]));
        }
        assert_eq!(cache.len(), 100);
        assert_eq!(&cache.get("fp_00").unwrap()[..], &[0]);
        assert_eq!(&cache.get("fp_63").unwrap()[..], &[99]);
    }

    /// Build a synthetic x509 tile entry.
    fn build_x509_entry(timestamp: u64, cert_der: &[u8], chain_fps: &[[u8; 32]]) -> Vec<u8> {
        let mut data = Vec::new();
        // 8 bytes timestamp
        data.extend_from_slice(&timestamp.to_be_bytes());
        // 2 bytes entry_type = 0 (x509)
        data.extend_from_slice(&0u16.to_be_bytes());
        // 3 bytes cert length
        let cert_len = cert_der.len() as u32;
        data.push((cert_len >> 16) as u8);
        data.push((cert_len >> 8) as u8);
        data.push(cert_len as u8);
        // cert DER
        data.extend_from_slice(cert_der);
        // 2 bytes extensions length = 0
        data.extend_from_slice(&0u16.to_be_bytes());
        // 2 bytes chain count
        data.extend_from_slice(&(chain_fps.len() as u16).to_be_bytes());
        // chain fingerprints
        for fp in chain_fps {
            data.extend_from_slice(fp);
        }
        data
    }

    /// Build a synthetic precert tile entry.
    fn build_precert_entry(
        timestamp: u64,
        issuer_key_hash: &[u8; 32],
        tbs_cert: &[u8],
        precert_der: &[u8],
        chain_fps: &[[u8; 32]],
    ) -> Vec<u8> {
        let mut data = Vec::new();
        // 8 bytes timestamp
        data.extend_from_slice(&timestamp.to_be_bytes());
        // 2 bytes entry_type = 1 (precert)
        data.extend_from_slice(&1u16.to_be_bytes());
        // 32 bytes issuer_key_hash
        data.extend_from_slice(issuer_key_hash);
        // 3 bytes TBS cert length
        let tbs_len = tbs_cert.len() as u32;
        data.push((tbs_len >> 16) as u8);
        data.push((tbs_len >> 8) as u8);
        data.push(tbs_len as u8);
        // TBS cert
        data.extend_from_slice(tbs_cert);
        // 2 bytes extensions length = 0
        data.extend_from_slice(&0u16.to_be_bytes());
        // 3 bytes pre-certificate DER length
        let precert_len = precert_der.len() as u32;
        data.push((precert_len >> 16) as u8);
        data.push((precert_len >> 8) as u8);
        data.push(precert_len as u8);
        // pre-certificate DER
        data.extend_from_slice(precert_der);
        // 2 bytes chain count
        data.extend_from_slice(&(chain_fps.len() as u16).to_be_bytes());
        // chain fingerprints
        for fp in chain_fps {
            data.extend_from_slice(fp);
        }
        data
    }

    #[test]
    fn test_parse_tile_leaves_single_x509() {
        let cert = b"fake_cert_der_data";
        let fp1 = [0xaa; 32];
        let data = build_x509_entry(1700000000000, cert, &[fp1]);

        let leaves = parse_tile_leaves(&data);
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].timestamp, 1700000000000);
        assert_eq!(leaves[0].entry_type, 0);
        assert!(!leaves[0].is_precert);
        assert_eq!(leaves[0].cert_der, cert);
        assert_eq!(leaves[0].chain_fingerprints.len(), 1);
        assert_eq!(leaves[0].chain_fingerprints[0], fp1);
    }

    #[test]
    fn test_parse_tile_leaves_single_precert() {
        let issuer_hash = [0xbb; 32];
        let tbs = b"fake_tbs_cert";
        let precert = b"fake_precert_der";
        let fp1 = [0xcc; 32];
        let fp2 = [0xdd; 32];
        let data = build_precert_entry(1700000001000, &issuer_hash, tbs, precert, &[fp1, fp2]);

        let leaves = parse_tile_leaves(&data);
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].timestamp, 1700000001000);
        assert_eq!(leaves[0].entry_type, 1);
        assert!(leaves[0].is_precert);
        assert_eq!(leaves[0].cert_der, precert);
        assert_eq!(leaves[0].chain_fingerprints.len(), 2);
        assert_eq!(leaves[0].chain_fingerprints[0], fp1);
        assert_eq!(leaves[0].chain_fingerprints[1], fp2);
    }

    #[test]
    fn test_parse_tile_leaves_multiple_entries() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_x509_entry(1000, b"cert1", &[]));
        data.extend_from_slice(&build_x509_entry(2000, b"cert2", &[[0xee; 32]]));

        let issuer_hash = [0xff; 32];
        data.extend_from_slice(&build_precert_entry(3000, &issuer_hash, b"tbs", b"precert", &[]));

        let leaves = parse_tile_leaves(&data);
        assert_eq!(leaves.len(), 3);

        assert_eq!(leaves[0].timestamp, 1000);
        assert!(!leaves[0].is_precert);
        assert_eq!(leaves[0].cert_der, b"cert1");

        assert_eq!(leaves[1].timestamp, 2000);
        assert!(!leaves[1].is_precert);
        assert_eq!(leaves[1].cert_der, b"cert2");
        assert_eq!(leaves[1].chain_fingerprints.len(), 1);

        assert_eq!(leaves[2].timestamp, 3000);
        assert!(leaves[2].is_precert);
        assert_eq!(leaves[2].cert_der, b"precert");
    }

    #[test]
    fn test_parse_tile_leaves_empty() {
        let data: &[u8] = &[];
        let leaves = parse_tile_leaves(data);
        assert!(leaves.is_empty());
    }

    #[test]
    fn test_parse_tile_leaves_truncated() {
        // Only 5 bytes (needs at least 10 for timestamp + entry_type)
        let data = [0u8; 5];
        let leaves = parse_tile_leaves(&data);
        assert!(leaves.is_empty());
    }

    #[test]
    fn test_parse_tile_leaves_x509_no_chain() {
        let cert = b"test";
        let data = build_x509_entry(5000, cert, &[]);

        let leaves = parse_tile_leaves(&data);
        assert_eq!(leaves.len(), 1);
        assert!(leaves[0].chain_fingerprints.is_empty());
    }

    #[test]
    fn test_parse_tile_leaves_precert_no_chain() {
        let data = build_precert_entry(6000, &[0; 32], b"tbs", b"precert", &[]);
        let leaves = parse_tile_leaves(&data);
        assert_eq!(leaves.len(), 1);
        assert!(leaves[0].chain_fingerprints.is_empty());
    }

    #[test]
    fn test_read_u24() {
        assert_eq!(read_u24(&[0, 0, 5], 0), Some(5));
        assert_eq!(read_u24(&[0, 1, 0], 0), Some(256));
        assert_eq!(read_u24(&[1, 0, 0], 0), Some(65536));
        assert_eq!(read_u24(&[0xff, 0xff, 0xff], 0), Some(16777215));
        // With offset
        assert_eq!(read_u24(&[0x99, 0, 0, 10], 1), Some(10));
        // Too short
        assert_eq!(read_u24(&[0, 0], 0), None);
    }

    #[test]
    fn test_parse_checkpoint_large_tree_size() {
        let text = "origin\n999999999999\nhash\n\nsig";
        let cp = parse_checkpoint(text).unwrap();
        assert_eq!(cp.tree_size, 999999999999);
    }

    #[test]
    fn test_parse_checkpoint_zero_tree_size() {
        let text = "origin\n0\nhash";
        let cp = parse_checkpoint(text).unwrap();
        assert_eq!(cp.tree_size, 0);
    }

    #[test]
    fn test_parse_checkpoint_empty_string() {
        assert!(parse_checkpoint("").is_none());
    }

    #[test]
    fn test_tile_url_large_index() {
        let url = tile_url("https://example.com", 0, 1234567, 0);
        assert_eq!(url, "https://example.com/tile/data/x001/x234/567");
    }

    #[test]
    fn test_tile_url_full_256_is_not_partial() {
        // partial_width=256 should be treated as full tile
        let url = tile_url("https://example.com", 0, 0, 256);
        assert_eq!(url, "https://example.com/tile/data/000");
    }

    #[test]
    fn test_fingerprint_hex_zeros() {
        let fp = [0u8; 32];
        let hex = fingerprint_hex(&fp);
        assert_eq!(hex, "0000000000000000000000000000000000000000000000000000000000000000");
    }

    #[test]
    fn test_fingerprint_hex_all_ff() {
        let fp = [0xff; 32];
        let hex = fingerprint_hex(&fp);
        assert_eq!(hex, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    }

    #[test]
    fn test_fingerprint_hex_mixed() {
        let mut fp = [0u8; 32];
        fp[0] = 0xde;
        fp[1] = 0xad;
        fp[2] = 0xbe;
        fp[3] = 0xef;
        let hex = fingerprint_hex(&fp);
        assert!(hex.starts_with("deadbeef"));
        assert_eq!(hex.len(), 64);
    }
}
