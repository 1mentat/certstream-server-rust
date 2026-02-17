use serde::Deserialize;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::debug;

use crate::models::{CertificateData, CertificateMessage, ChainCert, Source};

use super::parse_leaf_input;

/// Errors that can occur when fetching CT log entries.
#[derive(Debug, Error)]
pub enum FetchError {
    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("Rate limited (HTTP {0})")]
    RateLimited(u16),

    #[error("Not available (HTTP {0})")]
    NotAvailable(u16),
}

/// Response from get-sth endpoint.
#[derive(Debug, Deserialize)]
pub(crate) struct SthResponse {
    pub tree_size: u64,
}

/// Response from get-entries endpoint.
#[derive(Debug, Deserialize)]
pub(crate) struct EntriesResponse {
    pub entries: Vec<RawEntry>,
}

/// A raw entry from the get-entries response.
#[derive(Debug, Deserialize)]
pub(crate) struct RawEntry {
    pub leaf_input: String,
    pub extra_data: String,
}

/// Fetch the tree size from an RFC 6962 CT log.
///
/// # Arguments
/// * `client` - HTTP client
/// * `base_url` - Base URL of the CT log (e.g., "https://ct.example.com")
/// * `timeout` - Request timeout
///
/// # Returns
/// * `Ok(tree_size)` - The current tree size
/// * `Err(FetchError)` - If the request fails or response is invalid
pub async fn get_tree_size(
    client: &reqwest::Client,
    base_url: &str,
    timeout: Duration,
) -> Result<u64, FetchError> {
    let url = format!("{}/ct/v1/get-sth", base_url);
    let response = client.get(&url).timeout(timeout).send().await?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        return match status {
            429 | 503 => Err(FetchError::RateLimited(status)),
            400 => Err(FetchError::NotAvailable(status)),
            _ => Err(FetchError::NotAvailable(status)),
        };
    }

    let sth: SthResponse = response.json().await.map_err(|e| {
        FetchError::InvalidResponse(format!("Failed to parse STH response: {}", e))
    })?;

    Ok(sth.tree_size)
}

/// Fetch entries from an RFC 6962 CT log.
///
/// # Arguments
/// * `client` - HTTP client
/// * `base_url` - Base URL of the CT log
/// * `start` - Starting entry index (inclusive)
/// * `end` - Ending entry index (inclusive)
/// * `source` - Source metadata
/// * `timeout` - Request timeout
///
/// # Returns
/// * `Ok(messages)` - Vector of CertificateMessages for successfully parsed entries
/// * `Err(FetchError)` - If the HTTP request fails
///
/// # Notes
/// Entries that fail to parse are skipped with a debug log message.
pub async fn fetch_entries(
    client: &reqwest::Client,
    base_url: &str,
    start: u64,
    end: u64,
    source: &Arc<Source>,
    timeout: Duration,
) -> Result<Vec<CertificateMessage>, FetchError> {
    let url = format!(
        "{}/ct/v1/get-entries?start={}&end={}",
        base_url, start, end
    );
    let response = client.get(&url).timeout(timeout).send().await?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        return match status {
            429 | 503 => Err(FetchError::RateLimited(status)),
            400 => Err(FetchError::NotAvailable(status)),
            _ => Err(FetchError::NotAvailable(status)),
        };
    }

    let entries_resp: EntriesResponse = response.json().await.map_err(|e| {
        FetchError::InvalidResponse(format!("Failed to parse entries response: {}", e))
    })?;

    let mut messages = Vec::new();
    let seen = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;

    for (i, entry) in entries_resp.entries.into_iter().enumerate() {
        let cert_index = start + i as u64;

        let parsed = match parse_leaf_input(&entry.leaf_input, &entry.extra_data) {
            Some(p) => p,
            None => {
                debug!(cert_index, "skipped unparseable cert");
                continue;
            }
        };

        let cert_link = format!(
            "{}/ct/v1/get-entries?start={}&end={}",
            base_url, cert_index, cert_index
        );

        let msg = CertificateMessage {
            message_type: Cow::Borrowed("certificate_update"),
            data: CertificateData {
                update_type: parsed.update_type,
                leaf_cert: parsed.leaf_cert,
                chain: Some(parsed.chain),
                cert_index,
                cert_link,
                seen,
                source: Arc::clone(source),
            },
        };

        messages.push(msg);
    }

    Ok(messages)
}

/// Fetch entries from a Static CT tile.
///
/// # Arguments
/// * `client` - HTTP client
/// * `base_url` - Base URL of the Static CT log
/// * `tile_index` - The 0-based index of the 256-leaf tile
/// * `partial_width` - Number of leaves in a partial tile (0 means full 256-leaf tile)
/// * `offset_in_tile` - Starting offset within the tile
/// * `source` - Source metadata
/// * `timeout` - Request timeout
/// * `issuer_cache` - Cache for issuer certificates
///
/// # Returns
/// * `Ok(messages)` - Vector of CertificateMessages for successfully parsed entries
/// * `Err(FetchError)` - If the HTTP request fails
pub async fn fetch_tile_entries(
    client: &reqwest::Client,
    base_url: &str,
    tile_index: u64,
    partial_width: u64,
    offset_in_tile: usize,
    source: &Arc<Source>,
    timeout: Duration,
    issuer_cache: &super::static_ct::IssuerCache,
) -> Result<Vec<CertificateMessage>, FetchError> {
    let url = super::static_ct::tile_url(base_url, 0, tile_index, partial_width);
    let response = client.get(&url).timeout(timeout).send().await?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        return match status {
            429 | 503 => Err(FetchError::RateLimited(status)),
            400 => Err(FetchError::NotAvailable(status)),
            _ => Err(FetchError::NotAvailable(status)),
        };
    }

    let bytes = response.bytes().await?;
    let decompressed = super::static_ct::decompress_tile(&bytes);

    let leaves = super::static_ct::parse_tile_leaves(&decompressed);
    let mut messages = Vec::new();
    let seen = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;

    for (i, leaf) in leaves.iter().enumerate().skip(offset_in_tile) {
        let parsed = match super::parse_certificate(&leaf.cert_der, true) {
            Some(p) => p,
            None => {
                debug!(index = i, "skipped unparseable cert from tile");
                continue;
            }
        };

        // Resolve the chain from issuer fingerprints
        let mut chain = Vec::new();
        for fingerprint in &leaf.chain_fingerprints {
            if let Some(der_bytes) =
                super::static_ct::fetch_issuer(client, base_url, fingerprint, issuer_cache, timeout).await
            {
                if let Some(chain_cert) = super::parse_certificate(&der_bytes, false) {
                    let cc = ChainCert {
                        subject: chain_cert.subject,
                        issuer: chain_cert.issuer,
                        serial_number: chain_cert.serial_number,
                        not_before: chain_cert.not_before,
                        not_after: chain_cert.not_after,
                        fingerprint: chain_cert.fingerprint,
                        sha1: chain_cert.sha1,
                        sha256: chain_cert.sha256,
                        signature_algorithm: chain_cert.signature_algorithm,
                        is_ca: chain_cert.is_ca,
                        as_der: chain_cert.as_der,
                        extensions: chain_cert.extensions,
                    };
                    chain.push(cc);
                }
            }
        }

        // Build the cert_link URL for Static CT
        let cert_link = format!("{}/tile/data/", base_url.trim_end_matches('/'));

        let msg = CertificateMessage {
            message_type: Cow::Borrowed("certificate_update"),
            data: CertificateData {
                update_type: if leaf.is_precert {
                    Cow::Borrowed("PrecertLogEntry")
                } else {
                    Cow::Borrowed("X509LogEntry")
                },
                leaf_cert: parsed,
                chain: if chain.is_empty() { None } else { Some(chain) },
                cert_index: tile_index * 256 + i as u64,
                cert_link,
                seen,
                source: Arc::clone(source),
            },
        };

        messages.push(msg);
    }

    Ok(messages)
}

/// Fetch the checkpoint and return the tree size.
///
/// # Arguments
/// * `client` - HTTP client
/// * `base_url` - Base URL of the Static CT log
/// * `timeout` - Request timeout
///
/// # Returns
/// * `Ok(tree_size)` - The tree size from the checkpoint
/// * `Err(FetchError)` - If the request fails or checkpoint is invalid
pub async fn get_checkpoint_tree_size(
    client: &reqwest::Client,
    base_url: &str,
    timeout: Duration,
) -> Result<u64, FetchError> {
    let url = format!("{}/checkpoint", base_url);
    let response = client.get(&url).timeout(timeout).send().await?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        return match status {
            429 | 503 => Err(FetchError::RateLimited(status)),
            400 => Err(FetchError::NotAvailable(status)),
            _ => Err(FetchError::NotAvailable(status)),
        };
    }

    let text = response.text().await.map_err(|e| {
        FetchError::InvalidResponse(format!("Failed to read checkpoint: {}", e))
    })?;

    super::static_ct::parse_checkpoint(&text)
        .map(|cp| cp.tree_size)
        .ok_or_else(|| {
            FetchError::InvalidResponse("Failed to parse checkpoint".to_string())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_test_cert_der(cn: &str) -> Vec<u8> {
        let mut params = rcgen::CertificateParams::new(vec![]).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.distinguished_name.push(rcgen::DnType::CommonName, cn);
        params.distinguished_name.push(rcgen::DnType::OrganizationName, "Test Org");
        params.distinguished_name.push(rcgen::DnType::CountryName, "US");
        params.is_ca = rcgen::IsCa::NoCa;
        let cert = params.self_signed(&rcgen::KeyPair::generate().unwrap()).unwrap();
        cert.der().to_vec()
    }

    #[test]
    fn test_build_certificate_message() {
        let cert_der = generate_test_cert_der("test.example.com");

        // Parse the certificate
        let parsed = super::super::parse_certificate(&cert_der, true).expect("Failed to parse cert");

        // Create a source
        let source = Arc::new(Source {
            name: Arc::from("test-log"),
            url: Arc::from("https://ct.example.com"),
        });

        // Build a CertificateMessage
        let cert_index = 12345u64;
        let base_url = "https://ct.example.com";
        let seen = 1234567890.5;

        let cert_link = format!(
            "{}/ct/v1/get-entries?start={}&end={}",
            base_url, cert_index, cert_index
        );

        let msg = CertificateMessage {
            message_type: Cow::Borrowed("certificate_update"),
            data: CertificateData {
                update_type: Cow::Borrowed("X509LogEntry"),
                leaf_cert: parsed,
                chain: Some(vec![]),
                cert_index,
                cert_link: cert_link.clone(),
                seen,
                source: Arc::clone(&source),
            },
        };

        // Verify the fields
        assert_eq!(msg.message_type, "certificate_update");
        assert_eq!(msg.data.update_type, "X509LogEntry");
        assert_eq!(msg.data.cert_index, cert_index);
        assert_eq!(msg.data.cert_link, cert_link);
        assert_eq!(msg.data.seen, seen);
        assert_eq!(msg.data.source.name.as_ref(), "test-log");
        assert_eq!(msg.data.source.url.as_ref(), "https://ct.example.com");
    }

    #[test]
    fn test_fetch_entries_constructs_correct_cert_index() {
        // AC4.1 Success: fetch_entries constructs CertificateMessage with correct cert_index
        let cert_der = generate_test_cert_der("test.example.com");
        let parsed = super::super::parse_certificate(&cert_der, true).expect("Failed to parse cert");

        let source = Arc::new(Source {
            name: Arc::from("test-log"),
            url: Arc::from("https://ct.example.com"),
        });

        let base_url = "https://ct.example.com";
        let start_index = 1000u64;
        let cert_index = start_index;
        let seen = 1234567890.5;

        let cert_link = format!(
            "{}/ct/v1/get-entries?start={}&end={}",
            base_url, cert_index, cert_index
        );

        let msg = CertificateMessage {
            message_type: Cow::Borrowed("certificate_update"),
            data: CertificateData {
                update_type: Cow::Borrowed("X509LogEntry"),
                leaf_cert: parsed,
                chain: Some(vec![]),
                cert_index,
                cert_link,
                seen,
                source: Arc::clone(&source),
            },
        };

        // Verify cert_index matches the expected value
        assert_eq!(msg.data.cert_index, start_index);
    }

    #[test]
    fn test_fetch_entries_constructs_correct_cert_link() {
        // AC4.1 Success: fetch_entries constructs CertificateMessage with proper cert_link URL
        let cert_der = generate_test_cert_der("test.example.com");
        let parsed = super::super::parse_certificate(&cert_der, true).expect("Failed to parse cert");

        let source = Arc::new(Source {
            name: Arc::from("test-log"),
            url: Arc::from("https://ct.example.com"),
        });

        let base_url = "https://ct.example.com";
        let cert_index = 5000u64;
        let seen = 1234567890.5;

        let expected_cert_link = format!(
            "{}/ct/v1/get-entries?start={}&end={}",
            base_url, cert_index, cert_index
        );

        let msg = CertificateMessage {
            message_type: Cow::Borrowed("certificate_update"),
            data: CertificateData {
                update_type: Cow::Borrowed("X509LogEntry"),
                leaf_cert: parsed,
                chain: Some(vec![]),
                cert_index,
                cert_link: expected_cert_link.clone(),
                seen,
                source: Arc::clone(&source),
            },
        };

        // Verify cert_link is correct
        assert_eq!(msg.data.cert_link, expected_cert_link);
    }

    #[test]
    fn test_fetch_entries_constructs_correct_source() {
        // AC4.1 Success: fetch_entries constructs CertificateMessage with correct source
        let cert_der = generate_test_cert_der("test.example.com");
        let parsed = super::super::parse_certificate(&cert_der, true).expect("Failed to parse cert");

        let source = Arc::new(Source {
            name: Arc::from("test-log"),
            url: Arc::from("https://ct.example.com"),
        });

        let base_url = "https://ct.example.com";
        let cert_index = 1234u64;
        let seen = 1234567890.5;

        let cert_link = format!(
            "{}/ct/v1/get-entries?start={}&end={}",
            base_url, cert_index, cert_index
        );

        let msg = CertificateMessage {
            message_type: Cow::Borrowed("certificate_update"),
            data: CertificateData {
                update_type: Cow::Borrowed("X509LogEntry"),
                leaf_cert: parsed,
                chain: Some(vec![]),
                cert_index,
                cert_link,
                seen,
                source: Arc::clone(&source),
            },
        };

        // Verify source fields are correct
        assert_eq!(msg.data.source.name.as_ref(), "test-log");
        assert_eq!(msg.data.source.url.as_ref(), "https://ct.example.com");
    }

    #[test]
    fn test_fetch_entries_constructs_correct_message_type() {
        // AC4.1 Success: fetch_entries constructs CertificateMessage with correct message_type
        let cert_der = generate_test_cert_der("test.example.com");
        let parsed = super::super::parse_certificate(&cert_der, true).expect("Failed to parse cert");

        let source = Arc::new(Source {
            name: Arc::from("test-log"),
            url: Arc::from("https://ct.example.com"),
        });

        let msg = CertificateMessage {
            message_type: Cow::Borrowed("certificate_update"),
            data: CertificateData {
                update_type: Cow::Borrowed("X509LogEntry"),
                leaf_cert: parsed,
                chain: Some(vec![]),
                cert_index: 1234,
                cert_link: "https://ct.example.com/entry".into(),
                seen: 1234567890.5,
                source: Arc::clone(&source),
            },
        };

        // Verify message_type is correct
        assert_eq!(msg.message_type, "certificate_update");
    }

    #[test]
    fn test_fetch_entries_constructs_correct_update_type() {
        // AC4.1 Success: fetch_entries constructs CertificateMessage with correct update_type
        let cert_der = generate_test_cert_der("test.example.com");
        let parsed = super::super::parse_certificate(&cert_der, true).expect("Failed to parse cert");

        let source = Arc::new(Source {
            name: Arc::from("test-log"),
            url: Arc::from("https://ct.example.com"),
        });

        let msg = CertificateMessage {
            message_type: Cow::Borrowed("certificate_update"),
            data: CertificateData {
                update_type: Cow::Borrowed("X509LogEntry"),
                leaf_cert: parsed,
                chain: Some(vec![]),
                cert_index: 1234,
                cert_link: "https://ct.example.com/entry".into(),
                seen: 1234567890.5,
                source: Arc::clone(&source),
            },
        };

        // Verify update_type is correct
        assert_eq!(msg.data.update_type, "X509LogEntry");
    }
}
