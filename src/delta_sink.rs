use crate::models::CertificateMessage;
use chrono::prelude::*;
use serde_json;

/// Represents a flattened certificate record for Delta table storage.
///
/// This struct deserializes from the `full` JSON bytes (a serialized `CertificateMessage`)
/// and flattens the nested hierarchy into a single-level structure suitable for Arrow conversion.
pub struct DeltaCertRecord {
    pub cert_index: u64,
    pub update_type: String,
    pub seen: f64,
    pub seen_date: String,
    pub source_name: String,
    pub source_url: String,
    pub cert_link: String,
    pub serial_number: String,
    pub fingerprint: String,
    pub sha256: String,
    pub sha1: String,
    pub not_before: i64,
    pub not_after: i64,
    pub is_ca: bool,
    pub signature_algorithm: String,
    pub subject_aggregated: String,
    pub issuer_aggregated: String,
    pub all_domains: Vec<String>,
    pub as_der: String,
    pub chain: Vec<String>,
}

impl DeltaCertRecord {
    /// Deserialize from JSON bytes and flatten the nested certificate message structure.
    ///
    /// # Arguments
    /// * `bytes` - JSON bytes containing a serialized `CertificateMessage`
    ///
    /// # Returns
    /// * `Ok(DeltaCertRecord)` with all fields populated
    /// * `Err(serde_json::Error)` if deserialization fails
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let msg: CertificateMessage = serde_json::from_slice(bytes)?;

        // Convert seen (f64 seconds since epoch) to a date string (YYYY-MM-DD)
        let seen_date = {
            let seconds = msg.data.seen as i64;
            let nanos = ((msg.data.seen - seconds as f64) * 1e9) as u32;
            if let Some(dt) = Utc.timestamp_opt(seconds, nanos).single() {
                dt.format("%Y-%m-%d").to_string()
            } else {
                "1970-01-01".to_string()
            }
        };

        // Serialize chain certs to JSON strings, or empty vec if None
        let chain = msg
            .data
            .chain
            .unwrap_or_default()
            .into_iter()
            .map(|cert| serde_json::to_string(&cert).unwrap_or_default())
            .collect();

        // Extract all_domains as Vec<String>
        let all_domains: Vec<String> = msg
            .data
            .leaf_cert
            .all_domains
            .iter()
            .cloned()
            .collect();

        Ok(DeltaCertRecord {
            cert_index: msg.data.cert_index,
            update_type: msg.data.update_type.to_string(),
            seen: msg.data.seen,
            seen_date,
            source_name: msg.data.source.name.to_string(),
            source_url: msg.data.source.url.to_string(),
            cert_link: msg.data.cert_link,
            serial_number: msg.data.leaf_cert.serial_number,
            fingerprint: msg.data.leaf_cert.fingerprint,
            sha256: msg.data.leaf_cert.sha256,
            sha1: msg.data.leaf_cert.sha1,
            not_before: msg.data.leaf_cert.not_before,
            not_after: msg.data.leaf_cert.not_after,
            is_ca: msg.data.leaf_cert.is_ca,
            signature_algorithm: msg.data.leaf_cert.signature_algorithm,
            subject_aggregated: msg.data.leaf_cert.subject.aggregated.unwrap_or_default(),
            issuer_aggregated: msg.data.leaf_cert.issuer.aggregated.unwrap_or_default(),
            all_domains,
            as_der: msg.data.leaf_cert.as_der.unwrap_or_default(),
            chain,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CertificateData, ChainCert, Extensions, LeafCert, Source, Subject};
    use smallvec::smallvec;
    use std::borrow::Cow;
    use std::sync::Arc;

    fn make_test_message() -> CertificateMessage {
        CertificateMessage {
            message_type: Cow::Borrowed("certificate_update"),
            data: CertificateData {
                update_type: Cow::Borrowed("X509LogEntry"),
                leaf_cert: LeafCert {
                    subject: {
                        let mut s = Subject::default();
                        s.cn = Some("example.com".into());
                        s.aggregated = Some("/CN=example.com".into());
                        s
                    },
                    issuer: {
                        let mut s = Subject::default();
                        s.cn = Some("Test CA".into());
                        s.aggregated = Some("/CN=Test CA".into());
                        s
                    },
                    serial_number: "01".into(),
                    not_before: 1700000000,
                    not_after: 1730000000,
                    fingerprint: "AA:BB".into(),
                    sha1: "CC:DD".into(),
                    sha256: "EE:FF".into(),
                    signature_algorithm: "sha256, rsa".into(),
                    is_ca: false,
                    all_domains: smallvec!["example.com".into(), "www.example.com".into()],
                    as_der: Some("base64encodedderdata".into()),
                    extensions: Extensions::default(),
                },
                chain: Some(vec![ChainCert {
                    subject: {
                        let mut s = Subject::default();
                        s.cn = Some("Intermediate CA".into());
                        s.aggregated = Some("/CN=Intermediate CA".into());
                        s
                    },
                    issuer: {
                        let mut s = Subject::default();
                        s.cn = Some("Root CA".into());
                        s.aggregated = Some("/CN=Root CA".into());
                        s
                    },
                    serial_number: "02".into(),
                    not_before: 1600000000,
                    not_after: 1800000000,
                    fingerprint: "GG:HH".into(),
                    sha1: "II:JJ".into(),
                    sha256: "KK:LL".into(),
                    signature_algorithm: "sha256, rsa".into(),
                    is_ca: true,
                    as_der: None,
                    extensions: Extensions::default(),
                }]),
                cert_index: 12345,
                cert_link: "https://ct.example.com/entry/12345".into(),
                seen: 1700000000.0,
                source: Arc::new(Source {
                    name: Arc::from("Test Log"),
                    url: Arc::from("https://ct.example.com/"),
                }),
            },
        }
    }

    #[test]
    fn test_from_json_deserializes_all_fields() {
        let msg = make_test_message();
        let json_bytes = serde_json::to_vec(&msg).expect("serialization failed");

        let record = DeltaCertRecord::from_json(&json_bytes).expect("deserialization failed");

        assert_eq!(record.cert_index, 12345);
        assert_eq!(record.update_type, "X509LogEntry");
        assert_eq!(record.seen, 1700000000.0);
        assert_eq!(record.source_name, "Test Log");
        assert_eq!(record.source_url, "https://ct.example.com/");
        assert_eq!(record.cert_link, "https://ct.example.com/entry/12345");
        assert_eq!(record.serial_number, "01");
        assert_eq!(record.fingerprint, "AA:BB");
        assert_eq!(record.sha256, "EE:FF");
        assert_eq!(record.sha1, "CC:DD");
        assert_eq!(record.not_before, 1700000000);
        assert_eq!(record.not_after, 1730000000);
        assert_eq!(record.is_ca, false);
        assert_eq!(record.signature_algorithm, "sha256, rsa");
        assert_eq!(record.subject_aggregated, "/CN=example.com");
        assert_eq!(record.issuer_aggregated, "/CN=Test CA");
        assert_eq!(record.all_domains, vec!["example.com", "www.example.com"]);
        assert_eq!(record.as_der, "base64encodedderdata");
    }

    #[test]
    fn test_from_json_derives_seen_date() {
        let msg = make_test_message();
        let json_bytes = serde_json::to_vec(&msg).expect("serialization failed");

        let record = DeltaCertRecord::from_json(&json_bytes).expect("deserialization failed");

        // 1700000000 seconds since epoch is approximately 2023-11-15
        assert_eq!(record.seen_date, "2023-11-15");
    }

    #[test]
    fn test_from_json_serializes_chain_to_json_strings() {
        let msg = make_test_message();
        let json_bytes = serde_json::to_vec(&msg).expect("serialization failed");

        let record = DeltaCertRecord::from_json(&json_bytes).expect("deserialization failed");

        assert_eq!(record.chain.len(), 1);
        // Chain cert should be serialized as JSON
        assert!(record.chain[0].contains("Intermediate CA"));
    }

    #[test]
    fn test_from_json_with_empty_chain() {
        let mut msg = make_test_message();
        msg.data.chain = None;

        let json_bytes = serde_json::to_vec(&msg).expect("serialization failed");
        let record = DeltaCertRecord::from_json(&json_bytes).expect("deserialization failed");

        assert_eq!(record.chain.len(), 0);
    }

    #[test]
    fn test_from_json_with_empty_as_der() {
        let mut msg = make_test_message();
        msg.data.leaf_cert.as_der = None;

        let json_bytes = serde_json::to_vec(&msg).expect("serialization failed");
        let record = DeltaCertRecord::from_json(&json_bytes).expect("deserialization failed");

        assert_eq!(record.as_der, "");
    }

    #[test]
    fn test_from_json_with_empty_domains() {
        let mut msg = make_test_message();
        msg.data.leaf_cert.all_domains = smallvec![];

        let json_bytes = serde_json::to_vec(&msg).expect("serialization failed");
        let record = DeltaCertRecord::from_json(&json_bytes).expect("deserialization failed");

        assert_eq!(record.all_domains.len(), 0);
    }
}
