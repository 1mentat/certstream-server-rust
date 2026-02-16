use crate::models::CertificateMessage;
use chrono::prelude::*;
use deltalake::arrow::array::*;
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use deltalake::arrow::record_batch::RecordBatch;
use serde_json;
use std::sync::Arc;

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

/// Returns the Arrow schema for the Delta table.
///
/// The schema defines the columnar structure for storing certificate records.
pub fn delta_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("cert_index", DataType::UInt64, false),
        Field::new("update_type", DataType::Utf8, false),
        Field::new(
            "seen",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("seen_date", DataType::Utf8, false),
        Field::new("source_name", DataType::Utf8, false),
        Field::new("source_url", DataType::Utf8, false),
        Field::new("cert_link", DataType::Utf8, false),
        Field::new("serial_number", DataType::Utf8, false),
        Field::new("fingerprint", DataType::Utf8, false),
        Field::new("sha256", DataType::Utf8, false),
        Field::new("sha1", DataType::Utf8, false),
        Field::new("not_before", DataType::Int64, false),
        Field::new("not_after", DataType::Int64, false),
        Field::new("is_ca", DataType::Boolean, false),
        Field::new("signature_algorithm", DataType::Utf8, false),
        Field::new("subject_aggregated", DataType::Utf8, false),
        Field::new("issuer_aggregated", DataType::Utf8, false),
        Field::new(
            "all_domains",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("as_der", DataType::Utf8, false),
        Field::new(
            "chain",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]))
}

/// Converts a batch of `DeltaCertRecord`s into an Arrow `RecordBatch`.
///
/// # Arguments
/// * `records` - Slice of DeltaCertRecord to convert
/// * `schema` - Arrow schema matching the table structure
///
/// # Returns
/// * `Ok(RecordBatch)` with all columns properly typed and populated
/// * `Err(ArrowError)` if array construction fails
pub fn records_to_batch(
    records: &[DeltaCertRecord],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, deltalake::arrow::error::ArrowError> {
    // Build cert_index column (UInt64)
    let cert_index: UInt64Array = records.iter().map(|r| r.cert_index).collect();

    // Build update_type column (Utf8)
    let update_type: StringArray = records
        .iter()
        .map(|r| Some(r.update_type.as_str()))
        .collect();

    // Build seen column (Timestamp in microseconds)
    let seen: TimestampMicrosecondArray = records
        .iter()
        .map(|r| {
            let micros = (r.seen * 1_000_000.0) as i64;
            Some(micros)
        })
        .collect();
    let seen = seen.with_timezone("UTC");

    // Build seen_date column (Utf8)
    let seen_date: StringArray = records
        .iter()
        .map(|r| Some(r.seen_date.as_str()))
        .collect();

    // Build source_name column (Utf8)
    let source_name: StringArray = records
        .iter()
        .map(|r| Some(r.source_name.as_str()))
        .collect();

    // Build source_url column (Utf8)
    let source_url: StringArray = records
        .iter()
        .map(|r| Some(r.source_url.as_str()))
        .collect();

    // Build cert_link column (Utf8)
    let cert_link: StringArray = records
        .iter()
        .map(|r| Some(r.cert_link.as_str()))
        .collect();

    // Build serial_number column (Utf8)
    let serial_number: StringArray = records
        .iter()
        .map(|r| Some(r.serial_number.as_str()))
        .collect();

    // Build fingerprint column (Utf8)
    let fingerprint: StringArray = records
        .iter()
        .map(|r| Some(r.fingerprint.as_str()))
        .collect();

    // Build sha256 column (Utf8)
    let sha256: StringArray = records
        .iter()
        .map(|r| Some(r.sha256.as_str()))
        .collect();

    // Build sha1 column (Utf8)
    let sha1: StringArray = records.iter().map(|r| Some(r.sha1.as_str())).collect();

    // Build not_before column (Int64)
    let not_before: Int64Array = records.iter().map(|r| r.not_before).collect();

    // Build not_after column (Int64)
    let not_after: Int64Array = records.iter().map(|r| r.not_after).collect();

    // Build is_ca column (Boolean)
    let is_ca: BooleanArray = records.iter().map(|r| Some(r.is_ca)).collect();

    // Build signature_algorithm column (Utf8)
    let signature_algorithm: StringArray = records
        .iter()
        .map(|r| Some(r.signature_algorithm.as_str()))
        .collect();

    // Build subject_aggregated column (Utf8)
    let subject_aggregated: StringArray = records
        .iter()
        .map(|r| Some(r.subject_aggregated.as_str()))
        .collect();

    // Build issuer_aggregated column (Utf8)
    let issuer_aggregated: StringArray = records
        .iter()
        .map(|r| Some(r.issuer_aggregated.as_str()))
        .collect();

    // Build all_domains column (List(Utf8))
    let mut all_domains_builder = ListBuilder::new(StringBuilder::new());
    for record in records {
        for domain in &record.all_domains {
            all_domains_builder.values().append_value(domain);
        }
        all_domains_builder.append(true);
    }
    let all_domains = all_domains_builder.finish();

    // Build chain column (List(Utf8))
    let mut chain_builder = ListBuilder::new(StringBuilder::new());
    for record in records {
        for chain_json in &record.chain {
            chain_builder.values().append_value(chain_json);
        }
        chain_builder.append(true);
    }
    let chain = chain_builder.finish();

    // Build as_der column (Utf8)
    let as_der: StringArray = records
        .iter()
        .map(|r| Some(r.as_der.as_str()))
        .collect();

    // Create RecordBatch
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(cert_index),
            Arc::new(update_type),
            Arc::new(seen),
            Arc::new(seen_date),
            Arc::new(source_name),
            Arc::new(source_url),
            Arc::new(cert_link),
            Arc::new(serial_number),
            Arc::new(fingerprint),
            Arc::new(sha256),
            Arc::new(sha1),
            Arc::new(not_before),
            Arc::new(not_after),
            Arc::new(is_ca),
            Arc::new(signature_algorithm),
            Arc::new(subject_aggregated),
            Arc::new(issuer_aggregated),
            Arc::new(all_domains),
            Arc::new(as_der),
            Arc::new(chain),
        ],
    )
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
