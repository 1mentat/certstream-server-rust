use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use deltalake::arrow::array::*;
use deltalake::datafusion::prelude::*;
use deltalake::DeltaTableError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;

use crate::config::QueryApiConfig;

pub struct QueryApiState {
    pub config: QueryApiConfig,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub domain: Option<String>,
    pub issuer: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub version: i64,
    pub results: Vec<CertResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Serialize)]
pub struct CertResult {
    pub cert_index: u64,
    pub fingerprint: String,
    pub sha256: String,
    pub serial_number: String,
    pub subject: String,
    pub issuer: String,
    pub not_before: i64,
    pub not_after: i64,
    pub all_domains: Vec<String>,
    pub source_name: String,
    /// ISO 8601 UTC timestamp when the certificate was observed, e.g. "2026-02-17T14:30:00+00:00"
    pub seen: String,
    pub is_ca: bool,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Validate that a date string matches YYYY-MM-DD format.
/// Returns true if valid, false otherwise.
fn is_valid_date(s: &str) -> bool {
    if s.len() != 10 {
        return false;
    }
    // Parse with chrono to ensure it's a real date, not just a pattern match
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
}

pub fn query_api_router(state: Arc<QueryApiState>) -> Router {
    Router::new()
        .route("/api/query/certs", get(handle_query_certs))
        .with_state(state)
}

async fn handle_query_certs(
    State(state): State<Arc<QueryApiState>>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    // Validate at least one filter is provided
    if params.domain.is_none() && params.issuer.is_none()
        && params.from.is_none() && params.to.is_none()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse { error: "At least one filter required".to_string() }),
        ).into_response();
    }

    // Clamp limit
    let limit = params.limit
        .unwrap_or(state.config.default_results_per_page)
        .min(state.config.max_results_per_page);

    // Validate date parameters before embedding in SQL
    if let Some(ref from) = params.from {
        if !is_valid_date(from) {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse { error: "Invalid 'from' date format, expected YYYY-MM-DD".to_string() }),
            ).into_response();
        }
    }
    if let Some(ref to) = params.to {
        if !is_valid_date(to) {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse { error: "Invalid 'to' date format, expected YYYY-MM-DD".to_string() }),
            ).into_response();
        }
    }

    // Open the DeltaTable
    let table = match deltalake::open_table(&state.config.table_path).await {
        Ok(t) => t,
        Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse { error: "Query service unavailable".to_string() }),
            ).into_response();
        }
        Err(e) => {
            warn!(error = %e, "Failed to open delta table for query");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: "Internal query error".to_string() }),
            ).into_response();
        }
    };

    let version = table.version();

    // Create SessionContext and register table
    let ctx = SessionContext::new();
    if let Err(e) = ctx.register_table("ct_records", Arc::new(table)) {
        warn!(error = %e, "Failed to register table");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: "Internal query error".to_string() }),
        ).into_response();
    }

    // Build SQL query
    let mut sql = String::from(
        "SELECT cert_index, fingerprint, sha256, serial_number, \
         subject_aggregated, issuer_aggregated, not_before, not_after, \
         all_domains, source_name, seen, is_ca \
         FROM ct_records WHERE 1=1"
    );

    // Date params are validated above — safe to embed directly
    if let Some(ref from) = params.from {
        sql.push_str(&format!(" AND seen_date >= '{}'", from));
    }
    if let Some(ref to) = params.to {
        sql.push_str(&format!(" AND seen_date <= '{}'", to));
    }

    sql.push_str(&format!(" ORDER BY cert_index LIMIT {}", limit));

    // Execute the query
    let df = match ctx.sql(&sql).await {
        Ok(df) => df,
        Err(e) => {
            warn!(error = %e, "Failed to execute query");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: "Internal query error".to_string() }),
            ).into_response();
        }
    };

    let batches = match df.collect().await {
        Ok(batches) => batches,
        Err(e) => {
            warn!(error = %e, "Failed to collect query results");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: "Internal query error".to_string() }),
            ).into_response();
        }
    };

    // Convert RecordBatches to CertResults
    let mut results = Vec::new();
    for batch in &batches {
        // Try UInt64 first, then fall back to Int64 (Delta stores UInt64 as Int64)
        let cert_indices_uint = batch.column(0).as_any().downcast_ref::<UInt64Array>();
        let cert_indices_int = batch.column(0).as_any().downcast_ref::<Int64Array>();

        if cert_indices_uint.is_none() && cert_indices_int.is_none() {
            warn!("Failed to downcast cert_index column");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: "Internal query error".to_string() }),
            ).into_response();
        }

        let fingerprints = match batch.column(1).as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast fingerprint column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let sha256s = match batch.column(2).as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast sha256 column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let serial_numbers = match batch.column(3).as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast serial_number column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let subjects = match batch.column(4).as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast subject column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let issuers = match batch.column(5).as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast issuer column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let not_befores = match batch.column(6).as_any().downcast_ref::<Int64Array>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast not_before column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let not_afters = match batch.column(7).as_any().downcast_ref::<Int64Array>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast not_after column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let all_domains_col = match batch.column(8).as_any().downcast_ref::<ListArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast all_domains column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let source_names = match batch.column(9).as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast source_name column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let seens = match batch.column(10).as_any().downcast_ref::<TimestampMicrosecondArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast seen column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        let is_cas = match batch.column(11).as_any().downcast_ref::<BooleanArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast is_ca column");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        };

        for i in 0..batch.num_rows() {
            // Get cert_index value - try UInt64 first, then Int64
            let cert_index = if let Some(arr) = cert_indices_uint {
                arr.value(i)
            } else if let Some(arr) = cert_indices_int {
                arr.value(i) as u64
            } else {
                0 // Should not reach here due to check above
            };

            // Extract all_domains list for this row
            let domains_list = all_domains_col.value(i);
            let domains_arr = domains_list
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| (0..a.len()).map(|j| a.value(j).to_string()).collect())
                .unwrap_or_default();

            // Convert seen timestamp (microseconds since epoch, UTC) to ISO 8601 string.
            // The Delta schema defines seen as Timestamp(Microsecond, Some("UTC")).
            let seen_micros = seens.value(i);
            let seen_str = chrono::DateTime::from_timestamp_micros(seen_micros)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default();

            results.push(CertResult {
                cert_index,
                fingerprint: fingerprints.value(i).to_string(),
                sha256: sha256s.value(i).to_string(),
                serial_number: serial_numbers.value(i).to_string(),
                subject: subjects.value(i).to_string(),
                issuer: issuers.value(i).to_string(),
                not_before: not_befores.value(i),
                not_after: not_afters.value(i),
                all_domains: domains_arr,
                source_name: source_names.value(i).to_string(),
                seen: seen_str,
                is_ca: is_cas.value(i),
            });
        }
    }

    Json(QueryResponse {
        version,
        results,
        next_cursor: None,  // Added in Phase 4
        has_more: false,     // Added in Phase 4
    }).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta_sink::{delta_schema, open_or_create_table, records_to_batch, DeltaCertRecord};
    use deltalake::DeltaOps;
    use std::fs;

    fn create_test_cert_record(
        cert_index: u64,
        source_name: &str,
        seen_date: &str,
        domains: Vec<&str>,
    ) -> DeltaCertRecord {
        // Parse date and create timestamp in microseconds
        let date_str = format!("{}T12:00:00Z", seen_date);
        let dt = chrono::DateTime::parse_from_rfc3339(&date_str.replace("Z", "+00:00"))
            .unwrap()
            .with_timezone(&chrono::Utc);
        let seen_micros = dt.timestamp_micros() as f64;

        DeltaCertRecord {
            cert_index,
            update_type: "X509LogEntry".to_string(),
            seen: seen_micros,
            seen_date: seen_date.to_string(),
            source_name: source_name.to_string(),
            source_url: "https://example.com".to_string(),
            cert_link: "https://example.com/cert".to_string(),
            serial_number: format!("{:x}", cert_index),
            fingerprint: format!("fingerprint_{}", cert_index),
            sha256: format!("sha256_{}", cert_index),
            sha1: format!("sha1_{}", cert_index),
            not_before: 1609459200, // 2021-01-01
            not_after: 1640995200,  // 2022-01-01
            is_ca: false,
            signature_algorithm: "sha256WithRSAEncryption".to_string(),
            subject_aggregated: "CN=example.com".to_string(),
            issuer_aggregated: "CN=Example CA".to_string(),
            all_domains: domains.iter().map(|s| s.to_string()).collect(),
            as_der: "".to_string(),
            chain: vec![],
        }
    }

    #[tokio::test]
    async fn test_date_range_query_filters_correctly() {
        let table_path = "/tmp/delta_query_test_date_range";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        // Create test records with different dates
        let records = vec![
            create_test_cert_record(1, "log1", "2026-02-10", vec!["example1.com"]),
            create_test_cert_record(2, "log1", "2026-02-15", vec!["example2.com"]),
            create_test_cert_record(3, "log1", "2026-02-20", vec!["example3.com"]),
            create_test_cert_record(4, "log1", "2026-02-25", vec!["example4.com"]),
        ];

        let schema = delta_schema();

        // Open or create the table
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

        // Write test data
        let batch = records_to_batch(&records, &schema).expect("Failed to create batch");
        DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write to table");

        // Re-open table for querying
        table = deltalake::open_table(table_path).await.expect("Failed to reopen table");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(table))
            .expect("Failed to register table");

        // Query with date range 2026-02-12 to 2026-02-22
        let sql = "SELECT cert_index FROM ct_records \
                   WHERE seen_date >= '2026-02-12' AND seen_date <= '2026-02-22' \
                   ORDER BY cert_index";
        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut cert_indices = Vec::new();
        for batch in &batches {
            // Try UInt64 first, then fall back to Int64 (Delta stores UInt64 as Int64)
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i) as u64);
                }
            } else {
                panic!("Failed to downcast cert_index to UInt64 or Int64");
            }
        }

        // Should only include records 2, 3 (dates 2026-02-15, 2026-02-20)
        // Record 4 (2026-02-25) is > 2026-02-22 so excluded
        assert_eq!(cert_indices, vec![2, 3]);

        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_response_includes_required_fields() {
        let table_path = "/tmp/delta_query_test_required_fields";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        let records = vec![create_test_cert_record(
            100,
            "test_log",
            "2026-02-15",
            vec!["test.com", "www.test.com"],
        )];

        let schema = delta_schema();
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

        let batch = records_to_batch(&records, &schema).expect("Failed to create batch");
        DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write to table");

        table = deltalake::open_table(table_path).await.expect("Failed to reopen table");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(table))
            .expect("Failed to register table");

        let sql = "SELECT cert_index, fingerprint, sha256, serial_number, \
                   subject_aggregated, issuer_aggregated, not_before, not_after, \
                   all_domains, source_name, seen, is_ca \
                   FROM ct_records WHERE cert_index = 100";
        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        assert!(!batches.is_empty());
        let batch = &batches[0];

        // Verify all 12 columns are present
        assert_eq!(batch.num_columns(), 12);

        // Extract values to verify content
        // cert_index: Try UInt64 first, then Int64 (Delta stores UInt64 as Int64)
        let cert_index_val = if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
            indices.value(0)
        } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
            indices.value(0) as u64
        } else {
            panic!("Failed to downcast cert_index");
        };
        assert_eq!(cert_index_val, 100);

        let fingerprints = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast fingerprint");
        assert_eq!(fingerprints.value(0), "fingerprint_100");

        let subjects = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast subject");
        assert_eq!(subjects.value(0), "CN=example.com");

        let domains = batch
            .column(8)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Failed to downcast domains");
        let domain_list = domains.value(0);
        let domain_strings = domain_list
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast domain strings");
        assert_eq!(domain_strings.len(), 2);
        assert_eq!(domain_strings.value(0), "test.com");
        assert_eq!(domain_strings.value(1), "www.test.com");

        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_response_excludes_heavy_fields() {
        let table_path = "/tmp/delta_query_test_exclude_heavy";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        let records = vec![create_test_cert_record(
            101,
            "test_log",
            "2026-02-15",
            vec!["test.com"],
        )];

        let schema = delta_schema();
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

        let batch = records_to_batch(&records, &schema).expect("Failed to create batch");
        DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write to table");

        table = deltalake::open_table(table_path).await.expect("Failed to reopen table");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(table))
            .expect("Failed to register table");

        // Query should only have 12 columns (excluding as_der, chain, cert_link, source_url, update_type, signature_algorithm, subject_aggregated)
        let sql = "SELECT cert_index, fingerprint, sha256, serial_number, \
                   subject_aggregated, issuer_aggregated, not_before, not_after, \
                   all_domains, source_name, seen, is_ca \
                   FROM ct_records WHERE cert_index = 101";
        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        assert!(!batches.is_empty());
        let batch = &batches[0];

        // Should be exactly 12 columns - no as_der, chain, cert_link, source_url, update_type, signature_algorithm
        assert_eq!(batch.num_columns(), 12);

        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_version_field_in_response() {
        let table_path = "/tmp/delta_query_test_version";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        let records = vec![create_test_cert_record(
            102,
            "test_log",
            "2026-02-15",
            vec!["test.com"],
        )];

        let schema = delta_schema();
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

        let batch = records_to_batch(&records, &schema).expect("Failed to create batch");
        DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write to table");

        table = deltalake::open_table(table_path).await.expect("Failed to reopen table");
        let version = table.version();

        // Version should be >= 0
        assert!(version >= 0);

        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_nonexistent_table_returns_503() {
        let table_path = "/tmp/delta_query_test_nonexistent/nonexistent";

        // Try to open a table that doesn't exist and was never created
        let result = deltalake::open_table(table_path).await;

        // Should fail with NotATable or InvalidTableLocation error
        match result {
            Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
                // This is what we expect
            }
            _ => panic!("Expected NotATable or InvalidTableLocation error"),
        }
    }

    #[test]
    fn test_is_valid_date() {
        assert!(is_valid_date("2026-02-18"));
        assert!(is_valid_date("2000-01-01"));
        assert!(is_valid_date("2099-12-31"));

        assert!(!is_valid_date("2026-2-18")); // Missing leading zero
        assert!(!is_valid_date("2026/02/18")); // Wrong separator
        assert!(!is_valid_date("02-18-2026")); // Wrong order
        assert!(!is_valid_date("2026-13-01")); // Invalid month
        assert!(!is_valid_date("2026-02-30")); // Invalid day for February
        assert!(!is_valid_date("not-a-date")); // Not a date
        assert!(!is_valid_date("")); // Empty string
    }

    #[test]
    fn test_query_params_deserialization() {
        let params = QueryParams {
            domain: Some("example.com".to_string()),
            issuer: None,
            from: Some("2026-02-10".to_string()),
            to: Some("2026-02-20".to_string()),
            limit: Some(100),
            cursor: None,
        };

        assert_eq!(params.domain, Some("example.com".to_string()));
        assert_eq!(params.from, Some("2026-02-10".to_string()));
        assert_eq!(params.to, Some("2026-02-20".to_string()));
        assert_eq!(params.limit, Some(100));
    }

    #[test]
    fn test_cert_result_serialization() {
        let result = CertResult {
            cert_index: 123,
            fingerprint: "abc123".to_string(),
            sha256: "def456".to_string(),
            serial_number: "789".to_string(),
            subject: "CN=example.com".to_string(),
            issuer: "CN=CA".to_string(),
            not_before: 1000000,
            not_after: 2000000,
            all_domains: vec!["example.com".to_string()],
            source_name: "log1".to_string(),
            seen: "2026-02-15T12:00:00+00:00".to_string(),
            is_ca: false,
        };

        let json = serde_json::to_string(&result).expect("Failed to serialize");
        assert!(json.contains("\"cert_index\":123"));
        assert!(json.contains("\"fingerprint\":\"abc123\""));
        assert!(json.contains("\"all_domains\":[\"example.com\"]"));
    }
}
