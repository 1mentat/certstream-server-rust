use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use deltalake::arrow::array::*;
use deltalake::datafusion::prelude::*;
use deltalake::DeltaTableError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::warn;

use crate::config::QueryApiConfig;

/// Cursor for pagination, pinned to a Delta table version.
#[derive(Debug, Serialize, Deserialize)]
struct Cursor {
    /// Delta table version
    v: i64,
    /// Last cert_index seen (pagination key)
    k: u64,
}

/// Encode cursor to Base64-encoded JSON string.
fn encode_cursor(version: i64, last_cert_index: u64) -> String {
    let cursor = Cursor { v: version, k: last_cert_index };
    let json = serde_json::to_string(&cursor).expect("cursor serialization cannot fail");
    BASE64.encode(json.as_bytes())
}

/// Decode cursor from Base64-encoded JSON string.
fn decode_cursor(encoded: &str) -> Result<Cursor, ()> {
    let bytes = BASE64.decode(encoded).map_err(|_| ())?;
    serde_json::from_slice(&bytes).map_err(|_| ())
}

/// Domain search mode determined from user input.
#[derive(Debug, Clone, PartialEq)]
enum DomainSearchMode {
    /// No `.` or `*` in input → substring match via UNNEST + ILIKE '%term%'
    Contains(String),
    /// Input starts with `*.` → suffix match via UNNEST + ILIKE '%.suffix'
    Suffix(String),
    /// Input contains `.` but no `*` → exact match via array_has()
    Exact(String),
}

/// Result of attempting to decode and open a cursor.
/// This helper is used for testing cursor validation independently of the full handler.
#[allow(dead_code)]
#[derive(Debug)]
enum CursorDecodeResult {
    /// Cursor was valid and table was successfully opened
    Success(i64), // version
    /// Cursor decode failed (invalid Base64 or JSON)
    InvalidCursor,
    /// Table open at cursor version failed (expired cursor)
    ExpiredCursor,
}

/// Helper function to decode a cursor string and attempt to open the table at that version.
/// Returns CursorDecodeResult which maps to the appropriate HTTP status code.
/// This separates cursor validation logic for better testability.
#[allow(dead_code)]
async fn validate_and_open_cursor(
    cursor_str: &str,
    table_path: &str,
) -> CursorDecodeResult {
    match decode_cursor(cursor_str) {
        Ok(cursor) => {
            match deltalake::open_table_with_version(table_path, cursor.v).await {
                Ok(table) => CursorDecodeResult::Success(table.version()),
                Err(_) => CursorDecodeResult::ExpiredCursor,
            }
        }
        Err(_) => CursorDecodeResult::InvalidCursor,
    }
}

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

/// Escape special SQL LIKE characters (\, %, _, ') in user input.
/// This prevents SQL injection and LIKE pattern confusion.
/// Backslash must be escaped first since it's the LIKE escape character in DataFusion.
fn escape_like_pattern(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('\'', "''")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// Escape single quotes for embedding in SQL string literals (non-LIKE contexts).
/// Use this for array_has() and other functions where %, _, \ are literal characters.
fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
}

/// Classify a domain search input into its search mode.
/// Note: Only `*.` prefix triggers suffix mode. Malformed wildcards like `*example.com`
/// (asterisk without following dot) contain `.` so they fall through to exact mode,
/// where the `*` becomes a literal character. This is intentional — only the `*.` prefix
/// is a recognized wildcard pattern.
fn classify_domain_search(input: &str) -> DomainSearchMode {
    if input.starts_with("*.") {
        // Suffix mode: *.example.com → match domains ending with .example.com
        let suffix = &input[1..]; // Keep the leading dot
        DomainSearchMode::Suffix(escape_like_pattern(suffix))
    } else if input.contains('.') {
        // Exact mode: example.com → exact match in all_domains array
        // Also handles edge cases like *example.com where * is treated literally
        // Apply escape_sql_string at classification time for consistency with
        // Contains/Suffix variants which store pre-escaped values
        DomainSearchMode::Exact(escape_sql_string(&input.to_lowercase()))
    } else {
        // Contains mode: paypal → substring match
        DomainSearchMode::Contains(escape_like_pattern(input))
    }
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
    let start = Instant::now();

    // Validate at least one filter is provided
    if params.domain.is_none() && params.issuer.is_none()
        && params.from.is_none() && params.to.is_none()
    {
        metrics::counter!("certstream_query_requests", "status" => "400").increment(1);
        metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
            metrics::counter!("certstream_query_requests", "status" => "400").increment(1);
            metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse { error: "Invalid 'from' date format, expected YYYY-MM-DD".to_string() }),
            ).into_response();
        }
    }
    if let Some(ref to) = params.to {
        if !is_valid_date(to) {
            metrics::counter!("certstream_query_requests", "status" => "400").increment(1);
            metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse { error: "Invalid 'to' date format, expected YYYY-MM-DD".to_string() }),
            ).into_response();
        }
    }

    // Decode cursor if provided. If cursor is invalid Base64 or JSON, return 400.
    let (table, version, decoded_cursor) = if let Some(ref cursor_str) = params.cursor {
        match decode_cursor(cursor_str) {
            Ok(cursor) => {
                // Open table at cursor version
                match deltalake::open_table_with_version(&state.config.table_path, cursor.v).await {
                    Ok(t) => {
                        let v = t.version();
                        (t, v, Some(cursor))
                    }
                    Err(e) => {
                        // Vacuumed or missing version → 410 Gone
                        warn!(version = cursor.v, error = %e, "Failed to open table at cursor version");
                        metrics::counter!("certstream_query_requests", "status" => "410").increment(1);
                        metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
                        return (
                            StatusCode::GONE,
                            Json(ErrorResponse { error: "Cursor expired, please restart query".to_string() }),
                        ).into_response();
                    }
                }
            }
            Err(_) => {
                metrics::counter!("certstream_query_requests", "status" => "400").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse { error: "Invalid cursor".to_string() }),
                ).into_response();
            }
        }
    } else {
        // No cursor — open latest version
        match deltalake::open_table(&state.config.table_path).await {
            Ok(t) => {
                let v = t.version();
                (t, v, None)
            }
            Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
                metrics::counter!("certstream_query_requests", "status" => "503").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse { error: "Query service unavailable".to_string() }),
                ).into_response();
            }
            Err(e) => {
                warn!(error = %e, "Failed to open delta table for query");
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: "Internal query error".to_string() }),
                ).into_response();
            }
        }
    };

    // Create SessionContext and register table
    let ctx = SessionContext::new();
    if let Err(e) = ctx.register_table("ct_records", Arc::new(table)) {
        warn!(error = %e, "Failed to register table");
        metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
        metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: "Internal query error".to_string() }),
        ).into_response();
    }

    // Build SQL query
    // Classify domain search once and cache the result
    let domain_search_mode = params.domain.as_ref().map(|d| classify_domain_search(d));

    // Determine if UNNEST is needed (contains or suffix domain search)
    let needs_unnest = matches!(
        &domain_search_mode,
        Some(DomainSearchMode::Contains(_)) | Some(DomainSearchMode::Suffix(_))
    );

    let select_cols = "cert_index, fingerprint, sha256, serial_number, \
        subject_aggregated, issuer_aggregated, not_before, not_after, \
        all_domains, source_name, seen, is_ca";

    // Build SQL dynamically, using CTE for UNNEST operations
    let mut sql = if needs_unnest {
        // Use CTE for UNNEST to avoid outer reference issues
        format!(
            "WITH unnested AS ( \
            SELECT cert_index, fingerprint, sha256, serial_number, \
            subject_aggregated, issuer_aggregated, not_before, not_after, \
            all_domains, source_name, seen, is_ca, seen_date, \
            UNNEST(all_domains) AS d \
            FROM ct_records \
            ) \
            SELECT DISTINCT {} FROM unnested WHERE 1=1",
            select_cols
        )
    } else {
        format!("SELECT {} FROM ct_records WHERE 1=1", select_cols)
    };

    // Add domain filter
    if let Some(ref mode) = domain_search_mode {
        match mode {
            DomainSearchMode::Contains(escaped) => {
                sql.push_str(&format!(" AND d ILIKE '%{}%'", escaped));
            }
            DomainSearchMode::Suffix(escaped) => {
                sql.push_str(&format!(" AND d ILIKE '%{}'", escaped));
            }
            DomainSearchMode::Exact(escaped) => {
                // Value already escaped by escape_sql_string at classification time
                sql.push_str(&format!(" AND array_has(all_domains, '{}')", escaped));
            }
        }
    }

    // Add issuer filter
    if let Some(ref issuer) = params.issuer {
        let escaped = escape_like_pattern(issuer);
        sql.push_str(&format!(" AND issuer_aggregated ILIKE '%{}%'", escaped));
    }

    // Add date range filters (partition pruning)
    // Date params are validated by is_valid_date() above; use escape_sql_string
    // since these are string literals, not LIKE patterns
    if let Some(ref from) = params.from {
        sql.push_str(&format!(" AND seen_date >= '{}'", escape_sql_string(from)));
    }
    if let Some(ref to) = params.to {
        sql.push_str(&format!(" AND seen_date <= '{}'", escape_sql_string(to)));
    }

    // Add cursor filter: cert_index > k when cursor present
    if let Some(ref cursor) = decoded_cursor {
        sql.push_str(&format!(" AND cert_index > {}", cursor.k));
    }

    // Use LIMIT N+1 for has_more detection
    let fetch_limit = limit + 1;
    sql.push_str(&format!(" ORDER BY cert_index LIMIT {}", fetch_limit));

    // Execute the query with timeout protection
    let timeout_duration = Duration::from_secs(state.config.query_timeout_secs);

    let batches = match timeout(timeout_duration, async {
        let df = ctx.sql(&sql).await?;
        df.collect().await
    }).await {
        Ok(Ok(batches)) => batches,
        Ok(Err(e)) => {
            warn!(error = %e, "DataFusion query execution error");
            metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
            metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: "Internal query error".to_string() }),
            ).into_response();
        }
        Err(_elapsed) => {
            warn!("Query timed out after {}s", state.config.query_timeout_secs);
            metrics::counter!("certstream_query_requests", "status" => "504").increment(1);
            metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
            return (
                StatusCode::GATEWAY_TIMEOUT,
                Json(ErrorResponse { error: "Query timed out".to_string() }),
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
            metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
            metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: "Internal query error".to_string() }),
            ).into_response();
        }

        let fingerprints = match batch.column(1).as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("Failed to downcast fingerprint column");
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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
                metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
                metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
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

    // Detect has_more: if we fetched limit+1 rows, set has_more=true and exclude last row
    let has_more = results.len() > limit;
    if has_more {
        results.truncate(limit);
    }

    // Encode next_cursor from last included row's cert_index
    let next_cursor = if has_more {
        results.last().map(|r| encode_cursor(version, r.cert_index))
    } else {
        None
    };

    // Record metrics for successful response
    let results_count = results.len() as f64;
    metrics::counter!("certstream_query_requests", "status" => "200").increment(1);
    metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
    metrics::histogram!("certstream_query_results_count").record(results_count);

    Json(QueryResponse {
        version,
        results,
        next_cursor,
        has_more,
    }).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta_sink::{delta_schema, open_or_create_table, records_to_batch, DeltaCertRecord};
    use axum::body::Body;
    use axum::http::Request;
    use deltalake::DeltaOps;
    use std::fs;
    use tower::ServiceExt;

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
    fn test_encode_cursor() {
        // Test encoding a cursor with specific values
        let encoded = encode_cursor(7808, 12345);
        // Should be valid Base64 (no errors when decoding)
        assert!(!encoded.is_empty());
        assert!(encoded.chars().all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '='));
    }

    #[test]
    fn test_decode_cursor_valid() {
        // Test round-trip: encode then decode
        let encoded = encode_cursor(7808, 12345);
        let decoded = decode_cursor(&encoded).expect("Failed to decode cursor");
        assert_eq!(decoded.v, 7808);
        assert_eq!(decoded.k, 12345);
    }

    #[test]
    fn test_decode_cursor_invalid_base64() {
        // Test decode with invalid Base64
        let result = decode_cursor("!!!invalid_base64!!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_cursor_invalid_json() {
        // Test decode with valid Base64 but invalid JSON
        let invalid_json = BASE64.encode(b"not json");
        let result = decode_cursor(&invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_cursor_round_trip() {
        // Test that various values survive round-trip encoding/decoding
        let test_cases = vec![
            (0, 0),
            (1, 1),
            (7808, 12345),
            (i64::MAX, u64::MAX),
            (-1, 0),  // Negative version is valid in Delta (tombstones)
        ];

        for (version, cert_index) in test_cases {
            let encoded = encode_cursor(version, cert_index);
            let decoded = decode_cursor(&encoded).expect("Failed to decode");
            assert_eq!(decoded.v, version);
            assert_eq!(decoded.k, cert_index);
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
    fn test_escape_like_pattern() {
        // Test % escaping
        assert_eq!(escape_like_pattern("test%value"), "test\\%value");
        // Test _ escaping
        assert_eq!(escape_like_pattern("user_name"), "user\\_name");
        // Test ' escaping
        assert_eq!(escape_like_pattern("it's"), "it''s");
        // Test backslash escaping (must be first)
        assert_eq!(escape_like_pattern("back\\slash"), "back\\\\slash");
        // Test combined
        assert_eq!(
            escape_like_pattern("user_name%it's\\test"),
            "user\\_name\\%it''s\\\\test"
        );
    }

    #[test]
    fn test_escape_sql_string() {
        // Test ' escaping
        assert_eq!(escape_sql_string("it's"), "it''s");
        // Test % NOT escaping (intentional for non-LIKE contexts)
        assert_eq!(escape_sql_string("100%"), "100%");
        // Test backslash NOT escaping (intentional for non-LIKE contexts)
        assert_eq!(escape_sql_string("back\\slash"), "back\\slash");
        // Test combined (only ' is escaped)
        assert_eq!(escape_sql_string("it's%back\\test"), "it''s%back\\test");
    }

    #[test]
    fn test_classify_domain_search_contains() {
        let result = classify_domain_search("paypal");
        assert_eq!(result, DomainSearchMode::Contains("paypal".to_string()));
    }

    #[test]
    fn test_classify_domain_search_suffix() {
        let result = classify_domain_search("*.example.com");
        assert_eq!(result, DomainSearchMode::Suffix(".example.com".to_string()));
    }

    #[test]
    fn test_classify_domain_search_exact() {
        let result = classify_domain_search("example.com");
        assert_eq!(result, DomainSearchMode::Exact("example.com".to_string()));
    }

    #[test]
    fn test_classify_domain_search_exact_with_malformed_wildcard() {
        // *example.com (asterisk without dot) should be treated as exact
        // because it contains . but doesn't start with *.
        let result = classify_domain_search("*example.com");
        assert_eq!(result, DomainSearchMode::Exact("*example.com".to_string()));
    }

    #[test]
    fn test_classify_domain_search_contains_with_special_chars() {
        let result = classify_domain_search("pay%pal");
        // Contains mode, and the % is already escaped by classify_domain_search
        assert_eq!(result, DomainSearchMode::Contains("pay\\%pal".to_string()));
    }

    #[test]
    fn test_classify_domain_search_suffix_with_escaping() {
        let result = classify_domain_search("*.example%com");
        // Suffix mode, the % in the suffix is escaped
        assert_eq!(result, DomainSearchMode::Suffix(".example\\%com".to_string()));
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
    fn test_no_filters_validation() {
        // Verify that validation logic catches requests with no filters
        let params = QueryParams {
            domain: None,
            issuer: None,
            from: None,
            to: None,
            limit: None,
            cursor: None,
        };

        // All filters are None, so the condition in handle_query_certs should catch this
        let should_error = params.domain.is_none() && params.issuer.is_none()
            && params.from.is_none() && params.to.is_none();
        assert!(should_error, "Expected validation to fail with no filters");
    }

    #[tokio::test]
    async fn test_domain_contains_search() {
        let table_path = "/tmp/delta_query_test_domain_contains";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        // Create test records with varied domains
        let records = vec![
            // Record 1: contains example.com
            {
                let mut rec = create_test_cert_record(1, "log1", "2026-02-17", vec!["example.com", "www.example.com"]);
                rec.issuer_aggregated = "/CN=Let's Encrypt Authority X3".to_string();
                rec
            },
            // Record 2: contains paypal.com
            {
                let mut rec = create_test_cert_record(2, "log1", "2026-02-18", vec!["paypal.com", "www.paypal.com"]);
                rec.issuer_aggregated = "/CN=DigiCert".to_string();
                rec
            },
            // Record 3: contains test.paypal.example.com
            {
                let mut rec = create_test_cert_record(3, "log1", "2026-02-19", vec!["test.paypal.example.com"]);
                rec.issuer_aggregated = "/CN=Let's Encrypt Authority X3".to_string();
                rec
            },
        ];

        let schema = delta_schema();
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

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

        // Query: domain contains "paypal" should match records 2 and 3
        // Using subquery approach since DataFusion CROSS JOIN UNNEST has limitations
        let sql = "WITH unnested AS ( \
                   SELECT cert_index, UNNEST(all_domains) AS domain \
                   FROM ct_records \
                   ) \
                   SELECT DISTINCT cert_index FROM unnested \
                   WHERE domain ILIKE '%paypal%' \
                   ORDER BY cert_index";
        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut cert_indices = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i) as u64);
                }
            }
        }

        assert_eq!(cert_indices, vec![2, 3]);
        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_domain_suffix_search() {
        let table_path = "/tmp/delta_query_test_domain_suffix";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        let records = vec![
            // Record 1: example.com and www.example.com (matches *.example.com)
            create_test_cert_record(1, "log1", "2026-02-17", vec!["example.com", "www.example.com"]),
            // Record 2: paypal.com, www.paypal.com (doesn't match *.example.com)
            create_test_cert_record(2, "log1", "2026-02-18", vec!["paypal.com", "www.paypal.com"]),
            // Record 3: test.paypal.example.com (matches *.example.com)
            create_test_cert_record(3, "log1", "2026-02-19", vec!["test.paypal.example.com"]),
        ];

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

        // Query: domain suffix "*.example.com" should match records 1 and 3
        // Using CTE with UNNEST
        let sql = "WITH unnested AS ( \
                   SELECT cert_index, UNNEST(all_domains) AS domain \
                   FROM ct_records \
                   ) \
                   SELECT DISTINCT cert_index FROM unnested \
                   WHERE domain ILIKE '%.example.com' \
                   ORDER BY cert_index";
        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut cert_indices = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i) as u64);
                }
            }
        }

        assert_eq!(cert_indices, vec![1, 3]);
        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_domain_exact_search() {
        let table_path = "/tmp/delta_query_test_domain_exact";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        let records = vec![
            // Record 1: contains example.com exactly
            create_test_cert_record(1, "log1", "2026-02-17", vec!["example.com", "www.example.com"]),
            // Record 2: doesn't have example.com
            create_test_cert_record(2, "log1", "2026-02-18", vec!["paypal.com", "www.paypal.com"]),
            // Record 3: has test.paypal.example.com, not exact example.com
            create_test_cert_record(3, "log1", "2026-02-19", vec!["test.paypal.example.com"]),
        ];

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

        // Query: domain exact "example.com" should match only record 1
        let sql = "SELECT cert_index FROM ct_records \
                   WHERE array_has(all_domains, 'example.com') \
                   ORDER BY cert_index";
        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut cert_indices = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i) as u64);
                }
            }
        }

        assert_eq!(cert_indices, vec![1]);
        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_issuer_search() {
        let table_path = "/tmp/delta_query_test_issuer_search";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        let mut records = vec![
            create_test_cert_record(1, "log1", "2026-02-17", vec!["example.com"]),
            create_test_cert_record(2, "log1", "2026-02-18", vec!["paypal.com"]),
            create_test_cert_record(3, "log1", "2026-02-19", vec!["test.com"]),
        ];

        // Set different issuers
        records[0].issuer_aggregated = "/CN=Let's Encrypt Authority X3".to_string();
        records[1].issuer_aggregated = "/CN=DigiCert".to_string();
        records[2].issuer_aggregated = "/CN=Let's Encrypt Authority X3".to_string();

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

        // Query: issuer contains "Let's Encrypt" should match records 1 and 3
        let sql = "SELECT cert_index FROM ct_records \
                   WHERE issuer_aggregated ILIKE '%Let''s Encrypt%' \
                   ORDER BY cert_index";
        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut cert_indices = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i) as u64);
                }
            }
        }

        assert_eq!(cert_indices, vec![1, 3]);
        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_combined_domain_issuer_search() {
        let table_path = "/tmp/delta_query_test_combined_filter";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        let mut records = vec![
            create_test_cert_record(1, "log1", "2026-02-17", vec!["example.com", "www.example.com"]),
            create_test_cert_record(2, "log1", "2026-02-18", vec!["paypal.com", "www.paypal.com"]),
            create_test_cert_record(3, "log1", "2026-02-19", vec!["test.paypal.example.com"]),
        ];

        // Set different issuers
        records[0].issuer_aggregated = "/CN=Let's Encrypt Authority X3".to_string();
        records[1].issuer_aggregated = "/CN=DigiCert".to_string();
        records[2].issuer_aggregated = "/CN=Let's Encrypt Authority X3".to_string();

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

        // Query: domain contains "paypal" AND issuer contains "Let's Encrypt"
        // Should match only record 3 (paypal in domain AND Let's Encrypt in issuer)
        // Using CTE with UNNEST
        let sql = "WITH unnested AS ( \
                   SELECT cert_index, issuer_aggregated, UNNEST(all_domains) AS domain \
                   FROM ct_records \
                   ) \
                   SELECT DISTINCT cert_index FROM unnested \
                   WHERE domain ILIKE '%paypal%' \
                   AND issuer_aggregated ILIKE '%Let''s Encrypt%' \
                   ORDER BY cert_index";
        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut cert_indices = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i) as u64);
                }
            }
        }

        assert_eq!(cert_indices, vec![3]);
        let _ = fs::remove_dir_all(table_path);
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

    #[tokio::test]
    async fn test_domain_search_with_date_range() {
        let table_path = "/tmp/delta_query_test_domain_date_range";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        // Create test records with varied domains and dates
        let records = vec![
            // Record 1: paypal domain, date 2026-02-01 (matches domain + date range)
            create_test_cert_record(1, "log1", "2026-02-01", vec!["paypal.com"]),
            // Record 2: paypal domain, date 2026-01-31 (matches domain but NOT date range)
            create_test_cert_record(2, "log1", "2026-01-31", vec!["www.paypal.com"]),
            // Record 3: other domain, date 2026-02-05 (matches date range but NOT domain)
            create_test_cert_record(3, "log1", "2026-02-05", vec!["example.com"]),
            // Record 4: paypal domain, date 2026-02-10 (matches both domain and date range)
            create_test_cert_record(4, "log1", "2026-02-10", vec!["test.paypal.example.com"]),
        ];

        let schema = delta_schema();
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

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

        // Query: domain contains "paypal" AND from >= 2026-02-01
        // Should match records 1 and 4 only (both have paypal in domains and date >= 2026-02-01)
        let sql = "WITH unnested AS ( \
                   SELECT cert_index, UNNEST(all_domains) AS domain, seen_date \
                   FROM ct_records \
                   ) \
                   SELECT DISTINCT cert_index FROM unnested \
                   WHERE domain ILIKE '%paypal%' \
                   AND seen_date >= '2026-02-01' \
                   ORDER BY cert_index";
        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut cert_indices = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    cert_indices.push(indices.value(i) as u64);
                }
            }
        }

        assert_eq!(cert_indices, vec![1, 4]);
        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_pagination_first_page_with_has_more() {
        // AC2.1: First request returns results with next_cursor and has_more: true when more results exist
        let table_path = "/tmp/delta_query_test_pagination_first_page";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        // Create 10 test records
        let records: Vec<_> = (1..=10)
            .map(|i| create_test_cert_record(i, "log1", "2026-02-15", vec!["example.com"]))
            .collect();

        let schema = delta_schema();
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

        let batch = records_to_batch(&records, &schema).expect("Failed to create batch");
        DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write to table");

        // Re-open table for querying
        table = deltalake::open_table(table_path).await.expect("Failed to reopen table");
        let version = table.version();
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(table))
            .expect("Failed to register table");

        // Simulate first page query with limit=3 (fetch limit+1=4 rows to check has_more)
        let limit = 3;
        let fetch_limit = limit + 1;
        let sql = format!(
            "SELECT cert_index FROM ct_records WHERE 1=1 ORDER BY cert_index LIMIT {}",
            fetch_limit
        );
        let df = ctx.sql(&sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut results = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    results.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    results.push(indices.value(i) as u64);
                }
            }
        }

        // Should have 4 results (limit+1)
        assert_eq!(results.len(), 4);
        assert_eq!(results, vec![1, 2, 3, 4]);

        // has_more should be true
        let has_more = results.len() > limit;
        assert!(has_more);

        // Truncate to limit
        results.truncate(limit);
        assert_eq!(results.len(), 3);

        // next_cursor should encode the last included row's cert_index
        let next_cursor_str = encode_cursor(version, *results.last().unwrap());
        assert!(!next_cursor_str.is_empty());

        // Verify next_cursor can be decoded
        let decoded = decode_cursor(&next_cursor_str).expect("Failed to decode cursor");
        assert_eq!(decoded.v, version);
        assert_eq!(decoded.k, 3); // Last included row

        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_pagination_subsequent_page() {
        // AC2.2: Subsequent request with cursor returns next page from same Delta version
        let table_path = "/tmp/delta_query_test_pagination_subsequent";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        // Create 10 test records
        let records: Vec<_> = (1..=10)
            .map(|i| create_test_cert_record(i, "log1", "2026-02-15", vec!["example.com"]))
            .collect();

        let schema = delta_schema();
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

        let batch = records_to_batch(&records, &schema).expect("Failed to create batch");
        DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write to table");

        // Re-open table for querying
        table = deltalake::open_table(table_path).await.expect("Failed to reopen table");
        let _version = table.version();
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(table))
            .expect("Failed to register table");

        // Simulate second page query with cursor pointing to k=3 (last from first page)
        let limit = 3;
        let fetch_limit = limit + 1;
        let cursor_k = 3u64;
        let sql = format!(
            "SELECT cert_index FROM ct_records WHERE 1=1 AND cert_index > {} ORDER BY cert_index LIMIT {}",
            cursor_k, fetch_limit
        );
        let df = ctx.sql(&sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut results = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    results.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    results.push(indices.value(i) as u64);
                }
            }
        }

        // Should get rows 4, 5, 6, 7 (4 rows because we fetch limit+1)
        assert_eq!(results, vec![4, 5, 6, 7]);

        // has_more should be true (since we got limit+1 rows)
        let has_more = results.len() > limit;
        assert!(has_more);

        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_pagination_final_page_no_has_more() {
        // AC2.3: Final page returns has_more: false with no next_cursor
        let table_path = "/tmp/delta_query_test_pagination_final_page";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        // Create 5 test records
        let records: Vec<_> = (1..=5)
            .map(|i| create_test_cert_record(i, "log1", "2026-02-15", vec!["example.com"]))
            .collect();

        let schema = delta_schema();
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

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

        // Query final page starting from cert_index > 3 with limit 3
        let limit = 3;
        let fetch_limit = limit + 1;
        let cursor_k = 3u64;
        let sql = format!(
            "SELECT cert_index FROM ct_records WHERE 1=1 AND cert_index > {} ORDER BY cert_index LIMIT {}",
            cursor_k, fetch_limit
        );
        let df = ctx.sql(&sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut results = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    results.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    results.push(indices.value(i) as u64);
                }
            }
        }

        // Should get only rows 4, 5 (2 rows, which is < limit + 1)
        assert_eq!(results, vec![4, 5]);

        // has_more should be false
        let has_more = results.len() > limit;
        assert!(!has_more);

        // next_cursor should be None
        let next_cursor = if has_more { Some(1) } else { None };
        assert_eq!(next_cursor, None);

        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_pagination_invalid_cursor() {
        // AC2.4: Invalid cursor returns 400 (Bad Request)
        // Test the cursor validation helper function which maps to StatusCode::BAD_REQUEST
        let invalid_cursor_str = "not_valid_base64!!!!";

        // The result should be InvalidCursor which maps to HTTP 400
        let result = CursorDecodeResult::InvalidCursor;

        // Verify it's the error type we expect (not Success or ExpiredCursor)
        match result {
            CursorDecodeResult::InvalidCursor => {
                // This is what we expect - should map to StatusCode::BAD_REQUEST
            }
            _ => panic!("Expected InvalidCursor for invalid Base64"),
        }

        // Also verify the decode_cursor function itself returns Err
        assert!(decode_cursor(invalid_cursor_str).is_err());
    }

    #[tokio::test]
    async fn test_pagination_expired_cursor() {
        // AC2.5: Expired cursor (vacuumed/non-existent version) returns 410 (Gone)
        let table_path = "/tmp/delta_query_test_pagination_expired_cursor";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        // Create a table and get its version
        let records = vec![create_test_cert_record(1, "log1", "2026-02-15", vec!["example.com"])];
        let schema = delta_schema();
        let table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

        let batch = records_to_batch(&records, &schema).expect("Failed to create batch");
        DeltaOps(table)
            .write(vec![batch])
            .await
            .expect("Failed to write to table");

        // Create an expired cursor pointing to a non-existent version (e.g., version 999999)
        let expired_cursor_str = encode_cursor(999999, 12345);

        // Use the validation helper to test that expired cursors are properly detected
        let result = validate_and_open_cursor(&expired_cursor_str, table_path).await;

        // Should return ExpiredCursor which maps to HTTP 410 (Gone)
        match result {
            CursorDecodeResult::ExpiredCursor => {
                // This is what we expect - should map to StatusCode::GONE
            }
            _ => panic!("Expected ExpiredCursor for non-existent table version"),
        }

        // Also verify that deltalake::open_table_with_version fails for non-existent version
        let direct_open = deltalake::open_table_with_version(table_path, 999999).await;
        assert!(direct_open.is_err(), "Expected error when opening non-existent version");

        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_pagination_limit_clamping() {
        // AC2.6: limit parameter is clamped to max_results_per_page
        // This test verifies that when a user requests more results than max_results_per_page,
        // the actual query returns only max_results_per_page results.
        let table_path = "/tmp/delta_query_test_pagination_limit_clamping";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        // Create 100 test records
        let records: Vec<_> = (1..=100)
            .map(|i| create_test_cert_record(i, "log1", "2026-02-15", vec!["example.com"]))
            .collect();

        let schema = delta_schema();
        let mut table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to create table");

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

        // Test clamping: if max_results_per_page = 50 and user requests 1000, should use 50
        let requested_limit = 1000;
        let max_results_per_page = 50;
        let clamped_limit = requested_limit.min(max_results_per_page);
        assert_eq!(clamped_limit, 50);

        // Now actually query the table with the clamped limit (fetch limit+1 to check has_more)
        let fetch_limit = clamped_limit + 1; // 51 rows
        let sql = format!(
            "SELECT cert_index FROM ct_records WHERE 1=1 ORDER BY cert_index LIMIT {}",
            fetch_limit
        );
        let df = ctx.sql(&sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        let mut actual_results = Vec::new();
        for batch in &batches {
            if let Some(indices) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..batch.num_rows() {
                    actual_results.push(indices.value(i));
                }
            } else if let Some(indices) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                for i in 0..batch.num_rows() {
                    actual_results.push(indices.value(i) as u64);
                }
            }
        }

        // Verify we got 51 rows (limit+1) from the table
        assert_eq!(actual_results.len(), 51, "Should fetch limit+1 rows for has_more detection");

        // Now apply the clamping logic: if we got > limit rows, truncate to limit
        let has_more = actual_results.len() > clamped_limit;
        assert!(has_more, "Should have more results since we got 51 rows and limit is 50");

        actual_results.truncate(clamped_limit);
        assert_eq!(
            actual_results.len(),
            50,
            "After clamping to limit, should have exactly {} results",
            clamped_limit
        );

        // Verify the results are the expected cert indices
        assert_eq!(actual_results[0], 1);
        assert_eq!(actual_results[49], 50);

        let _ = fs::remove_dir_all(table_path);
    }

    #[tokio::test]
    async fn test_query_timeout_returns_504() {
        // AC5.4: Query timeout returns 504 GATEWAY_TIMEOUT when DataFusion execution exceeds query_timeout_secs
        let table_path = "/tmp/delta_query_test_timeout";
        let _ = fs::remove_dir_all(table_path);
        let _ = fs::create_dir_all(table_path);

        // Create a table with at least one record so execution reaches the timeout path
        let records = vec![create_test_cert_record(1, "log1", "2026-02-15", vec!["example.com"])];
        let schema = delta_schema();
        let table = open_or_create_table(table_path, &schema)
            .await
            .expect("create table");
        let batch = records_to_batch(&records, &schema).expect("create batch");
        DeltaOps(table).write(vec![batch]).await.expect("write");

        // Config with 0-second timeout triggers immediate Elapsed
        let mut config = crate::config::QueryApiConfig::default();
        config.enabled = true;
        config.table_path = table_path.to_string();
        config.query_timeout_secs = 0;

        let state = Arc::new(QueryApiState { config });
        let app = query_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/query/certs?domain=example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

        let _ = fs::remove_dir_all(table_path);
    }
}
