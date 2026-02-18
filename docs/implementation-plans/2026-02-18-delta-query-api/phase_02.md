# Delta Query API Implementation Plan - Phase 2: Table Opening + Date Range Query

**Goal:** Open Delta table per request and execute basic date range filtering with Arrow RecordBatch to JSON response mapping.

**Architecture:** Per-request `DeltaTable` opening via `deltalake::open_table()`, `SessionContext` registration, SQL query building for `seen_date` partition column filtering, and Arrow RecordBatch column downcasting to build JSON responses. Follows the same DataFusion patterns already used in `src/backfill.rs`.

**Tech Stack:** Rust, Axum 0.8, deltalake 0.25, DataFusion (via deltalake's `datafusion` feature), Arrow arrays

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-18

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-query-api.AC1: Certificate search by domain, issuer, and date range
- **delta-query-api.AC1.1 Success:** Date range query (`from`/`to`) returns only certificates with `seen_date` within range

### delta-query-api.AC3: Response format for domain monitoring
- **delta-query-api.AC3.1 Success:** Response includes cert_index, fingerprint, sha256, serial_number, subject, issuer, not_before, not_after, all_domains, source_name, seen, is_ca
- **delta-query-api.AC3.2 Success:** Response excludes heavy fields (as_der, chain)
- **delta-query-api.AC3.3 Success:** Response includes `version` field showing Delta table version

### delta-query-api.AC5: Configuration and operational
- **delta-query-api.AC5.6 Failure:** Table not found at query time returns 503

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Add query parameter parsing and response types to src/query.rs

**Files:**
- Modify: `src/query.rs` (replace stub handler with real types and query param parsing)

**Implementation:**

Add Axum query parameter extraction and response structs. The handler should parse query params, validate that at least one filter is provided (domain, issuer, from, or to), and return structured JSON.

Add these types to `src/query.rs`:

```rust
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
```

The handler signature becomes:
```rust
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

    // Phase 2: implement table opening and date range query
    // Domain and issuer search added in Phase 3
    // Cursor pagination added in Phase 4
    // ...
}
```

Update the router function to pass state properly.

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat(query-api): add query parameter types and response structs`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement Delta table opening, date range SQL, and RecordBatch mapping

**Verifies:** delta-query-api.AC1.1, delta-query-api.AC3.1, delta-query-api.AC3.2, delta-query-api.AC3.3, delta-query-api.AC5.6

**Files:**
- Modify: `src/query.rs` (implement the handler body with table opening, SQL, RecordBatch mapping)

**Implementation:**

Implement the handler to:
1. Open the DeltaTable at the configured `table_path` (or return 503 if not found)
2. Create a per-request `SessionContext`, register the table as `ct_records`
3. Build SQL with `seen_date` filters when `from`/`to` params are provided
4. Execute the query, collect RecordBatches
5. Downcast columns to extract cert result data, mapping to `CertResult` structs

Key imports (follow the backfill.rs pattern at lines 7-9):
```rust
use deltalake::arrow::array::*;
use deltalake::datafusion::prelude::*;
use deltalake::DeltaTableError;
use tracing::warn;
```

Table opening pattern (from backfill.rs:193-219):
```rust
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
```

SessionContext registration (from backfill.rs:222-223):
```rust
let ctx = SessionContext::new();
ctx.register_table("ct_records", Arc::new(table))
    .map_err(|e| {
        warn!(error = %e, "Failed to register table");
        (StatusCode::INTERNAL_SERVER_ERROR,
         Json(ErrorResponse { error: "Internal query error".to_string() }))
    })?;
```

SQL query building — select only the columns needed for the response (exclude `as_der`, `chain`, `cert_link`, `source_url`, `update_type`, `signature_algorithm`, `subject_aggregated`). Note: `subject_aggregated` is selected and renamed to `subject`, `issuer_aggregated` renamed to `issuer`, and `seen` is a Timestamp column:

Add a date validation helper to reject malformed date strings before embedding in SQL:

```rust
/// Validate that a date string matches YYYY-MM-DD format.
/// Returns true if valid, false otherwise.
fn is_valid_date(s: &str) -> bool {
    if s.len() != 10 {
        return false;
    }
    // Parse with chrono to ensure it's a real date, not just a pattern match
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
}
```

Use the validator before embedding `from`/`to` in SQL:

```rust
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
```

Note: `from` and `to` are date strings in `YYYY-MM-DD` format validated by `is_valid_date()` before embedding. They filter on the `seen_date` partition column which enables Delta Lake partition pruning. The chrono parse check ensures both format correctness and prevents SQL injection.

RecordBatch to CertResult mapping — downcast each column following the backfill.rs pattern:

```rust
let mut results = Vec::new();
for batch in &batches {
    let cert_indices = batch.column(0).as_any()
        .downcast_ref::<UInt64Array>().ok_or("downcast cert_index")?;
    let fingerprints = batch.column(1).as_any()
        .downcast_ref::<StringArray>().ok_or("downcast fingerprint")?;
    let sha256s = batch.column(2).as_any()
        .downcast_ref::<StringArray>().ok_or("downcast sha256")?;
    let serial_numbers = batch.column(3).as_any()
        .downcast_ref::<StringArray>().ok_or("downcast serial_number")?;
    let subjects = batch.column(4).as_any()
        .downcast_ref::<StringArray>().ok_or("downcast subject")?;
    let issuers = batch.column(5).as_any()
        .downcast_ref::<StringArray>().ok_or("downcast issuer")?;
    let not_befores = batch.column(6).as_any()
        .downcast_ref::<Int64Array>().ok_or("downcast not_before")?;
    let not_afters = batch.column(7).as_any()
        .downcast_ref::<Int64Array>().ok_or("downcast not_after")?;
    let all_domains_col = batch.column(8).as_any()
        .downcast_ref::<ListArray>().ok_or("downcast all_domains")?;
    let source_names = batch.column(9).as_any()
        .downcast_ref::<StringArray>().ok_or("downcast source_name")?;
    let seens = batch.column(10).as_any()
        .downcast_ref::<TimestampMicrosecondArray>().ok_or("downcast seen")?;
    let is_cas = batch.column(11).as_any()
        .downcast_ref::<BooleanArray>().ok_or("downcast is_ca")?;

    for i in 0..batch.num_rows() {
        // Extract all_domains list for this row
        let domains_list = all_domains_col.value(i);
        let domains_arr = domains_list.as_any()
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
            cert_index: cert_indices.value(i),
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
```

Return the response:
```rust
Json(QueryResponse {
    version,
    results,
    next_cursor: None,  // Added in Phase 4
    has_more: false,     // Added in Phase 4
}).into_response()
```

Add `chrono` import at the top of `src/query.rs` (already a dependency in Cargo.toml).

**Testing:**
Tests must verify each AC listed above. Tests should create a real Delta table in a temp directory, write test records, then call the query functions directly.

- delta-query-api.AC1.1: Write records with different `seen_date` values, query with `from`/`to` range, verify only records within range are returned
- delta-query-api.AC3.1: Verify response includes all required fields (cert_index, fingerprint, sha256, serial_number, subject, issuer, not_before, not_after, all_domains, source_name, seen, is_ca)
- delta-query-api.AC3.2: Verify response does NOT contain `as_der`, `chain`, `cert_link`, `source_url` fields
- delta-query-api.AC3.3: Verify response includes `version` field matching the Delta table version
- delta-query-api.AC5.6: Test with nonexistent table path, verify 503 status is returned

Follow the existing test pattern from `src/backfill.rs:911-965`: create temp dir at `/tmp/delta_query_test_*`, use `open_or_create_table()`, `records_to_batch()`, `DeltaOps(table).write()` to seed test data, then exercise query functions.

Add `use chrono;` and any needed chrono imports.

**Verification:**
Run: `cargo test query::tests`
Expected: All query tests pass

Run: `cargo test`
Expected: All tests pass (existing 249+ tests unaffected)

**Commit:** `feat(query-api): implement table opening, date range query, and RecordBatch mapping`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->
