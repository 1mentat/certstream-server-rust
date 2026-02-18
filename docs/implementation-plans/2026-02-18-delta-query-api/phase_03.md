# Delta Query API Implementation Plan - Phase 3: Domain + Issuer Search

**Goal:** Implement domain pattern matching (exact, contains, suffix) and issuer filtering with proper input sanitization.

**Architecture:** Domain search mode detection classifies user input as exact (contains `.`, no `*`), contains (no `.` or `*`), or suffix (starts with `*.`). Contains and suffix modes use `UNNEST(all_domains) AS d` with `ILIKE` and `DISTINCT`. Exact mode uses `array_has(all_domains, value)`. Issuer filtering uses `ILIKE` on `issuer_aggregated`. All user input is sanitized to escape SQL special characters before embedding in LIKE patterns.

**Tech Stack:** Rust, DataFusion SQL (UNNEST, ILIKE, array_has), Axum

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-18

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-query-api.AC1: Certificate search by domain, issuer, and date range
- **delta-query-api.AC1.2 Success:** Domain contains search (`paypal`) matches certificates where any domain in `all_domains` contains the substring (case-insensitive)
- **delta-query-api.AC1.3 Success:** Domain suffix search (`*.example.com`) matches certificates where any domain ends with `.example.com`
- **delta-query-api.AC1.4 Success:** Domain exact search (`example.com`) matches certificates where `all_domains` contains the exact value
- **delta-query-api.AC1.5 Success:** Issuer search matches certificates where `issuer_aggregated` contains the substring (case-insensitive)
- **delta-query-api.AC1.6 Success:** Filters combine correctly (domain + issuer + date range returns intersection)
- **delta-query-api.AC1.7 Failure:** Request with no filters returns 400

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Add input sanitization and domain search mode detection

**Files:**
- Modify: `src/query.rs` (add sanitization function and domain mode enum)

**Implementation:**

Add a SQL LIKE pattern escape function and domain search mode classifier to `src/query.rs`:

```rust
/// Domain search mode determined from user input.
enum DomainSearchMode {
    /// No `.` or `*` in input → substring match via UNNEST + ILIKE '%term%'
    Contains(String),
    /// Input starts with `*.` → suffix match via UNNEST + ILIKE '%.suffix'
    Suffix(String),
    /// Input contains `.` but no `*` → exact match via array_has()
    Exact(String),
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
```

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat(query-api): add input sanitization and domain search mode detection`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement domain and issuer SQL filter generation

**Files:**
- Modify: `src/query.rs` (refactor handler to use domain search modes and issuer filter in SQL)

**Implementation:**

Refactor the SQL query builder in `handle_query_certs` to support domain search, issuer filter, and their combination with date range filters.

The SQL structure differs based on domain search mode:

**Contains mode** (`paypal` → `UNNEST + ILIKE`):
```sql
SELECT DISTINCT cert_index, fingerprint, sha256, serial_number,
    subject_aggregated, issuer_aggregated, not_before, not_after,
    all_domains, source_name, seen, is_ca
FROM ct_records, UNNEST(all_domains) AS d
WHERE d ILIKE '%paypal%'
  AND seen_date >= '2026-02-01'
ORDER BY cert_index LIMIT 50
```

**Suffix mode** (`*.example.com` → `UNNEST + ILIKE`):
```sql
SELECT DISTINCT cert_index, fingerprint, sha256, serial_number,
    subject_aggregated, issuer_aggregated, not_before, not_after,
    all_domains, source_name, seen, is_ca
FROM ct_records, UNNEST(all_domains) AS d
WHERE d ILIKE '%.example.com'
  AND seen_date >= '2026-02-01'
ORDER BY cert_index LIMIT 50
```

**Exact mode** (`example.com` → `array_has`):
```sql
SELECT cert_index, fingerprint, sha256, serial_number,
    subject_aggregated, issuer_aggregated, not_before, not_after,
    all_domains, source_name, seen, is_ca
FROM ct_records
WHERE array_has(all_domains, 'example.com')
  AND seen_date >= '2026-02-01'
ORDER BY cert_index LIMIT 50
```

**Issuer filter** (appended to WHERE clause regardless of domain mode):
```sql
AND issuer_aggregated ILIKE '%Let''s Encrypt%'
```

Implementation approach — build SQL dynamically:

```rust
// Determine if UNNEST is needed (contains or suffix domain search)
let needs_unnest = matches!(
    params.domain.as_ref().map(|d| classify_domain_search(d)),
    Some(DomainSearchMode::Contains(_)) | Some(DomainSearchMode::Suffix(_))
);

let select_cols = "cert_index, fingerprint, sha256, serial_number, \
    subject_aggregated, issuer_aggregated, not_before, not_after, \
    all_domains, source_name, seen, is_ca";

let mut sql = if needs_unnest {
    format!("SELECT DISTINCT {} FROM ct_records, UNNEST(all_domains) AS d WHERE 1=1", select_cols)
} else {
    format!("SELECT {} FROM ct_records WHERE 1=1", select_cols)
};

// Add domain filter
if let Some(ref domain) = params.domain {
    match classify_domain_search(domain) {
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
// Date params are validated by is_valid_date() in Phase 2; use escape_sql_string
// since these are string literals, not LIKE patterns
if let Some(ref from) = params.from {
    sql.push_str(&format!(" AND seen_date >= '{}'", escape_sql_string(from)));
}
if let Some(ref to) = params.to {
    sql.push_str(&format!(" AND seen_date <= '{}'", escape_sql_string(to)));
}

sql.push_str(&format!(" ORDER BY cert_index LIMIT {}", limit));
```

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat(query-api): implement domain and issuer SQL filter generation`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Tests for domain search modes, issuer filter, and combined filters

**Verifies:** delta-query-api.AC1.2, delta-query-api.AC1.3, delta-query-api.AC1.4, delta-query-api.AC1.5, delta-query-api.AC1.6, delta-query-api.AC1.7

**Files:**
- Modify: `src/query.rs` (add tests in `#[cfg(test)] mod tests` block)

**Testing:**
Tests must verify each AC listed above. Follow the existing test pattern from `src/backfill.rs:911-965`: create temp dir at `/tmp/delta_query_test_*`, seed with test records using `open_or_create_table()` + `records_to_batch()` + `DeltaOps(table).write()`, then exercise the query handler or SQL execution directly.

Seed data should include records with varied domains, issuers, and dates:
- Record with `all_domains: ["example.com", "www.example.com"]`, `issuer_aggregated: "/CN=Let's Encrypt Authority X3"`, `seen_date: "2026-02-17"`
- Record with `all_domains: ["paypal.com", "www.paypal.com"]`, `issuer_aggregated: "/CN=DigiCert"`, `seen_date: "2026-02-18"`
- Record with `all_domains: ["test.paypal.example.com"]`, `issuer_aggregated: "/CN=Let's Encrypt Authority X3"`, `seen_date: "2026-02-19"`

Tests for each AC:
- delta-query-api.AC1.2: Domain contains search `paypal` should match records 2 and 3 (both contain "paypal" substring in at least one domain)
- delta-query-api.AC1.3: Domain suffix search `*.example.com` should match records 1 and 3 (domains ending with `.example.com`)
- delta-query-api.AC1.4: Domain exact search `example.com` should match only record 1 (exact match in all_domains array)
- delta-query-api.AC1.5: Issuer search `Let's Encrypt` should match records 1 and 3
- delta-query-api.AC1.6: Combined domain contains `paypal` + issuer `Let's Encrypt` should match only record 3 (intersection)
- delta-query-api.AC1.7: Request with no filters returns 400 (test the param validation logic)

Also add unit tests for the sanitization and classification functions:
- `escape_like_pattern("test%value")` returns `"test\\%value"`
- `escape_like_pattern("user_name")` returns `"user\\_name"`
- `escape_like_pattern("it's")` returns `"it''s"`
- `escape_like_pattern("back\\slash")` returns `"back\\\\slash"` (backslash escaped first)
- `escape_sql_string("it's")` returns `"it''s"`
- `escape_sql_string("100%")` returns `"100%"` (% is not escaped in non-LIKE context)
- `escape_sql_string("back\\slash")` returns `"back\\slash"` (backslash passes through unchanged — intentional for non-LIKE contexts)
- `classify_domain_search("paypal")` returns `Contains`
- `classify_domain_search("*.example.com")` returns `Suffix`
- `classify_domain_search("example.com")` returns `Exact`
- `classify_domain_search("*example.com")` returns `Exact` (asterisk without dot → literal)

**Verification:**
Run: `cargo test query::tests`
Expected: All query tests pass

Run: `cargo test`
Expected: All tests pass

**Commit:** `test(query-api): add domain search, issuer filter, and combined filter tests`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->
