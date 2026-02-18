# Delta Query API - Test Requirements

Generated from: docs/design-plans/2026-02-18-delta-query-api.md

All 22 acceptance criteria are mapped below to either automated tests or human verification.
Each entry references the implementation phase that covers it and the test approach
prescribed by that phase's implementation plan.

---

## Automated Tests

### delta-query-api.AC1: Certificate search by domain, issuer, and date range

#### AC1.1 Success: Date range query (from/to) returns only certificates with seen_date within range

- **Test type:** Integration
- **Implementation phase:** Phase 2
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Create a Delta table in `/tmp/delta_query_test_*` with records having
  different `seen_date` values (e.g., 2026-02-17, 2026-02-18, 2026-02-19). Query with
  `from=2026-02-17` and `to=2026-02-18`. Assert that only records within the date range
  are returned and records outside the range are excluded. Uses `open_or_create_table()`,
  `records_to_batch()`, and `DeltaOps(table).write()` to seed data, then exercises the
  query handler or SQL execution directly.

#### AC1.2 Success: Domain contains search (paypal) matches certificates where any domain in all_domains contains the substring (case-insensitive)

- **Test type:** Integration
- **Implementation phase:** Phase 3
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Seed a Delta table with records containing varied domains (e.g.,
  `["paypal.com", "www.paypal.com"]` and `["test.paypal.example.com"]`). Query with
  `domain=paypal`. Assert that all records with any domain containing "paypal" as a
  substring are returned, and records without it are excluded. Verify case-insensitivity
  by including a record with `PAYPAL` in a domain.

#### AC1.3 Success: Domain suffix search (*.example.com) matches certificates where any domain ends with .example.com

- **Test type:** Integration
- **Implementation phase:** Phase 3
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Seed a Delta table with records containing domains like
  `["example.com", "www.example.com"]` and `["test.paypal.example.com"]`. Query with
  `domain=*.example.com`. Assert that records with domains ending in `.example.com` match
  (e.g., `www.example.com`, `test.paypal.example.com`) and that `example.com` itself does
  NOT match (no leading dot).

#### AC1.4 Success: Domain exact search (example.com) matches certificates where all_domains contains the exact value

- **Test type:** Integration
- **Implementation phase:** Phase 3
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Seed a Delta table with records where one has `all_domains` containing
  `"example.com"` and another has only `"www.example.com"`. Query with `domain=example.com`.
  Assert only the record with the exact `"example.com"` entry matches. Uses `array_has()`
  SQL path, not UNNEST+ILIKE.

#### AC1.5 Success: Issuer search matches certificates where issuer_aggregated contains the substring (case-insensitive)

- **Test type:** Integration
- **Implementation phase:** Phase 3
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Seed a Delta table with records having different `issuer_aggregated`
  values (e.g., `"/CN=Let's Encrypt Authority X3"` and `"/CN=DigiCert"`). Query with
  `issuer=Let's Encrypt`. Assert that only records whose issuer contains the substring
  match. Verify case-insensitivity.

#### AC1.6 Success: Filters combine correctly (domain + issuer + date range returns intersection)

- **Test type:** Integration
- **Implementation phase:** Phase 3
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Seed a Delta table with three records: (1) domain matches but issuer
  does not, (2) issuer matches but domain does not, (3) both domain and issuer match.
  Query with `domain=paypal` + `issuer=Let's Encrypt`. Assert only record (3) is returned.
  Extend to include date range filters and verify the intersection narrows correctly.

#### AC1.7 Failure: Request with no filters returns 400

- **Test type:** Unit
- **Implementation phase:** Phase 3
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Call the query handler (or its validation logic) with `QueryParams`
  where all filter fields (`domain`, `issuer`, `from`, `to`) are `None`. Assert that the
  response status is 400 and the error message is `"At least one filter required"`.

---

### delta-query-api.AC2: Stable cursor pagination pinned to Delta version

#### AC2.1 Success: First request returns results with next_cursor and has_more: true when more results exist

- **Test type:** Integration
- **Implementation phase:** Phase 4
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Seed a Delta table with 10 records. Query with `limit=3` and a date
  range filter. Assert that the response contains exactly 3 results, `has_more` is `true`,
  and `next_cursor` is a non-empty string. Decode the cursor and verify it encodes the
  correct Delta table version and the `cert_index` of the last returned result.

#### AC2.2 Success: Subsequent request with cursor returns next page from same Delta version

- **Test type:** Integration
- **Implementation phase:** Phase 4
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Continue from AC2.1's cursor. Pass the `next_cursor` value as the
  `cursor` parameter in a second request. Assert that the next 3 records are returned
  (non-overlapping with the first page) and that the response `version` field matches
  the version from the first response, confirming the query reads from the same Delta
  table snapshot.

#### AC2.3 Success: Final page returns has_more: false with no next_cursor

- **Test type:** Integration
- **Implementation phase:** Phase 4
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Continue paginating through the 10-record table with `limit=3` until
  the final page. Assert that the last page has `has_more: false` and `next_cursor` is
  `None`. Verify that the total number of records across all pages equals 10.

#### AC2.4 Failure: Invalid cursor returns 400

- **Test type:** Unit
- **Implementation phase:** Phase 4
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Test `decode_cursor` with several invalid inputs: (1) a non-Base64
  string (e.g., `"not-valid!!!"`), (2) valid Base64 encoding of non-JSON data, (3) valid
  Base64 encoding of JSON missing required fields. Assert that all return `Err`. Also test
  the handler-level behavior: pass a garbage cursor string to the handler and assert 400
  status with `"Invalid cursor"` error message.

#### AC2.5 Failure: Expired cursor (vacuumed version) returns 410

- **Test type:** Integration
- **Implementation phase:** Phase 4
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Encode a cursor with a non-existent Delta table version (e.g., version
  999999) using `encode_cursor(999999, 0)`. Pass this cursor to the handler with a valid
  table path. Assert that `deltalake::open_table_with_version` fails and the handler
  returns 410 with `"Cursor expired, please restart query"`.

#### AC2.6 Edge: limit parameter respected, capped at max_results_per_page

- **Test type:** Unit + Integration
- **Implementation phase:** Phase 4
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** (1) Unit test: verify that when `limit` exceeds `max_results_per_page`
  (e.g., limit=1000 with max=500), the effective limit is clamped to 500. (2) Integration
  test: seed a table with 600 records, query with `limit=1000` and `max_results_per_page=500`
  in config, assert that at most 500 results are returned per page.

---

### delta-query-api.AC3: Response format for domain monitoring

#### AC3.1 Success: Response includes cert_index, fingerprint, sha256, serial_number, subject, issuer, not_before, not_after, all_domains, source_name, seen, is_ca

- **Test type:** Integration
- **Implementation phase:** Phase 2
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Seed a Delta table with a known record. Query it and deserialize the
  response into `CertResult`. Assert that every required field is present and populated
  with the expected values from the seed data: `cert_index`, `fingerprint`, `sha256`,
  `serial_number`, `subject` (from `subject_aggregated`), `issuer` (from
  `issuer_aggregated`), `not_before`, `not_after`, `all_domains` (as `Vec<String>`),
  `source_name`, `seen` (ISO 8601 UTC string), and `is_ca`.

#### AC3.2 Success: Response excludes heavy fields (as_der, chain)

- **Test type:** Integration
- **Implementation phase:** Phase 2
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Seed a Delta table with a record that has non-empty `as_der` and
  `chain` columns. Query it and inspect the raw JSON response (or the `CertResult` struct
  fields). Assert that the response does NOT contain `as_der`, `chain`, `cert_link`,
  or `source_url` fields. This is verified structurally by the SQL SELECT clause which
  omits these columns, and by the `CertResult` struct which has no fields for them.

#### AC3.3 Success: Response includes version field showing Delta table version

- **Test type:** Integration
- **Implementation phase:** Phase 2
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Open a Delta table, note its version number. Query through the handler.
  Assert that the `QueryResponse.version` field matches the Delta table's version. After
  writing additional records (creating a new version), query again without a cursor and
  verify the version increments.

---

### delta-query-api.AC4: Integration with existing server

#### AC4.1 Success: Query endpoint protected by existing auth middleware when auth is enabled

- **Test type:** Structural verification (automated build check)
- **Implementation phase:** Phase 1
- **Test file:** `src/main.rs` (verified by code structure, not a dedicated test)
- **Description:** The query router is merged into the Axum `app` BEFORE the auth
  middleware layer is applied (Phase 1, Task 3). This means when `config.auth.enabled =
  true`, all routes including `/api/query/certs` require valid auth tokens. Verified by:
  (1) `cargo build` succeeds with the router placement, (2) code review confirms the
  `app.merge(query_router)` call occurs before `app.layer(auth_layer)`. No dedicated
  integration test spins up a full server with auth enabled, as the middleware application
  is unconditional once enabled and applies to all merged routes equally.

#### AC4.2 Success: Query endpoint subject to existing rate limiting when rate limiting is enabled

- **Test type:** Structural verification (automated build check)
- **Implementation phase:** Phase 1
- **Test file:** `src/main.rs` (verified by code structure, not a dedicated test)
- **Description:** Same structural guarantee as AC4.1. The query router is merged before
  the rate-limit middleware layer. When `config.rate_limit.enabled = true`, the query
  endpoint inherits rate limiting. Verified by code placement and `cargo build`.

#### AC4.3 Success: Query API disabled by default (enabled: false), server starts normally without it

- **Test type:** Unit
- **Implementation phase:** Phase 1
- **Test file:** `src/config.rs` (`#[cfg(test)] mod tests`)
- **Description:** Test `QueryApiConfig::default()` and assert that `enabled` is `false`.
  This guarantees the server starts without the query API unless explicitly enabled. The
  conditional router merge in `main.rs` (`if config.query_api.enabled`) ensures no query
  routes are added when disabled.

---

### delta-query-api.AC5: Configuration and operational

#### AC5.1 Success: YAML config parsed with defaults for all fields

- **Test type:** Unit
- **Implementation phase:** Phase 1
- **Test file:** `src/config.rs` (`#[cfg(test)] mod tests`)
- **Description:** Two tests: (1) `test_query_api_config_defaults` -- create
  `QueryApiConfig::default()` and assert all fields match design spec defaults:
  `enabled=false`, `table_path="./data/certstream"`, `max_results_per_page=500`,
  `default_results_per_page=50`, `query_timeout_secs=30`. (2)
  `test_query_api_config_deserialize` -- deserialize a YAML string with non-default values
  and assert all fields are parsed correctly.

#### AC5.2 Success: Env var overrides work (CERTSTREAM_QUERY_API_*)

- **Test type:** Unit
- **Implementation phase:** Phase 1
- **Test file:** `src/config.rs` (`#[cfg(test)] mod tests`)
- **Description:** The YAML deserialization round-trip test in AC5.1 verifies that
  non-default values can be parsed. Env var override logic follows the identical pattern
  as existing `CERTSTREAM_DELTA_SINK_*` overrides (string parse with `unwrap_or` fallback).
  Full env var integration testing would require setting process-level env vars which
  conflicts with parallel test execution. The structural pattern is verified by code review
  and the YAML deserialization test confirms the config fields accept overridden values.

#### AC5.3 Failure: Inaccessible table_path logged as warning at startup (non-fatal)

- **Test type:** Structural verification (code review + manual)
- **Implementation phase:** Phase 5
- **Test file:** `src/main.rs` (verified by code structure)
- **Description:** Phase 5, Task 3 adds a `Path::exists()` check inside the
  `if config.query_api.enabled` block in `main.rs`. If the path does not exist, a
  `warn!()` is emitted but the server continues starting. Verified by: (1) `cargo build`
  succeeds, (2) the check does not return early or panic. See Human Verification section
  below for log output confirmation.

#### AC5.4 Success: Query timeout returns 504 when DataFusion execution exceeds query_timeout_secs

- **Test type:** Integration
- **Implementation phase:** Phase 5
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Create a `QueryApiState` with `query_timeout_secs = 0` (or a
  sufficiently small value like 1 nanosecond). Seed a Delta table and execute a query
  through the handler. Assert that the response status is 504 and the error message is
  `"Query timed out"`. The `tokio::time::timeout()` wrapper around DataFusion execution
  triggers the timeout before the query can complete.

#### AC5.5 Success: Prometheus metrics emitted (request count by status, duration, result count)

- **Test type:** Unit (with fallback to structural verification)
- **Implementation phase:** Phase 5
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Preferred approach: use `metrics_util::debugging::DebuggingRecorder`
  (added as a dev-dependency). Install the recorder, execute a query, then inspect the
  snapshot to verify: (1) `certstream_query_requests` counter incremented with correct
  `status` label, (2) `certstream_query_duration_seconds` histogram recorded, (3)
  `certstream_query_results_count` histogram recorded. Fallback approach if the global
  recorder conflicts with parallel tests: verify that the metrics calls compile and execute
  without panicking by running a query in a test. See Human Verification section for
  Prometheus endpoint confirmation.

#### AC5.6 Failure: Table not found at query time returns 503

- **Test type:** Integration
- **Implementation phase:** Phase 2
- **Test file:** `src/query.rs` (`#[cfg(test)] mod tests`)
- **Description:** Create a `QueryApiState` pointing to a nonexistent table path (e.g.,
  `/tmp/delta_query_test_nonexistent_RANDOM`). Execute a query through the handler. Assert
  that the response status is 503 and the error message is `"Query service unavailable"`.
  The `deltalake::open_table()` call fails with `DeltaTableError::NotATable` or
  `InvalidTableLocation`, triggering the 503 path.

---

## Automated Test Summary by File

| Test File | Test Count | ACs Covered |
|-----------|-----------|-------------|
| `src/config.rs` tests | 2-3 | AC4.3, AC5.1, AC5.2 |
| `src/query.rs` tests | ~20-25 | AC1.1-AC1.7, AC2.1-AC2.6, AC3.1-AC3.3, AC5.4, AC5.5, AC5.6 |

Additional unit tests in `src/query.rs` for helper functions:
- `escape_like_pattern` -- 4 cases (%, _, ', backslash)
- `escape_sql_string` -- 3 cases (', %, backslash passthrough)
- `classify_domain_search` -- 4 cases (contains, suffix, exact, `*example.com` edge case)
- `encode_cursor` / `decode_cursor` -- 5 cases (round-trip, valid, invalid Base64, invalid JSON, missing fields)
- `is_valid_date` -- valid and invalid date formats

---

## Human Verification

The following criteria require human verification because they involve runtime behaviors
that cannot be fully captured in automated unit or integration tests.

### AC4.1 Success: Query endpoint protected by existing auth middleware when auth is enabled

- **Justification:** Full end-to-end auth verification requires starting the server with
  `auth.enabled = true` and sending HTTP requests with and without valid tokens. The
  codebase does not have an integration test harness that boots the full Axum server.
  Structural verification (router merged before middleware layer) provides high confidence,
  but a manual smoke test confirms the runtime behavior.
- **Verification approach:**
  1. Set `auth.enabled = true` and `query_api.enabled = true` in config.yaml
  2. Start the server with `cargo run`
  3. Send `GET /api/query/certs?from=2026-02-01` without an auth token -- expect 401
  4. Send the same request with a valid auth token -- expect 200 or 400 (depending on data)

### AC4.2 Success: Query endpoint subject to existing rate limiting when rate limiting is enabled

- **Justification:** Same as AC4.1. Rate limiting behavior requires sending multiple rapid
  requests to a running server and observing throttling.
- **Verification approach:**
  1. Set `rate_limit.enabled = true` and `query_api.enabled = true` in config.yaml
  2. Start the server with `cargo run`
  3. Send rapid requests to `GET /api/query/certs?from=2026-02-01` -- expect 429 after
     exceeding the rate limit

### AC5.3 Failure: Inaccessible table_path logged as warning at startup (non-fatal)

- **Justification:** This criterion verifies log output at server startup. The automated
  build confirms the code compiles and does not panic, but verifying the actual warning
  message in logs requires running the server and inspecting stdout/stderr.
- **Verification approach:**
  1. Set `query_api.enabled = true` and `query_api.table_path = "/nonexistent/path"` in
     config.yaml
  2. Start the server with `cargo run`
  3. Verify that a warning log line appears containing "Query API table path does not exist"
  4. Verify the server continues to start and listen on its configured port

### AC5.5 Success: Prometheus metrics emitted (request count by status, duration, result count)

- **Justification:** If the `DebuggingRecorder` approach is used in automated tests, this
  criterion is fully automated. However, if the fallback approach is used (due to global
  recorder conflicts), the actual Prometheus endpoint output must be verified manually.
- **Verification approach (fallback only):**
  1. Set `query_api.enabled = true` and `protocols.metrics = true` in config.yaml
  2. Start the server with `cargo run`
  3. Send a few queries to `GET /api/query/certs?from=2026-02-01`
  4. Fetch `GET /metrics` and verify the following metric names appear:
     - `certstream_query_requests` (counter with `status` label)
     - `certstream_query_duration_seconds` (histogram)
     - `certstream_query_results_count` (histogram)

---

## AC-to-Phase Traceability Matrix

| AC ID | Phase | Automated Test | Human Verification |
|-------|-------|---------------|-------------------|
| AC1.1 | Phase 2 | Integration (date range query) | -- |
| AC1.2 | Phase 3 | Integration (domain contains) | -- |
| AC1.3 | Phase 3 | Integration (domain suffix) | -- |
| AC1.4 | Phase 3 | Integration (domain exact) | -- |
| AC1.5 | Phase 3 | Integration (issuer substring) | -- |
| AC1.6 | Phase 3 | Integration (combined filters) | -- |
| AC1.7 | Phase 3 | Unit (no-filter rejection) | -- |
| AC2.1 | Phase 4 | Integration (first page) | -- |
| AC2.2 | Phase 4 | Integration (cursor continuation) | -- |
| AC2.3 | Phase 4 | Integration (final page) | -- |
| AC2.4 | Phase 4 | Unit (invalid cursor decode) | -- |
| AC2.5 | Phase 4 | Integration (expired version) | -- |
| AC2.6 | Phase 4 | Unit + Integration (limit capping) | -- |
| AC3.1 | Phase 2 | Integration (all fields present) | -- |
| AC3.2 | Phase 2 | Integration (heavy fields excluded) | -- |
| AC3.3 | Phase 2 | Integration (version field) | -- |
| AC4.1 | Phase 1 | Structural (router placement) | Smoke test with auth |
| AC4.2 | Phase 1 | Structural (router placement) | Smoke test with rate limit |
| AC4.3 | Phase 1 | Unit (default enabled=false) | -- |
| AC5.1 | Phase 1 | Unit (config defaults + YAML parse) | -- |
| AC5.2 | Phase 1 | Unit (YAML deserialization) | -- |
| AC5.3 | Phase 5 | Structural (code compiles) | Log output inspection |
| AC5.4 | Phase 5 | Integration (zero timeout) | -- |
| AC5.5 | Phase 5 | Unit (DebuggingRecorder or fallback) | Prometheus endpoint (fallback) |
| AC5.6 | Phase 2 | Integration (nonexistent table) | -- |
