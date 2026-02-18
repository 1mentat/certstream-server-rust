# Human Test Plan: Delta Query API

## Prerequisites

- Rust toolchain installed (edition 2024)
- `cargo test` passing (288 tests, 0 failures)
- A config.yaml file available for editing in the project root
- Access to a Delta Lake table with CT certificate data (or the ability to create one by running the server with `delta_sink.enabled = true`)

---

## Phase 1: Configuration and Startup

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Open `config.example.yaml` and verify it contains a `query_api` section | Section present with `enabled`, `table_path`, `max_results_per_page`, `default_results_per_page`, `query_timeout_secs` |
| 1.2 | Start the server with default config (query_api not present or `enabled: false`). Run `cargo run` | Server starts normally. No "Query API enabled" log line. `GET /api/query/certs?from=2026-02-01` returns 404 |
| 1.3 | Set `query_api.enabled: true` and `query_api.table_path: "/nonexistent/path"` in config.yaml. Run `cargo run` | Server starts. Log contains warning: "Query API table path does not exist yet; queries will return 503 until data is written". Server continues listening |
| 1.4 | With server from 1.3, send `GET /api/query/certs?from=2026-02-01` | Response status 503 with body `{"error":"Query service unavailable"}` |

## Phase 2: Authentication Integration

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Set `auth.enabled: true`, `auth.tokens: ["test-token-123"]`, `query_api.enabled: true`. Start server | "token authentication enabled" and "Query API enabled" in logs |
| 2.2 | Send `GET /api/query/certs?from=2026-02-01` without Authorization header | Response status 401 |
| 2.3 | Send with `Authorization: Bearer test-token-123` | Response status is NOT 401 (200, 400, or 503 depending on data) |
| 2.4 | Send with `Authorization: Bearer wrong-token` | Response status 401 |

## Phase 3: Rate Limiting Integration

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Set `rate_limit.enabled: true`, `rate_limit.free_max_tokens: 3`, `rate_limit.free_refill_rate: 0.1`, `query_api.enabled: true`. Disable auth. Start server | "rate limiting enabled" and "Query API enabled" in logs |
| 3.2 | Send 10 rapid sequential requests to `GET /api/query/certs?from=2026-02-01` | First few return 200/503. After exhausting token bucket, subsequent return 429 |

## Phase 4: Query Functionality (requires data in Delta table)

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Send `GET /api/query/certs` (no params) | Status 400: `{"error":"At least one filter required"}` |
| 4.2 | Send `GET /api/query/certs?from=2026-02-17&to=2026-02-18` | Status 200. Body has `version` (int), `results` (array), `has_more` (bool). Each result has: cert_index, fingerprint, sha256, serial_number, subject, issuer, not_before, not_after, all_domains, source_name, seen, is_ca. No as_der, chain, cert_link, source_url |
| 4.3 | Send `GET /api/query/certs?domain=google` | Status 200. All results have "google" substring in at least one domain (case-insensitive) |
| 4.4 | Send `GET /api/query/certs?domain=*.google.com&from=2026-02-17` | Status 200. All results have at least one domain ending in ".google.com" |
| 4.5 | Send `GET /api/query/certs?domain=google.com&from=2026-02-17` | Status 200. All results have exact "google.com" in all_domains array |
| 4.6 | Send `GET /api/query/certs?issuer=Let%27s%20Encrypt&from=2026-02-17` | Status 200. All results have "Let's Encrypt" substring in issuer |
| 4.7 | Send `GET /api/query/certs?from=invalid-date` | Status 400: "Invalid 'from' date format, expected YYYY-MM-DD" |

## Phase 5: Pagination

| Step | Action | Expected |
|------|--------|----------|
| 5.1 | Send `GET /api/query/certs?from=2026-02-01&limit=3` | Status 200. At most 3 results. If `has_more: true`, `next_cursor` is non-empty Base64 string. Note `version` value |
| 5.2 | Use `next_cursor` from 5.1: `GET /api/query/certs?from=2026-02-01&limit=3&cursor=<next_cursor>` | Status 200. Non-overlapping results (no duplicate cert_indices). Same `version` as 5.1 |
| 5.3 | Continue paginating until `has_more: false` | Final page has `has_more: false`, no `next_cursor`. Total unique results consistent |
| 5.4 | Send `GET /api/query/certs?from=2026-02-01&limit=1000` | At most 500 results (max_results_per_page default) |
| 5.5 | Send `GET /api/query/certs?from=2026-02-01&cursor=not-valid-base64!!!` | Status 400: `{"error":"Invalid cursor"}` |

## Phase 6: Prometheus Metrics

| Step | Action | Expected |
|------|--------|----------|
| 6.1 | Set `protocols.metrics: true` and `query_api.enabled: true`. Start server | Server starts normally |
| 6.2 | Send a few queries (some valid, some without filters for 400) | Requests complete with expected status codes |
| 6.3 | Fetch `GET /metrics` | Contains: `certstream_query_requests` with status labels, `certstream_query_duration_seconds` histogram, `certstream_query_results_count` histogram |

## End-to-End: Full Query Lifecycle

| Step | Action | Expected |
|------|--------|----------|
| E2E.1 | Configure `delta_sink.enabled: true`, `delta_sink.table_path: "./data/e2e_test"`, `query_api.enabled: true`, `query_api.table_path: "./data/e2e_test"`. Start server | Both "delta sink enabled" and "Query API enabled" in logs |
| E2E.2 | Wait 60 seconds for CT data to flow | Delta table files appear under `./data/e2e_test/` |
| E2E.3 | Send `GET /api/query/certs?from=<today YYYY-MM-DD>` | Status 200 with recently ingested certificates |
| E2E.4 | Pick a domain from results. Query with that domain | Response includes the matching certificate |
| E2E.5 | Wait 60 seconds. Repeat E2E.3 | Version may increment. Additional results may appear |

---

## Traceability Matrix

| AC | Automated Test | Manual Step |
|----|---------------|-------------|
| AC1.1 Date range | `test_date_range_query_filters_correctly` | 4.2 |
| AC1.2 Domain contains | `test_domain_contains_search` | 4.3 |
| AC1.3 Domain suffix | `test_domain_suffix_search` | 4.4 |
| AC1.4 Domain exact | `test_domain_exact_search` | 4.5 |
| AC1.5 Issuer search | `test_issuer_search` | 4.6 |
| AC1.6 Combined filters | `test_combined_domain_issuer_search`, `test_domain_search_with_date_range` | 4.3-4.6 |
| AC1.7 No filters 400 | `test_no_filters_validation` | 4.1 |
| AC2.1 First page | `test_pagination_first_page_with_has_more` | 5.1 |
| AC2.2 Cursor continuation | `test_pagination_subsequent_page` | 5.2 |
| AC2.3 Final page | `test_pagination_final_page_no_has_more` | 5.3 |
| AC2.4 Invalid cursor 400 | `test_invalid_cursor_returns_400` | 5.5 |
| AC2.5 Expired cursor 410 | `test_pagination_expired_cursor` | -- |
| AC2.6 Limit clamping | `test_pagination_limit_clamping` | 5.4 |
| AC3.1 All fields | `test_response_includes_required_fields` | 4.2 |
| AC3.2 Heavy fields excluded | `test_response_excludes_heavy_fields` | 4.2 |
| AC3.3 Version field | `test_version_field_in_response` | 5.1 |
| AC4.1 Auth middleware | Structural | 2.1-2.4 |
| AC4.2 Rate limiting | Structural | 3.1-3.2 |
| AC4.3 Disabled by default | `test_query_api_config_defaults` | 1.2 |
| AC5.1 Config defaults | `test_query_api_config_defaults` | 1.1 |
| AC5.2 Env var overrides | `test_query_api_config_deserialize` | -- |
| AC5.3 Startup warning | Structural | 1.3-1.4 |
| AC5.4 Timeout 504 | `test_query_timeout_returns_504` | -- |
| AC5.5 Prometheus metrics | Structural | 6.1-6.3 |
| AC5.6 Table not found 503 | `test_handler_nonexistent_table_returns_503` | 1.4 |
