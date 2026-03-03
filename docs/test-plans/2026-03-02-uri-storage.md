# URI-Based Storage Backend — Human Test Plan

**Feature:** URI-based storage backend with S3/Tigris support
**Implementation Plan:** `docs/implementation-plans/2026-03-02-uri-storage/`
**Date:** 2026-03-02

## Prerequisites

- Rust toolchain with `cargo test` passing (388 tests, 0 failures)
- A Tigris (S3-compatible) bucket with valid credentials:
  - Endpoint URL (e.g., `https://fly.storage.tigris.dev`)
  - Bucket name
  - Access key ID
  - Secret access key
- A `config.yaml` file configured with:
  ```yaml
  storage:
    s3:
      endpoint: "https://fly.storage.tigris.dev"
      region: "auto"
      access_key_id: "<YOUR_KEY>"
      secret_access_key: "<YOUR_SECRET>"
      conditional_put: "etag"
  delta_sink:
    enabled: true
    table_path: "s3://<bucket>/certstream"
  ```
- A valid `certstream_state.json` with at least one small CT log entry (small log recommended for fast backfill)
- The binary built via `cargo build --release`

## Phase 1: Backfill Gap Detection Against S3 (HV-1)

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Edit `config.yaml` to set `delta_sink.table_path = "s3://<bucket>/certstream"` with valid `storage.s3` credentials. Ensure `certstream_state.json` has a small CT log entry with a `current_index` of at least 100. | Config file saved. |
| 1.2 | Run `cargo run -- --backfill --from 0 --logs "<log-name-substring>"` where the log name matches a small log in the state file. Let it write approximately 50 records, then interrupt with Ctrl-C. | Terminal shows records being written (check for `records_written` in log output). Process exits after Ctrl-C with some records written to S3. |
| 1.3 | Verify records exist on S3 by checking the bucket contents for files under `<bucket>/certstream/seen_date=<date>/` prefix. | Parquet files are present under the expected partition path. |
| 1.4 | Re-run backfill in catch-up mode: `cargo run -- --backfill --logs "<same-log-name>"` (no `--from` flag). | Logs show gap detection running. Output shows `LEAD()` window function finding gaps where Ctrl-C interrupted. Backfill fills the gaps. Log output reports records written > 0. |
| 1.5 | Verify the process exits with code 0: `echo $?` | Output: `0` |
| 1.6 | Re-run catch-up backfill again: `cargo run -- --backfill --logs "<same-log-name>"` | Gap detection finds no gaps (or minimal frontier gaps). Either no work items generated or exit code 0 with no records written. |

## Phase 2: Backfill Writer with S3 Staging (HV-2)

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Run `cargo run -- --backfill --from 0 --logs "<log-name>" --staging-path "s3://<bucket>/staging"` with `delta_sink.table_path = "s3://<bucket>/certstream"` and valid S3 credentials. | Process starts, creates staging table at `s3://<bucket>/staging/`, begins writing records. |
| 2.2 | Allow the backfill to write at least 20 records, then let it finish or interrupt with Ctrl-C. | Log output shows `records_written` count. Process exits. |
| 2.3 | Verify staging table exists on S3 by checking the bucket for files under `<bucket>/staging/seen_date=<date>/` and `<bucket>/staging/_delta_log/`. | Parquet data files and Delta log JSON files are present. |
| 2.4 | Check exit code: `echo $?` | Output: `0` if completed; `1` if interrupted. |

## Phase 3: Merge with S3 Tables (HV-3)

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Ensure Phase 2 completed successfully and staging table has data at `s3://<bucket>/staging`. | Staging table exists with records. |
| 3.2 | Run `cargo run -- --merge --staging-path "s3://<bucket>/staging"` with `delta_sink.table_path = "s3://<bucket>/certstream"` and valid S3 credentials. | Process starts, reads staging records, merges into main table. |
| 3.3 | Watch log output for merge record counts. | Logs show batch processing with record counts. No errors reported. |
| 3.4 | Verify staging objects are deleted from S3: check bucket for `<bucket>/staging/` prefix. | The `staging/` prefix should be empty or absent (objects deleted by `cleanup_staging()`). |
| 3.5 | Verify main table has merged records: use a DataFusion query or check that the main table's parquet files increased in count/size. | Main table contains records from both the original backfill and the staging merge. |
| 3.6 | Check exit code: `echo $?` | Output: `0` |

## Phase 4: Query API Against S3 Table (HV-4)

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Update `config.yaml` to set `query_api.enabled = true` and `query_api.table_path = "s3://<bucket>/certstream"`. Ensure the S3 table has data from prior backfill runs. | Config saved. |
| 4.2 | Start the server: `cargo run` | Server starts, WebSocket/SSE endpoints available. No errors for query API initialization. |
| 4.3 | Run `curl "http://localhost:8080/api/query/certs?domain=<known-domain>"` where `<known-domain>` is a domain present in the backfilled data (e.g., a domain from the CT log you backfilled). | HTTP 200 response with JSON body containing `version`, `results` array (non-empty), `has_more` field. |
| 4.4 | Test pagination: `curl "http://localhost:8080/api/query/certs?domain=<known-domain>&limit=2"` | Response includes `has_more: true` and `next_cursor` (if more than 2 matching records exist). |
| 4.5 | Follow cursor: `curl "http://localhost:8080/api/query/certs?domain=<known-domain>&limit=2&cursor=<next_cursor>"` | Response includes additional results. |
| 4.6 | Test missing table: update `query_api.table_path` to `"s3://<bucket>/nonexistent-prefix"`, restart server, then `curl "http://localhost:8080/api/query/certs?domain=test"` | HTTP 503 response. |

## Phase 5: S3 Connection Failure Handled Non-Fatally (HV-5)

| Step | Action | Expected |
|------|--------|----------|
| 5.1 | **delta_sink non-fatal exit:** Update `config.yaml` with `delta_sink.enabled = true`, `delta_sink.table_path = "s3://nonexistent-bucket-xyz/path"`, and `storage.s3.endpoint = "https://invalid.endpoint.example.com"`. Start the server: `cargo run` | Server starts successfully. WebSocket endpoint at `ws://localhost:8080/` is reachable. Delta sink logs an error about table creation failure and exits its task. No panic. |
| 5.2 | Verify WebSocket still works: open `wscat -c ws://localhost:8080/` or similar WebSocket client. | Connection established. CT log data (if CT log fetching is configured) streams to the client. |
| 5.3 | **backfill exit code 1:** Run `cargo run -- --backfill --from 0 --logs "test"` with the same invalid S3 config. | Process exits with an error about table open failure. |
| 5.4 | Check exit code: `echo $?` | Output: `1` |
| 5.5 | **query returns 503:** Update `config.yaml` with `query_api.enabled = true`, `query_api.table_path = "s3://nonexistent-bucket-xyz/path"`, valid `storage.s3` credentials pointing to a real endpoint but nonexistent bucket. Start server and run `curl "http://localhost:8080/api/query/certs?domain=test"` | HTTP 503 response with a message about the table not being available. |

## End-to-End: Full S3 Lifecycle

**Purpose:** Validates the complete flow of writing certificates to S3, staging, merging, and querying — all over S3 storage.

| Step | Action | Expected |
|------|--------|----------|
| E2E.1 | Configure the server with `delta_sink.table_path = "s3://<bucket>/e2e-test"`, valid S3 credentials, and a small CT log. | Config ready. |
| E2E.2 | Run historical backfill to staging: `cargo run -- --backfill --from 0 --logs "<small-log>" --staging-path "s3://<bucket>/e2e-staging"` | Records written to S3 staging table. Exit code 0. |
| E2E.3 | Merge staging into main: `cargo run -- --merge --staging-path "s3://<bucket>/e2e-staging"` | Merge succeeds. Staging cleaned up. Exit code 0. |
| E2E.4 | Run catch-up backfill against main table: `cargo run -- --backfill --logs "<small-log>"` | Gap detection runs against S3 main table. Any remaining gaps filled. Exit code 0. |
| E2E.5 | Start server with `query_api.enabled = true`, `query_api.table_path = "s3://<bucket>/e2e-test"`. Query: `curl "http://localhost:8080/api/query/certs?from=2020-01-01"` | Results returned from S3-backed query. HTTP 200. |
| E2E.6 | Clean up: delete all objects under `<bucket>/e2e-test/` and `<bucket>/e2e-staging/` prefixes in the Tigris bucket. | Bucket cleaned. |

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|-----------|-------|
| AC3.4 — Gap detection against S3 | The `LEAD()` window function SQL with `UNION ALL` staging and `COUNT(DISTINCT cert_index)` is not exercised against S3 in integration tests. | Phase 1, Steps 1.1-1.6 |
| AC3.5 — Backfill writer to S3 staging | Full backfill pipeline (fetcher tasks -> mpsc channel -> writer task -> S3 Delta table) requires real CT log HTTP responses. | Phase 2, Steps 2.1-2.4 |
| AC3.6 — Merge with S3 tables | Merge between two S3 tables including `cleanup_staging()` S3 object deletion is not automated. | Phase 3, Steps 3.1-3.6 |
| AC3.7 — Query API against S3 | Query API is an HTTP endpoint requiring a running server process. | Phase 4, Steps 4.1-4.6 |
| AC3.9 — S3 connection failure non-fatal | Runtime behavior of unreachable S3 endpoints through existing error paths. | Phase 5, Steps 5.1-5.5 |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 | `config::tests::test_parse_table_uri_absolute_path` | — |
| AC1.2 | `config::tests::test_parse_table_uri_relative_path` | — |
| AC1.3 | `config::tests::test_parse_table_uri_s3` | — |
| AC1.4 | `config::tests::test_parse_table_uri_bare_path` | — |
| AC1.5 | `config::tests::test_parse_table_uri_unsupported_scheme` | — |
| AC1.6 | `config::tests::test_parse_table_uri_empty_string` | — |
| AC2.1 | `test_storage_config_yaml_with_s3_section` + `test_validate_s3_uri_with_full_config` | — |
| AC2.2 | `test_storage_config_yaml_no_s3_section` + `test_validate_file_uri_without_s3_config` | — |
| AC2.3 | `test_validate_s3_uri_without_storage_config` | — |
| AC2.4 | `test_validate_s3_uri_with_empty_endpoint` | — |
| AC2.5 | 3 env var tests (override, trigger, no-endpoint) | — |
| AC2.6 | `test_validate_staging_path_uri_format` | — |
| AC3.1 | `test_s3_create_write_read_roundtrip` (Step 3) | — |
| AC3.2 | `test_s3_create_write_read_roundtrip` (Step 1) | — |
| AC3.3 | `test_s3_create_write_read_roundtrip` (Step 2) | — |
| AC3.4 | — | Phase 1, Steps 1.1-1.6 |
| AC3.5 | Regression: backfill tests with `HashMap::new()` | Phase 2, Steps 2.1-2.4 |
| AC3.6 | Regression: merge tests with `HashMap::new()` | Phase 3, Steps 3.1-3.6 |
| AC3.7 | Regression: query tests with `HashMap::new()` | Phase 4, Steps 4.1-4.6 |
| AC3.8 | All 388 unit tests pass with `HashMap::new()` | — |
| AC3.9 | Regression: existing error-path tests | Phase 5, Steps 5.1-5.5 |
| AC4.1 | `test_s3_create_write_read_roundtrip` (Step 1) | — |
| AC4.2 | `test_s3_create_write_read_roundtrip` (Steps 2-3) | — |
| AC4.3 | `test_s3_conditional_put_etag` | — |
