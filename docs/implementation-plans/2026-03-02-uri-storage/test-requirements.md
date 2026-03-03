# URI-Based Storage Backend: Test Requirements

Maps every acceptance criterion from the [URI storage design](design-plans/2026-03-02-uri-storage.md)
to either an automated test or a documented human verification step.

Rationalized against implementation decisions in
[phase_01.md](implementation-plans/2026-03-02-uri-storage/phase_01.md) through
[phase_07.md](implementation-plans/2026-03-02-uri-storage/phase_07.md).

---

## Legend

| Column | Meaning |
|--------|---------|
| **Criterion** | Acceptance criterion ID and description |
| **Test type** | `unit` (cargo test), `integration` (cargo test --features integration), `e2e` (human/CI), `human` (manual) |
| **Test file** | Expected source file containing the test |
| **Phase** | Implementation phase that introduces the test |
| **Notes** | Rationalization against implementation decisions |

---

## uri-storage.AC1: URI scheme parsing

All AC1 criteria are pure function tests on `parse_table_uri()`. No I/O, no external dependencies.
Implemented in Phase 1, Task 4.

| Criterion | Description | Test type | Test file | Phase | Notes |
|-----------|-------------|-----------|-----------|-------|-------|
| AC1.1 | `file:///absolute/path` parses to `Local { path: "/absolute/path" }` | unit | `src/config.rs` (mod tests) | 1 | Direct assertion on `parse_table_uri()` return value. Phase 1 Task 3 implements the function using `strip_prefix("file://")`. |
| AC1.2 | `file://./relative/path` parses to `Local { path: "./relative/path" }` | unit | `src/config.rs` (mod tests) | 1 | Same `strip_prefix` logic preserves the relative `./` prefix. No path canonicalization is performed, matching the design's explicit decision. |
| AC1.3 | `s3://bucket/prefix` parses to `S3 { uri: "s3://bucket/prefix" }` | unit | `src/config.rs` (mod tests) | 1 | Full URI preserved in the `S3` variant. |
| AC1.4 | Bare path `./data/certstream` rejected with error suggesting `file://` prefix | unit | `src/config.rs` (mod tests) | 1 | The `else` branch in `parse_table_uri()` catches URIs without `://` and produces the suggestion message. Test asserts `Err` contains both the input path and `file://`. |
| AC1.5 | Unknown scheme `gcs://bucket/path` rejected with error listing supported schemes | unit | `src/config.rs` (mod tests) | 1 | The `uri.contains("://")` branch matches unknown schemes and lists `file://`, `s3://` in the error. Test asserts `Err` contains "Unsupported URI scheme" and the supported list. |
| AC1.6 | Empty string rejected | unit | `src/config.rs` (mod tests) | 1 | First guard in `parse_table_uri()` checks `uri.is_empty()`. Test asserts `Err` contains "cannot be empty". |

---

## uri-storage.AC2: Storage config validation

AC2 criteria span config deserialization, env var overrides, and `Config::validate()` logic.
Implemented in Phase 1, Tasks 1-2 (config structs), Tasks 5-6 (validation).

| Criterion | Description | Test type | Test file | Phase | Notes |
|-----------|-------------|-----------|-----------|-------|-------|
| AC2.1 | Config loads with `storage.s3` section when S3 URIs present | unit | `src/config.rs` (mod tests) | 1 | Two tests: (a) YAML deserialization test verifies `S3StorageConfig` fields populate correctly from a YAML string with `storage.s3` section. (b) Validation test constructs Config with `delta_sink.enabled=true`, `delta_sink.table_path="s3://bucket/path"`, `storage.s3` fully populated, and asserts `validate()` returns `Ok(())`. Phase 1 Task 2 covers deserialization; Task 6 covers validation. |
| AC2.2 | Config loads without `storage.s3` section when only `file://` URIs used | unit | `src/config.rs` (mod tests) | 1 | Two tests: (a) Deserialization test verifies `StorageConfig::default()` has `s3: None`. (b) Validation test constructs Config with `delta_sink.enabled=true`, `delta_sink.table_path="file://./data/certstream"`, `storage.s3=None`, and asserts `validate()` returns `Ok(())`. |
| AC2.3 | S3 URI present but `storage.s3` missing -- validation error | unit | `src/config.rs` (mod tests) | 1 | Config with `delta_sink.enabled=true`, `table_path="s3://bucket/path"`, `storage.s3=None`. Assert `validate()` returns `Err` with error on field `"storage.s3"`. Phase 1 Task 5 implements the `has_s3_uri` check in `Config::validate()`. |
| AC2.4 | S3 URI present but `storage.s3.endpoint` empty -- validation error | unit | `src/config.rs` (mod tests) | 1 | Config with `delta_sink.enabled=true`, `table_path="s3://bucket/path"`, `storage.s3=Some(S3StorageConfig { endpoint: "", ... })`. Assert `validate()` returns `Err` with error on field `"storage.s3.endpoint"`. Phase 1 Task 5 validates all four required S3 fields when S3 URIs are present. |
| AC2.5 | Env vars override config.yaml S3 fields | unit | `src/config.rs` (mod tests) | 1 | Following the existing `unsafe { env::set_var(...) }` pattern (see zerobus env var tests at config.rs line 1149). Test sets `CERTSTREAM_STORAGE_S3_ENDPOINT` and verifies override. Additional edge case test: set `CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID` without setting `CERTSTREAM_STORAGE_S3_ENDPOINT` and verify no S3 config is created (endpoint is the trigger per Phase 1 Task 1 implementation). |
| AC2.6 | `--staging-path` CLI arg validated as URI with recognized scheme | unit | `src/config.rs` (mod tests) | 1 | Tested via `parse_table_uri()` directly: (a) `parse_table_uri("file:///tmp/staging")` returns `Ok`. (b) `parse_table_uri("s3://bucket/staging")` returns `Ok`. (c) `parse_table_uri("/tmp/staging")` returns `Err` suggesting `file://` prefix. The actual CLI validation is implemented in `src/main.rs` (Phase 1 Task 5) using the same `parse_table_uri()` function, so testing the function covers the criterion. Runtime validation of the main.rs code path is covered by existing integration patterns (the server process exits with a clear error on invalid staging paths). |

---

## uri-storage.AC3: Delta code paths work over S3

AC3 criteria require either a real S3 endpoint (integration tests) or verification that local
paths still work (existing unit tests as regression). The implementation strategy across Phases 2-6
is to thread `HashMap<String, String>` storage options through all table open/create call sites.
Passing `HashMap::new()` for local paths preserves identical behavior to the previous bare-path code.

| Criterion | Description | Test type | Test file | Phase | Notes |
|-----------|-------------|-----------|-----------|-------|-------|
| AC3.1 | `open_or_create_table()` opens existing S3-backed table | integration | `src/delta_sink.rs` (mod s3_integration_tests) | 7 | Phase 7 `test_s3_create_write_read_roundtrip`: creates a table, then reopens it via `DeltaTableBuilder::from_uri()` with storage options. The reopen step validates AC3.1. Cannot be unit-tested without a real S3 endpoint because `DeltaTableBuilder` attempts a network connection. |
| AC3.2 | `open_or_create_table()` creates new table at S3 location when none exists | integration | `src/delta_sink.rs` (mod s3_integration_tests) | 7 | Phase 7 `test_s3_create_write_read_roundtrip`: the initial `open_or_create_table()` call targets a unique `s3://bucket/integration-test-{uuid}` path that does not exist. Asserts `table.version() == 0` after creation. |
| AC3.3 | delta_sink writes batches to S3-backed Delta table | integration | `src/delta_sink.rs` (mod s3_integration_tests) | 7 | Phase 7 `test_s3_create_write_read_roundtrip`: writes 5 records via `DeltaOps::write()` and asserts `table.version() == 1`. The write path uses the same `DeltaOps` and `WriterProperties` that `flush_buffer()` uses in production. Phase 3 threads storage options through `run_delta_sink()` and `flush_buffer()`; the integration test validates the end-to-end write. |
| AC3.4 | Backfill gap detection queries S3-backed Delta table via DataFusion | integration + human | `src/delta_sink.rs` (mod s3_integration_tests) | 7 | **Partially automated.** The integration test in Phase 7 validates that DataFusion can query an S3-backed table (Step 3 of the round-trip test runs `SELECT ... FROM certs` via `SessionContext`). However, the specific `detect_gaps()` SQL with `LEAD()` window functions is not exercised against S3 in the integration test. See **Human verification** section below. |
| AC3.5 | Backfill writer writes records to S3-backed Delta table (including staging) | integration | `src/delta_sink.rs` (mod s3_integration_tests) | 7 | **Partially automated.** The round-trip test validates the write primitives (`open_or_create_table`, `DeltaOps::write`) that the backfill writer uses. Phase 4 threads storage options through `run_writer()` and `detect_gaps()`, and all existing backfill unit tests pass with `HashMap::new()` confirming no local regression. Full backfill-to-S3 flow validation requires running `cargo run -- --backfill --staging-path s3://bucket/staging` against a real S3 endpoint. See **Human verification** section below. |
| AC3.6 | Merge reads S3 staging table and merges into S3 main table | integration + human | -- | 5, 7 | **Not directly automated.** Phase 5 threads storage options through `run_merge()` including S3-aware staging cleanup via `cleanup_staging()`. Existing merge unit tests pass with `HashMap::new()` confirming local behavior is preserved. Merge against S3 requires running `cargo run -- --merge --staging-path s3://bucket/staging` with data already written. See **Human verification** section below. |
| AC3.7 | Query API reads and searches S3-backed Delta table with pagination | integration + human | -- | 6, 7 | **Partially automated.** The integration test validates DataFusion queries against S3 tables. Phase 6 replaces `deltalake::open_table()` with `DeltaTableBuilder` in the query handler, and all existing query unit tests pass with `HashMap::new()`. Full Query API validation against S3 requires a running server with `query_api.table_path = "s3://..."`. See **Human verification** section below. |
| AC3.8 | All `file://` paths continue to work identically to previous bare-path behavior | unit | `src/delta_sink.rs`, `src/backfill.rs`, `src/query.rs` (all mod tests) | 2-6 | **Regression coverage via existing test suite.** Phase 2 updates all ~60 callers of `open_or_create_table()` to pass `HashMap::new()`. All existing 359 unit tests must pass after each phase. The implementation decision in Phase 2 is that `DeltaTableBuilder::from_uri()` accepts bare paths for local filesystem, and `TableLocation::as_uri()` returns the stripped path (no `file://` prefix) for Local variants. `HashMap::new()` produces identical behavior to the previous `deltalake::open_table()` call. |
| AC3.9 | S3 connection failure handled non-fatally | unit + human | `src/delta_sink.rs`, `src/backfill.rs`, `src/query.rs` (existing error path tests) | 3-6 | **Covered by existing error handling patterns, verified by code review.** Phase 3 notes that `run_delta_sink()` already returns early on table open failure (non-fatal, server continues). `run_backfill()` returns exit code 1 on table errors. `query_certs()` returns 503 on `NotATable`/`InvalidTableLocation` errors. The `DeltaTableBuilder` errors for unreachable S3 endpoints fall into the same `Err(e)` match arms as local filesystem errors. No new error handling code is introduced -- the existing match arms cover S3 failures identically to local failures. See **Human verification** section for explicit validation approach. |

---

## uri-storage.AC4: Tigris validation

All AC4 criteria require a real Tigris bucket with valid credentials. Tests are gated behind
`feature = "integration"` following the existing ZeroBus integration test pattern
(see `src/zerobus_sink.rs:591`). Environment variables required:
- `CERTSTREAM_TEST_S3_ENDPOINT` (e.g., `https://fly.storage.tigris.dev`)
- `CERTSTREAM_TEST_S3_BUCKET`
- `CERTSTREAM_TEST_S3_ACCESS_KEY_ID`
- `CERTSTREAM_TEST_S3_SECRET_ACCESS_KEY`

Tests skip gracefully when env vars are not set (match on `Err(_)` with `eprintln` + `return`).

| Criterion | Description | Test type | Test file | Phase | Notes |
|-----------|-------------|-----------|-----------|-------|-------|
| AC4.1 | Integration test creates Delta table on Tigris bucket | integration | `src/delta_sink.rs` (mod s3_integration_tests) | 7 | `test_s3_create_write_read_roundtrip` Step 1: calls `open_or_create_table()` with a unique `s3://bucket/integration-test-{uuid}` path. Asserts `table.version() == 0`. Cleanup deletes all objects under the prefix after test completion. |
| AC4.2 | Integration test writes batch and reads it back with matching data | integration | `src/delta_sink.rs` (mod s3_integration_tests) | 7 | `test_s3_create_write_read_roundtrip` Steps 2-3: writes 5 `DeltaCertRecord` instances via `DeltaOps::write()`, reopens table via `DeltaTableBuilder`, queries with DataFusion SQL, and asserts `total_rows == 5`. |
| AC4.3 | Conditional put (etag) prevents concurrent write corruption | integration | `src/delta_sink.rs` (mod s3_integration_tests) | 7 | `test_s3_conditional_put_etag`: creates table, writes batch via handle A, opens stale handle B at version 1, writes second batch via handle A (advancing to version 2), then attempts write via stale handle B. With `conditional_put: "etag"` in storage options, Delta's optimistic concurrency either resolves the conflict (version >= 3) or rejects it with a conflict error. Both outcomes prove etag is working. The test asserts on both success and error paths. |

---

## Human verification

The following criteria cannot be fully automated with unit or integration tests as currently
designed. Each includes a justification and a concrete verification approach.

### HV-1: AC3.4 -- Backfill gap detection against S3 Delta table

**Justification:** The Phase 7 integration test validates DataFusion queries against S3 (simple
`SELECT`), but does not exercise the specific `detect_gaps()` SQL that uses `LEAD()` window
functions, `UNION ALL` between main and staging tables, and `COUNT(DISTINCT cert_index)`. Writing
a full gap detection integration test would require pre-populating an S3 table with known gaps,
which adds significant setup complexity.

**Verification approach:**
1. Set up a Tigris bucket with credentials.
2. Run a historical backfill to S3: `cargo run -- --backfill --from 0 --logs "some-small-log"` with
   `delta_sink.table_path = "s3://bucket/certstream"` and `storage.s3` configured.
3. After some records are written, interrupt the backfill (Ctrl-C) to create a gap.
4. Re-run backfill in catch-up mode: `cargo run -- --backfill` (no `--from`).
5. Verify that gap detection identifies and fills the gap (check logs for gap detection output and
   confirm records_written increases).
6. Verify exit code 0.

### HV-2: AC3.5 -- Backfill writer with S3 staging

**Justification:** The integration test validates the underlying write primitives but not the full
backfill pipeline (fetcher tasks -> mpsc channel -> writer task -> S3 Delta table). A full pipeline
test would require mocking CT log HTTP responses or running against real CT logs, which is
out of scope for the integration test module.

**Verification approach:**
1. Set up a Tigris bucket with credentials.
2. Run: `cargo run -- --backfill --from 0 --logs "some-small-log" --staging-path "s3://bucket/staging"`
   with `delta_sink.table_path = "s3://bucket/certstream"` and `storage.s3` configured.
3. Verify staging table is created at the S3 staging path (check bucket contents).
4. Verify records are written (check logs for `records_written` count).
5. Verify exit code 0.

### HV-3: AC3.6 -- Merge with S3 tables

**Justification:** Merge requires both a main table and a staging table to exist with data. The
Phase 7 integration tests focus on table creation and write/read round-trips. A merge integration
test would need to pre-populate two separate S3 tables, run the merge, verify data moved correctly,
and confirm S3 staging cleanup via `cleanup_staging()`. The S3 object listing and deletion in
`cleanup_staging()` (Phase 5 Task 2) uses `object_store` APIs that differ from the
`std::fs::remove_dir_all()` path.

**Verification approach:**
1. Complete HV-2 (backfill to S3 staging) so staging table has data.
2. Run: `cargo run -- --merge --staging-path "s3://bucket/staging"` with
   `delta_sink.table_path = "s3://bucket/certstream"` and `storage.s3` configured.
3. Verify merge completes (check logs for merge record counts).
4. Verify staging objects are deleted from S3 (check bucket for `staging/` prefix -- should be empty).
5. Verify main table has merged records (query via DataFusion or S3 file listing).
6. Verify exit code 0.

### HV-4: AC3.7 -- Query API against S3 table

**Justification:** The Query API is an HTTP endpoint (`GET /api/query/certs`) that requires a
running server process. Integration testing an HTTP server with S3-backed storage requires either
an e2e test harness with process management or in-process HTTP client tests. The existing query
unit tests cover all query logic (domain search, pagination, cursor, timeouts) against local tables;
the only delta for S3 is the table open path, which is validated by the Phase 7 integration test.

**Verification approach:**
1. Start the server with `query_api.enabled = true`, `query_api.table_path = "s3://bucket/certstream"`,
   and `storage.s3` configured. Ensure the S3 table has data (from prior backfill runs).
2. `curl "http://localhost:8080/api/query/certs?domain=example.com"` -- verify 200 response with results.
3. Test pagination: make a request with `limit=2`, follow `next_cursor` in the response, verify
   next page returns additional results.
4. Test missing table: point `query_api.table_path` to a non-existent S3 prefix, verify 503 response.

### HV-5: AC3.9 -- S3 connection failure handled non-fatally

**Justification:** Simulating S3 connection failures in unit tests would require mocking the
`object_store` layer, which the codebase does not use (tests run against real local Delta tables).
The implementation relies on existing error handling: `DeltaTableBuilder::from_uri().load()` returns
`Err(DeltaTableError::...)` for unreachable endpoints, and all call sites already handle `Err`
branches identically for local and S3 paths. Phase 3 explicitly documents that no new error
handling code is needed.

**Verification approach:**
1. **delta_sink non-fatal exit:** Start the server with `delta_sink.enabled = true`,
   `delta_sink.table_path = "s3://nonexistent-bucket/path"`, and `storage.s3` configured with an
   invalid endpoint. Verify the server starts (WebSocket/SSE still work), delta_sink task logs an
   error and exits, and no panic occurs.
2. **backfill exit code 1:** Run `cargo run -- --backfill --from 0 --logs "test"` with an
   unreachable S3 endpoint. Verify exit code is 1 and error is logged.
3. **query returns 503:** Start the server with `query_api.enabled = true` and
   `query_api.table_path = "s3://nonexistent-bucket/path"`. Send a query request and verify
   503 response.

---

## Test execution summary

### Automated tests (cargo test)

| Suite | Command | Criteria covered | Count |
|-------|---------|-----------------|-------|
| Unit tests (all) | `cargo test` | AC1.1-AC1.6, AC2.1-AC2.6, AC3.8, AC3.9 (partial) | 14 |
| Integration tests (S3) | `cargo test --features integration s3_integration_tests -- --nocapture` | AC3.1-AC3.3, AC4.1-AC4.3 | 5 |

### Automated test file locations

| Test file | Test module | Criteria | Type |
|-----------|-------------|----------|------|
| `src/config.rs` | `mod tests` | AC1.1, AC1.2, AC1.3, AC1.4, AC1.5, AC1.6 | unit |
| `src/config.rs` | `mod tests` | AC2.1, AC2.2, AC2.3, AC2.4, AC2.5, AC2.6 | unit |
| `src/delta_sink.rs` | `mod tests` | AC3.8 (regression) | unit |
| `src/backfill.rs` | `mod tests` | AC3.8 (regression) | unit |
| `src/query.rs` | `mod tests` | AC3.8 (regression) | unit |
| `src/delta_sink.rs` | `mod s3_integration_tests` | AC3.1, AC3.2, AC3.3, AC4.1, AC4.2, AC4.3 | integration |

### Human verification steps

| ID | Criteria | Requires |
|----|----------|----------|
| HV-1 | AC3.4 | Tigris bucket, CT log state file, two backfill runs |
| HV-2 | AC3.5 | Tigris bucket, CT log state file, one backfill run with --staging-path |
| HV-3 | AC3.6 | Tigris bucket, completed HV-2, one merge run |
| HV-4 | AC3.7 | Tigris bucket, running server, curl |
| HV-5 | AC3.9 | Invalid S3 endpoint, server start, backfill run, query request |

---

## Coverage matrix

Every acceptance criterion is accounted for below. "Auto" means a dedicated automated test exists.
"Regression" means existing tests serve as regression coverage. "Human" means manual verification
is required.

| Criterion | Auto | Regression | Human | Justification |
|-----------|------|------------|-------|---------------|
| AC1.1 | Yes | -- | -- | Pure function test |
| AC1.2 | Yes | -- | -- | Pure function test |
| AC1.3 | Yes | -- | -- | Pure function test |
| AC1.4 | Yes | -- | -- | Pure function test |
| AC1.5 | Yes | -- | -- | Pure function test |
| AC1.6 | Yes | -- | -- | Pure function test |
| AC2.1 | Yes | -- | -- | Config deserialization + validation test |
| AC2.2 | Yes | -- | -- | Config deserialization + validation test |
| AC2.3 | Yes | -- | -- | Validation test |
| AC2.4 | Yes | -- | -- | Validation test |
| AC2.5 | Yes | -- | -- | Env var override test |
| AC2.6 | Yes | -- | -- | parse_table_uri covers same code path as CLI validation |
| AC3.1 | Yes | -- | -- | Integration test opens S3 table |
| AC3.2 | Yes | -- | -- | Integration test creates S3 table |
| AC3.3 | Yes | -- | -- | Integration test writes to S3 table |
| AC3.4 | -- | -- | HV-1 | Gap detection SQL not exercised against S3 in integration tests |
| AC3.5 | -- | Yes | HV-2 | Write primitives tested; full pipeline needs real CT logs |
| AC3.6 | -- | Yes | HV-3 | Merge logic tested locally; S3 merge + cleanup needs manual run |
| AC3.7 | -- | Yes | HV-4 | Query logic tested locally; HTTP endpoint on S3 needs running server |
| AC3.8 | -- | Yes | -- | All 359 existing tests pass with HashMap::new() |
| AC3.9 | -- | Yes | HV-5 | Error branches identical for S3 and local; manual confirms runtime behavior |
| AC4.1 | Yes | -- | -- | Integration test |
| AC4.2 | Yes | -- | -- | Integration test |
| AC4.3 | Yes | -- | -- | Integration test |
