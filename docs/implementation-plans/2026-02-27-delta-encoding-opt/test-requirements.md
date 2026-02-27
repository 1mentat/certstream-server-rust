# Delta Encoding Optimization -- Test Requirements

This document maps every acceptance criterion (AC1.1 through AC5.3) from the
design plan to specific automated tests or documented human verification steps.

**Design deviation note:** The original design specifies changing `LeafCert.as_der`
from `Option<String>` to `Option<Vec<u8>>` and removing `STANDARD.encode()` from
`parser.rs`. Phase 2 planning discovered this would break JSON serialization for
WebSocket/SSE consumers (`Vec<u8>` serializes as an integer array, not a base64
string). Instead, `LeafCert.as_der` stays as `Option<String>` and the base64
decoding happens in `DeltaCertRecord::from_message()`. All Delta and ZeroBus
paths pass through `from_message()`, so the ACs are satisfied identically. Tests
reference the actual implementation boundary (`DeltaCertRecord.as_der: Vec<u8>`)
rather than the originally-planned `LeafCert` change.

---

## Automated Tests

### delta-encoding-opt.AC1: Per-column encoding optimization

| AC | Criterion Text | Test Type | Test Location | Description |
|----|---------------|-----------|---------------|-------------|
| AC1.1 | WriterProperties enables dictionary encoding for update_type, source_name, source_url, signature_algorithm, issuer_aggregated, and chain list items | unit | `src/delta_sink.rs` (test module, after `test_flush_buffer_writes_zstd_compression`) | Calls `delta_writer_properties(9, 15)` and asserts `WriterProperties::dictionary_enabled(&ColumnPath)` returns `true` for each of the six low-cardinality column paths (`update_type`, `source_name`, `source_url`, `signature_algorithm`, `issuer_aggregated`, `chain.list.item`). The `chain.list.item` path may need adjustment depending on the Parquet 3-level list naming convention; the test should write a table with chain data and inspect metadata to confirm the correct path. |
| AC1.2 | WriterProperties disables dictionary encoding for high-cardinality columns (cert_link, serial_number, fingerprint, sha256, sha1, subject_aggregated, as_der) | unit | `src/delta_sink.rs` (test module, same test or adjacent) | Calls `delta_writer_properties(9, 15)` and asserts `WriterProperties::dictionary_enabled(&ColumnPath)` returns `false` for each of the seven high-cardinality column paths. |
| AC1.3 | as_der column uses heavy_column_compression_level; all other columns use compression_level | integration | `src/delta_sink.rs` (test module, new test extending `test_flush_buffer_writes_zstd_compression` pattern) | Writes records to a Delta table via `flush_buffer` with `compression_level=9` and `heavy_column_compression_level=15`. Opens the resulting Parquet file with `SerializedFileReader`, iterates row group column metadata, and asserts: (a) the `as_der` column has ZSTD compression, (b) other columns also have ZSTD compression, (c) the compression codec settings reflect the per-column levels. |
| AC1.4 | All three write paths (live sink, backfill, merge) use the centralized delta_writer_properties() function | verification-only | N/A (structural/compile-time) | Structurally verified: `flush_buffer()` (used by live sink and backfill writer) and `run_merge()` both call `delta_writer_properties()`. If the code compiles after the refactor and all `flush_buffer` callers pass both compression level parameters, centralization is complete. An additional unit test calls `delta_writer_properties()` directly and asserts it returns non-default `WriterProperties` (has per-column settings), confirming the function exists and is callable. |

### delta-encoding-opt.AC2: as_der schema change

| AC | Criterion Text | Test Type | Test Location | Description |
|----|---------------|-----------|---------------|-------------|
| AC2.1 | as_der stored as Binary (raw DER bytes) in Delta table, not base64 Utf8 | unit + integration | `src/delta_sink.rs` (test module) | **Unit:** Asserts `delta_schema()` returns a schema where the `as_der` field has `DataType::Binary`. **Unit:** `test_records_to_batch_contains_as_der_binary` downcasts the `as_der` column to `BinaryArray` (not `StringArray`) and verifies values are raw bytes. **Unit:** `test_from_json_deserializes_all_fields` asserts `record.as_der == vec![1u8, 2, 3]` (decoded from base64 `"AQID"` in test JSON). **Integration:** `test_flush_buffer_writes_zstd_compression` (updated) writes records and reads back via Parquet metadata confirming Binary column type. |
| AC2.2 | Round-trip: write DER bytes -> read back -> bytes match original | integration | `src/delta_sink.rs` (test module, new round-trip test) | Creates a `DeltaCertRecord` with known `as_der` bytes (e.g., `vec![0xDE, 0xAD, 0xBE, 0xEF]`). Writes to a temp Delta table via `flush_buffer`. Reads back via DataFusion SQL `SELECT as_der FROM ct_records`. Downcasts result to `BinaryArray`. Asserts read-back bytes match the original input exactly. |
| AC2.3 | ZeroBus sink correctly base64-encodes raw bytes for protobuf string field | unit | `src/zerobus_sink.rs` (test module) | `test_from_delta_cert_all_fields` creates a `DeltaCertRecord` with `as_der: vec![1u8, 2, 3]`, calls `CertRecord::from_delta_cert()`, and asserts `cert_record.as_der == STANDARD.encode(&[1u8, 2, 3])` (i.e., `"AQID"`). Confirms the protobuf string field receives a valid base64 string, not raw bytes. |

### delta-encoding-opt.AC3: Table migration

| AC | Criterion Text | Test Type | Test Location | Description |
|----|---------------|-----------|---------------|-------------|
| AC3.1 | --migrate --output <path> reads source table and writes new table with Binary as_der at output path | integration | `src/backfill.rs` (test module) + `src/cli.rs` (test module) | **CLI parsing:** Tests in `src/cli.rs` verify `--migrate` sets `migrate: true` and `--output /tmp/out` sets `migrate_output: Some("/tmp/out")`. **Migration execution:** Test in `src/backfill.rs` creates a source Delta table with old schema (`DataType::Utf8` for `as_der`, base64-encoded strings), runs `run_migrate()`, asserts exit code 0, opens the output table, and verifies the `as_der` column is `DataType::Binary`. |
| AC3.2 | Migrated as_der values match STANDARD.decode() of original base64 strings | integration | `src/backfill.rs` (test module, same basic migration test) | After running `run_migrate()`, reads the output table via DataFusion, downcasts `as_der` to `BinaryArray`, and asserts each value equals `STANDARD.decode()` of the corresponding base64 string from the source table. Uses known test values (e.g., source `"3q2+7w=="` -> output `[0xDE, 0xAD, 0xBE, 0xEF]`). |
| AC3.3 | All non-as_der columns pass through unchanged | integration | `src/backfill.rs` (test module, dedicated column-preservation test) | Creates a source table with multiple records having distinct values for all 20 columns. Runs `run_migrate()`. Reads both source and output tables. Asserts every column except `as_der` has identical values between source and output (same cert_index, fingerprint, sha256, serial_number, subject, issuer, domains, chain, etc.). |
| AC3.4 | Graceful shutdown (CancellationToken) leaves completed partitions in output table and does not corrupt data | integration | `src/backfill.rs` (test module, graceful shutdown test) | Creates a source table with records in at least 2 different `seen_date` partitions. Creates a pre-cancelled `CancellationToken`. Runs `run_migrate()` with the cancelled token. Asserts exit code is 1. Verifies the output directory either does not exist or contains only valid, non-corrupted data for any partitions that completed before cancellation was detected. |

### delta-encoding-opt.AC4: Configuration

| AC | Criterion Text | Test Type | Test Location | Description |
|----|---------------|-----------|---------------|-------------|
| AC4.1 | heavy_column_compression_level defaults to 15, configurable via CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL | unit | `src/config.rs` (test module) | Three tests: (a) `DeltaSinkConfig::default().heavy_column_compression_level == 15`, (b) deserializing YAML without the field defaults to 15, (c) deserializing YAML with a custom value (e.g., 18) preserves it. Follows the existing pattern of `test_delta_sink_config_defaults`. |
| AC4.2 | Invalid heavy_column_compression_level (outside 1-22) rejected at config validation | unit | `src/config.rs` (test module) | Two tests following the pattern of `test_validate_compression_level_zero` and `test_validate_compression_level_too_high`: (a) `heavy_column_compression_level = 0` causes `Config::validate()` to return an error mentioning `heavy_column_compression_level`, (b) `heavy_column_compression_level = 23` likewise fails. An additional boundary test verifies values 1 and 22 pass validation. |

### delta-encoding-opt.AC5: No regression

| AC | Criterion Text | Test Type | Test Location | Description |
|----|---------------|-----------|---------------|-------------|
| AC5.1 | Query API returns correct results against tables written with new schema | integration | `src/query.rs` (test module) | Creates a Delta table with the new schema (Binary `as_der`) using `records_to_batch()` + `DeltaOps::write()`. Runs a Query API search (domain or issuer). Verifies results are returned correctly with all 12 response fields populated. Confirms the query does not fail due to schema mismatch, since the Query API's SELECT explicitly excludes `as_der` and `chain`. Follows the `test_response_includes_required_fields` pattern at `src/query.rs:740`. |
| AC5.2 | All existing tests pass after changes | integration | All test files (`cargo test`) | Running `cargo test` with zero failures after all Phase 1-4 changes are applied. Any test that references `as_der` as a String must be updated in Phase 2 Task 3 to use `Vec<u8>`. Key files requiring updates: `src/delta_sink.rs` tests (String -> Vec<u8> assertions, StringArray -> BinaryArray downcasts), `src/zerobus_sink.rs` tests (DeltaCertRecord.as_der field), `src/backfill.rs` test helpers (make_test_record JSON fixture). |
| AC5.3 | ZeroBus sink continues to function (protobuf as_der field populated correctly) | unit | `src/zerobus_sink.rs` (test module) | Covered by the updated `test_from_delta_cert_all_fields` (same test as AC2.3) plus `test_cert_record_round_trip` which verifies protobuf encode/decode round-trip of the string `as_der` field. Together these confirm the ZeroBus protobuf contract is preserved: raw bytes in -> base64 string in protobuf -> identical base64 string out after decode. |

---

## Test Summary by File

| Test File | ACs Covered | New Tests | Modified Tests |
|-----------|-------------|-----------|----------------|
| `src/config.rs` | AC4.1, AC4.2 | 4-5 new (default value, YAML without field, YAML with field, validation of 0, validation of 23, boundary 1/22) | 0 |
| `src/delta_sink.rs` | AC1.1, AC1.2, AC1.3, AC1.4, AC2.1, AC2.2, AC5.2 | 3-4 new (dictionary enabled check, dictionary disabled check, per-column compression check, Binary round-trip) | 3-4 modified (`test_flush_buffer_writes_zstd_compression` gets new param; `test_from_json_deserializes_all_fields` asserts Vec<u8>; `test_records_to_batch_contains_as_der_string` renamed to `_binary` with BinaryArray; `test_records_to_batch_with_empty_domains_and_chain` uses BinaryArray) |
| `src/zerobus_sink.rs` | AC2.3, AC5.3 | 0 | 2 modified (`make_test_record` uses Vec<u8>; `test_from_delta_cert_all_fields` asserts base64 re-encoding) |
| `src/backfill.rs` | AC3.1, AC3.2, AC3.3, AC3.4, AC5.2 | 4-5 new (basic migration, column preservation, graceful shutdown, empty table edge case, backfill writer with Binary as_der) | 1-2 modified (`make_test_record` JSON fixture updated; `make_test_config` adds `heavy_column_compression_level: 15`) |
| `src/cli.rs` | AC3.1 (partial) | 3-4 new (--migrate parsed, --output parsed, both together, neither present) | 0 |
| `src/query.rs` | AC5.1 | 1 new (query against Binary as_der table) | 0 |

---

## Human Verification

### AC1.3: Per-column compression level values in Parquet metadata

**Criterion:** as_der column uses heavy_column_compression_level; all other columns use compression_level

**Why partial human verification is needed:** The Parquet file metadata exposes the
compression *codec* (e.g., ZSTD) per column but does not expose the compression
*level* in the column metadata. The `parquet` crate's `ColumnChunkMetaData::compression()`
returns `Compression::ZSTD(ZstdLevel)` but the level information may not be
recoverable from the on-disk format -- Parquet stores only the codec name, not the
level parameter. The automated test can verify that (a) `as_der` uses ZSTD and
(b) other columns use ZSTD, but cannot directly assert level 15 vs level 9 from
the file metadata. The `WriterProperties` unit test (AC1.1/AC1.2 tests) verifies
the properties object is configured correctly, which provides strong confidence the
levels are applied. To fully verify distinct compression levels in practice, an
operator can compare file sizes between a table written with uniform level 9 vs
one written with level 15 on `as_der` and confirm the `as_der` column is more
compressed.

**Verification approach:** The automated test confirms the `WriterProperties` object
is constructed with the correct per-column `Compression::ZSTD(ZstdLevel)` values.
The integration test confirms the Parquet file uses ZSTD codec on all columns.
Together these provide sufficient confidence without manual intervention for normal
development. For production deployment, an operator should compare table sizes
before and after to confirm the expected compression improvement.

### AC1.4: All three write paths use centralized delta_writer_properties()

**Criterion:** All three write paths (live sink, backfill, merge) use the centralized delta_writer_properties() function

**Why partial human verification is needed:** This is a structural property of the
code. The automated verification is that the code compiles after the refactor (all
`flush_buffer` callers pass both compression level parameters, `run_merge` calls
`delta_writer_properties`). A code review should confirm no write path constructs
`WriterProperties` inline. Grepping for `WriterProperties::builder()` outside of
`delta_writer_properties()` and the test module confirms no stale inline construction
exists.

**Verification approach:** After implementation, run:
`grep -n "WriterProperties::builder()" src/delta_sink.rs src/backfill.rs`
and confirm the only occurrence is inside `delta_writer_properties()` (plus test
code). This can be added as a CI check or documented as a review step.

### AC3.4: Graceful shutdown does not corrupt data

**Criterion:** Graceful shutdown (CancellationToken) leaves completed partitions in output table and does not corrupt data

**Why partial human verification is needed:** The automated test uses a pre-cancelled
token, which means migration exits before processing any partitions. This tests the
"no work done" case cleanly. Testing the "partial work done" case (some partitions
completed, then interrupted mid-partition) is harder to automate reliably because
it requires precise timing control. Delta Lake's transactional writes mean that a
partition either commits fully or not at all, so corruption is structurally
prevented. However, an operator running the migration against a large production
table should verify that if they Ctrl-C mid-migration, the output directory
contains only complete, readable partitions.

**Verification approach:** The automated test covers the immediate-cancellation path.
For the mid-migration interruption case, the operator should:
1. Run `--migrate --output /tmp/test-output` against a multi-day production table
2. Send SIGINT after a few partitions complete
3. Verify exit code is 1
4. Open the output table in DataFusion or `delta-inspect` and confirm all present
   partitions are readable with valid data

### AC5.2: All existing tests pass after changes

**Criterion:** All existing tests pass after changes

**Why partial human verification is needed:** This is verified by running `cargo test`
and confirming zero failures. It is fully automatable and should be part of CI.
However, it requires a human to confirm the test run covers the full codebase
(no tests were accidentally deleted or `#[ignore]`d). A comparison of test count
before and after the changes provides additional confidence.

**Verification approach:** Run `cargo test` and confirm all tests pass. Compare the
total test count to the pre-change baseline to ensure no tests were dropped.

---

## Phase-to-Test Mapping

| Phase | Tasks | ACs Tested | New/Modified Tests |
|-------|-------|------------|-------------------|
| Phase 1 | Tasks 1-5 | AC1.1, AC1.2, AC1.3, AC1.4, AC4.1, AC4.2 | Config defaults + validation tests, WriterProperties unit tests, per-column Parquet metadata test |
| Phase 2 | Tasks 1-3 | AC2.1, AC2.2, AC2.3, AC5.2, AC5.3 | Schema type assertion, Binary round-trip test, ZeroBus base64 re-encoding test, all existing tests updated for Vec<u8> |
| Phase 3 | Tasks 1-3 | AC3.1, AC3.2, AC3.3, AC3.4, AC5.2 | CLI flag parsing tests, migration basic test, column preservation test, graceful shutdown test, empty table edge case |
| Phase 4 | Tasks 1-6 | AC4.1, AC4.2, AC5.1, AC5.2, AC5.3 | Per-column encoding Parquet verification, Binary+WriterProperties round-trip, backfill/merge with Binary, Query API against new schema, full `cargo test` run |
