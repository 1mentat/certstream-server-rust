# Delta Encoding Optimization — Human Test Plan

## Prerequisites
- Rust toolchain installed (edition 2024)
- `cargo test` passing (386 tests, 0 failures)
- Access to a Delta table with old schema (Utf8 `as_der`) for migration testing, or ability to create one

## Phase 1: Per-column Encoding Configuration

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Open `src/config.rs` and find `DeltaSinkConfig`. Verify `heavy_column_compression_level` field exists with `#[serde(default = "default_heavy_column_compression_level")]`. | Field is declared with default value function. |
| 1.2 | Open `config.example.yaml`. Verify `heavy_column_compression_level` is documented. | Example config includes the new field with a comment. |
| 1.3 | Set env var `CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL=20` and run `cargo run -- --validate-config`. | Config validation passes. The heavy_column_compression_level is accepted as 20. |
| 1.4 | Set env var `CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL=0` and run `cargo run -- --validate-config`. | Config validation fails with error mentioning `heavy_column_compression_level`. |

## Phase 2: as_der Binary Schema Change

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Start the server with delta_sink enabled. Send a certificate through WebSocket. Verify the WebSocket consumer receives `as_der` as a base64 string (not raw bytes or integer array). | WebSocket JSON message contains `"as_der": "<base64-string>"`, not `"as_der": [1, 2, 3, ...]`. |
| 2.2 | After step 2.1, open the Parquet file written by the delta sink. Use `parquet-cli schema <file>` or equivalent to inspect the `as_der` column. | The `as_der` column has physical type `BYTE_ARRAY` with no logical annotation (or `BINARY`), not `UTF8`/`STRING`. |
| 2.3 | Read the as_der values from the Parquet file. Base64-decode the original WebSocket `as_der` value. | The raw bytes in the Parquet file match the base64-decoded bytes from the WebSocket message. |

## Phase 3: Migration Tool

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Create a Delta table with old schema: `as_der` as Utf8 containing base64-encoded strings. Run `cargo run -- --migrate --output /tmp/migrated`. | Exit code 0. Output directory `/tmp/migrated` contains a valid Delta table. |
| 3.2 | Open the output table. Query `SELECT as_der FROM <output>` and inspect column type. | Column type is Binary. Values are raw DER bytes (not base64 strings). |
| 3.3 | Compare the output table to the source: verify `cert_index`, `fingerprint`, `sha256`, `serial_number`, `source_name`, `source_url`, and all other non-as_der columns have identical values. | All 19 non-as_der columns match exactly between source and output. |
| 3.4 | Run `cargo run -- --migrate --output /tmp/migrated2` against a multi-partition source table. While it is running, send SIGINT (Ctrl+C) after at least one partition has been written. | Exit code 1. The output directory either does not exist or contains only complete, readable partitions. No corrupted data. |
| 3.5 | Run `cargo run -- --migrate` (without `--output`). | Error message indicating `--output` is required when using `--migrate`. |

## Phase 4: No Regression - Full Write Path Verification

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Run `cargo run -- --backfill --from 0` with delta_sink enabled and a state file present. Let it write at least one batch. Open the resulting Parquet files. | All columns use ZSTD compression. `as_der` is Binary type. Dictionary-encoded columns (update_type, source_name, source_url, signature_algorithm, issuer_aggregated) have dictionary pages. |
| 4.2 | Create a staging table with `--backfill --staging-path /tmp/staging`. Then merge with `--merge --staging-path /tmp/staging`. Query the merged main table. | Merge succeeds (exit code 0). Main table contains records from both original and staging. `as_der` is Binary throughout. |
| 4.3 | With the query API enabled (`query_api.enabled = true`), issue `GET /api/query/certs?domain=example.com` against a table written with the new Binary schema. | Query returns results with all 12 response fields populated. No errors about schema mismatch (the query excludes `as_der` and `chain`). |

## End-to-End: Live Sink + Migration + Query

**Purpose:** Validate the full lifecycle from live ingestion through migration to query.

1. Start the server with `delta_sink.enabled = true`, `heavy_column_compression_level = 15`, `compression_level = 9`.
2. Let the server ingest live CT log entries for a few minutes, producing Parquet files.
3. Stop the server.
4. Verify the `as_der` column in the Parquet files is Binary (not Utf8).
5. Run the Query API against the table: `GET /api/query/certs?domain=google&from=<today>`. Verify results are returned correctly.
6. Compare file sizes of `as_der` column chunks to other string columns of similar length -- `as_der` should show higher compression ratio due to `heavy_column_compression_level = 15`.

## End-to-End: ZeroBus Sink Preserved

**Purpose:** Validate ZeroBus sink continues to function with Binary as_der.

1. Enable ZeroBus sink with valid credentials.
2. Start the server. Let it process certificates.
3. Verify ZeroBus records are ingested without errors (check `certstream_zerobus_ingest_errors` metric).
4. Query the Databricks table. Verify `as_der` column contains valid base64 strings (re-encoded from the raw bytes).

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|------------|-------|
| AC1.3 (compression levels) | Parquet metadata exposes the compression codec (ZSTD) but not the compression level parameter. The automated test verifies WriterProperties is configured correctly and Parquet uses ZSTD, but cannot confirm level 15 vs level 9 from file metadata. | Compare Parquet file sizes: write a test table with uniform level 9 vs one with level 15 on `as_der`. The `as_der` column chunks should be smaller in the level-15 table. Alternatively, trust the WriterProperties unit test which verifies `Compression::ZSTD(ZstdLevel(15))` for `as_der` and `Compression::ZSTD(ZstdLevel(9))` for all other columns. |
| AC1.4 (centralized write paths) | Structural property that all write paths use `delta_writer_properties()`. | Run: `grep -n "WriterProperties::builder()" src/delta_sink.rs src/backfill.rs`. Confirm the only occurrence is inside `delta_writer_properties()` at `src/delta_sink.rs:507`. No inline construction exists in `flush_buffer`, `run_merge`, or `run_migrate`. |
| AC3.4 (graceful shutdown mid-migration) | The automated test uses a pre-cancelled token (immediate exit). Testing partial-work-done requires precise timing. Delta Lake's transactional writes prevent corruption structurally, but an operator should verify for production tables. | Run `--migrate --output /tmp/test-output` against a multi-day table. Send SIGINT after a few partitions complete. Open the output in DataFusion and confirm all present partitions are readable with valid data. |
| AC5.2 (all tests pass) | Fully automatable via CI, but requires human to confirm test count has not decreased. | Run `cargo test` and confirm 386 tests pass, 0 failures, 0 ignored. Compare to the pre-change baseline test count to ensure no tests were dropped or silently `#[ignore]`d. |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 Dictionary encoding enabled for low-cardinality columns | `test_delta_writer_properties_dictionary_enabled_for_low_cardinality`, `test_writer_properties_per_column_encoding` | 4.1 |
| AC1.2 Dictionary encoding disabled for high-cardinality columns | `test_delta_writer_properties_dictionary_disabled_for_high_cardinality`, `test_writer_properties_per_column_encoding` | 4.1 |
| AC1.3 Per-column compression levels | `test_delta_writer_properties_compression_levels`, `test_writer_properties_per_column_encoding` | AC1.3 manual verification |
| AC1.4 Centralized delta_writer_properties() | `test_delta_writer_properties_returns_valid_properties` | AC1.4 manual verification (grep) |
| AC2.1 as_der stored as Binary | `test_delta_schema_field_types`, `test_records_to_batch_contains_as_der_binary`, `test_from_json_deserializes_all_fields`, `test_as_der_binary_round_trip` | 2.2 |
| AC2.2 Round-trip Binary bytes | `test_as_der_binary_round_trip` | 2.3 |
| AC2.3 ZeroBus base64 re-encoding | `test_from_delta_cert_all_fields` | E2E: ZeroBus Sink |
| AC3.1 --migrate writes Binary as_der | `test_migrate_basic_schema_and_data`, CLI tests | 3.1, 3.2 |
| AC3.2 Migrated values match decode | `test_migrate_basic_schema_and_data` | 3.2 |
| AC3.3 Non-as_der columns unchanged | `test_migrate_basic_schema_and_data` | 3.3 |
| AC3.4 Graceful shutdown safe | `test_migrate_graceful_shutdown` | 3.4, AC3.4 manual verification |
| AC4.1 heavy_column_compression_level defaults | 3 config tests in `src/config.rs` | 1.3 |
| AC4.2 Invalid heavy_column_compression_level rejected | 4 validation tests in `src/config.rs` | 1.4 |
| AC5.1 Query API against new schema | `test_query_api_with_binary_as_der_schema` | 4.3 |
| AC5.2 All existing tests pass | `cargo test` (386 passed, 0 failed) | AC5.2 manual verification |
| AC5.3 ZeroBus protobuf preserved | `test_from_delta_cert_all_fields`, `test_cert_record_round_trip` | E2E: ZeroBus Sink |
