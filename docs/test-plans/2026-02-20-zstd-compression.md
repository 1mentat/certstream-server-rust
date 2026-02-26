# Human Test Plan: Zstd Compression

## Prerequisites

- Rust toolchain installed (edition 2024)
- `cargo test` passing (314 tests, 0 failures) in the worktree
- `parquet-tools` CLI installed (via `pip install parquet-tools`) OR access to a Rust script using `SerializedFileReader`
- A CT log state file (`certstream_state.json`) with at least one log entry, or access to a running CT environment
- The delta_sink feature enabled in config.yaml (`delta_sink.enabled: true`)

## Phase 1: Configuration Validation

| Step | Action | Expected |
|------|--------|----------|
| 1 | Create a `config.yaml` with `delta_sink` section that omits the `compression_level` field entirely. Run `cargo run -- --validate-config`. | Server starts config validation successfully. No error related to compression_level. |
| 2 | Set `delta_sink.compression_level: 5` in `config.yaml`. Run `cargo run -- --validate-config`. | Config validation passes. |
| 3 | Set `delta_sink.compression_level: 0` in `config.yaml`. Run `cargo run -- --validate-config`. | Server exits with a clear error message mentioning `compression_level` is invalid and must be between 1 and 22. |
| 4 | Set `delta_sink.compression_level: 23` in `config.yaml`. Run `cargo run -- --validate-config`. | Server exits with a clear error message mentioning `compression_level` is invalid and must be between 1 and 22. |
| 5 | Remove `compression_level` from `config.yaml`. Set env var `CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL=3`. Run `cargo run -- --validate-config`. | Config validation passes. Env var override is accepted. |
| 6 | Set env var `CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL=0`. Run `cargo run -- --validate-config`. | Server exits with a clear error message about invalid compression_level. |

## Phase 2: Live Delta Sink ZSTD Output

| Step | Action | Expected |
|------|--------|----------|
| 1 | Configure `config.yaml` with `delta_sink.enabled: true`, `delta_sink.table_path: /tmp/zstd_live_test`, `delta_sink.batch_size: 100`, `delta_sink.flush_interval_secs: 10`, `delta_sink.compression_level: 9`. Start the server with `cargo run`. | Server starts successfully. Delta sink task logs "Delta sink task started". |
| 2 | Wait for at least one flush to complete (check logs for "records_written" or similar flush message). Then stop the server (Ctrl+C). | Server shuts down gracefully, flushing remaining buffer. |
| 3 | Find a `.parquet` file in `/tmp/zstd_live_test/` (excluding the `_delta_log/` directory). Run `parquet-tools meta <file>`. | The row group column chunk metadata reports `ZSTD` as the compression codec (not `SNAPPY` or `UNCOMPRESSED`). |

## Phase 3: Backfill Writer ZSTD Output

| Step | Action | Expected |
|------|--------|----------|
| 1 | Ensure `certstream_state.json` exists with at least one log entry that has a `current_index` value. Configure `delta_sink.table_path: /tmp/zstd_backfill_test` and `delta_sink.compression_level: 9`. | State file and config are ready. |
| 2 | Run `cargo run -- --backfill --from 0 --staging-path /tmp/zstd_staging_test` (or use a known small index range with `--logs` to limit scope). | Backfill completes, writing records to `/tmp/zstd_staging_test/`. Exit code is 0. |
| 3 | Find a `.parquet` file in `/tmp/zstd_staging_test/` (excluding `_delta_log/`). Run `parquet-tools meta <file>`. | Column chunks report `ZSTD` compression. |

## Phase 4: Merge Operation ZSTD Output (Human Verification Required -- AC2.3)

| Step | Action | Expected |
|------|--------|----------|
| 1 | Use the staging table created in Phase 3 Step 2 (at `/tmp/zstd_staging_test`). Ensure a main table exists at the configured `delta_sink.table_path` (e.g., `/tmp/zstd_backfill_test`). If no main table exists, create one by running a brief backfill directly to it. | Both staging and main tables exist. |
| 2 | Run `cargo run -- --merge --staging-path /tmp/zstd_staging_test`. | Merge completes with exit code 0. Staging directory is deleted. |
| 3 | Identify the newest `.parquet` file in the main table directory (`/tmp/zstd_backfill_test/`, excluding `_delta_log/`) -- this will be the file written by the merge operation. Run `parquet-tools meta <file>`. | Column chunks report `ZSTD` compression. The merge operation's `with_writer_properties()` call applied the configured ZSTD level. |

## End-to-End: Full Pipeline with Mixed Codecs

**Purpose:** Validate that a Delta table can contain a mixture of old Snappy-compressed files and new ZSTD-compressed files, and all query/read operations continue to work.

| Step | Action | Expected |
|------|--------|----------|
| 1 | If you have access to an existing Delta table with Snappy-compressed Parquet files, copy it to `/tmp/zstd_mixed_test/`. Otherwise, use the pre-zstd codebase (commit `a6b38b1`) to write some records to `/tmp/zstd_mixed_test/` (which will produce Snappy-compressed files). | Table directory contains `.parquet` files with Snappy compression. |
| 2 | Switch to the zstd-compression branch. Configure `delta_sink.table_path: /tmp/zstd_mixed_test` and `delta_sink.compression_level: 9`. Run the server briefly to write new ZSTD-compressed files to the same table. Stop the server. | New `.parquet` files are created in the table alongside the old Snappy files. |
| 3 | Run `parquet-tools meta` on an old file and a new file. | Old file shows `SNAPPY` compression. New file shows `ZSTD` compression. |
| 4 | If the query API is enabled (`query_api.enabled: true`), query the table via `GET /api/query/certs?domain=<some_domain>`. Alternatively, open the table with DataFusion/DeltaLake to read all records. | All records (from both Snappy and ZSTD files) are returned correctly. No errors about incompatible codecs. |

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|-----------|-------|
| zstd-compression.AC2.3 -- Merge writes ZSTD-compressed Parquet | The merge operation requires a fully populated staging Delta table and a main Delta table. Constructing this end-to-end in an automated test would duplicate extensive existing merge infrastructure for a single `.with_writer_properties()` chain addition with no branching logic. | See Phase 4 above. |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| zstd-compression.AC1.1 -- Default compression_level=9 | `config::tests::test_delta_sink_config_default_compression_level`, `test_delta_sink_config_deserialize_compression_level_defaults_to_9`, `test_validate_compression_level_default` | Phase 1, Step 1 |
| zstd-compression.AC1.2 -- Explicit level accepted | `config::tests::test_delta_sink_config_deserialize_compression_level`, `test_validate_compression_level_valid_min` | Phase 1, Step 2 |
| zstd-compression.AC1.3 -- Level 0 rejected | `config::tests::test_validate_compression_level_zero` | Phase 1, Step 3 |
| zstd-compression.AC1.4 -- Level 23 rejected | `config::tests::test_validate_compression_level_too_high` | Phase 1, Step 4 |
| zstd-compression.AC2.1 -- Live sink writes ZSTD | `delta_sink::tests::test_flush_buffer_writes_zstd_compression` | Phase 2, Steps 1-3 |
| zstd-compression.AC2.2 -- Backfill writes ZSTD | 9 `run_writer` call sites with `compression_level=9` (compilation proof) + AC2.1 test (shared `flush_buffer`) | Phase 3, Steps 1-3 |
| zstd-compression.AC2.3 -- Merge writes ZSTD | None (human verification) | Phase 4, Steps 1-3 |
| zstd-compression.AC2.4 -- Mixed codec coexistence | `delta_sink::tests::test_flush_buffer_writes_zstd_compression` (implicit) | End-to-End, Steps 1-4 |
