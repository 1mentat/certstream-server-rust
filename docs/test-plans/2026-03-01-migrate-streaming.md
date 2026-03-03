# Human Test Plan: Streaming Migration

## Prerequisites

- Rust toolchain installed (edition 2024)
- Working directory: project root
- All automated tests passing: `cargo test test_migrate` (6 tests should pass)
- A sample Delta table with old schema (as_der as base64 Utf8 strings) available for testing. If none exists, one can be created by running the server with an older version of the code and writing some data to Delta, or by using the test helper `old_schema_batch()` in a standalone script.

## Phase 1: Code Review -- Streaming API Usage

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Open `src/backfill.rs` and navigate to the `run_migrate` function (line 1138). | Function signature is `pub async fn run_migrate(config, output_path, source_path, from_date, to_date, shutdown) -> i32`. |
| 1.2 | Locate the per-partition data fetch code around line 1279. | Line 1279 reads `let mut stream = match partition_df.execute_stream().await {`. There should be NO `partition_df.collect().await` call in the per-partition data path. |
| 1.3 | Verify the stream consumption loop starting at line 1288. | Line 1288 reads `while let Some(batch_result) = stream.next().await {`. The loop body transforms one batch and writes it via `DeltaOps.write()` before the next iteration. |
| 1.4 | Confirm no batch accumulation exists within the loop. | There should be no `Vec<RecordBatch>` or similar collection accumulating batches between lines 1288 and 1398. Each `batch` variable goes out of scope at the end of the loop iteration (line 1398 closing brace) before `stream.next().await` fetches the next one. |
| 1.5 | Verify the `StreamExt` import at line 14 of the file. | Line 14 reads `use futures::stream::StreamExt;`. |
| 1.6 | Verify the error handling for mid-stream batch failure at lines 1289-1295. | The `match batch_result` has an `Err(e)` arm that logs a warning with `"failed to read batch from stream"` and returns exit code 1. |

## Phase 2: Code Review -- No Remaining collect() in Migrate Path

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Search for all `.collect().await` calls within the `run_migrate` function body (lines 1138-1417). | Only one `.collect().await` should remain: line 1191, which collects the partition date list (`partitions_df.collect().await`). This is the metadata query for partition names, not the data path. |
| 2.2 | Confirm line 1191 collects partition dates (metadata), not record data. | The SQL query at line 1183 is `SELECT DISTINCT seen_date FROM source ORDER BY seen_date`. This returns at most one row per partition date, which is small and appropriate to collect. |

## Phase 3: End-to-End Migration with Real Data

Purpose: Validate the full migration pipeline produces correct output with a realistic dataset.

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Create a test source table using the old schema. Run the following from the project root: `cargo run -- --migrate --output /tmp/e2e_migrate_out --source /tmp/e2e_migrate_src`. (If the source path does not exist, it should fail gracefully.) | Exit code 1 with a log message indicating the source table could not be opened. |
| 3.2 | If a real old-schema Delta table exists (e.g., from a prior deployment), run: `cargo run -- --migrate --output /tmp/e2e_migrate_out --source <PATH_TO_OLD_TABLE>`. | Exit code 0. Log output should show the number of partitions found and total rows migrated. |
| 3.3 | After a successful migration, inspect the output table using a Delta-capable tool (e.g., Python `deltalake` library or the query API). Verify the `as_der` column is Binary type, not Utf8. | `as_der` column dtype should be `binary` (not `string`/`utf8`). Values should be raw DER bytes, not base64-encoded strings. |
| 3.4 | Run migration with date filters: `cargo run -- --migrate --output /tmp/e2e_filtered_out --source <PATH> --from 2024-06-01 --to 2024-06-30`. | Only partitions with `seen_date` between 2024-06-01 and 2024-06-30 (inclusive) should appear in the output. |
| 3.5 | Run migration and send SIGINT (Ctrl+C) during processing. | Migration should exit with code 1. Partial output may exist in the output table. The source table should be unmodified. |

## Phase 4: Memory Behavior Verification (Optional, Large Dataset)

Purpose: Validate that streaming reduces memory usage compared to collect() on a large partition.

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Identify or create a source table with at least one partition containing more than 1 million records (or enough data that collecting all batches into memory would be noticeable, e.g., 500MB+). | Source table exists with a large partition. |
| 4.2 | Run the migration with memory monitoring: `RUST_LOG=info cargo run -- --migrate --output /tmp/large_migrate_out --source <PATH>` while observing RSS via `top`, `htop`, or `/proc/<pid>/status`. | Peak RSS should remain significantly below the total size of the largest partition's data. If streaming is working correctly, memory should be proportional to a single batch size, not the entire partition. |

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|------------|-------|
| migrate-streaming.AC1.1 | Confirming `execute_stream()` is used instead of `collect()` is a structural code property. Automated tests verify correct output but cannot distinguish which API produced it. | Phase 1, Steps 1.1-1.5 |
| migrate-streaming.AC1.2 | Proving no batch accumulation occurs is a memory-behavior property. Tests verify correct output but cannot observe whether batches are held in memory simultaneously. | Phase 1, Step 1.4 |
| migrate-streaming.AC1.3 (mid-stream error) | A mid-stream batch read failure is difficult to trigger without mocking DataFusion internals. The code path is straightforward. | Phase 1, Step 1.6 |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| migrate-streaming.AC1.1 (execute_stream used) | `test_migrate_basic_schema_and_data`, `test_migrate_streaming_processes_batches` | Phase 1, Steps 1.1-1.5 |
| migrate-streaming.AC1.2 (per-batch processing, no accumulation) | `test_migrate_streaming_processes_batches` | Phase 1, Step 1.4 |
| migrate-streaming.AC1.3 (errors propagated) | `test_migrate_nonexistent_source_table` | Phase 1, Step 1.6 |
| migrate-streaming.AC2.1 (as_der base64 to binary) | `test_migrate_basic_schema_and_data`, `test_migrate_streaming_processes_batches` | Phase 3, Step 3.3 |
| migrate-streaming.AC2.2 (column alignment by name) | `test_migrate_basic_schema_and_data`, `test_migrate_streaming_processes_batches` | Phase 3, Step 3.2 |
| migrate-streaming.AC2.3 (date filters, graceful shutdown) | `test_migrate_with_date_filters`, `test_migrate_graceful_shutdown` | Phase 3, Steps 3.4-3.5 |
| migrate-streaming.AC2.4 (backward compatibility) | `test_migrate_basic_schema_and_data`, `test_migrate_graceful_shutdown`, `test_migrate_nonexistent_source_table`, `test_migrate_empty_source_table`, `test_migrate_with_date_filters` | N/A (fully covered by automated tests) |
