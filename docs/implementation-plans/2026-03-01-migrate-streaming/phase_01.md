# Streaming Migration Implementation Plan

**Goal:** Replace `partition_df.collect().await` with `partition_df.execute_stream().await` in `run_migrate` to eliminate OOM on large partitions.

**Architecture:** The per-partition data query currently materializes all rows via `.collect()` (~97 GB for 25-36M row partitions). Replacing with `.execute_stream()` yields one `RecordBatch` at a time (~8K rows, ~5-20 MB). Each batch is transformed (base64-to-binary for `as_der`, column alignment) and written to the output Delta table immediately, then dropped before the next batch is fetched.

**Tech Stack:** Rust, DataFusion (`execute_stream()`, `SendableRecordBatchStream`), `futures::stream::StreamExt`, Delta Lake (`DeltaOps.write()`)

**Scope:** 1 phase from original design (phase 1 of 1)

**Codebase verified:** 2026-03-01

---

## Acceptance Criteria Coverage

This phase implements and tests:

### migrate-streaming.AC1: Streaming batch processing
- **migrate-streaming.AC1.1 Success:** run_migrate processes partition data via execute_stream() instead of collect(), yielding one RecordBatch at a time
- **migrate-streaming.AC1.2 Success:** Each batch is transformed and written before the next batch is fetched (no accumulation)
- **migrate-streaming.AC1.3 Success:** Stream errors (DataFusion batch read failures) are handled gracefully with exit code 1

### migrate-streaming.AC2: Existing functionality preserved
- **migrate-streaming.AC2.1 Success:** as_der base64-to-binary conversion produces identical output to the collect-based implementation
- **migrate-streaming.AC2.2 Success:** Column alignment by name with type casting works identically per batch
- **migrate-streaming.AC2.3 Success:** Date filtering (--from, --to), source path override (--source), and graceful shutdown all work unchanged
- **migrate-streaming.AC2.4 Success:** Existing migrate tests pass without modification (test_migrate_basic_schema_and_data, test_migrate_graceful_shutdown, test_migrate_nonexistent_source_table, test_migrate_empty_source_table, test_migrate_with_date_filters)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Replace collect() with execute_stream() in run_migrate

**Verifies:** migrate-streaming.AC1.1, migrate-streaming.AC1.2, migrate-streaming.AC1.3, migrate-streaming.AC2.1, migrate-streaming.AC2.2, migrate-streaming.AC2.3

**Files:**
- Modify: `src/backfill.rs:1` (add import)
- Modify: `src/backfill.rs:1278-1289` (replace collect with execute_stream)

**Implementation:**

Two changes in `src/backfill.rs`:

**Change 1: Add import.** Add `use futures::stream::StreamExt;` to the imports at the top of `src/backfill.rs` (after line 6, alongside the other `use` statements). The `futures` crate (v0.3.31) is already in `Cargo.toml`.

**Change 2: Replace the collect-and-iterate pattern with stream-and-iterate.** In `run_migrate`, inside the per-partition loop, replace lines 1278-1289:

Current code (lines 1278-1289):
```rust
        let batches = match partition_df.collect().await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, partition = %seen_date, "failed to collect partition batches");
                return 1;
            }
        };

        // Process each batch in the partition
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
```

Replace with:
```rust
        let mut stream = match partition_df.execute_stream().await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, partition = %seen_date, "failed to start partition stream");
                return 1;
            }
        };

        // Process each batch from the stream
        while let Some(batch_result) = stream.next().await {
            let batch = match batch_result {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, partition = %seen_date, "failed to read batch from stream");
                    return 1;
                }
            };
            if batch.num_rows() == 0 {
                continue;
            }
```

The rest of the per-batch body (lines 1293-1389: `as_der` transformation, column alignment, `DeltaOps` write, `current_output_table` reassignment) remains unchanged. The closing brace of the `for batch in batches` loop (which becomes the `while let` loop) also stays.

This is the complete change. The `execute_stream()` call returns a `SendableRecordBatchStream` (already pinned via `Pin<Box<...>>`). Each `stream.next().await` yields `Option<Result<RecordBatch, DataFusionError>>`. When the stream is exhausted, `next()` returns `None` and the loop exits. Batches are processed and written one at a time — no accumulation.

**Verification:**

Run: `cargo build`
Expected: Compiles without errors.

Run: `cargo test -- test_migrate`
Expected: All 5 existing migrate tests pass (test_migrate_basic_schema_and_data, test_migrate_graceful_shutdown, test_migrate_nonexistent_source_table, test_migrate_empty_source_table, test_migrate_with_date_filters).

**Commit:** `feat: replace collect() with execute_stream() in run_migrate`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add streaming verification test

**Verifies:** migrate-streaming.AC1.1, migrate-streaming.AC1.2, migrate-streaming.AC2.4

**Files:**
- Modify: `src/backfill.rs` (add test in `#[cfg(test)] mod tests` block)

**Implementation:**

Add a new test `test_migrate_streaming_processes_batches` in the `#[cfg(test)] mod tests` module of `src/backfill.rs`. This test verifies that:
1. `run_migrate` successfully processes a source table with multiple batches (create enough records across multiple partitions to produce multiple RecordBatches)
2. All records from source appear correctly transformed in the output table (as_der converted from base64 to binary)
3. The output is identical regardless of batch count (functional correctness, not implementation detail)

The test should follow the existing migrate test patterns:
- Use `#[tokio::test]` attribute
- Create temp directories at `/tmp/delta_migrate_test_streaming_src` and `_out`
- Use `old_delta_schema()` and `old_schema_batch()` helpers to create source data
- Write multiple partitions (e.g., 3 different `seen_date` values) with multiple records each to ensure multiple batches are produced
- Call `run_migrate()` and verify exit code 0
- Query the output table with DataFusion to verify all records exist with correct `as_der` binary values
- Clean up temp directories with `fs::remove_dir_all()`

**Testing:**
Tests must verify:
- migrate-streaming.AC1.1: run_migrate completes successfully processing data that previously would have been collected (proven by correct output from multi-partition source)
- migrate-streaming.AC1.2: Each batch is transformed and written individually (proven by correct output — if accumulation occurred and failed, output would be wrong or exit code 1)
- migrate-streaming.AC2.4: Existing tests still pass (verified by running all migrate tests together)

Follow existing test patterns in `src/backfill.rs` (lines 3862-4146). Use `make_test_config()`, `old_delta_schema()`, `old_schema_batch()`, `open_or_create_table()`, `DeltaOps().write()`, and `SessionContext` for verification queries.

**Verification:**

Run: `cargo test -- test_migrate`
Expected: All 6 migrate tests pass (5 existing + 1 new).

**Commit:** `test: add streaming verification test for run_migrate`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->
