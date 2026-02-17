# Delta Backfill Implementation Plan - Phase 5: Writer Task and End-to-End Integration

**Goal:** Implement the single writer task that receives DeltaCertRecords from all fetcher tasks via mpsc channel and flushes them to the delta table. Wire up the full pipeline: gap detection -> concurrent fetch -> write -> completion summary.

**Architecture:** The writer task receives `DeltaCertRecord` from the mpsc channel receiver, buffers them, and flushes to delta using the existing `records_to_batch()` and `flush_buffer()` functions from `delta_sink.rs`. Flushes trigger on batch_size threshold, periodic timer, or channel close (all fetchers done). After all fetchers complete and the channel drains, the writer produces a completion summary and `run_backfill()` returns the appropriate exit code.

**Tech Stack:** Rust, tokio (mpsc, select!, Instant), existing delta_sink functions, tracing for progress/summary logging

**Scope:** 5 phases from original design (phase 5 of 5)

**Codebase verified:** 2026-02-17

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-backfill.AC4: Works with existing delta table schema and partitioning
- **delta-backfill.AC4.2 Success:** Records written by backfill are indistinguishable from records written by the live sink (same schema, same field population)
- **delta-backfill.AC4.3 Edge:** Backfill into an empty/nonexistent table creates it with the correct schema and partitioning

### delta-backfill.AC5: Handles interruption gracefully
- **delta-backfill.AC5.3 Failure:** Permanently unreachable log does not block other logs — fetcher for that log exits after retry exhaustion, others continue
- **delta-backfill.AC5.4 Success:** Exit code reflects whether all logs completed (0) or some failed (non-zero)

### delta-backfill.AC6: Cross-Cutting Behaviors
- **delta-backfill.AC6.1:** Progress is logged periodically per log (entries fetched / total, gaps remaining)
- **delta-backfill.AC6.2:** Rate limit errors (HTTP 429/503) are handled with exponential backoff per log
- **delta-backfill.AC6.3:** Completion summary reports total records written, logs completed/failed, elapsed time

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Implement the writer task

**Verifies:** delta-backfill.AC4.2, delta-backfill.AC4.3

**Files:**
- Modify: `src/backfill.rs`

**Implementation:**

Add an async function for the writer task:

```rust
async fn run_writer(
    table_path: String,
    batch_size: usize,
    flush_interval_secs: u64,
    mut rx: mpsc::Receiver<DeltaCertRecord>,
    shutdown: CancellationToken,
) -> WriterResult
```

Where `WriterResult` is a struct containing `total_records_written: u64` and `write_errors: u64`.

The writer:

1. **Opens/creates the delta table** using existing `delta_sink::open_or_create_table()` with the `delta_schema()`. If the table doesn't exist, it creates it with the correct 20-column schema and `seen_date` partitioning (AC4.3).

2. **Main loop** using `tokio::select!`:
   ```rust
   let mut buffer: Vec<DeltaCertRecord> = Vec::with_capacity(batch_size);
   let mut flush_timer = tokio::time::interval(Duration::from_secs(flush_interval_secs));

   loop {
       tokio::select! {
           Some(record) = rx.recv() => {
               buffer.push(record);
               if buffer.len() >= batch_size {
                   // Flush
               }
           }
           _ = flush_timer.tick() => {
               if !buffer.is_empty() {
                   // Time-triggered flush
               }
           }
           _ = shutdown.cancelled() => {
               // Graceful shutdown: flush remaining buffer and exit
               break;
           }
       }
   }
   ```
   When `rx.recv()` returns `None` (all senders dropped / fetchers done), drain remaining buffer and exit loop.

3. **Flush logic:** Calls existing `delta_sink::flush_buffer()` which handles:
   - Arrow batch conversion via `records_to_batch()`
   - DeltaOps write with `SaveMode::Append`
   - Error recovery (reopen table on write failure)
   - Buffer overflow protection (drop oldest half if > 2x batch_size)

4. **Metrics:** Track total_records_written and write_errors for the completion summary.

The writer uses the SAME functions as the live delta_sink, ensuring records are identical (AC4.2).

**Verification:**

Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat(backfill): implement writer task for delta table flushing`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Wire up full pipeline and completion logic

**Verifies:** delta-backfill.AC5.3, delta-backfill.AC5.4, delta-backfill.AC6.1, delta-backfill.AC6.3

**Files:**
- Modify: `src/backfill.rs` (update `run_backfill()`)

**Implementation:**

Update `run_backfill()` to wire together all phases:

1. **Startup logging:** Log backfill configuration (mode, from, logs filter).

2. **Spawn writer task** before spawning fetchers. Pass the `mpsc::Receiver` and config values.

3. **Spawn fetcher tasks** (from Phase 4). Collect all `JoinHandle<Result<u64, String>>`.

4. **Drop orchestrator's Sender clone** so the channel closes when all fetchers finish.

5. **Wait for fetchers:** `join_all(fetcher_handles).await`. Collect results:
   - Count successful fetchers (returned Ok)
   - Count failed fetchers (returned Err) (AC5.3: failed fetcher doesn't block others)

6. **Wait for writer:** After all fetchers are done and channel is drained, the writer exits. Await its JoinHandle.

7. **Completion summary** (AC6.3):
   ```
   info!("backfill complete: {} records written, {}/{} logs succeeded, {} failed, elapsed: {:?}",
       writer_result.total_records_written,
       successful_count,
       total_count,
       failed_count,
       elapsed);
   ```

8. **Exit code** (AC5.4):
   - 0 if all fetchers succeeded and writer had no errors
   - 1 if some fetchers failed or writer had errors

9. **Progress logging** (AC6.1): Each fetcher logs progress periodically (implemented in Phase 4). The writer logs flush events: `"flushed N records to delta (total: M)"`.

**Verification:**

Run: `cargo build`
Expected: Compiles without errors

Run: `cargo run -- --backfill`
Expected: Runs gap detection, reports no work items (empty table in catch-up mode), exits with code 0

Run: `cargo test`
Expected: All tests pass

**Commit:** `feat(backfill): wire up full pipeline with completion summary and exit codes`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add writer and end-to-end tests

**Verifies:** delta-backfill.AC4.2, delta-backfill.AC4.3, delta-backfill.AC5.3, delta-backfill.AC5.4, delta-backfill.AC6.3

**Files:**
- Modify: `src/backfill.rs` (add to `#[cfg(test)]` module)

**Testing:**

Tests must verify:

- **delta-backfill.AC4.2:** Create DeltaCertRecords via `from_message()`, send through mpsc to writer, verify records in delta table match what the live sink would produce. Compare schema and field values by reading back from delta table using DataFusion query.

- **delta-backfill.AC4.3:** Run writer task against a non-existent table path. Verify it creates the table with correct 20-column schema and `seen_date` partitioning. Read back table metadata to confirm.

- **delta-backfill.AC5.3:** Spawn multiple fetcher tasks with one sending to a closed receiver (simulating failure). Verify other fetchers complete independently.

- **delta-backfill.AC5.4:** Test that `run_backfill()` returns 0 when all fetchers succeed and non-zero when some fail. (Test with mock work items that immediately complete vs immediately error.)

- **delta-backfill.AC6.3:** Verify completion summary contains expected fields (use tracing test subscriber to capture log output, or just verify the function returns the correct WriterResult values).

Each test follows project patterns: real delta tables in `/tmp/`, manual cleanup, `#[tokio::test]`.

**Verification:**

Run: `cargo test`
Expected: All tests pass including new writer tests

**Commit:** `test(backfill): add writer and end-to-end integration tests`
<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->
