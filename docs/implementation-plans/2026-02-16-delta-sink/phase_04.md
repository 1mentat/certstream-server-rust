# Delta Sink Implementation Plan - Phase 4: Sink Task with Batching and Flush

**Goal:** Implement the core async task that receives from broadcast channel, buffers, and flushes to Delta

**Architecture:** The sink task subscribes to `broadcast::Sender<Arc<PreSerializedMessage>>` (same as WS/SSE handlers), deserializes `full` JSON into `DeltaCertRecord`, buffers records, and flushes to Delta on size or time thresholds. Uses `tokio::select!` over broadcast receiver, flush timer, and shutdown signal.

**Tech Stack:** tokio (broadcast, select!, Interval), deltalake (DeltaOps write), tokio_util CancellationToken

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-sink.AC1: CT entries are written to Delta table with correct schema
- **delta-sink.AC1.4 Success:** Records are partitioned by `seen_date` (YYYY-MM-DD derived from `seen` timestamp)
- **delta-sink.AC1.5 Edge:** Messages that fail deserialization are skipped and logged, not causing the sink to crash or stall

### delta-sink.AC3: Batching and flush behavior
- **delta-sink.AC3.1 Success:** Buffer flushes when it reaches `batch_size` records
- **delta-sink.AC3.2 Success:** Buffer flushes when `flush_interval_secs` elapses, even if buffer is below `batch_size`
- **delta-sink.AC3.3 Success:** Graceful shutdown flushes remaining buffered records before exit
- **delta-sink.AC3.4 Failure:** Delta write failure retains buffer and retries on next flush cycle; buffer exceeding 2x `batch_size` drops oldest half

---

<!-- START_TASK_1 -->
### Task 1: Implement flush_buffer function

**Files:**
- Modify: `src/delta_sink.rs`

**Implementation:**

Add a function that writes buffered records to the Delta table:

```rust
async fn flush_buffer(
    table: DeltaTable,
    buffer: &mut Vec<DeltaCertRecord>,
    schema: &Arc<Schema>,
    batch_size: usize,
) -> (DeltaTable, Result<usize, Box<dyn std::error::Error + Send + Sync>>)
```

**Important ownership note:** `DeltaOps(table)` consumes the `DeltaTable` by value. The `DeltaOps::write().await` operation returns a **new** `DeltaTable` after the commit. Therefore, `flush_buffer` takes `table` by value (not `&mut`) and returns the updated table. The caller must reassign: `table = flush_buffer(table, ...).await.0;`.

This function should:
1. If buffer is empty, return `(table, Ok(0))`
2. Check buffer overflow first: if buffer exceeds `2 * batch_size`, drop oldest half with `buffer.drain(..buffer.len() / 2)` and log warning (AC3.4)
3. Call `records_to_batch(buffer, schema)` to create a RecordBatch
4. Write to Delta: `let result = DeltaOps(table).write(vec![batch]).with_save_mode(SaveMode::Append).await;`
5. On success: clear buffer, return `(new_table, Ok(count))`
6. On failure: retain buffer (don't clear). Since `DeltaOps` consumed the table and the write failed, reopen the table via `open_table(table_path)` to get a fresh handle. Return `(reopened_table, Err(...))`

The partitioning by `seen_date` happens automatically because `seen_date` is a column in the RecordBatch and was configured as a partition column when the table was created (Phase 3).

**Verification:**

Run: `cargo build 2>&1 | tail -3`
Expected: Build succeeds

**Commit:** `feat: add flush_buffer for writing RecordBatch to Delta table`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement run_delta_sink async task

**Files:**
- Modify: `src/delta_sink.rs`

**Implementation:**

Add the public entry point function. This is an async function body (not a spawner — `main.rs` wraps it in `tokio::spawn`):

```rust
pub async fn run_delta_sink(
    config: DeltaSinkConfig,
    mut rx: broadcast::Receiver<Arc<PreSerializedMessage>>,
    shutdown: CancellationToken,
)
```

This follows the existing pattern from `src/main.rs` where tasks receive a `CancellationToken` for graceful shutdown (see lines 79, 100, 126, 169, etc.).

The function body should:

1. **Startup:** Call `open_or_create_table(&config.table_path, &schema).await`. If this fails, log error and return (AC4.4 — table creation failure doesn't prevent app from running).

2. **Receive loop** using `tokio::select!`. Note the table ownership pattern — `flush_buffer` consumes and returns the table:
   ```rust
   let mut flush_interval = tokio::time::interval(Duration::from_secs(config.flush_interval_secs));
   let mut buffer: Vec<DeltaCertRecord> = Vec::with_capacity(config.batch_size);
   let table_path = config.table_path.clone();

   loop {
       tokio::select! {
           result = rx.recv() => {
               match result {
                   Ok(msg) => {
                       // Deserialize msg.full into DeltaCertRecord
                       // On success: push to buffer
                       // On deser failure: log warning, skip (AC1.5)
                       // If buffer.len() >= config.batch_size: flush (AC3.1)
                       // let (new_table, result) = flush_buffer(table, ...).await;
                       // table = new_table;
                   }
                   Err(broadcast::error::RecvError::Lagged(n)) => {
                       // Log warning with count, continue (AC4.3)
                   }
                   Err(broadcast::error::RecvError::Closed) => {
                       // Channel closed, break
                       break;
                   }
               }
           }
           _ = flush_interval.tick() => {
               // Time-triggered flush (AC3.2)
               if !buffer.is_empty() {
                   let (new_table, _result) = flush_buffer(table, &mut buffer, &schema, config.batch_size).await;
                   table = new_table;
               }
           }
           _ = shutdown.cancelled() => {
               // Graceful shutdown: flush remaining buffer (AC3.3)
               if !buffer.is_empty() {
                   let (_final_table, _result) = flush_buffer(table, &mut buffer, &schema, config.batch_size).await;
               }
               break;
           }
       }
   }
   ```

3. **Error handling in flush:** On write failure, `flush_buffer` retains the buffer and reopens the table. The next flush cycle (timer or size threshold) will retry. If buffer exceeds 2x batch_size from repeated failures, `flush_buffer` drops oldest half (AC3.4).

4. **Lagged handling:** Match the WS handler pattern from `src/websocket/server.rs:206-208` — log and continue. The design says lagged errors are logged and metriced (metrics added in Phase 5).

The broadcast subscription pattern follows SSE handler (`src/sse.rs:40`): `state.tx.subscribe()`.

The shutdown pattern follows watcher tasks in `src/main.rs:289-293`: `tokio::select!` with `cancel.cancelled()` branch.

**Verification:**

Run: `cargo build 2>&1 | tail -3`
Expected: Build succeeds

**Commit:** `feat: implement run_delta_sink async task with batching`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Tests for sink task behavior

**Verifies:** delta-sink.AC1.4, delta-sink.AC1.5, delta-sink.AC3.1, delta-sink.AC3.2, delta-sink.AC3.3, delta-sink.AC3.4

**Files:**
- Modify: `src/delta_sink.rs` (add to `#[cfg(test)] mod tests`)

**Testing:**

All tests use `#[tokio::test]` and create temp directories for Delta tables. Tests construct `PreSerializedMessage` by serializing `CertificateMessage` (reusing the `make_test_message()` pattern from `src/models/certificate.rs:272-308`).

Tests must verify:
- **delta-sink.AC1.4:** Write records with different `seen` timestamps. Read back the Delta table and verify that data is partitioned into directories by `seen_date` (check the filesystem for `seen_date=YYYY-MM-DD/` directories).
- **delta-sink.AC1.5:** Send a `PreSerializedMessage` with malformed JSON in the `full` field. Assert the sink does not crash and continues processing subsequent valid messages.
- **delta-sink.AC3.1:** Send exactly `batch_size` messages through a broadcast channel to the sink. Verify records appear in the Delta table without waiting for the timer.
- **delta-sink.AC3.2:** Send fewer than `batch_size` messages, wait for `flush_interval_secs` to elapse. Verify records appear in the Delta table after the timer fires. Use a short `flush_interval_secs` (e.g., 1 second) for testing.
- **delta-sink.AC3.3:** Send messages, then cancel the shutdown token. Verify all buffered records are flushed to the Delta table before the task exits.
- **delta-sink.AC3.4:** Test the buffer overflow logic directly. Extract the overflow check into a testable helper function `check_buffer_overflow(buffer: &mut Vec<DeltaCertRecord>, batch_size: usize) -> usize` that returns the number of dropped records. Unit test this function: create a buffer with 2x+1 `batch_size` entries, call the function, assert buffer is halved and the oldest records were dropped. Also test that buffers at exactly 2x `batch_size` or below are not modified.

For reading back Delta table contents to verify writes, use `open_table()` and then read via `DeltaOps::load()` or check table version/file count.

**Verification:**

Run: `cargo test delta_sink 2>&1 | tail -10`
Expected: All delta_sink tests pass

**Commit:** `test: add integration tests for sink batching and flush behavior`
<!-- END_TASK_3 -->
