# Delta Backfill Implementation Plan - Phase 4: Concurrent Fetcher Tasks

**Goal:** Spawn per-log fetcher tasks that process work items from gap detection, fetch CT log entries using shared fetch functions, convert to DeltaCertRecords, and send them via mpsc channel to the writer.

**Architecture:** The backfill orchestrator in `run_backfill()` spawns one tokio task per CT log. Each fetcher iterates its work items sequentially, calling the shared fetch functions (from Phase 2) in configurable batch sizes. Parsed entries are converted to `DeltaCertRecord` via `from_message()` (from Phase 2) and sent through a `tokio::sync::mpsc` channel to the writer task (Phase 5). Each fetcher handles rate limit errors (HTTP 429/503) with exponential backoff. The `CancellationToken` stops all fetchers on Ctrl+C.

**Tech Stack:** Rust, tokio (mpsc, spawn, CancellationToken), reqwest, existing ct/fetch and delta_sink modules

**Scope:** 5 phases from original design (phase 4 of 5)

**Codebase verified:** 2026-02-17

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-backfill.AC1: CLI mode that fetches and writes directly to delta
- **delta-backfill.AC1.1 Success:** `--backfill` runs, fetches entries from configured CT logs, writes to delta table, and exits with code 0
- **delta-backfill.AC1.2 Success:** `--logs <filter>` limits backfill to matching logs only
- **delta-backfill.AC1.3 Success:** `delta_sink.enabled = false` in config does not prevent backfill from writing to delta

### delta-backfill.AC5: Handles interruption gracefully
- **delta-backfill.AC5.1 Success:** Ctrl+C triggers graceful shutdown — fetchers stop, writer flushes buffered records, process exits
- **delta-backfill.AC5.2 Success:** Re-running `--backfill` after interruption picks up where it left off (gap detection re-queries delta, only missing ranges are fetched)

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Implement the backfill orchestrator in run_backfill()

**Verifies:** delta-backfill.AC1.1, delta-backfill.AC1.2, delta-backfill.AC1.3

**Files:**
- Modify: `src/backfill.rs`

**Implementation:**

Replace the stub `run_backfill()` from Phase 1 with the full orchestrator:

1. **Log discovery:** Build the list of CT logs to backfill:
   - Use `fetch_log_list()` (from `src/ct/log_list.rs`) to get available logs from the configured `ct_logs_url`
   - Add `custom_logs` and `static_logs` from config
   - If `backfill_logs` filter is provided (AC1.2): filter the list to only include logs whose `description` or `url` contains the filter substring (case-insensitive)
   - Log the final count: `"backfilling N logs"`

2. **Tree size discovery:** For each log, call the shared `get_tree_size()` or `get_checkpoint_tree_size()` function (from Phase 2's `ct/fetch.rs`) to get current tree_size. Build a `Vec<(String, u64)>` of `(source_url, tree_size)` pairs.

3. **Gap detection:** Call `detect_gaps()` (from Phase 3) with the table path from `config.delta_sink.table_path` (AC1.3: use this path regardless of `delta_sink.enabled`), the logs+tree_sizes, and `backfill_from`.

4. **Channel setup:** Create a `tokio::sync::mpsc::channel::<DeltaCertRecord>` with a buffer size of `config.delta_sink.batch_size * 2`.

5. **Spawning fetchers:** Group work items by source_url. For each source_url that has work items, spawn a fetcher task (Task 2). Pass the `mpsc::Sender`, `CancellationToken`, HTTP client, and the work items for that log.

6. **Writer placeholder:** The writer task (`run_writer()`) is not implemented until Phase 5. For now, spawn a trivial drain task that consumes and discards all records from the receiver so fetcher sends don't block:
   ```rust
   // TODO: Phase 5 replaces this with the real writer task
   let writer_handle = tokio::spawn(async move { while rx.recv().await.is_some() {} });
   ```
   Drop the orchestrator's `Sender` clone after spawning fetchers. Wait for all fetcher JoinHandles, then await the writer handle.

7. **Return exit code:** 0 if all fetchers completed successfully, non-zero if any failed.

**Verification:**

Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat(backfill): implement backfill orchestrator with log discovery and gap detection`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement per-log fetcher task

**Verifies:** delta-backfill.AC1.1, delta-backfill.AC5.1, delta-backfill.AC5.2

**Files:**
- Modify: `src/backfill.rs`

**Implementation:**

**Required imports for `backfill.rs`** (needed for this phase):
```rust
use crate::ct::fetch;
use crate::ct::log_list::{LogType, CtLog};
use crate::models::certificate::Source;
use crate::delta_sink::DeltaCertRecord;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use std::sync::Arc;
use std::time::Duration;
```

Add an async function for the per-log fetcher task:

```rust
async fn run_fetcher(
    client: reqwest::Client,
    source: Arc<Source>,
    log_type: LogType,
    work_items: Vec<BackfillWorkItem>,
    batch_size: u64,
    request_timeout: Duration,
    tx: mpsc::Sender<DeltaCertRecord>,
    shutdown: CancellationToken,
) -> Result<u64, String>  // returns total records fetched or error description
```

The fetcher:

1. **Iterates work items sequentially.** For each `BackfillWorkItem { start, end, .. }`:
   - Process in batches of `batch_size` entries: `for batch_start in (start..=end).step_by(batch_size as usize)`
   - Calculate `batch_end = (batch_start + batch_size - 1).min(end)`

2. **Calls shared fetch function** (from Phase 2):
   - For `LogType::Rfc6962`: `fetch::fetch_entries(&client, &source.url, batch_start, batch_end, &source, timeout)`
   - For `LogType::StaticCt`: `fetch::fetch_tile_entries(...)` with appropriate tile index calculation

3. **Handles errors with exponential backoff:**
   - `FetchError::RateLimited(_)`: exponential backoff starting at 1s, max 60s, then retry the same batch
   - `FetchError::HttpError(_)`: log error, backoff, retry up to `retry_max_attempts` times
   - `FetchError::NotAvailable(_)`: log warning, skip this batch (log may not support the range)
   - After max retries exhausted: log error, skip this batch, continue to next

4. **Converts and sends records:**
   - For each `CertificateMessage` returned by fetch: convert to `DeltaCertRecord::from_message(&msg)` and send via `tx.send(record).await`
   - If `tx.send()` returns error (receiver dropped), return early

5. **Checks CancellationToken** (AC5.1):
   - In the batch loop, check `shutdown.is_cancelled()` before each batch fetch
   - If cancelled, return with current progress

6. **Progress logging** (periodic):
   - Log progress every N batches: `"[source_url] fetched X/Y entries (Z% complete)"`

**Verification:**

Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat(backfill): implement per-log fetcher task with retry and backoff`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add tests for fetcher and orchestrator

**Verifies:** delta-backfill.AC1.1, delta-backfill.AC1.2, delta-backfill.AC5.1

**Files:**
- Modify: `src/backfill.rs` (add to `#[cfg(test)]` module)

**Testing:**

Tests must verify:

- **delta-backfill.AC1.1:** Test that `run_backfill` returns exit code 0 when there are no work items (empty delta table in catch-up mode with no logs in delta). This tests the orchestrator flow without requiring real CT log servers.

- **delta-backfill.AC1.2:** Test the log filtering logic: given a list of `CtLog` entries, verify that `--logs "google"` filters to only logs whose description or URL contains "google".

- **delta-backfill.AC5.1:** Test that a fetcher task respects CancellationToken: create a mock scenario where the token is cancelled after a short delay, verify the fetcher exits promptly without processing remaining work items.

Note: Full end-to-end testing of the fetcher against real CT log servers is not feasible in unit tests (the project doesn't use HTTP mocking). The fetcher's HTTP interaction is tested indirectly through the shared fetch functions (Phase 2) and through manual verification.

**Verification:**

Run: `cargo test`
Expected: All tests pass

**Commit:** `test(backfill): add fetcher and orchestrator tests`
<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->
