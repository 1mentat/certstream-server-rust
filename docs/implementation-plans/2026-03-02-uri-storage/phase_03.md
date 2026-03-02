# URI-Based Storage Backend — Phase 3: Delta Sink Write Path

**Goal:** Live delta_sink writes to S3-backed tables.

**Architecture:** `run_delta_sink()` receives `StorageConfig`, parses the table path URI at startup, resolves storage options, and passes them to `open_or_create_table()`. `flush_buffer()` receives storage options for its error recovery path (table reopen). All callers of `flush_buffer()` updated mechanically.

**Tech Stack:** Rust, deltalake DeltaTableBuilder, std::collections::HashMap

**Scope:** Phase 3 of 7 from original design

**Codebase verified:** 2026-03-02

---

## Acceptance Criteria Coverage

This phase implements:

### uri-storage.AC3: Delta code paths work over S3
- **uri-storage.AC3.3 Success:** delta_sink writes batches to S3-backed Delta table
- **uri-storage.AC3.8 Success:** All `file://` paths continue to work identically to previous bare-path behavior
- **uri-storage.AC3.9 Failure:** S3 connection failure handled non-fatally (delta_sink logs and exits task, backfill exits code 1, query returns 503)

**Verification approach:** AC3.3 is validated in Phase 7 integration tests. AC3.8 verified by existing unit tests passing. AC3.9 is covered by existing error handling patterns: `run_delta_sink()` already returns early on table open failure (non-fatal, server continues), `run_backfill()` returns exit code 1 on table errors, and `query_certs()` returns 503 on `NotATable`/`InvalidTableLocation` errors. The `DeltaTableBuilder` errors for unreachable S3 endpoints follow the same `Err(e)` branches as local filesystem errors. No new error handling code is needed — the existing match arms cover S3 failures identically to local failures.

---

## Codebase verification findings

- ✓ `run_delta_sink()` at `src/delta_sink.rs:652` takes `DeltaSinkConfig` — needs `StorageConfig` added
- ✓ `run_delta_sink()` calls `open_or_create_table(&config.table_path, &schema)` at line 660
- ✓ `flush_buffer()` at `src/delta_sink.rs:474` uses `deltalake::open_table()` at line 536 for recovery — needs storage options
- ✓ `flush_buffer()` callers: delta_sink.rs (lines 697, ~730, ~760) + backfill.rs (lines 490, 505, 515, 530)
- ✓ `flush_buffer()` gets `table_path` from `table.table_uri()` at line 490

---

<!-- START_TASK_1 -->
### Task 1: Update run_delta_sink() to parse URI and resolve storage options

**Files:**
- Modify: `src/delta_sink.rs:652-670` — add `StorageConfig` parameter, parse URI, resolve options
- Modify: `src/main.rs` — update `run_delta_sink()` call site to pass `StorageConfig`

**Implementation:**

1. Add import at top of `src/delta_sink.rs`:
```rust
use crate::config::{StorageConfig, parse_table_uri, resolve_storage_options};
```

2. Update `run_delta_sink()` signature at line 652:
```rust
pub async fn run_delta_sink(
    config: DeltaSinkConfig,
    storage: StorageConfig,
    mut rx: broadcast::Receiver<Arc<PreSerializedMessage>>,
    shutdown: CancellationToken,
) {
```

3. Add URI parsing at the start of the function (after `let schema = delta_schema();`). Use `location.as_uri()` for the table path passed to `open_or_create_table`, because `parse_table_uri` strips `file://` prefix for Local paths while `DeltaTableBuilder::from_uri()` accepts bare paths:
```rust
let location = match parse_table_uri(&config.table_path) {
    Ok(loc) => loc,
    Err(e) => {
        error!(error = %e, table_path = %config.table_path, "Invalid table path URI, delta-sink task exiting");
        return;
    }
};
let storage_options = resolve_storage_options(&location, &storage);
let table_uri = location.as_uri().to_string();

let mut table = match open_or_create_table(&table_uri, &schema, storage_options.clone()).await {
```

5. Update `src/main.rs` to pass `config.storage.clone()` (or `config.storage`) to `run_delta_sink()`. Find the spawn call for delta_sink and add the storage config parameter.

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: Compiles (but flush_buffer callers still use old signature until Task 2)
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update flush_buffer() to accept storage options for recovery path

**Files:**
- Modify: `src/delta_sink.rs:474-590` — add `storage_options` parameter, update recovery path
- Modify: `src/delta_sink.rs` — update ALL `flush_buffer()` call sites in production code and tests
- Modify: `src/backfill.rs` — update ALL `flush_buffer()` call sites in production code

**Implementation:**

1. Update `flush_buffer()` signature at line 474:
```rust
pub async fn flush_buffer(
    table: DeltaTable,
    buffer: &mut Vec<DeltaCertRecord>,
    schema: &Arc<Schema>,
    batch_size: usize,
    compression_level: i32,
    storage_options: &HashMap<String, String>,
) -> (Option<DeltaTable>, Result<usize, Box<dyn std::error::Error + Send + Sync>>) {
```

2. Update the recovery path at line 536 — replace `deltalake::open_table(&table_path)` with `DeltaTableBuilder`:
```rust
match DeltaTableBuilder::from_uri(&table_path)
    .with_storage_options(storage_options.clone())
    .load()
    .await
{
```

3. Update the `open_or_create_table` recovery calls at lines 553 and 571 to pass storage options:
```rust
match open_or_create_table(&table_path, schema, storage_options.clone()).await {
```

4. Update ALL `flush_buffer()` callers in `src/delta_sink.rs` to pass `&storage_options`:
   - Production calls in `run_delta_sink()`: pass `&storage_options`
   - Test calls: pass `&HashMap::new()`

5. Update ALL `flush_buffer()` callers in `src/backfill.rs` (lines 490, 505, 515, 530) to pass `&HashMap::new()`:
   These backfill callers will be updated to pass real storage options in Phase 4. For now, `&HashMap::new()` preserves backward compatibility.

**Verification:**
Run: `cargo test` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All 359 tests pass

**Commit:** `feat(delta-sink): parse URI and pass storage options through write path`
<!-- END_TASK_2 -->
