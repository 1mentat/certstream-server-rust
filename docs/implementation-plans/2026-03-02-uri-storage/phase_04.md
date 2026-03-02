# URI-Based Storage Backend — Phase 4: Backfill Read/Write Paths

**Goal:** Backfill gap detection, fetcher-writer pipeline, and staging all work with S3 URIs.

**Architecture:** `detect_gaps()` uses `DeltaTableBuilder` with storage options instead of `deltalake::open_table()`. `run_writer()` accepts storage options and passes them to `open_or_create_table()` and `flush_buffer()`. `--staging-path` CLI arg is validated as a URI before use.

**Tech Stack:** Rust, deltalake DeltaTableBuilder, std::collections::HashMap

**Scope:** Phase 4 of 7 from original design

**Codebase verified:** 2026-03-02

---

## Acceptance Criteria Coverage

This phase implements:

### uri-storage.AC3: Delta code paths work over S3
- **uri-storage.AC3.4 Success:** Backfill gap detection queries S3-backed Delta table via DataFusion
- **uri-storage.AC3.5 Success:** Backfill writer writes records to S3-backed Delta table (including staging)
- **uri-storage.AC3.8 Success:** All `file://` paths continue to work identically to previous bare-path behavior

**Verification approach:** AC3.4 and AC3.5 are validated in Phase 7 integration tests. AC3.8 verified by existing unit tests passing.

---

## Codebase verification findings

- ✓ `detect_gaps()` at `src/backfill.rs:289` — takes `table_path: &str`, uses `deltalake::open_table(table_path)` at line 296 and `deltalake::open_table(staging)` at lines 301 and 341
- ✓ `run_writer()` at `src/backfill.rs:438` — takes `table_path: String`, calls `open_or_create_table(&table_path, &schema)` at line 458
- ✓ `run_writer()` calls `flush_buffer()` at lines 490, 505, 515, 530
- ✓ `run_backfill()` at `src/backfill.rs:678` — takes `Config`, orchestrates detect_gaps + writer; calls `detect_gaps` at the gap detection call site, spawns `run_writer`
- ✓ `run_writer()` creates directory at line 449 with `std::fs::create_dir_all` — only valid for local paths
- ✓ `backfill.rs` already imports `HashMap` (line 15)
- ✓ `staging_path` passed as `Option<String>` from CLI args

---

<!-- START_TASK_1 -->
### Task 1: Update detect_gaps() to use DeltaTableBuilder with storage options

**Files:**
- Modify: `src/backfill.rs:289-360` — add `storage_options` parameter, replace `deltalake::open_table()` calls

**Implementation:**

1. Add imports at top of `src/backfill.rs`:
```rust
use crate::config::{StorageConfig, parse_table_uri, resolve_storage_options};
use deltalake::DeltaTableBuilder;
```

2. Update `detect_gaps()` signature:
```rust
pub async fn detect_gaps(
    table_path: &str,
    staging_path: Option<&str>,
    logs: &[(String, u64)],
    backfill_from: Option<u64>,
    storage_options: &HashMap<String, String>,
) -> Result<Vec<BackfillWorkItem>, Box<dyn Error>> {
```

3. Replace `deltalake::open_table(table_path)` at line 296 with:
```rust
let table = match DeltaTableBuilder::from_uri(table_path)
    .with_storage_options(storage_options.clone())
    .load()
    .await
{
```

4. Replace `deltalake::open_table(staging)` at lines 301 and 341 with:
```rust
DeltaTableBuilder::from_uri(staging)
    .with_storage_options(storage_options.clone())
    .load()
    .await
```

5. Update all callers of `detect_gaps()` in `run_backfill()` to pass `&storage_options`.

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: Compiles
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update run_writer() and run_backfill() to pass storage options

**Files:**
- Modify: `src/backfill.rs:438-445` — add `storage_options` parameter to `run_writer()`
- Modify: `src/backfill.rs:678-685` — parse URIs and resolve storage options in `run_backfill()`

**Implementation:**

1. Update `run_writer()` signature:
```rust
async fn run_writer(
    table_path: String,
    batch_size: usize,
    flush_interval_secs: u64,
    compression_level: i32,
    storage_options: HashMap<String, String>,
    mut rx: mpsc::Receiver<DeltaCertRecord>,
    shutdown: CancellationToken,
) -> WriterResult {
```

2. Make `std::fs::create_dir_all` at line 449 conditional on path scheme:
```rust
// Only create local directories; S3 paths don't need this
if !table_path.starts_with("s3://") {
    if let Err(e) = std::fs::create_dir_all(&table_path) {
        warn!(error = %e, table_path = %table_path, "Failed to create table directory");
        return WriterResult { total_records_written: 0, write_errors: 1 };
    }
}
```

3. Update `open_or_create_table` call at line 458:
```rust
let mut table = match open_or_create_table(&table_path, &schema, storage_options.clone()).await {
```

4. Update all `flush_buffer()` calls (lines 490, 505, 515, 530) to pass `&storage_options`.

5. In `run_backfill()`, parse URIs and resolve storage options near the top:
```rust
let storage_options = match parse_table_uri(&config.delta_sink.table_path) {
    Ok(location) => resolve_storage_options(&location, &config.storage),
    Err(e) => {
        error!(error = %e, "Invalid delta_sink.table_path URI");
        return 1;
    }
};
```

6. For staging path, also parse and resolve (reuse same storage options since same backend):
```rust
let writer_table_path = if let Some(ref staging) = staging_path {
    match parse_table_uri(staging) {
        Ok(location) => location.as_uri().to_string(),
        Err(e) => {
            error!(error = %e, "Invalid staging path URI");
            return 1;
        }
    }
} else {
    match parse_table_uri(&config.delta_sink.table_path) {
        Ok(location) => location.as_uri().to_string(),
        Err(e) => {
            error!(error = %e, "Invalid delta_sink.table_path URI");
            return 1;
        }
    }
};
```

7. Pass `storage_options` to `detect_gaps()`, `run_writer()`, and any other callers.

8. Update all test code in `mod tests` that calls `detect_gaps()` to pass `&HashMap::new()`.

**Verification:**
Run: `cargo test` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All tests pass

**Commit:** `feat(backfill): pass storage options through gap detection and writer paths`
<!-- END_TASK_2 -->
