# Staging Backfill Implementation Plan - Phase 2: Staging Writes

**Goal:** Backfill writes to staging table when `--staging-path` is provided

**Architecture:** `run_backfill()` gains a `staging_path: Option<String>` parameter. When present, the writer task targets the staging path instead of `config.delta_sink.table_path`. Gap detection continues to query the main table only (staging union comes in Phase 3). The staging table is created with the same schema and partitioning as the main table via the existing `open_or_create_table()` helper.

**Tech Stack:** Rust, deltalake (existing `open_or_create_table`, `flush_buffer`)

**Scope:** 5 phases from original design (this is phase 2 of 5)

**Codebase verified:** 2026-02-18

**Note:** Line references throughout are approximate and should be verified against the actual file at implementation time, as prior phase changes may shift line numbers. The `staging_path` parameter is deliberately placed before `backfill_from` to group path-related parameters together.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### staging-backfill.AC1: Backfill writes to staging table
- **staging-backfill.AC1.1 Success:** `--backfill --staging-path /tmp/staging` creates a Delta table at `/tmp/staging` with the standard 20-column schema partitioned by `seen_date`
- **staging-backfill.AC1.2 Success:** Fetched CT records are written to the staging table, not the main table
- **staging-backfill.AC1.3 Success:** Works with both catch-up (`--backfill --staging-path`) and historical (`--backfill --from N --staging-path`) modes

### staging-backfill.AC5: Backward compatibility
- **staging-backfill.AC5.1 Success:** `--backfill` without `--staging-path` writes to main table (existing behavior unchanged)
- **staging-backfill.AC5.2 Success:** All existing backfill tests pass without modification

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Add staging_path parameter to run_backfill and wire writer target

**Files:**
- Modify: `src/backfill.rs:487-492` (run_backfill signature)
- Modify: `src/backfill.rs:559` (detect_gaps call — no change needed yet, still queries main only)
- Modify: `src/backfill.rs:623-631` (writer task setup — use staging_path if provided)
- Modify: `src/main.rs:83-89` (pass cli_args.staging_path to run_backfill)

**Implementation:**

1. Add `staging_path: Option<String>` as the second parameter to `run_backfill()`:

```rust
pub async fn run_backfill(
    config: Config,
    staging_path: Option<String>,
    backfill_from: Option<u64>,
    backfill_logs: Option<String>,
    shutdown: CancellationToken,
) -> i32 {
```

2. At line 623, compute the effective write path using the staging_path or falling back to config:

```rust
let table_path = staging_path
    .unwrap_or_else(|| config.delta_sink.table_path.clone());
```

This replaces the existing `let table_path = config.delta_sink.table_path.clone();` at line 624. Lines 625-631 (batch_size, flush_interval_secs, writer spawn) remain unchanged.

3. Add a log line when staging path is in use (after the initial log lines, around line 501):

```rust
if let Some(ref path) = staging_path {
    info!(staging_path = %path, "writing to staging table");
}
```

4. Update the `main.rs` call to pass the new parameter:

```rust
let exit_code = backfill::run_backfill(
    config,
    cli_args.staging_path,
    cli_args.backfill_from,
    cli_args.backfill_logs,
    shutdown_token,
)
.await;
```

Note: `staging_path` must be a non-reference `Option<String>` because it is moved into the function (consumed by `unwrap_or_else`). The parameter is placed before `backfill_from` to group the path-related parameters together.

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

Run: `cargo test`
Expected: All 249 existing tests pass (they don't call run_backfill directly)

**Commit:** `feat(backfill): add staging_path parameter to run_backfill for staging writes`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Test staging write creates table at staging path

**Verifies:** staging-backfill.AC1.1, staging-backfill.AC1.2

**Files:**
- Modify: `src/backfill.rs` (test module, after existing tests)

**Implementation:**

Add a test that verifies the writer task creates a Delta table at the staging path with correct schema. This test exercises the writer directly (same pattern as existing `test_writer_basic_functionality` at line 1357) but writes to a staging-specific temp path.

The test should:
1. Create a temp directory for the staging path (not the main table path)
2. Create an mpsc channel and spawn `run_writer()` with the staging temp path
3. Send a few `DeltaCertRecord` records via the channel
4. Drop the sender and await the writer
5. Verify: Delta table exists at the staging path with correct 20-column schema partitioned by `seen_date`
6. Verify: Records are present in the staging table (query via DataFusion)

This reuses the existing `make_test_record()` helper (line 919) and the same temp directory cleanup pattern used by other backfill tests.

**Testing:**
Tests must verify each AC listed above:
- staging-backfill.AC1.1: Assert table at staging path has 20-column schema with `seen_date` partition
- staging-backfill.AC1.2: Assert records are in the staging table, not in any main table path

Follow existing test patterns — real Delta table in `/tmp/`, DataFusion SQL for verification, manual cleanup.

**Verification:**
Run: `cargo test staging`
Expected: New staging test passes

Run: `cargo test`
Expected: All tests pass (249 existing + new)

**Commit:** `test(backfill): add staging write tests for AC1.1, AC1.2`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->
