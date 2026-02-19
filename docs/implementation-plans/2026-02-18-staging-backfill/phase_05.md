# Staging Backfill Implementation Plan - Phase 5: Error Handling & Edge Cases

**Goal:** Robust handling of missing tables, interruptions, and failure recovery in `run_merge()`

**Architecture:** Phase 4's `run_merge()` is enhanced with specific error paths: missing/empty staging exits 0 (nothing to merge), missing main table is auto-created via `open_or_create_table()`, merge batch failure leaves staging intact and exits 1, and cancellation (SIGINT/SIGTERM) skips staging cleanup and exits 1. The `--merge` without `--staging-path` validation was already implemented in Phase 1.

**Tech Stack:** Rust, deltalake (DeltaTableError variants), tokio_util (CancellationToken)

**Scope:** 5 phases from original design (this is phase 5 of 5)

**Codebase verified:** 2026-02-18

**Note:** Line references throughout are approximate and should be verified against the actual file at implementation time, as prior phase changes may shift line numbers.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### staging-backfill.AC4: Error handling
- **staging-backfill.AC4.1 Edge:** Missing/empty staging table exits 0 with info log (nothing to merge)
- **staging-backfill.AC4.2 Edge:** Missing main table is created automatically, then merge proceeds as pure inserts
- **staging-backfill.AC4.3 Failure:** Merge batch failure leaves staging intact for retry, exits 1
- **staging-backfill.AC4.4 Failure:** Process interruption (SIGINT) leaves staging intact, exits 1
- **staging-backfill.AC4.5 Failure:** `--merge` without `--staging-path` prints error and exits with non-zero code

---

<!-- START_TASK_1 -->
### Task 1: Handle missing/empty staging table (exit 0)

**Verifies:** staging-backfill.AC4.1

**Files:**
- Modify: `src/backfill.rs` (run_merge function — update the staging table open error handling)

**Implementation:**

In the `run_merge()` function from Phase 4, the staging table open failure currently returns 1. Change it to distinguish between "table doesn't exist" (benign, exit 0) and other errors (real failure, exit 1):

```rust
let staging_table = match deltalake::open_table(&staging_path).await {
    Ok(t) => t,
    Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
        info!(staging_path = %staging_path, "staging table does not exist, nothing to merge");
        return 0;
    }
    Err(e) => {
        warn!(error = %e, "failed to open staging table");
        return 1;
    }
};
```

This matches the existing pattern in `detect_gaps()` (backfill.rs:195) where `NotATable` and `InvalidTableLocation` are treated as "doesn't exist."

The empty staging case (table exists but has no records) is already handled in Phase 4's implementation:
```rust
if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
    info!("staging table is empty, nothing to merge");
    return 0;
}
```

**Verification of prior phase coverage (AC4.2, AC4.3, AC4.4):**
- **AC4.2 (missing main auto-created):** Phase 4 uses `open_or_create_table()` which automatically creates the table if missing. No additional code needed.
- **AC4.3 (merge failure preserves staging):** Phase 4 returns 1 immediately on batch failure, before staging cleanup. No additional code needed.
- **AC4.4 (cancellation preserves staging):** Phase 4 checks `shutdown.is_cancelled()` at the start of each batch and returns 1, skipping staging cleanup. No additional code needed.

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `fix(backfill): missing staging table exits 0 instead of 1`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Tests for error handling edge cases

**Verifies:** staging-backfill.AC4.1, staging-backfill.AC4.2, staging-backfill.AC4.3, staging-backfill.AC4.4, staging-backfill.AC4.5

**Files:**
- Modify: `src/backfill.rs` (test module)

**Implementation:**

Add tests for the error handling edge cases. These tests call `run_merge()` directly with crafted inputs.

For creating a test Config, construct it with the required fields. The `DeltaSinkConfig` has a `Default` impl (config.rs:299-307). For the full `Config`, either use `Config::load()` with env var overrides or build a test helper that constructs the minimal config needed.

**Test 1: Missing staging table exits 0 (AC4.1)**
- Call run_merge with a staging_path pointing to a nonexistent directory (e.g., `/tmp/staging_test_nonexistent_<uuid>`)
- Assert: returns 0

**Test 2: Empty staging table exits 0 (AC4.1)**
- Create a staging Delta table with 0 records (just schema, no data)
- Call run_merge
- Assert: returns 0

**Test 3: Missing main table auto-created (AC4.2)**
- Create a staging table with records at [10, 11, 12]
- Set config.delta_sink.table_path to a nonexistent path
- Call run_merge
- Assert: returns 0
- Assert: main table now exists at the config path with records [10, 11, 12]

**Test 4: Cancellation preserves staging (AC4.4)**
- Create staging and main tables
- Create a CancellationToken and cancel it before calling run_merge
- Call run_merge with the pre-cancelled token
- Assert: returns 1
- Assert: staging directory still exists

**Test 5: --merge without --staging-path (AC4.5)**
- This was validated in Phase 1 (CLI validation in main.rs)
- Test verifies the CLI parsing: construct CliArgs with merge=true, staging_path=None
- Assert: the validation logic in main.rs would print error and exit 1
- Since this is tested at the main.rs level, a unit test for CLI args parsing is sufficient

**Testing:**
Tests must verify each AC listed above:
- staging-backfill.AC4.1: Assert missing/empty staging exits 0
- staging-backfill.AC4.2: Assert missing main table is auto-created with correct schema
- staging-backfill.AC4.3: Assert merge failure preserves staging (test by verifying return 1 path logic)
- staging-backfill.AC4.4: Assert pre-cancelled token causes immediate exit with staging preserved
- staging-backfill.AC4.5: Assert CLI validation catches --merge without --staging-path

Follow project testing patterns — real Delta tables in `/tmp/`, CancellationToken for cancellation tests, manual cleanup.

**Verification:**
Run: `cargo test error_handling`
Expected: All new error handling tests pass

Run: `cargo test`
Expected: All tests pass

**Commit:** `test(backfill): add error handling edge case tests for AC4.1-AC4.5`
<!-- END_TASK_2 -->
