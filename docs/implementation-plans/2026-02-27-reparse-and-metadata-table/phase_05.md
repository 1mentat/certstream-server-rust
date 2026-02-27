# Reparse Audit & Metadata Table Implementation Plan

**Goal:** Add two new batch execution modes (`--reparse-audit` and `--extract-metadata`) for auditing stored certificates against current parsing code and extracting metadata-only Delta tables.

**Architecture:** New `src/table_ops.rs` module with two entry points (`run_reparse_audit`, `run_extract_metadata`) dispatched from the existing CLI if/else chain in `main.rs`. Both read the Delta table via DataFusion SQL, process partition-by-partition, and exit with a status code.

**Tech Stack:** Rust, Tokio, DataFusion, deltalake 0.25, Arrow

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-27

---

## Acceptance Criteria Coverage

This phase implements and tests:

### reparse-and-metadata-table.AC4: CLI integration follows existing patterns (partial)
- **reparse-and-metadata-table.AC4.4 Success:** Both commands respond to SIGINT/SIGTERM by stopping cleanly between partitions

### reparse-and-metadata-table.AC1: Reparse audit reads and reparses stored certificates (edge cases)
- **reparse-and-metadata-table.AC1.6 Edge:** Audit against an empty date range exits 0 with informational message

### reparse-and-metadata-table.AC2: Metadata extraction produces correct metadata-only table (edge cases)
- **reparse-and-metadata-table.AC2.5 Failure:** Missing source table exits 1 with error message
- **reparse-and-metadata-table.AC2.6 Edge:** Extraction against empty date range exits 0 with informational message

---

## Phase 5: Graceful Shutdown and Edge Cases

This phase integrates CancellationToken checking into the batch processing loops of both `run_reparse_audit` and `run_extract_metadata`, and ensures error/edge cases are handled correctly. Some edge cases (empty date range, missing table) may have been partially addressed in Phase 3 and Phase 4 implementations — this phase ensures they are robust and tested.

<!-- START_TASK_1 -->
### Task 1: Add CancellationToken checking to both commands

**Verifies:** reparse-and-metadata-table.AC4.4

**Files:**
- Modify: `src/table_ops.rs` (update `run_reparse_audit` and `run_extract_metadata` to check cancellation)

**Implementation:**

In `run_reparse_audit`, add a cancellation check inside the streaming `while let` loop (Phase 3 uses `df.execute_stream().await` + `stream.next().await`), following the pattern from `src/backfill.rs:1014-1018`:

```rust
while let Some(batch_result) = stream.next().await {
    if shutdown.is_cancelled() {
        warn!("reparse audit interrupted by shutdown signal");
        // Print partial report before exiting
        print_reparse_report(/* pass accumulated state */);
        return 1;
    }
    let batch = batch_result?;
    // ... existing batch processing ...
}
```

The key detail: on cancellation, the reparse audit prints whatever partial report it has accumulated so far (partial mismatch counts, samples collected so far), then exits with code 1. This gives the operator useful data even on early termination. Because Phase 3 uses streaming execution (not `collect()`), the cancellation check fires between each batch from the stream, enabling responsive shutdown even on large tables.

In `run_extract_metadata`, add a cancellation check inside the streaming `while let` loop (Phase 4 uses `df.execute_stream().await` + `stream.next().await`):

```rust
while let Some(batch_result) = stream.next().await {
    if shutdown.is_cancelled() {
        warn!("metadata extraction interrupted by shutdown signal");
        info!(records_written = total_written, "partial extraction left intact at {}", output_path);
        return 1;
    }
    let batch = batch_result?;
    // ... existing batch write ...
}
```

On cancellation, metadata extraction leaves partial output intact (already the case since each batch is an independent Delta transaction) and exits with code 1.

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat: add CancellationToken checking to reparse audit and metadata extraction`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Harden missing source table and empty date range handling with specific error variants

**Verifies:** reparse-and-metadata-table.AC1.6, reparse-and-metadata-table.AC2.5, reparse-and-metadata-table.AC2.6

**Files:**
- Modify: `src/table_ops.rs` (verify/add error handling in both functions)

**Implementation:**

Phases 3 and 4 include basic error handling for table open failures and empty results. This task hardens that handling to use the specific `DeltaTableError` variant matching pattern from the existing codebase (`src/backfill.rs:295-298` and `src/query.rs:235`), ensuring the error messages are specific and user-friendly.

Verify both functions handle the source table open with specific `DeltaTableError` variant matching (not a generic catch-all). If Phase 3/4 implementations used a generic `.map_err()` or `?` pattern, replace with the explicit match:

```rust
let table = match deltalake::open_table(&config.delta_sink.table_path).await {
    Ok(t) => t,
    Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
        error!(
            table_path = %config.delta_sink.table_path,
            "source Delta table does not exist"
        );
        return 1;
    }
    Err(e) => {
        error!(error = %e, "failed to open source Delta table");
        return 1;
    }
};
```

This uses `DeltaTableError::NotATable` and `DeltaTableError::InvalidTableLocation` for missing tables, matching the existing codebase pattern. Import `DeltaTableError` from `deltalake`.

Also verify that both functions handle the `date_filter_clause()` `Result` return (introduced in Phase 2) by logging the error and returning exit code 1 on `Err`:

```rust
let where_clause = match date_filter_clause(&from_date, &to_date) {
    Ok(clause) => clause,
    Err(msg) => {
        error!("{}", msg);
        return 1;
    }
};
```

Verify that empty date range handling works with the streaming execution model: after the stream is exhausted, if no rows were processed, log "No records found in the specified date range" and return 0 (AC1.6, AC2.6). This should already be implemented in Phases 3/4 but verify it works correctly with the streaming pattern.

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat: robust error handling for missing tables and empty date ranges`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add tests for shutdown behavior and edge cases

**Verifies:** reparse-and-metadata-table.AC4.4, reparse-and-metadata-table.AC1.6, reparse-and-metadata-table.AC2.5, reparse-and-metadata-table.AC2.6

**Files:**
- Modify: `src/table_ops.rs` (add tests to existing `#[cfg(test)] mod tests` block)

**Testing:**

Tests must verify each AC listed above. Follow the project's existing test patterns.

- **reparse-and-metadata-table.AC4.4 (reparse audit shutdown):** Create a Delta table with test data. Create a `CancellationToken`, cancel it immediately before calling `run_reparse_audit()`. Verify the function returns exit code 1. This tests that the cancellation check at loop start causes early exit.

- **reparse-and-metadata-table.AC4.4 (metadata extraction shutdown):** Same pattern — cancel the token before calling `run_extract_metadata()`. Verify exit code 1. Verify the output path either doesn't exist or has partial data.

- **reparse-and-metadata-table.AC1.6 (reparse audit empty range):** Create a Delta table with `seen_date = "2026-01-01"`. Call `run_reparse_audit()` with `from_date = Some("2099-01-01".to_string())`. Verify exit code 0.

- **reparse-and-metadata-table.AC2.5 (metadata extraction missing table):** Create a `Config` with `delta_sink.table_path` pointing to a non-existent path. Call `run_extract_metadata()`. Verify exit code 1.

- **reparse-and-metadata-table.AC2.6 (metadata extraction empty range):** Create a Delta table with `seen_date = "2026-01-01"`. Call `run_extract_metadata()` with `from_date = Some("2099-01-01".to_string())`. Verify exit code 0.

Use `#[tokio::test]` for all async tests. Use `CancellationToken::new()` from `tokio_util::sync` for shutdown tests. Clean up temp directories after each test.

**Verification:**
Run: `cargo test --lib table_ops::tests`
Expected: All tests pass

Run: `cargo test`
Expected: All 359+ tests pass (existing + new)

**Commit:** `test: add shutdown and edge case tests for both table operations`
<!-- END_TASK_3 -->
