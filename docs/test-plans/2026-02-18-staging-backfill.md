# Human Test Plan: Staging Backfill

**Generated:** 2026-02-18
**Implementation Plan:** `docs/implementation-plans/2026-02-18-staging-backfill/`
**Automated Tests:** 266 passing (`cargo test`)

## Prerequisites

- Rust toolchain installed (edition 2024)
- Project builds successfully: `cargo build`
- All automated tests passing: `cargo test` (expect 266 tests to pass)
- A populated state file (`certstream_state.json`) with at least one log entry containing a `current_index` value. If none exists, run the live server briefly to generate one, or manually create a JSON file with the structure: `{"logs": {"https://some-ct-log.example.com/": {"current_index": 1000}}}`
- A main Delta table with known data at the path configured in `config.yaml` under `delta_sink.table_path` (or set via `CERTSTREAM_DELTA_SINK_TABLE_PATH`). If starting fresh, run a short backfill without `--staging-path` first to populate it.

## Phase 1: Staging Backfill Write Path

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Run `cargo run -- --backfill --staging-path /tmp/test_staging_phase1`. Let it run for 30-60 seconds, then Ctrl+C. | Process starts, prints "backfill mode starting" log with the staging path. On Ctrl+C, process exits cleanly. |
| 1.2 | Check that `/tmp/test_staging_phase1/_delta_log/` directory exists. | The staging Delta table was created at the specified path. |
| 1.3 | Verify the main table at `config.delta_sink.table_path` was NOT modified (check its `_delta_log/` for new transaction JSON files created during step 1.1). | No new transaction files in the main table's `_delta_log/` during the staging backfill window. Records went to staging only. |
| 1.4 | Run `cargo run -- --backfill --staging-path /tmp/test_staging_phase1 --from 0` and let it run briefly. | Historical mode activates. Log output shows work items starting from index 0. |
| 1.5 | Clean up: `rm -rf /tmp/test_staging_phase1` | Directory removed. |

## Phase 2: Gap Detection Union Behavior

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Run `cargo run -- --backfill --staging-path /tmp/test_staging_phase2` (catch-up mode). Note the work items logged. | Work items are logged showing gaps between existing main table data and the state file ceiling. |
| 2.2 | Let it partially complete (some records written to staging), then Ctrl+C. | Staging table exists with partial data. |
| 2.3 | Run the same command again: `cargo run -- --backfill --staging-path /tmp/test_staging_phase2`. Compare work items to step 2.1. | Fewer work items than step 2.1, because records already in staging are excluded from the gap list. |
| 2.4 | Clean up: `rm -rf /tmp/test_staging_phase2` | Directory removed. |

## Phase 3: Merge Deduplication and Cleanup

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Populate staging: run `cargo run -- --backfill --staging-path /tmp/test_staging_merge` and let it write some records. | Staging table at `/tmp/test_staging_merge` has records. |
| 3.2 | Run `RUST_LOG=info cargo run -- --merge --staging-path /tmp/test_staging_merge` | Process outputs "merge mode starting", reads staging records, performs batch-by-batch MERGE INTO, outputs "merge complete" with `total_inserted`, `total_skipped`, `total_staging_rows` fields. Exits with code 0. |
| 3.3 | Verify `/tmp/test_staging_merge` no longer exists. | Staging directory was deleted after successful merge. |
| 3.4 | To test deduplication: recreate staging with `cargo run -- --backfill --staging-path /tmp/test_staging_merge2`, then merge with `cargo run -- --merge --staging-path /tmp/test_staging_merge2`. | Merge succeeds. Log output shows `total_skipped` > 0 for records that already existed in main from step 3.2. `total_inserted + total_skipped = total_staging_rows`. |
| 3.5 | Clean up any remaining staging directories. | Directories removed. |

## Phase 4: Error Handling

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Run `cargo run -- --merge` (without `--staging-path`). | Stderr prints `Error: --merge requires --staging-path <PATH>`. Process exits with code 1. Verify with `echo $?`. |
| 4.2 | Run `cargo run -- --merge --staging-path /tmp/nonexistent_staging_path_xyz`. | Process outputs "staging table does not exist, nothing to merge" at info level. Exits with code 0. |
| 4.3 | Create a staging table with data, then start merge on a large staging table: `cargo run -- --merge --staging-path /tmp/large_staging`. While merge is running, send SIGINT (Ctrl+C). | Process exits with code 1. The staging directory at `/tmp/large_staging` still exists (was NOT deleted). This allows retry. |
| 4.4 | Code review: open `src/backfill.rs` at the `run_merge()` function. Confirm that every `Err(e)` match arm returns 1 BEFORE reaching the `remove_dir_all` call near the end of the function. | All error paths return early with code 1. The `remove_dir_all` call for staging cleanup only executes after all batches merge successfully. |

## End-to-End: Full Staging Backfill and Merge Cycle

**Purpose:** Validate the complete workflow from gap detection through staging writes through merge into main, ensuring data integrity and no duplicates across the entire pipeline.

1. Note the current record count in the main Delta table
2. Run `cargo run -- --backfill --staging-path /tmp/e2e_staging` and let it complete (or run until it writes some records)
3. Note the staging record count
4. Run `cargo run -- --merge --staging-path /tmp/e2e_staging`
5. Verify merge log shows `total_inserted` = (number of new records), `total_skipped` = (number of already-existing records), and `total_inserted + total_skipped = total_staging_rows`
6. Verify the main table's new record count = (original count) + `total_inserted`
7. Verify `/tmp/e2e_staging` no longer exists
8. Run `cargo run -- --backfill --staging-path /tmp/e2e_staging_2` again
9. Verify that the gap list is smaller than step 2 (the merged records filled gaps)

## End-to-End: Backward Compatibility

**Purpose:** Verify that the legacy workflow (backfill without `--staging-path`) still writes directly to the main table.

1. Note the current main table record count
2. Run `cargo run -- --backfill` (no `--staging-path`) and let it write some records
3. Verify the main table record count increased
4. Verify no staging directory was created anywhere

## Human Verification Required

| ID | Criterion | Why Manual | Steps |
|----|-----------|------------|-------|
| HV1 | AC3.5 -- Merge log output content | Structured tracing log fields require a tracing subscriber capture setup to verify programmatically. | Run `RUST_LOG=info cargo run -- --merge --staging-path /tmp/test_staging` with prepared test data. Confirm log output contains a line with "merge complete", `total_inserted`, `total_skipped`, and `total_staging_rows` fields. Verify inserted + skipped = total_staging_rows. |
| HV2 | AC4.3 -- Merge batch failure preserves staging | Simulating a real Delta MERGE INTO failure requires corrupting Delta transaction logs or injecting I/O errors. | Code review: confirm `run_merge()` in `src/backfill.rs` returns 1 on any `Err(e)` before reaching `remove_dir_all`. The cancellation test (AC4.4) exercises the same structural pattern. |
| HV3 | AC4.4 -- Real SIGINT handling | Automated test uses a pre-cancelled CancellationToken. The full signal path involves the signal handler in `main.rs`. | Start `cargo run -- --merge --staging-path /tmp/test_staging` with a large staging table. Send SIGINT (Ctrl+C) during merge. Verify exit code 1 (`echo $?`). Verify staging directory still exists. |
| HV4 | AC4.5 -- CLI error message format | Automated test verifies the condition logic only. The actual stderr output and exit code require running the binary as a subprocess. | Run `cargo run -- --merge` (without `--staging-path`). Verify stderr contains "Error: --merge requires --staging-path <PATH>". Verify exit code is 1. |
| HV5 | AC1.3 -- End-to-end staging with both modes | Full end-to-end test requires a running CT log endpoint. Component-level tests cover gap detection + writer individually. | See Phase 1 steps 1.1-1.4. |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1: Staging table at path with schema | `test_staging_write_creates_table_at_staging_path` | Phase 1, steps 1.1-1.2 |
| AC1.2: Records in staging, not main | `test_ac1_2_records_in_staging_not_in_main` | Phase 1, step 1.3 |
| AC1.3: Both catch-up and historical modes | `test_ac1_3_gap_detection_with_staging_catch_up_and_historical` | Phase 1, steps 1.1 + 1.4; HV5 |
| AC2.1: Union excludes entries in either table | `test_ac2_1_union_all_excludes_entries_in_either_table` | Phase 2, steps 2.1-2.3 |
| AC2.2: Second run fewer work items | `test_ac2_2_second_staging_run_produces_fewer_work_items` | Phase 2, steps 2.2-2.3 |
| AC2.3: Ceiling caps union max | `test_ac2_3_ceiling_caps_union_max_cert_index` | -- (fully covered by automated test) |
| AC2.4: Missing staging falls back to main | `test_ac2_4_missing_staging_falls_back_to_main_only` | Phase 4, step 4.2 |
| AC3.1: Merge inserts non-duplicates | `test_ac3_1_merge_inserts_non_duplicate_records` | Phase 3, step 3.2 |
| AC3.2: Merge skips existing | `test_ac3_2_merge_skips_existing_records` | Phase 3, step 3.4 |
| AC3.3: Merge idempotent | `test_ac3_3_merge_is_idempotent` | Phase 3, steps 3.2 + 3.4 |
| AC3.4: Staging deleted on success | `test_ac3_4_staging_directory_deleted_on_success` | Phase 3, step 3.3 |
| AC3.5: Merge returns 0, logs metrics | `test_ac3_5_merge_returns_zero_and_logs_metrics` | HV1 |
| AC4.1: Missing/empty staging exits 0 | `test_ac4_1_missing_staging_table_exits_zero`, `test_ac4_1_empty_staging_table_exits_zero` | Phase 4, step 4.2 |
| AC4.2: Missing main auto-created | `test_ac4_2_missing_main_table_auto_created` | -- (fully covered by automated test) |
| AC4.3: Merge failure preserves staging | -- (human verification only) | HV2; Phase 4, step 4.4 |
| AC4.4: Cancellation preserves staging | `test_ac4_4_cancellation_preserves_staging` | HV3; Phase 4, step 4.3 |
| AC4.5: --merge without --staging-path | `test_ac4_5_cli_validation_merge_without_staging_path` | HV4; Phase 4, step 4.1 |
| AC5.1: Backward compatibility | All existing tests pass | E2E Backward Compatibility scenario |
| AC5.2: All existing tests pass | `cargo test` full suite | Run `cargo test` as prerequisite |
