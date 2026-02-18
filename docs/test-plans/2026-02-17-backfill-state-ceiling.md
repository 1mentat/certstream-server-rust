# Backfill State Ceiling — Human Test Plan

## Prerequisites

- Rust toolchain installed (edition 2024)
- Working directory: project root
- All automated tests passing: `cargo test backfill::tests` (21 pass) and `cargo test state::tests` (12 pass)
- A `certstream_state.json` file available for testing (can be created manually or by running the server against real CT logs)
- Delta Lake table directory available at a writable path (e.g., `/tmp/delta_test_table`)

## Phase 1: Verify State File Ceiling Replaces Tree Size HTTP Calls

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `src/backfill.rs` and search for `get_tree_size` and `get_checkpoint_tree_size`. | Zero matches found in the file. These functions exist in `src/ct/fetch.rs` but are no longer called from `run_backfill()`. |
| 2 | Locate the "Step 2" section in `run_backfill()` (around line 534). Verify it reads `// Step 2: State file ceiling lookup` and creates a `StateManager`. | Line 535 should read `let state_manager = StateManager::new(config.ct_log.state_file.clone());` |
| 3 | Verify that the `log_ceilings` vector is built by iterating logs and calling `state_manager.get_index()`. | Lines 538-550 should show a `for log in &logs` loop that matches on `state_manager.get_index(&log.normalized_url())`, pushing `Some(ceiling)` matches and logging a `warn!` for `None`. |
| 4 | Verify that when `log_ceilings` is empty, `run_backfill()` returns exit code 0 (not 1). | Line 554-556: `if log_ceilings.is_empty() { warn!("no logs have ceiling values from state file"); return 0; }` |

## Phase 2: Verify Frontier Gap Removal in detect_gaps()

| Step | Action | Expected |
|------|--------|----------|
| 1 | In `detect_gaps()` (starting around line 187), locate the catch-up mode branch (`None =>` around line 232). | The catch-up branch should only call `find_internal_gaps()` on the range `[state.min_index, state.max_index]`. There should be no code that compares `state.max_index + 1 < ceiling` or generates frontier work items. |
| 2 | In `detect_gaps()`, locate the historical mode branch (`Some(from) =>` around line 260). | The historical branch should handle pre-existing gaps (`from < state.min_index`), internal gaps via `find_internal_gaps()`, and the "log not in delta" path using `ceiling - 1` as the end. There should be no frontier gap code comparing `state.max_index + 1 < ceiling`. |
| 3 | Run `git diff 9e6e1a1..HEAD -- src/backfill.rs` and search for removed `-` lines containing `frontier gap`. | Should find removed frontier gap blocks from `detect_gaps()`. |

## Phase 3: End-to-End Manual Backfill Verification

**Purpose:** Validate the full backfill pipeline works with a real state file and no HTTP tree size fetching.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Create a test state file at `/tmp/test_certstream_state.json` with content: `{"version":1,"logs":{"https://ct.googleapis.com/logs/us1/argon2025h1/":{"current_index":1000,"tree_size":2000,"last_success":1700000000}}}` | File created successfully. |
| 2 | Set environment variable `CERTSTREAM_CT_LOG_STATE_FILE=/tmp/test_certstream_state.json`. Run `cargo run -- --backfill --logs "argon2025"` with `CERTSTREAM_DELTA_SINK_TABLE_PATH` pointing to a test directory. | Backfill should start, use ceiling=1000 from the state file (not fetch tree size via HTTP), and either fill gaps or report "no gaps detected". The log output should NOT contain "failed to get tree size" or "failed to get checkpoint tree size". |
| 3 | Run the same command but without a state file (delete or rename the state file, or set path to nonexistent file). | Backfill should log "log not in state file, skipping (no ceiling reference)" for each log, then "no logs have ceiling values from state file", and exit with code 0. |
| 4 | Create a state file with a log URL that does not match any configured CT log. Run `cargo run -- --backfill`. | All logs should be skipped with "log not in state file, skipping" warnings. Exit code 0. |

## Phase 4: Verify Updated Test Assertions

| Step | Action | Expected |
|------|--------|----------|
| 1 | Run `cargo test test_ac2_1_catch_up_lower_bound_from_min -- --nocapture 2>&1`. | Test passes. Output should show assertion `work_items.len() == 0` (previously was 1 with frontier gap). |
| 2 | Run `cargo test test_ac2_2_catch_up_internal_gaps -- --nocapture 2>&1`. | Test passes. Asserts exactly 2 gaps `(13,14)` and `(17,19)` (previously was 3, including frontier gap `(21,21)`). |
| 3 | Run `cargo test test_ac2_3_catch_up_no_frontier_gap -- --nocapture 2>&1`. | Test passes. Function name was renamed from `test_ac2_3_catch_up_frontier_gap`. Asserts 0 work items (previously asserted frontier gap). |
| 4 | Run `cargo test test_ac3_1_historical_pre_existing_gap -- --nocapture 2>&1`. | Test passes. Asserts exactly 1 work item `(0,49)` (previously asserted 2+ including frontier gap `(53,54)`). |
| 5 | Run `cargo test test_ac3_2_historical_from_override -- --nocapture 2>&1`. | Test passes. Asserts exactly 1 work item `(40,49)` (previously asserted 2+ gaps). |

## End-to-End: State File Ceiling Determines Backfill Scope

**Purpose:** Validate that the ceiling value from the state file correctly bounds backfill operations without any network calls for tree size discovery.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Create a state file with `current_index: 50` for a known log. Populate a Delta table with records `[10, 11, 12, 15, 16, 20]` for that log. | State file and Delta table ready. |
| 2 | Run catch-up backfill (`cargo run -- --backfill`). | Should detect and fill only internal gaps `(13, 14)` and `(17, 19)`. Should NOT create a frontier gap `(21, 49)`. No HTTP calls for tree size. |
| 3 | Change state file `current_index` to 5 (below all data). Run catch-up backfill. | Should detect no frontier gap and only internal gaps within existing data range. The ceiling value is not consulted in catch-up mode for existing data. |
| 4 | Run historical backfill with `--from 0` and state file `current_index: 50`. | Should create work items from index 0 up to 49 (ceiling - 1), plus fill internal gaps. Should NOT extend beyond 49. |

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|------------|-------|
| AC1.3 / AC2.3 - Log not in state file is skipped | The skip logic lives in `run_backfill()` which requires a full config, HTTP client, and CT log list to test directly. Automated coverage is indirect via StateManager unit tests. | Create a state file missing a log that is in the CT log list. Run backfill. Verify the missing log generates a `warn!` message containing "log not in state file, skipping (no ceiling reference)" and does not appear in gap detection output. |
| AC3.1 - No HTTP tree size calls | Proving a negative (no network calls) is best verified by observing runtime behavior. | Run backfill in an environment with no outbound HTTPS access or with `RUST_LOG=debug` and verify no HTTP requests to CT log `/ct/v1/get-sth` or checkpoint endpoints appear in logs. |
| AC1.4 - No state file means no work | Edge case that spans `run_backfill()` integration. | Delete the state file (or set path to nonexistent). Run `cargo run -- --backfill`. Verify exit code 0 and log message "no logs have ceiling values from state file". |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 - Catch-up fills only internal gaps | `test_ac2_2_catch_up_internal_gaps`, `test_catch_up_ceiling_only_internal_gaps` | Phase 4 Steps 2-3 |
| AC1.2 - Contiguous data, no work items | `test_ac2_1_catch_up_lower_bound_from_min`, `test_ac2_3_catch_up_no_frontier_gap` | Phase 4 Steps 1, 3 |
| AC1.3 - Log not in state file skipped | `state::test_new_with_nonexistent_file`, `state::test_load_valid_state_file` (indirect) | Human Verification: AC1.3/AC2.3 |
| AC1.4 - No state file, no work items | `state::test_new_without_file`, `state::test_new_with_nonexistent_file` (indirect) | Human Verification: AC1.4 |
| AC2.1 - Historical mode uses ceiling | `test_ac3_1_historical_pre_existing_gap`, `test_ac3_2_historical_from_override`, `test_historical_ceiling_used_as_upper_bound` | E2E Step 4 |
| AC2.2 - Historical, no table, uses ceiling | `test_historical_ceiling_used_as_upper_bound`, `test_ac3_1_historical_empty_table_full_range` | Phase 3 Step 2 |
| AC2.3 - Historical, log not in state file | `state::test_new_without_file`, `state::test_new_with_nonexistent_file` (indirect) | Human Verification: AC1.3/AC2.3 |
| AC3.1 - No HTTP tree size calls | Structural (grep confirms zero matches) | Phase 1 Step 1, Human Verification: AC3.1 |
| AC3.2 - StateManager loaded from config | Structural (line 535) + `state::test_save_and_load_roundtrip`, `state::test_load_valid_state_file` | Phase 1 Steps 2-3 |
| AC3.3 - Ceiling below data, no work items | `test_ceiling_below_data_no_work_items` | E2E Step 3 |
