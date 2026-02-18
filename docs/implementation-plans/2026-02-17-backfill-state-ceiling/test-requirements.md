# Test Requirements: Backfill State Ceiling

Maps each acceptance criterion to automated tests or documented human verification.

**Test file:** `src/backfill.rs` (`mod tests` section)
**Supporting test file:** `src/state.rs` (`mod tests` section, pre-existing)

---

## AC1: Catch-up mode uses state file ceiling

### AC1.1 Success: Catch-up mode with internal gaps fills only internal gaps (no frontier gap generated)

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC1.1 |
| **Verification** | Automated |
| **Test type** | Integration (async, Delta Lake) |
| **Test file** | `src/backfill.rs` |
| **Test function (updated)** | `test_ac2_2_catch_up_internal_gaps` |
| **Test function (new)** | `test_catch_up_ceiling_only_internal_gaps` |
| **What it verifies** | The updated test writes records `[10, 11, 12, 15, 16, 20]` with ceiling=22 and asserts exactly 2 work items `(13, 14)` and `(17, 19)` are returned -- no frontier gap `(21, 21)`. The new test writes records `[10, 11, 15, 16]` with ceiling=1000 and asserts exactly 1 internal gap `(12, 14)` with no frontier gap despite the large ceiling. |
| **Phase** | Phase 3, Task 1 (updated test) and Task 2 (new test) |

### AC1.2 Success: Catch-up mode with contiguous data and ceiling > max_index generates no work items

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC1.2 |
| **Verification** | Automated |
| **Test type** | Integration (async, Delta Lake) |
| **Test file** | `src/backfill.rs` |
| **Test function (updated)** | `test_ac2_1_catch_up_lower_bound_from_min` |
| **Test function (updated)** | `test_ac2_3_catch_up_no_frontier_gap` (renamed from `test_ac2_3_catch_up_frontier_gap`) |
| **What it verifies** | `test_ac2_1` writes contiguous records `[100, 101, 102]` with ceiling=200 and asserts 0 work items (previously asserted frontier gap `(103, 199)`). `test_ac2_3` writes contiguous records `[0, 1, 2]` with ceiling=10 and asserts 0 work items (previously asserted frontier gap `(3, 9)`). Both confirm that no frontier gap is generated when data is contiguous. |
| **Phase** | Phase 3, Task 1 |

### AC1.3 Failure: Catch-up mode with log not in state file skips that log with warning

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC1.3 |
| **Verification** | Automated (structural) + Automated (indirect) |
| **Test type** | Structural verification of `run_backfill()` code |
| **Test file** | `src/backfill.rs` (code), `src/state.rs` (supporting tests) |
| **Test function** | N/A (structural), supported by `state::tests::test_new_with_nonexistent_file` and `state::tests::test_load_valid_state_file` |
| **What it verifies** | Phase 1 Task 2 modifies `run_backfill()` to iterate over configured logs and call `state_manager.get_index(&log.normalized_url())`. When `get_index()` returns `None`, the log is skipped with a `warn!()` log and not added to `log_ceilings`. This is not directly testable at the `detect_gaps()` level because the filtering happens in `run_backfill()` before `detect_gaps()` is called. The `StateManager` tests in `src/state.rs` verify that `get_index()` returns `None` for unknown logs (`test_new_with_nonexistent_file`) and `Some(index)` for known logs (`test_load_valid_state_file`). Testing `run_backfill()` directly would require a full config, HTTP client, and Delta table -- the structural change in Phase 1 Task 2 is the primary verification. |
| **Phase** | Phase 1, Task 2 (implementation), Phase 3, Task 2 (notes) |

### AC1.4 Edge: Catch-up mode with no state file generates no work items for any log

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC1.4 |
| **Verification** | Automated (indirect via StateManager tests) |
| **Test type** | Unit (StateManager behavior) |
| **Test file** | `src/state.rs` |
| **Test function** | `test_new_without_file`, `test_new_with_nonexistent_file` |
| **What it verifies** | When `StateManager::new(None)` or `StateManager::new(Some("nonexistent.json"))` is called, all subsequent `get_index()` calls return `None`. In the `run_backfill()` code (Phase 1 Task 2), this means every log is skipped, `log_ceilings` remains empty, and the function returns exit code 0 before calling `detect_gaps()`. Direct testing of `run_backfill()` with a missing state file is not feasible without a full integration harness; the StateManager unit tests plus the structural code change provide coverage. |
| **Phase** | Phase 3, Task 2 (documented in testing notes) |
| **Justification for indirect testing** | `run_backfill()` requires a full `Config`, `reqwest::Client`, configured CT logs, and optionally a Delta table. Constructing this in a unit test is disproportionate to the value. The behavior is a composition of two verified pieces: (1) StateManager returns `None` for all lookups when no file exists, and (2) `run_backfill()` skips logs where `get_index()` returns `None`. |

---

## AC2: Historical mode uses state file ceiling

### AC2.1 Success: Historical mode uses ceiling from state file as upper bound

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC2.1 |
| **Verification** | Automated |
| **Test type** | Integration (async, Delta Lake) |
| **Test file** | `src/backfill.rs` |
| **Test function (updated)** | `test_ac3_1_historical_pre_existing_gap` |
| **Test function (updated)** | `test_ac3_2_historical_from_override` |
| **Test function (new)** | `test_historical_ceiling_used_as_upper_bound` |
| **What it verifies** | `test_ac3_1` writes records `[50, 51, 52]` with `--from 0` and ceiling=55, asserts exactly 1 work item `(0, 49)` -- no frontier gap `(53, 54)`. `test_ac3_2` writes records `[50, 51, 52]` with `--from 40` and ceiling=55, asserts exactly 1 work item `(40, 49)` -- no frontier gap. The new `test_historical_ceiling_used_as_upper_bound` uses no Delta table, ceiling=100, `--from 0`, and asserts a single work item `(0, 99)`, confirming ceiling is used as the upper bound (ceiling-1 = end index). |
| **Phase** | Phase 3, Task 1 (updated tests) and Task 2 (new test) |

### AC2.2 Success: Historical mode with no delta table uses ceiling from state file

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC2.2 |
| **Verification** | Automated |
| **Test type** | Integration (async, no Delta table) |
| **Test file** | `src/backfill.rs` |
| **Test function (new)** | `test_historical_ceiling_used_as_upper_bound` |
| **Test function (existing)** | `test_ac3_1_historical_empty_table_full_range` |
| **What it verifies** | `test_historical_ceiling_used_as_upper_bound` calls `detect_gaps()` with no existing Delta table, ceiling=100, and `--from 0`. Asserts work item `(0, 99)` -- the ceiling value (from what would be the state file's `current_index`) is used as the upper bound, not any HTTP-fetched tree_size. The existing `test_ac3_1_historical_empty_table_full_range` also verifies this path with ceiling=55 and asserts work item `(0, 54)`. |
| **Phase** | Phase 3, Task 2 (new test); existing test unchanged |

### AC2.3 Failure: Historical mode with log not in state file skips that log with warning

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC2.3 |
| **Verification** | Automated (structural) + Automated (indirect) |
| **Test type** | Structural verification of `run_backfill()` code |
| **Test file** | `src/backfill.rs` (code), `src/state.rs` (supporting tests) |
| **Test function** | N/A (structural), supported by `state::tests::test_new_without_file`, `state::tests::test_new_with_nonexistent_file` |
| **What it verifies** | The same filtering logic in `run_backfill()` (Phase 1 Task 2) applies to both catch-up and historical modes -- the state file lookup and log skipping happen before the mode-specific code path in `detect_gaps()`. When `state_manager.get_index()` returns `None` for a log, it is excluded from the `log_ceilings` vector passed to `detect_gaps()`, regardless of whether `--from` is specified. StateManager unit tests verify the `None` return for unknown logs. |
| **Phase** | Phase 1, Task 2 (implementation), Phase 3, Task 2 (documented in notes) |
| **Justification for indirect testing** | Same as AC1.3 -- direct `run_backfill()` testing requires a full integration harness. The behavior is structurally guaranteed by the code in Phase 1 Task 2: the state file lookup loop runs unconditionally (both modes), and only logs with `Some(ceiling)` are included in the `log_ceilings` vector. |

---

## AC3: Tree size fetching removed

### AC3.1 Success: run_backfill() does not make HTTP calls to fetch tree sizes

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC3.1 |
| **Verification** | Automated (structural) |
| **Test type** | Code inspection / structural verification |
| **Test file** | `src/backfill.rs` |
| **Test function** | N/A (structural) |
| **What it verifies** | Phase 1 Task 2 removes the entire "Step 2: tree size discovery" block (lines 551-586) from `run_backfill()`. This block contained all calls to `get_tree_size()` and `get_checkpoint_tree_size()`. After the change, `run_backfill()` has no HTTP calls for tree size fetching -- it reads the ceiling from the local state file via `StateManager`. Verification is structural: the removed code is the only location where tree size HTTP calls were made in `run_backfill()`. No runtime test is needed because the functions are simply no longer called. |
| **Phase** | Phase 1, Task 2 (implementation), Phase 3, Task 2 (documented in notes) |
| **Justification for structural verification** | Proving a negative (no HTTP calls are made) would require either mocking the HTTP client or running a network-isolated test. The structural removal of all `get_tree_size` / `get_checkpoint_tree_size` calls from `run_backfill()` is deterministic and verifiable by code review. The functions remain available for the live watcher -- only the backfill code path no longer calls them. A `cargo build` success after the removal confirms no compilation dependency on the removed calls. |

### AC3.2 Success: State file is loaded via StateManager using config.ct_log.state_file path

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC3.2 |
| **Verification** | Automated (structural) + Automated (unit, supporting) |
| **Test type** | Code inspection + Unit (StateManager) |
| **Test file** | `src/backfill.rs` (code), `src/state.rs` (supporting tests) |
| **Test function** | N/A (structural), supported by `state::tests::test_save_and_load_roundtrip`, `state::tests::test_load_valid_state_file` |
| **What it verifies** | Phase 1 Task 2 adds the line `let state_manager = StateManager::new(config.ct_log.state_file.clone());` to `run_backfill()`. This uses the exact same pattern as the live server startup in `src/main.rs`. The StateManager unit tests verify that `new()` correctly loads from the given path, handles missing files, handles corrupt files, and returns `current_index` via `get_index()`. |
| **Phase** | Phase 1, Task 2 (implementation), Phase 3, Task 2 (documented in notes) |
| **Justification for structural verification** | The state file loading is a single line using an already-tested API (`StateManager::new`). Adding a separate test that constructs a full `Config` just to verify this line provides no additional confidence beyond the StateManager unit tests and the code review. |

### AC3.3 Edge: State file with current_index < delta min_index produces no work items for that log

| Field | Value |
|---|---|
| **Criterion ID** | backfill-state-ceiling.AC3.3 |
| **Verification** | Automated |
| **Test type** | Integration (async, Delta Lake) |
| **Test file** | `src/backfill.rs` |
| **Test function (new)** | `test_ceiling_below_data_no_work_items` |
| **What it verifies** | Writes records `[100, 101, 102]` to Delta, calls `detect_gaps()` in catch-up mode with ceiling=50 (below min_index=100). Asserts 0 work items. After the frontier gap removal, the ceiling value is not consulted in catch-up mode at all -- only internal gaps are detected via the LEAD window function. Since the data is contiguous, no internal gaps exist. This test confirms that a ceiling below the data range does not cause erroneous behavior (no negative-range work items, no panics). |
| **Phase** | Phase 3, Task 2 |
| **Note** | The phase 3 implementation notes acknowledge this is inherently satisfied by the frontier gap removal: ceiling is no longer used in catch-up mode. The test exists as a regression guard to ensure that if future changes reintroduce ceiling-aware logic, the edge case remains handled. |

---

## Summary Matrix

| Criterion | Verification Type | Test Function(s) | Phase |
|---|---|---|---|
| AC1.1 | Automated (integration) | `test_ac2_2_catch_up_internal_gaps` (updated), `test_catch_up_ceiling_only_internal_gaps` (new) | P3-T1, P3-T2 |
| AC1.2 | Automated (integration) | `test_ac2_1_catch_up_lower_bound_from_min` (updated), `test_ac2_3_catch_up_no_frontier_gap` (updated) | P3-T1 |
| AC1.3 | Structural + indirect unit | `state::test_new_with_nonexistent_file`, `state::test_load_valid_state_file` | P1-T2, P3-T2 |
| AC1.4 | Indirect unit (StateManager) | `state::test_new_without_file`, `state::test_new_with_nonexistent_file` | P3-T2 |
| AC2.1 | Automated (integration) | `test_ac3_1_historical_pre_existing_gap` (updated), `test_ac3_2_historical_from_override` (updated), `test_historical_ceiling_used_as_upper_bound` (new) | P3-T1, P3-T2 |
| AC2.2 | Automated (integration) | `test_historical_ceiling_used_as_upper_bound` (new), `test_ac3_1_historical_empty_table_full_range` (existing) | P3-T2 |
| AC2.3 | Structural + indirect unit | `state::test_new_without_file`, `state::test_new_with_nonexistent_file` | P1-T2, P3-T2 |
| AC3.1 | Structural (code removal) | N/A -- verified by absence of `get_tree_size`/`get_checkpoint_tree_size` calls in `run_backfill()` | P1-T2 |
| AC3.2 | Structural + indirect unit | `state::test_save_and_load_roundtrip`, `state::test_load_valid_state_file` | P1-T2, P3-T2 |
| AC3.3 | Automated (integration) | `test_ceiling_below_data_no_work_items` (new) | P3-T2 |

---

## Test Counts

**Updated tests:** 6 (existing tests modified to remove frontier gap assertions)
- `test_ac2_1_catch_up_lower_bound_from_min`
- `test_ac2_2_catch_up_internal_gaps`
- `test_ac2_3_catch_up_frontier_gap` (renamed to `test_ac2_3_catch_up_no_frontier_gap`)
- `test_ac2_4_catch_up_missing_log_skipped`
- `test_ac3_1_historical_pre_existing_gap`
- `test_ac3_2_historical_from_override`

**New tests:** 3
- `test_ceiling_below_data_no_work_items` (AC3.3)
- `test_catch_up_ceiling_only_internal_gaps` (AC1.1)
- `test_historical_ceiling_used_as_upper_bound` (AC2.1, AC2.2)

**Supporting pre-existing tests (unchanged, in `src/state.rs`):** 4
- `test_new_without_file` (AC1.4, AC2.3)
- `test_new_with_nonexistent_file` (AC1.3, AC1.4, AC2.3)
- `test_load_valid_state_file` (AC1.3, AC3.2)
- `test_save_and_load_roundtrip` (AC3.2)

**Structurally verified (no runtime test):** 2 criteria
- AC3.1: Tree size HTTP calls removed from `run_backfill()`
- AC3.2: `StateManager::new(config.ct_log.state_file.clone())` added to `run_backfill()`
