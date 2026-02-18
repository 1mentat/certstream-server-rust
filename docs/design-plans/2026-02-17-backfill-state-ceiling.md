# Backfill State Ceiling Design

## Summary

The certstream server monitors Certificate Transparency (CT) logs in real time and stores certificate records in a Delta Lake table. When the live service is offline or falls behind, gaps appear in the stored records. The backfill mode (`--backfill`) fills those gaps by re-fetching the missing index ranges from CT log servers and writing them into Delta Lake.

Currently, backfill determines *how far to fetch* by making HTTP requests to each CT log server to retrieve the current `tree_size` -- the total number of entries published in that log. This creates a mismatch: the live service may have already processed records up to a point well below `tree_size`, so backfill ends up attempting to fetch records that the live service has never seen. This design replaces that HTTP-based upper bound with the `current_index` value recorded in `certstream_state.json`, the same local state file the live service writes to as it processes entries. By anchoring the ceiling to where the live service actually stopped, backfill fetches only the entries the live service was responsible for, eliminates the "frontier gap" concept entirely, and removes all outbound HTTP calls during gap detection. The change applies to both catch-up mode (filling internal holes in existing Delta data) and historical mode (`--backfill --from N`). Logs absent from the state file are skipped, since without a recorded position there is no safe upper bound.

## Definition of Done
Rework backfill's gap detection so that in both catch-up mode (`--backfill`) and historical mode (`--backfill --from N`), the upper bound for each log is the `current_index` from `certstream_state.json` instead of `tree_size`. The frontier gap logic is removed entirely (no opt-in flag). Logs not present in the state file are skipped since there is no reference point for where the live service is tracking. Existing tests are updated to reflect the new behavior.

Specifically:
1. Both backfill modes use `current_index` from `certstream_state.json` as the per-log upper bound (replaces `tree_size` entirely)
2. No `--include-frontier` flag -- frontier gap logic is removed completely
3. Logs not in the state file are skipped with a warning (no reference point for where to stop)
4. State file path comes from existing config (`ct_log.state_file`)
5. Existing tests updated to match new default behavior

## Acceptance Criteria

### backfill-state-ceiling.AC1: Catch-up mode uses state file ceiling
- **backfill-state-ceiling.AC1.1 Success:** Catch-up mode with internal gaps fills only internal gaps (no frontier gap generated)
- **backfill-state-ceiling.AC1.2 Success:** Catch-up mode with contiguous data and ceiling > max_index generates no work items (previously would have generated frontier gap)
- **backfill-state-ceiling.AC1.3 Failure:** Catch-up mode with log not in state file skips that log with warning
- **backfill-state-ceiling.AC1.4 Edge:** Catch-up mode with no state file (missing file) generates no work items for any log

### backfill-state-ceiling.AC2: Historical mode uses state file ceiling
- **backfill-state-ceiling.AC2.1 Success:** Historical mode (`--from N`) uses ceiling from state file as upper bound instead of tree_size
- **backfill-state-ceiling.AC2.2 Success:** Historical mode with no delta table uses ceiling from state file (not tree_size)
- **backfill-state-ceiling.AC2.3 Failure:** Historical mode with log not in state file skips that log with warning

### backfill-state-ceiling.AC3: Tree size fetching removed
- **backfill-state-ceiling.AC3.1 Success:** `run_backfill()` does not make HTTP calls to fetch tree sizes
- **backfill-state-ceiling.AC3.2 Success:** State file is loaded via `StateManager` using `config.ct_log.state_file` path
- **backfill-state-ceiling.AC3.3 Edge:** State file with `current_index` < delta `min_index` produces no work items for that log (ceiling is below existing data)

## Glossary

- **tree_size**: The total number of entries a CT log server has published, returned by its signed tree head. Previously used as the backfill upper bound; replaced by `current_index` in this design.
- **current_index**: The last certificate index successfully processed by the live certstream service for a given log. Persisted per-log in `certstream_state.json`.
- **certstream_state.json**: Local JSON file written periodically by the live server. Records `current_index` and `tree_size` for every monitored CT log. Used here as the authoritative upper bound for backfill.
- **StateManager**: Rust struct (`src/state.rs`) that loads and caches the state file. Exposes `get_index(url)` to look up `current_index` per log. Handles missing or corrupt files gracefully.
- **frontier gap**: The range of indices between the highest index stored in Delta (`max_index`) and the upper bound. This concept is removed by this design since the ceiling now IS the tracked index.
- **internal gap**: A missing range of `cert_index` values within records already stored in the Delta table (holes within `[min_index, max_index]`). Found via SQL LEAD window function.
- **catch-up mode**: Backfill invoked without `--from`. Only processes logs already present in the Delta table and fills holes within their existing index range.
- **historical mode**: Backfill invoked with `--from N`. Fetches from index N up to the ceiling for every configured log, including logs not yet in the Delta table.
- **ceiling**: The per-log upper bound for backfill work items. Previously `tree_size`; now `current_index` from the state file.

## Architecture

Replace `tree_size` (fetched from CT log servers) with `current_index` (read from the local state file) as the upper bound in backfill gap detection.

Currently, `run_backfill()` in `src/backfill.rs` fetches each log's `tree_size` via HTTP (RFC 6962 `get-sth` or Static CT checkpoint), builds a `Vec<(String, u64)>` of `(source_url, tree_size)` pairs, and passes them to `detect_gaps()`. The `detect_gaps()` function uses `tree_size` to generate frontier gap work items (the range from `max_index + 1` to `tree_size - 1`).

The change:
1. `run_backfill()` loads `StateManager` from the configured state file path (`config.ct_log.state_file`)
2. For each discovered log, looks up `current_index` via `state_manager.get_index(url)`
3. Logs not in the state file are skipped with a warning
4. Passes `current_index` as the ceiling (replacing `tree_size`) in the `(source_url, ceiling)` tuple
5. Tree size fetching (HTTP calls to each log server) is removed entirely
6. Frontier gap blocks in `detect_gaps()` (lines 257-264 and 301-308) are removed since the ceiling now IS the tracked index

Data flow:
```
certstream_state.json -> StateManager -> per-log current_index
                                              |
run_backfill() builds (source_url, ceiling) pairs
                                              |
detect_gaps(table_path, logs, backfill_from) -- unchanged signature
                                              |
                  internal gaps only (no frontier)
```

The `detect_gaps()` function signature stays the same: `(table_path, logs: &[(String, u64)], backfill_from: Option<u64>)`. The second tuple element is now `ceiling` (renamed from `tree_size` in variable names for clarity).

## Existing Patterns

The state file loading pattern already exists in the live server startup path in `src/main.rs` (lines 122-128):
```rust
let state_manager = StateManager::new(config.ct_log.state_file.clone());
```

`StateManager::new(path)` handles all edge cases: missing file (returns `None` for all lookups), corrupt file (warns and starts fresh), and normal load. The backfill code reuses this exact pattern.

The `detect_gaps()` function already handles the "no data for this log" case by skipping it in catch-up mode (line 266 comment: "If log not in delta, skip"). The same pattern extends to "no state file entry for this log."

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Load state file in backfill and replace tree_size with ceiling

**Goal:** Replace HTTP tree_size fetching with state file lookup. Remove frontier gap logic.

**Components:**
- `src/backfill.rs` `run_backfill()` function -- replace Step 2 (tree size discovery, lines 551-581) with state file loading and ceiling lookup
- `src/backfill.rs` `detect_gaps()` function -- remove frontier gap blocks (lines 257-264 catch-up mode, lines 301-308 historical mode), rename `tree_size` references to `ceiling` in variable names
- `src/backfill.rs` `detect_gaps()` historical mode with no delta table (lines 196-215) -- use ceiling from state file instead of tree_size

**Dependencies:** None (first phase)

**Done when:** `cargo build` succeeds. `detect_gaps()` no longer generates frontier gap work items. `run_backfill()` loads state file and skips logs not present in it.

Covers: `backfill-state-ceiling.AC1.1`, `backfill-state-ceiling.AC1.2`, `backfill-state-ceiling.AC1.3`, `backfill-state-ceiling.AC2.1`, `backfill-state-ceiling.AC2.2`
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Update CLI help text

**Goal:** Update help text to reflect that backfill uses state file as ceiling.

**Components:**
- `src/cli.rs` `print_help()` function -- update BACKFILL OPTIONS section to note state file dependency

**Dependencies:** Phase 1

**Done when:** `cargo run -- --help` shows updated backfill documentation mentioning state file requirement.
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Update tests

**Goal:** Update existing tests to match new behavior (no frontier gaps) and add new tests for state file ceiling logic.

**Components:**
- `src/backfill.rs` `mod tests` -- update tests that assert frontier gap behavior (`test_ac2_1_catch_up_lower_bound_from_min`, `test_ac2_2_catch_up_internal_gaps`, `test_ac2_3_catch_up_frontier_gap`, `test_ac3_1_historical_pre_existing_gap`, `test_ac3_2_historical_from_override`)
- `src/backfill.rs` `mod tests` -- add new tests for: logs skipped when not in state file, ceiling used instead of tree_size, state file missing returns no work items

**Dependencies:** Phase 1

**Done when:** `cargo test` passes. All existing frontier-gap-asserting tests updated. New tests verify ceiling behavior.

Covers: `backfill-state-ceiling.AC1.4`, `backfill-state-ceiling.AC2.3`, `backfill-state-ceiling.AC3.1`, `backfill-state-ceiling.AC3.2`, `backfill-state-ceiling.AC3.3`
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Update CLAUDE.md contracts

**Goal:** Update project documentation to reflect new backfill behavior.

**Components:**
- `CLAUDE.md` Backfill Contracts section -- update to reflect state file ceiling, remove frontier gap references, add state file dependency

**Dependencies:** Phases 1-3

**Done when:** CLAUDE.md accurately describes the new backfill behavior.
<!-- END_PHASE_4 -->

## Additional Considerations

**State file must exist for backfill to work.** If the state file is missing or empty, all logs are skipped and backfill exits with "no gaps detected, nothing to backfill" (exit code 0). This is intentional -- without a state file reference, there's no safe ceiling to use.

**Operator workflow:** The expected workflow is: (1) run live service to populate state file, (2) stop live service, (3) run backfill to fill gaps, (4) restart live service. The state file persists across restarts.
