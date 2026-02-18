# Backfill State Ceiling Implementation Plan - Phase 1

**Goal:** Replace HTTP tree_size fetching with state file ceiling lookup and remove frontier gap logic from backfill gap detection.

**Architecture:** Load `StateManager` from `config.ct_log.state_file` in `run_backfill()`, look up `current_index` per log as the ceiling, skip logs not in the state file, and remove all frontier gap generation from `detect_gaps()`.

**Tech Stack:** Rust, Delta Lake (deltalake 0.25), DataFusion SQL

**Scope:** 4 phases from original design (phase 1 of 4)

**Codebase verified:** 2026-02-18

---

## Acceptance Criteria Coverage

This phase implements:

### backfill-state-ceiling.AC1: Catch-up mode uses state file ceiling
- **backfill-state-ceiling.AC1.1 Success:** Catch-up mode with internal gaps fills only internal gaps (no frontier gap generated)
- **backfill-state-ceiling.AC1.2 Success:** Catch-up mode with contiguous data and ceiling > max_index generates no work items (previously would have generated frontier gap)
- **backfill-state-ceiling.AC1.3 Failure:** Catch-up mode with log not in state file skips that log with warning

### backfill-state-ceiling.AC2: Historical mode uses state file ceiling
- **backfill-state-ceiling.AC2.1 Success:** Historical mode (`--from N`) uses ceiling from state file as upper bound instead of tree_size
- **backfill-state-ceiling.AC2.2 Success:** Historical mode with no delta table uses ceiling from state file (not tree_size)

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Remove frontier gap blocks from detect_gaps()

**Verifies:** backfill-state-ceiling.AC1.1, backfill-state-ceiling.AC1.2

**Files:**
- Modify: `src/backfill.rs:181-182` (doc comment — change `tree_size` to `ceiling`)
- Modify: `src/backfill.rs:229` (loop variable rename)
- Modify: `src/backfill.rs:257-264` (remove catch-up frontier gap block)
- Modify: `src/backfill.rs:301-308` (remove historical frontier gap block)

**Implementation:**

In `detect_gaps()`, apply steps in this exact order (deletions first, then renames):

1. Delete the catch-up mode frontier gap block (lines 257-264):
```rust
// DELETE THIS BLOCK:
                    // Check for frontier gap
                    if state.max_index + 1 < *tree_size {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: state.max_index + 1,
                            end: tree_size - 1,
                        });
                    }
```

2. Delete the historical mode frontier gap block (lines 301-308):
```rust
// DELETE THIS BLOCK:
                    // Frontier gap
                    if state.max_index + 1 < *tree_size {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: state.max_index + 1,
                            end: tree_size - 1,
                        });
                    }
```

3. Update the doc comment at line 181 from `/// * \`logs\` - Vec of (source_url, tree_size) pairs` to `/// * \`logs\` - Vec of (source_url, ceiling) pairs for active logs`.

4. At line 229, rename the loop variable from `tree_size` to `ceiling`:
```rust
for (source_url, ceiling) in logs {
```

5. In the no-table historical mode path (lines 204-211), rename `tree_size` to `ceiling`:
```rust
                    for (source_url, ceiling) in logs {
                        if *ceiling > from {
                            work_items.push(BackfillWorkItem {
                                source_url: source_url.clone(),
                                start: from,
                                end: ceiling - 1,
                            });
                        }
                    }
```

6. In the historical mode "log not in delta" else branch (lines 309-317), rename `tree_size` to `ceiling`:
```rust
                } else {
                    // Log not in delta: backfill entire range
                    if *ceiling > from {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: from,
                            end: ceiling - 1,
                        });
                    }
                }
```

**Verification:**
Run: `cargo build`
Expected: Compiles successfully (tests will fail until Phase 3 updates them)

**Commit:** `refactor: remove frontier gap logic from detect_gaps and rename tree_size to ceiling`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Replace tree_size HTTP fetching with state file ceiling lookup in run_backfill()

**Verifies:** backfill-state-ceiling.AC1.3, backfill-state-ceiling.AC2.1, backfill-state-ceiling.AC2.2

**Files:**
- Modify: `src/backfill.rs:1-16` (add `use crate::state::StateManager;` import)
- Modify: `src/backfill.rs:551-586` (replace Step 2 with state file loading)

**Implementation:**

1. Add import at line 5 (after `use crate::models::Source;`):
```rust
use crate::state::StateManager;
```

2. Replace Step 2 (lines 551-586) — the entire tree size discovery block including the empty check — with state file ceiling lookup:

```rust
    // Step 2: State file ceiling lookup
    let state_manager = StateManager::new(config.ct_log.state_file.clone());
    let mut log_ceilings = Vec::new();

    for log in &logs {
        match state_manager.get_index(&log.normalized_url()) {
            Some(ceiling) => {
                log_ceilings.push((log.normalized_url(), ceiling));
            }
            None => {
                warn!(
                    log = %log.description,
                    url = %log.normalized_url(),
                    "log not in state file, skipping (no ceiling reference)"
                );
            }
        }
    }

    if log_ceilings.is_empty() {
        warn!("no logs have ceiling values from state file");
        return 0;
    }
```

Key differences from the old code:
- Returns exit code `0` (not `1`) when no logs have ceilings — this is not an error, it means the state file doesn't track any of the configured logs. Note: the design describes the exit message as "no gaps detected, nothing to backfill" but this implementation uses a more informative message — intentional deviation.
- No HTTP calls are made
- `request_timeout` variable (line 552) is removed since it was only used for tree size fetching
- Note: the `LogType` import (`use crate::ct::{..., LogType}` at line 3) remains valid — it is used at line 619 in Step 6 for fetcher log type lookup

3. Update the Step 3 gap detection call (line 589) to use `log_ceilings` instead of `log_tree_sizes`:
```rust
    let work_items = match detect_gaps(&config.delta_sink.table_path, &log_ceilings, backfill_from).await {
```

**Verification:**
Run: `cargo build`
Expected: Compiles successfully

**Commit:** `feat: replace HTTP tree_size fetching with state file ceiling lookup in run_backfill`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Verify build succeeds

**Files:** None (verification only)

**Verification:**
Run: `cargo build`
Expected: Compiles with no errors. Warnings about unused imports of `fetch::get_tree_size` or `fetch::get_checkpoint_tree_size` are expected if those functions were only used in the removed code — leave them for now (they're used by the live watcher).

Run: `cargo test -- --list 2>&1 | head -5`
Expected: Tests compile and list (some may fail when run, which is expected until Phase 3)

**Commit:** No commit needed (verification only)

<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->
