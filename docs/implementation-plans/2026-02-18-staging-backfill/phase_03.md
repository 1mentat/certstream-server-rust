# Staging Backfill Implementation Plan - Phase 3: Gap Detection Union

**Goal:** Gap detection queries both main and staging tables when staging path is provided

**Architecture:** `detect_gaps()` gains a `staging_path: Option<&str>` parameter. When the staging table exists, both tables are registered in DataFusion and a `CREATE VIEW ct_records AS SELECT ... UNION ALL SELECT ...` view replaces the single-table registration. Downstream queries (`query_log_states()`, `find_internal_gaps()`) continue to query `"ct_records"` unchanged. When the staging table doesn't exist (first run), falls back to querying main table only. The gap-detection body (log iteration + work item generation) is extracted into a helper to avoid code duplication between the normal path and the staging-only path.

**Tech Stack:** Rust, DataFusion (SessionContext, SQL UNION ALL views), deltalake

**Scope:** 5 phases from original design (this is phase 3 of 5)

**Codebase verified:** 2026-02-18

**Note:** Line references throughout are approximate and should be verified against the actual file at implementation time, as prior phase changes may shift line numbers.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### staging-backfill.AC2: Gap detection unions main and staging
- **staging-backfill.AC2.1 Success:** When staging table exists, gap detection queries both main and staging — entries present in either table are excluded from gap list
- **staging-backfill.AC2.2 Success:** Running backfill with staging a second time produces fewer work items (previously staged entries not re-fetched)
- **staging-backfill.AC2.3 Success:** Per-log MAX(cert_index) from the union is capped at the state file ceiling
- **staging-backfill.AC2.4 Edge:** When staging table doesn't exist yet (first run), gap detection falls back to querying main table only

---

<!-- START_SUBCOMPONENT_A (tasks 1-4) -->
<!-- START_TASK_1 -->
### Task 1: Extract gap-detection body into helper function

**Files:**
- Modify: `src/backfill.rs:228-306` (extract into new helper function)

**Implementation:**

Before adding the staging_path parameter, extract the gap-detection body (the `for (source_url, ceiling) in logs` loop at lines 228-306) into a separate helper function. This avoids duplicating the logic when handling the staging-only path (main table doesn't exist but staging does).

```rust
/// Given a DataFusion context with a "ct_records" view already registered,
/// query log states and find gaps for each log in the provided list.
async fn detect_gaps_from_context(
    ctx: &SessionContext,
    logs: &[(String, u64)],
    backfill_from: Option<u64>,
) -> Result<Vec<BackfillWorkItem>, Box<dyn Error>> {
    let log_states = query_log_states(ctx).await?;
    let mut work_items = Vec::new();

    for (source_url, ceiling) in logs {
        match backfill_from {
            None => {
                // Catch-up mode
                if let Some(state) = log_states.get(source_url) {
                    let lower_bound = state.min_index;
                    let expected_count = state.max_index - state.min_index + 1;
                    if state.count < expected_count {
                        let effective_max = state.max_index.min(*ceiling);
                        match find_internal_gaps(ctx, source_url, lower_bound, effective_max).await {
                            Ok(gaps) => {
                                for (gap_start, gap_end) in gaps {
                                    work_items.push(BackfillWorkItem {
                                        source_url: source_url.clone(),
                                        start: gap_start,
                                        end: gap_end,
                                    });
                                }
                            }
                            Err(e) => {
                                warn!(source_url = %source_url, error = %e, "Failed to detect internal gaps");
                            }
                        }
                    }
                }
                // If log not in delta, skip (AC2.4)
            }
            Some(from) => {
                // Historical mode
                if let Some(state) = log_states.get(source_url) {
                    if from < state.min_index {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: from,
                            end: state.min_index - 1,
                        });
                    }
                    let expected_count = state.max_index - state.min_index + 1;
                    if state.count < expected_count {
                        let effective_max = state.max_index.min(*ceiling);
                        match find_internal_gaps(ctx, source_url, from.max(state.min_index), effective_max).await {
                            Ok(gaps) => {
                                for (gap_start, gap_end) in gaps {
                                    work_items.push(BackfillWorkItem {
                                        source_url: source_url.clone(),
                                        start: gap_start,
                                        end: gap_end,
                                    });
                                }
                            }
                            Err(e) => {
                                warn!(source_url = %source_url, error = %e, "Failed to detect internal gaps");
                            }
                        }
                    }
                } else {
                    if *ceiling > from {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: from,
                            end: ceiling - 1,
                        });
                    }
                }
            }
        }
    }

    Ok(work_items)
}
```

Then update `detect_gaps()` to call this helper:

```rust
// (after registering ct_records view)
let log_states = query_log_states(&ctx).await?;
// Replace lines 228-306 with:
detect_gaps_from_context(&ctx, logs, backfill_from).await
```

Note: This helper also incorporates the ceiling capping from Task 2 (`effective_max = state.max_index.min(*ceiling)`).

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

Run: `cargo test`
Expected: All 249 existing tests pass (behavior unchanged, only refactored)

**Commit:** `refactor(backfill): extract detect_gaps_from_context helper`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add staging_path parameter and UNION ALL view to detect_gaps

**Files:**
- Modify: `src/backfill.rs` (detect_gaps function signature, table registration, early return path)
- Modify: `src/backfill.rs` (detect_gaps call site in run_backfill)
- Modify: `src/backfill.rs` (all existing detect_gaps call sites in tests)

**Implementation:**

1. Add `staging_path: Option<&str>` as the second parameter to `detect_gaps()`:

```rust
pub async fn detect_gaps(
    table_path: &str,
    staging_path: Option<&str>,
    logs: &[(String, u64)],
    backfill_from: Option<u64>,
) -> Result<Vec<BackfillWorkItem>, Box<dyn Error>>
```

2. After opening the main table, register it as `"ct_main"` (rename from direct `"ct_records"` registration) and create a UNION ALL view:

```rust
let ctx = SessionContext::new();
ctx.register_table("ct_main", std::sync::Arc::new(table))?;

// Register UNION ALL view if staging table exists
let has_staging = if let Some(staging) = staging_path {
    match deltalake::open_table(staging).await {
        Ok(staging_table) => {
            ctx.register_table("ct_staging", std::sync::Arc::new(staging_table))?;
            ctx.sql(
                "CREATE VIEW ct_records AS \
                 SELECT cert_index, source_url FROM ct_main \
                 UNION ALL \
                 SELECT cert_index, source_url FROM ct_staging"
            ).await?;
            true
        }
        Err(_) => {
            // Staging table doesn't exist yet (first run) — fall back to main only
            false
        }
    }
} else {
    false
};

if !has_staging {
    ctx.sql(
        "CREATE VIEW ct_records AS SELECT cert_index, source_url FROM ct_main"
    ).await?;
}
```

3. Handle the early return path when the main table doesn't exist but staging does. Using the `detect_gaps_from_context` helper from Task 1:

```rust
Err(_e @ DeltaTableError::NotATable(_)) | Err(_e @ DeltaTableError::InvalidTableLocation(_)) => {
    // Main table doesn't exist — try staging if provided
    if let Some(staging) = staging_path {
        if let Ok(staging_table) = deltalake::open_table(staging).await {
            let ctx = SessionContext::new();
            ctx.register_table("ct_staging", std::sync::Arc::new(staging_table))?;
            ctx.sql(
                "CREATE VIEW ct_records AS SELECT cert_index, source_url FROM ct_staging"
            ).await?;
            return detect_gaps_from_context(&ctx, logs, backfill_from).await;
        }
    }
    // Neither main nor staging exists — original fallback
    return match backfill_from {
        None => Ok(Vec::new()),
        Some(from) => {
            let mut work_items = Vec::new();
            for (source_url, ceiling) in logs {
                if *ceiling > from {
                    work_items.push(BackfillWorkItem {
                        source_url: source_url.clone(),
                        start: from,
                        end: ceiling - 1,
                    });
                }
            }
            Ok(work_items)
        }
    };
}
```

4. Update the production call site in `run_backfill()`:

```rust
let staging_path_ref = staging_path.as_deref();
let work_items = match detect_gaps(
    &config.delta_sink.table_path,
    staging_path_ref,
    &log_ceilings,
    backfill_from,
).await {
```

Then at the writer setup, use `staging_path` (the owned value) for the write target:
```rust
let table_path = staging_path
    .unwrap_or_else(|| config.delta_sink.table_path.clone());
```

This works because `as_deref()` borrows (doesn't move), so the owned `staging_path` remains available for the writer.

5. **Update ALL existing detect_gaps call sites to pass `None` as staging_path.** The following test calls must be updated (pass `None` as the second argument):

All existing `detect_gaps(&table_path, &logs, ...)` calls in the test module must become `detect_gaps(&table_path, None, &logs, ...)`. These are found in approximately 15+ test functions including:
- `test_ac2_1_catch_up_lower_bound_from_min`
- `test_ac2_2_catch_up_internal_gaps`
- `test_ac2_3_catch_up_no_frontier_gap`
- `test_ac2_4_catch_up_missing_log_skipped`
- `test_ac3_1_historical_pre_existing_gap`
- `test_ac3_2_historical_from_override`
- `test_ac3_1_historical_empty_table_full_range`
- `test_ceiling_below_data_no_work_items`
- `test_catch_up_ceiling_only_internal_gaps`
- `test_historical_ceiling_used_as_upper_bound`
- And any other tests calling `detect_gaps`

Each call changes from:
```rust
detect_gaps(&table_path, &logs, backfill_from).await
```
To:
```rust
detect_gaps(&table_path, None, &logs, backfill_from).await
```

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

Run: `cargo test`
Expected: All existing tests pass with the new `None` parameter

**Commit:** `feat(backfill): add UNION ALL view for staging gap detection`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Cap union MAX(cert_index) at state file ceiling

**Verifies:** staging-backfill.AC2.3

**Files:**
- Verify: `src/backfill.rs` (detect_gaps_from_context helper)

**Implementation:**

The ceiling capping was already incorporated into the `detect_gaps_from_context` helper in Task 1. Verify that `effective_max = state.max_index.min(*ceiling)` is applied in both the catch-up and historical branches of the helper.

This ensures that even if the union returns a MAX(cert_index) higher than the ceiling, gap detection won't generate work items beyond the ceiling.

No additional code changes needed — this is covered by Task 1's helper extraction.

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** (included in Task 1 commit)
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Tests for gap detection union behavior

**Verifies:** staging-backfill.AC2.1, staging-backfill.AC2.2, staging-backfill.AC2.3, staging-backfill.AC2.4

**Files:**
- Modify: `src/backfill.rs` (test module)

**Implementation:**

Add tests following the existing pattern (`make_test_record`, temp directories, `open_or_create_table`, `records_to_batch`, `DeltaOps::write`):

**Test 1: UNION ALL excludes entries in either table (AC2.1)**
- Create main table with records at indices [10, 11, 15, 16]
- Create staging table with records at indices [12, 13]
- Call `detect_gaps(main_path, Some(staging_path), logs_with_ceiling_20, None)`
- Assert: only gap [14, 14] detected (index 14 is missing from both tables)

**Test 2: Second staging run produces fewer work items (AC2.2)**
- Create main table with records at indices [10, 15]
- First call with empty staging: should detect gap [11, 14]
- Create staging with records at [11, 12]
- Second call with staging: should detect smaller gap [13, 14]

**Test 3: Ceiling caps MAX(cert_index) (AC2.3)**
- Create main table with records at indices [10, 11, 12]
- Create staging table with records at indices [13, 14, 25] (25 is beyond ceiling)
- Call with ceiling=20: verify no work items generated beyond index 19
- Verify that index 25 in staging doesn't cause gap detection to generate work items up to index 24

**Test 4: Missing staging falls back to main only (AC2.4)**
- Create main table with records at [10, 11, 15]
- Call `detect_gaps(main_path, Some("/tmp/nonexistent_staging"), logs, None)`
- Assert: same results as calling without staging (gap [12, 14])

Follow project testing patterns — real Delta tables in `/tmp/`, `DeltaOps::write` for setup, manual cleanup.

**Testing:**
Tests must verify each AC listed above:
- staging-backfill.AC2.1: Assert entries in either table excluded from gaps
- staging-backfill.AC2.2: Assert second run with staging detects fewer gaps
- staging-backfill.AC2.3: Assert ceiling caps prevent inflation
- staging-backfill.AC2.4: Assert nonexistent staging falls back to main-only behavior

**Verification:**
Run: `cargo test staging`
Expected: All new staging tests pass

Run: `cargo test`
Expected: All tests pass

**Commit:** `test(backfill): add gap detection union tests for AC2.1-AC2.4`
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_A -->
