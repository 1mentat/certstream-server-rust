# Backfill State Ceiling Implementation Plan - Phase 3

**Goal:** Update existing tests to match new behavior (no frontier gaps) and add new tests for state file ceiling logic.

**Architecture:** Modify existing `detect_gaps()` tests in `src/backfill.rs` `mod tests` to remove frontier gap assertions, then add new tests that verify ceiling behavior, state file missing scenarios, and ceiling-below-data edge cases.

**Tech Stack:** Rust, Delta Lake (deltalake 0.25), DataFusion SQL, tokio::test

**Scope:** 4 phases from original design (phase 3 of 4)

**Codebase verified:** 2026-02-18

---

## Acceptance Criteria Coverage

This phase implements and tests:

### backfill-state-ceiling.AC1: Catch-up mode uses state file ceiling
- **backfill-state-ceiling.AC1.4 Edge:** Catch-up mode with no state file (missing file) generates no work items for any log

### backfill-state-ceiling.AC2: Historical mode uses state file ceiling
- **backfill-state-ceiling.AC2.3 Failure:** Historical mode with log not in state file skips that log with warning

### backfill-state-ceiling.AC3: Tree size fetching removed
- **backfill-state-ceiling.AC3.1 Success:** `run_backfill()` does not make HTTP calls to fetch tree sizes
- **backfill-state-ceiling.AC3.2 Success:** State file is loaded via `StateManager` using `config.ct_log.state_file` path
- **backfill-state-ceiling.AC3.3 Edge:** State file with `current_index` < delta `min_index` produces no work items for that log (ceiling is below existing data)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->

<!-- START_TASK_1 -->
### Task 1: Update existing tests that assert frontier gap behavior

**Verifies:** backfill-state-ceiling.AC1.1, backfill-state-ceiling.AC1.2

**Files:**
- Modify: `src/backfill.rs:957-996` (test_ac2_1_catch_up_lower_bound_from_min)
- Modify: `src/backfill.rs:998-1055` (test_ac2_2_catch_up_internal_gaps)
- Modify: `src/backfill.rs:1057-1095` (test_ac2_3_catch_up_frontier_gap)
- Modify: `src/backfill.rs:1097-1146` (test_ac2_4_catch_up_missing_log_skipped)
- Modify: `src/backfill.rs:1148-1190` (test_ac3_1_historical_pre_existing_gap)
- Modify: `src/backfill.rs:1192-1231` (test_ac3_2_historical_from_override)

**Implementation:**

Update 6 existing tests to reflect that frontier gaps are no longer generated:

**1. `test_ac2_1_catch_up_lower_bound_from_min` (lines 957-996):**
Currently expects 1 frontier gap work item `(103, 199)` for contiguous records `[100, 101, 102]` with tree_size=200. With ceiling replacing tree_size and no frontier gap logic, contiguous records with ceiling=200 should produce **0 work items** (no internal gaps, no frontier gap). Update the second tuple element comment from "tree_size" to "ceiling" and change assertions:

```rust
        // Call detect_gaps in catch-up mode (no --from)
        // Second tuple element is ceiling (was tree_size)
        let logs = vec![("https://log.example.com".to_string(), 200)];
        let work_items = detect_gaps(&table_path, &logs, None)
            .await
            .expect("detect_gaps failed");

        // With ceiling replacing tree_size and no frontier gap logic,
        // contiguous data produces no work items
        assert_eq!(work_items.len(), 0, "Contiguous data with ceiling > max_index should produce no work items");
```

**2. `test_ac2_2_catch_up_internal_gaps` (lines 998-1055):**
Currently expects 3 work items: `(13, 14)`, `(17, 19)`, and frontier `(21, 21)`. Remove the frontier gap assertion, expect only 2 internal gaps:

```rust
        // Call detect_gaps with ceiling=22 (no frontier gap generated)
        let logs = vec![("https://log.example.com".to_string(), 22)];
        let work_items = detect_gaps(&table_path, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should detect only internal gaps: (13, 14), (17, 19) — no frontier gap
        let gaps: Vec<_> = work_items
            .iter()
            .filter(|item| item.source_url == "https://log.example.com")
            .collect();

        assert_eq!(gaps.len(), 2, "Should detect internal gaps (13, 14) and (17, 19) only");

        let has_gap_13_14 = gaps.iter().any(|item| item.start == 13 && item.end == 14);
        assert!(has_gap_13_14, "Should detect gap (13, 14)");

        let has_gap_17_19 = gaps.iter().any(|item| item.start == 17 && item.end == 19);
        assert!(has_gap_17_19, "Should detect gap (17, 19)");
```

**3. `test_ac2_3_catch_up_frontier_gap` (lines 1057-1095):**
This test now tests the opposite: contiguous records `[0, 1, 2]` with ceiling=10 should produce **0 work items** (no frontier gap). Rename the test to reflect new behavior:

```rust
    #[tokio::test]
    async fn test_ac2_3_catch_up_no_frontier_gap() {
        let test_name = "ac2_3_no_frontier_gap";
        // ... same setup: create records [0, 1, 2] ...

        // Call detect_gaps with ceiling=10
        let logs = vec![("https://log.example.com".to_string(), 10)];
        let work_items = detect_gaps(&table_path, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should have NO work items (no frontier gap generated)
        assert_eq!(work_items.len(), 0, "Contiguous data should produce no work items (no frontier gap)");

        let _ = fs::remove_dir_all(&table_path);
    }
```

**4. `test_ac2_4_catch_up_missing_log_skipped` (lines 1097-1146):**
Currently asserts `!log_a_items.is_empty()` — log-a has records [0, 1] with tree_size=10 which produced a frontier gap (2, 9). After removing frontier gaps, contiguous records [0, 1] with ceiling=10 produce **0 work items**. Update the log-a assertion:

```rust
        // log-a has contiguous data [0, 1] — no internal gaps, no frontier gap
        let log_a_items: Vec<_> = work_items
            .iter()
            .filter(|item| item.source_url == "https://log-a.example.com")
            .collect();
        assert_eq!(log_a_items.len(), 0, "log-a with contiguous data should have no work items (no frontier gap)");
```

The log-b assertion (should be skipped) remains unchanged.

**5. `test_ac3_1_historical_pre_existing_gap` (lines 1149-1190):**
Currently expects pre-existing gap `(0, 49)` AND frontier gap `(53, 54)`. Remove frontier gap assertion:

```rust
        // Call detect_gaps with --from 0, ceiling=55
        let logs = vec![("https://log.example.com".to_string(), 55)];
        let work_items = detect_gaps(&table_path, &logs, Some(0))
            .await
            .expect("detect_gaps failed");

        // Should have only pre-existing gap (0, 49) — no frontier gap
        assert_eq!(work_items.len(), 1, "Should have only pre-existing gap");

        let has_pre_existing = work_items.iter().any(|item| item.start == 0 && item.end == 49);
        assert!(has_pre_existing, "Should have pre-existing gap (0, 49)");
```

**6. `test_ac3_2_historical_from_override` (lines 1193-1231):**
Currently expects gap `(40, 49)` and frontier gap `(53, 54)`. Remove frontier gap assertion:

```rust
        // Call detect_gaps with --from 40, ceiling=55
        let logs = vec![("https://log.example.com".to_string(), 55)];
        let work_items = detect_gaps(&table_path, &logs, Some(40))
            .await
            .expect("detect_gaps failed");

        // Should have only the pre-existing gap (40, 49) — no frontier gap
        assert_eq!(work_items.len(), 1, "Should have only pre-existing gap from overridden lower bound");

        let has_pre_gap = work_items.iter().any(|item| item.start == 40 && item.end == 49);
        assert!(has_pre_gap, "Should have gap (40, 49) from overridden lower bound");
```

**Verification:**
Run: `cargo test backfill`
Expected: All 6 updated tests pass. Other backfill tests unchanged.

**Commit:** `test: update existing tests to remove frontier gap assertions`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add new tests for state file ceiling behavior

**Verifies:** backfill-state-ceiling.AC1.4, backfill-state-ceiling.AC2.3, backfill-state-ceiling.AC3.1, backfill-state-ceiling.AC3.2, backfill-state-ceiling.AC3.3

**Files:**
- Modify: `src/backfill.rs` (add new tests at end of `mod tests` block, before closing `}`)

**Implementation:**

Add these new test functions at the end of the `mod tests` block in `src/backfill.rs`:

**Test 1: AC3.3 — Ceiling below existing data produces no work items (catch-up mode)**

Note on AC3.3: After removing frontier gap logic, the ceiling value is not consulted in catch-up mode at all — only internal gaps are detected. AC3.3 (ceiling < min_index) is inherently satisfied because the only place ceiling was used (frontier gap) has been removed. This test verifies the behavior holds: contiguous data with a low ceiling produces zero work items.

```rust
    #[tokio::test]
    async fn test_ceiling_below_data_no_work_items() {
        // backfill-state-ceiling.AC3.3: ceiling < min_index produces no work items.
        // After removing frontier gaps, ceiling is not consulted in catch-up mode.
        // This test confirms: contiguous data + any ceiling value = 0 work items.
        let test_name = "ceiling_below_data";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Create records [100, 101, 102]
        let records = vec![
            make_test_record(100, "https://log.example.com"),
            make_test_record(101, "https://log.example.com"),
            make_test_record(102, "https://log.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // ceiling=50 is below min_index=100; irrelevant in catch-up mode
        // but verifies AC3.3 is satisfied
        let logs = vec![("https://log.example.com".to_string(), 50)];
        let work_items = detect_gaps(&table_path, &logs, None)
            .await
            .expect("detect_gaps failed");

        // No work items: no internal gaps in contiguous data, ceiling not consulted
        assert_eq!(work_items.len(), 0, "Contiguous data with any ceiling should produce no work items in catch-up mode");

        let _ = fs::remove_dir_all(&table_path);
    }
```

**Test 2: AC1.1 — Catch-up with internal gaps fills only internal gaps**
```rust
    #[tokio::test]
    async fn test_catch_up_ceiling_only_internal_gaps() {
        // backfill-state-ceiling.AC1.1: catch-up fills only internal gaps, no frontier
        let test_name = "ceiling_only_internal";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Records with gap: [10, 11, 15, 16] — missing 12, 13, 14
        let records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(15, "https://log.example.com"),
            make_test_record(16, "https://log.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // ceiling=1000 (well above max_index=16)
        let logs = vec![("https://log.example.com".to_string(), 1000)];
        let work_items = detect_gaps(&table_path, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should have exactly 1 internal gap (12, 14) — no frontier gap despite ceiling=1000
        assert_eq!(work_items.len(), 1, "Should have only internal gap, no frontier");
        assert_eq!(work_items[0].start, 12);
        assert_eq!(work_items[0].end, 14);

        let _ = fs::remove_dir_all(&table_path);
    }
```

**Test 3: AC2.1 — Historical mode uses ceiling as upper bound**
```rust
    #[tokio::test]
    async fn test_historical_ceiling_used_as_upper_bound() {
        // backfill-state-ceiling.AC2.1: historical mode uses ceiling from state file
        let test_name = "historical_ceiling_upper";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);

        // No table — historical mode with no delta table
        let logs = vec![("https://log.example.com".to_string(), 100)];
        let work_items = detect_gaps(&table_path, &logs, Some(0))
            .await
            .expect("detect_gaps failed");

        // Should use ceiling=100 as upper bound: work item (0, 99)
        assert_eq!(work_items.len(), 1);
        assert_eq!(work_items[0].start, 0);
        assert_eq!(work_items[0].end, 99);

        let _ = fs::remove_dir_all(&table_path);
    }
```

**Testing notes:**
- AC1.4 (no state file = no work items) and AC2.3 (log not in state file skipped) test the `run_backfill()` function which requires a full config and HTTP client. These are verified by the code structure: `StateManager::new(None)` returns `None` for all `get_index()` calls, so all logs are skipped, `log_ceilings` is empty, and the function returns 0. The existing `test_new_without_file` and `test_new_with_nonexistent_file` tests in `src/state.rs` already verify this StateManager behavior.
- AC3.1 (no HTTP calls) is verified structurally: the `run_backfill()` code no longer contains `get_tree_size` or `get_checkpoint_tree_size` calls.
- AC3.2 (state file loaded via StateManager) is verified structurally by the Phase 1 code change.

**Verification:**
Run: `cargo test backfill`
Expected: All existing backfill tests pass, plus the 3 new tests

Run: `cargo test`
Expected: Full test suite passes with 3 additional tests compared to baseline

**Commit:** `test: add ceiling behavior tests for backfill state ceiling`

<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->
