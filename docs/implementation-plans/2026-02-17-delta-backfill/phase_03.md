# Delta Backfill Implementation Plan - Phase 3: Delta Table Gap Detection

**Goal:** Query the delta table to determine what cert_index ranges need backfilling, per source_url.

**Architecture:** Add gap detection functions to `src/backfill.rs` that use DataFusion SQL to query the existing delta table. A `SessionContext` registers the opened DeltaTable and runs aggregate + window queries to identify missing ranges. The gap detection orchestrator combines delta table state with live tree_size from CT log servers to produce work items for each log.

**Tech Stack:** Rust, deltalake 0.25 (datafusion feature), DataFusion SessionContext, Arrow arrays for result extraction

**Scope:** 5 phases from original design (phase 3 of 5)

**Codebase verified:** 2026-02-17

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-backfill.AC2: Catch-up mode (default)
- **delta-backfill.AC2.1 Success:** With no `--from` flag, lower bound per log is `MIN(cert_index)` from delta table for that source
- **delta-backfill.AC2.2 Success:** Internal gaps (dropped records) between MIN and MAX are detected and backfilled
- **delta-backfill.AC2.3 Success:** Frontier gap (MAX to current `tree_size`) is detected and backfilled
- **delta-backfill.AC2.4 Edge:** Log present in config but not in delta table is skipped in catch-up mode

### delta-backfill.AC3: Historical mode (`--from`)
- **delta-backfill.AC3.1 Success:** `--from 0` backfills from index 0 to current `tree_size`, including ranges before the existing MIN in delta
- **delta-backfill.AC3.2 Success:** `--from <N>` overrides the lower bound for all logs, gaps detected from N forward

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->

<!-- START_TASK_1 -->
### Task 1: Add gap detection functions to backfill module

**Verifies:** delta-backfill.AC2.1, delta-backfill.AC2.2, delta-backfill.AC2.3, delta-backfill.AC2.4, delta-backfill.AC3.1, delta-backfill.AC3.2

**Files:**
- Modify: `src/backfill.rs`

**Implementation:**

Add the following types and functions to `src/backfill.rs`:

1. A `BackfillWorkItem` struct representing a contiguous range to fetch:
   ```rust
   pub struct BackfillWorkItem {
       pub source_url: String,
       pub start: u64,
       pub end: u64, // inclusive
   }
   ```

2. A `DeltaLogState` struct for per-source delta table state (named `DeltaLogState` to avoid collision with `LogState` in `src/ct/log_list.rs`):
   ```rust
   struct DeltaLogState {
       min_index: u64,
       max_index: u64,
       count: u64,
   }
   ```

3. `query_log_states()` function:
   ```rust
   async fn query_log_states(ctx: &SessionContext) -> Result<HashMap<String, DeltaLogState>, Box<dyn Error>>
   ```
   - Uses the already-registered table in the provided `SessionContext`
   - Runs SQL: `SELECT source_url, MIN(cert_index) as min_idx, MAX(cert_index) as max_idx, COUNT(cert_index) as cnt FROM ct_records GROUP BY source_url`
   - Collects results, iterates RecordBatches, extracts values using Arrow array downcasting:
     - `batch.column(0).as_any().downcast_ref::<StringArray>()` for source_url
     - `batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap().value(i) as u64` for min_idx (Delta stores cert_index as Long/Int64; values are guaranteed non-negative CT log sequence numbers)
     - Same pattern for max_idx and count columns — downcast to `Int64Array`, cast values to `u64`
   - Returns `HashMap<String, DeltaLogState>`

4. `find_internal_gaps()` function:
   ```rust
   async fn find_internal_gaps(
       ctx: &SessionContext,
       source_url: &str,
       lower_bound: u64,
       upper_bound: u64,
   ) -> Result<Vec<(u64, u64)>, Box<dyn Error>>
   ```
   - Uses the already-registered table in the provided `SessionContext` (avoids re-opening the table)
   - Only called when `count < max - min + 1` (indicating gaps exist)
   - Builds the query using DataFusion's expression API to avoid SQL injection from `source_url` values (URLs may contain single quotes or other SQL-special characters):
     ```rust
     let filtered = ctx.table("ct_records").await?
         .filter(col("source_url").eq(lit(source_url)))?
         .filter(col("cert_index").gt_eq(lit(lower_bound)))?
         .filter(col("cert_index").lt_eq(lit(upper_bound)))?;
     ```
   - Then uses SQL over a CTE or applies a window function via the DataFrame API to detect gaps using LEAD:
     ```rust
     // Register filtered as temp table, then run LEAD query
     ctx.register_table("filtered_entries", filtered.into_view())?;
     let gaps = ctx.sql("SELECT cert_index + 1 as gap_start, next_index - 1 as gap_end
         FROM (SELECT cert_index, LEAD(cert_index) OVER (ORDER BY cert_index) as next_index
               FROM filtered_entries) sub
         WHERE next_index - cert_index > 1").await?.collect().await?;
     // IMPORTANT: Deregister the temp table after use to avoid name collision on repeated calls
     ctx.deregister_table("filtered_entries")?;
     ```
   - The `deregister_table()` call is required because `detect_gaps()` calls `find_internal_gaps()` once per source_url; without cleanup the second call would fail with a duplicate table name error
   - This approach uses parameterized filtering (via `lit()`) for the source_url, avoiding SQL injection
   - Returns Vec of (start, end) tuples for each gap

5. `detect_gaps()` orchestrator function (opens table ONCE and shares SessionContext):
   ```rust
   pub async fn detect_gaps(
       table_path: &str,
       logs: &[(String, u64)], // (source_url, tree_size) pairs
       backfill_from: Option<u64>,
   ) -> Result<Vec<BackfillWorkItem>, Box<dyn Error>>
   ```
   - Opens the delta table ONCE: `deltalake::open_table(table_path).await` (or returns empty HashMap if table doesn't exist)
   - Creates a single `SessionContext` and registers the table: `ctx.register_table("ct_records", Arc::new(table))?`
   - Passes `&ctx` to both `query_log_states()` and `find_internal_gaps()` to avoid redundant table opens
   - Calls `query_log_states(&ctx)` to get current delta state
   - For each log in `logs`:
     - **Catch-up mode** (`backfill_from` is None):
       - If log not in delta → skip (AC2.4)
       - Lower bound = delta's `min_index` (AC2.1)
       - Check for internal gaps (AC2.2): if `count < max - min + 1`, call `find_internal_gaps()`
       - Frontier gap (AC2.3): if `max_index + 1 < tree_size`, add work item for `(max_index + 1, tree_size - 1)`
     - **Historical mode** (`backfill_from` is Some(from)):
       - Lower bound = `from` (AC3.2)
       - If log not in delta → create work item for entire range `(from, tree_size - 1)` (AC3.1)
       - If log in delta:
         - Pre-existing gap: if `from < min_index`, add work item `(from, min_index - 1)` (AC3.1)
         - Internal gaps: call `find_internal_gaps()` with lower_bound=from
         - Frontier gap: if `max_index + 1 < tree_size`, add work item `(max_index + 1, tree_size - 1)`
   - Returns all work items

**Verification:**

Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat(backfill): add delta table gap detection functions`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add tests for gap detection

**Verifies:** delta-backfill.AC2.1, delta-backfill.AC2.2, delta-backfill.AC2.3, delta-backfill.AC2.4, delta-backfill.AC3.1, delta-backfill.AC3.2

**Files:**
- Modify: `src/backfill.rs` (add `#[cfg(test)]` module)

**Testing:**

Tests must verify each AC case using real delta tables (following project testing patterns from `delta_sink.rs` tests: `/tmp/` directories, real DeltaTable operations, manual cleanup):

- **delta-backfill.AC2.1:** Create delta table with records for source_url="https://log.example.com" with cert_index values [100, 101, 102]. Call `detect_gaps()` with no `--from`. Verify lower bound is 100 (MIN), not 0.

- **delta-backfill.AC2.2:** Create delta table with cert_index values [10, 11, 12, 15, 16, 20] for one source. Call `detect_gaps()` with tree_size=21. Verify work items include gaps (13, 14) and (17, 19).

- **delta-backfill.AC2.3:** Create delta table with cert_index values [0, 1, 2] for a source. Call `detect_gaps()` with tree_size=10. Verify work item includes frontier gap (3, 9).

- **delta-backfill.AC2.4:** Create delta table with records for "https://log-a.example.com" only. Call `detect_gaps()` in catch-up mode with logs including both "log-a" and "log-b". Verify "log-b" produces no work items.

- **delta-backfill.AC3.1:** Create delta table with cert_index values [50, 51, 52] for a source. Call `detect_gaps()` with `--from 0`, tree_size=55. Verify work items include (0, 49) pre-existing range and (53, 54) frontier.

- **delta-backfill.AC3.2:** Similar to AC3.1 but with `--from 40`. Verify lower bound is 40, not 50.

- **Empty table:** Call `detect_gaps()` on non-existent table path. In catch-up mode, verify empty work items. In `--from 0` mode, verify work items cover full range.

Each test should:
1. Create test delta table using existing `open_or_create_table()`, `records_to_batch()`, and DeltaOps write
2. Write test DeltaCertRecords with specific cert_index values
3. Call gap detection functions
4. Assert correct work items
5. Clean up temp directory

**Verification:**

Run: `cargo test`
Expected: All tests pass

**Commit:** `test(backfill): add gap detection tests`
<!-- END_TASK_2 -->

<!-- END_SUBCOMPONENT_A -->
