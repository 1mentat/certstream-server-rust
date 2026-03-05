# Named Targets Implementation Plan — Phase 5: Refactor Operation Functions

**Goal:** Change operation function signatures to accept `ResolvedTarget` instead of `Config` + raw path strings. Remove internal `parse_table_uri()` / `resolve_storage_options()` calls from operation functions (resolution moves to dispatch layer).

**Architecture:** Each operation function receives pre-resolved `ResolvedTarget` values containing `table_path`, `storage_options`, `compression_level`, `heavy_column_compression_level`, and `offline_batch_size`. Internal URI parsing and storage resolution are removed. The dispatch layer in `main.rs` is updated to pass `ResolvedTarget` values directly.

**Tech Stack:** Rust

**Scope:** 6 phases from original design (this is phase 5 of 6)

**Codebase verified:** 2026-03-05

---

## Acceptance Criteria Coverage

This phase implements and tests:

### named-targets.AC6: Operation function signatures
- **named-targets.AC6.1 Success:** `run_migrate` accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` and writes to target table_path
- **named-targets.AC6.2 Success:** `run_extract_metadata` accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` and writes to target table_path
- **named-targets.AC6.3 Success:** `run_merge` accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` and merges source into target
- **named-targets.AC6.4 Success:** `run_backfill` accepts `(config, target: ResolvedTarget, ...)` and writes to target table_path
- **named-targets.AC6.5 Success:** `run_reparse_audit` accepts `(source: ResolvedTarget, ...)` and reads from source table_path
- **named-targets.AC6.6 Success:** No operation function references `config.delta_sink` for table paths or write settings

---

## Codebase Verification Findings

**run_migrate** at `src/backfill.rs:1281-1288`:
- ✓ Current signature: `(config: Config, output_path: String, source_path: String, from_date, to_date, shutdown)`
- ✓ `config.delta_sink.*` refs: compression_level (line 1403), heavy_column_compression_level (line 1404), offline_batch_size (line 1412)
- ✓ `parse_table_uri` calls at lines 1298, 1305; `resolve_storage_options` calls at lines 1299, 1306
- ✓ `config.storage` used for both source and output resolution

**run_extract_metadata** at `src/table_ops.rs:660-666`:
- ✓ Current signature: `(config: Config, output_path: String, from_date, to_date, shutdown)`
- ✓ `config.delta_sink.*` refs: table_path (lines 674, 682, 699, 710), compression_level (line 799), offline_batch_size (line 809)
- ✓ `parse_table_uri` calls at lines 682, 689; `resolve_storage_options` calls at lines 683, 690
- ✓ Manually builds WriterProperties (does not use `delta_writer_properties()` helper)

**run_merge** at `src/backfill.rs:1070-1074`:
- ✓ Current signature: `(config: Config, staging_path: String, shutdown)`
- ✓ `config.delta_sink.*` refs: table_path (lines 1080, 1083, 1116), compression_level (line 1156), heavy_column_compression_level (line 1157)
- ✓ `parse_table_uri` calls at lines 1080, 1088; `resolve_storage_options` calls at lines 1081, 1089

**run_backfill** at `src/backfill.rs:711-718`:
- ✓ Current signature: `(config: Config, staging_path: Option<String>, backfill_from, backfill_logs, backfill_sink, shutdown)`
- ✓ `config.delta_sink.*` refs: table_path (line 734), batch_size (lines 847, 910), flush_interval_secs (line 911), compression_level (line 912), heavy_column_compression_level (line 913)
- ✓ `parse_table_uri` calls at lines 734, 744; `resolve_storage_options` calls at lines 735, 745
- ✓ `config` also needed for: `config.ct_log.batch_size`, `config.ct_log.state_file`, CT log settings, zerobus config

**run_reparse_audit** at `src/table_ops.rs:209-214`:
- ✓ Current signature: `(config: Config, from_date, to_date, shutdown)`
- ✓ `config.delta_sink.*` refs: table_path (lines 216, 223, 233, 241)
- ✓ `parse_table_uri` call at line 223; `resolve_storage_options` call at line 224
- ✓ Read-only operation, no compression/write settings used

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Refactor run_migrate to accept ResolvedTarget

**Verifies:** named-targets.AC6.1, named-targets.AC6.6

**Files:**
- Modify: `src/backfill.rs` (`run_migrate` function, starting at line 1281)
- Modify: `src/main.rs` (migrate dispatch, update call site)

**Implementation:**

Change `run_migrate` signature from:
```rust
pub async fn run_migrate(
    config: Config,
    output_path: String,
    source_path: String,
    from_date: Option<String>,
    to_date: Option<String>,
    shutdown: CancellationToken,
) -> i32
```
to:
```rust
pub async fn run_migrate(
    source: ResolvedTarget,
    target: ResolvedTarget,
    from_date: Option<String>,
    to_date: Option<String>,
    shutdown: CancellationToken,
) -> i32
```

Import `ResolvedTarget` from `crate::config::ResolvedTarget` in `src/backfill.rs`.

Inside the function body:
- Remove `parse_table_uri(&source_path)` and `resolve_storage_options()` calls (lines 1298-1311). Use `source.table_path`, `source.storage_options` directly.
- Remove `parse_table_uri(&output_path)` and `resolve_storage_options()` calls. Use `target.table_path`, `target.storage_options` directly.
- Replace `config.delta_sink.compression_level` (line 1403) with `target.compression_level`
- Replace `config.delta_sink.heavy_column_compression_level` (line 1404) with `target.heavy_column_compression_level`
- Replace `config.delta_sink.offline_batch_size` (line 1412) with `target.offline_batch_size`
- Replace `open_or_create_table(&output_path, ...)` (line 1332) with `open_or_create_table(&target.table_path, &schema, target.storage_options.clone())`
- Replace source table opens with `source.table_path` and `source.storage_options`

Update main.rs migrate dispatch to call:
```rust
let exit_code = backfill::run_migrate(
    resolved_source,
    resolved_target,
    cli_args.backfill_from,
    cli_args.to,
    shutdown_token,
)
.await;
```

Remove the transitional `config` parameter — `run_migrate` no longer needs `Config`.

**Verification:**

Run: `cargo build`
Expected: Builds without errors

Run: `cargo test backfill::tests::test_migrate`
Expected: Migrate tests pass (test helper functions may need updating to construct ResolvedTarget)

**Commit:** `refactor: run_migrate accepts ResolvedTarget instead of Config + paths`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Refactor run_extract_metadata to accept ResolvedTarget

**Verifies:** named-targets.AC6.2, named-targets.AC6.6

**Files:**
- Modify: `src/table_ops.rs` (`run_extract_metadata` function, starting at line 660)
- Modify: `src/main.rs` (extract-metadata dispatch, update call site)

**Implementation:**

Change `run_extract_metadata` signature from:
```rust
pub async fn run_extract_metadata(
    config: Config,
    output_path: String,
    from_date: Option<String>,
    to_date: Option<String>,
    shutdown: CancellationToken,
) -> i32
```
to:
```rust
pub async fn run_extract_metadata(
    source: ResolvedTarget,
    target: ResolvedTarget,
    from_date: Option<String>,
    to_date: Option<String>,
    shutdown: CancellationToken,
) -> i32
```

Import `ResolvedTarget` from `crate::config::ResolvedTarget` in `src/table_ops.rs`.

Inside the function body:
- Replace `config.delta_sink.table_path` references (lines 674, 682, 699, 710) with `source.table_path`
- Remove `parse_table_uri` and `resolve_storage_options` calls (lines 682-695). Use `source.storage_options` and `target.storage_options` directly.
- Replace `config.delta_sink.compression_level` (line 799) with `target.compression_level`
- Replace `config.delta_sink.offline_batch_size` (line 809) with `target.offline_batch_size`
- Replace `open_or_create_table(&output_path, ...)` (line 786) with `open_or_create_table(&target.table_path, &metadata_schema(), target.storage_options.clone())`
- Replace source table open with `source.table_path` and `source.storage_options`

Update main.rs extract-metadata dispatch to call:
```rust
let exit_code = table_ops::run_extract_metadata(
    resolved_source,
    resolved_target,
    cli_args.from_date,
    cli_args.to_date,
    shutdown_token,
)
.await;
```

Remove the transitional config clone and `delta_sink.table_path` override from Phase 4.

**Verification:**

Run: `cargo build`
Expected: Builds without errors

Run: `cargo test table_ops::tests::test_ac2`
Expected: Extract-metadata tests pass (test helpers may need updating)

**Commit:** `refactor: run_extract_metadata accepts ResolvedTarget instead of Config + paths`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Update migrate and extract-metadata tests

**Verifies:** named-targets.AC6.1, named-targets.AC6.2

**Files:**
- Modify: `src/backfill.rs` (update migrate test helpers and tests)
- Modify: `src/table_ops.rs` (update extract-metadata test helpers and tests)

**Implementation:**

Tests that call `run_migrate` and `run_extract_metadata` need to construct `ResolvedTarget` values instead of passing raw path strings and Config.

Create a test helper in each module (or reuse if shared):
```rust
fn make_test_resolved_target(table_path: &str) -> ResolvedTarget {
    ResolvedTarget {
        table_path: table_path.to_string(),
        storage_options: HashMap::new(),
        compression_level: 9,
        heavy_column_compression_level: 15,
        offline_batch_size: 100000,
    }
}
```

Update each test that calls `run_migrate` or `run_extract_metadata` to use `ResolvedTarget` instead of Config + path strings. For example:
- `test_migrate_basic_schema_and_data` — construct source and target ResolvedTarget with temp dirs
- `test_migrate_with_date_filters` — same pattern
- `test_ac2_1_extract_metadata_output_schema_has_19_columns_no_as_der` — construct source and target ResolvedTarget

**Testing:**

Tests must verify:
- named-targets.AC6.1: Migrate tests pass with ResolvedTarget parameters, writes to target.table_path
- named-targets.AC6.2: Extract-metadata tests pass with ResolvedTarget parameters, writes to target.table_path

**Verification:**

Run: `cargo test backfill::tests::test_migrate`
Expected: All migrate tests pass

Run: `cargo test table_ops::tests::test_ac2`
Expected: All extract-metadata tests pass

**Commit:** `test: update migrate and extract-metadata tests for ResolvedTarget`

<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 4-6) -->

<!-- START_TASK_4 -->
### Task 4: Refactor run_merge to accept ResolvedTarget

**Verifies:** named-targets.AC6.3, named-targets.AC6.6

**Files:**
- Modify: `src/backfill.rs` (`run_merge` function, starting at line 1070)
- Modify: `src/main.rs` (merge dispatch, update call site)

**Implementation:**

Change `run_merge` signature from:
```rust
pub async fn run_merge(
    config: Config,
    staging_path: String,
    shutdown: CancellationToken,
) -> i32
```
to:
```rust
pub async fn run_merge(
    source: ResolvedTarget,
    target: ResolvedTarget,
    shutdown: CancellationToken,
) -> i32
```

Inside the function body:
- The `source` is the staging table (what was `staging_path`). The `target` is the main table (what was `config.delta_sink.table_path`).
- Remove `parse_table_uri` and `resolve_storage_options` calls (lines 1080-1094). Use `target.table_path`, `target.storage_options` for the main table; `source.table_path`, `source.storage_options` for the staging table.
- Replace `config.delta_sink.table_path` in `open_or_create_table()` (line 1116) with `target.table_path` and `target.storage_options`
- Replace `config.delta_sink.compression_level` (line 1156) with `target.compression_level`
- Replace `config.delta_sink.heavy_column_compression_level` (line 1157) with `target.heavy_column_compression_level`
- Replace staging table opens with `source.table_path` and `source.storage_options`
- Staging cleanup (line 1263): use `source.table_path` for the directory to delete

Update main.rs merge dispatch to call:
```rust
let exit_code = backfill::run_merge(
    resolved_source,
    resolved_target,
    shutdown_token,
)
.await;
```

Remove the transitional config clone from Phase 4.

**Verification:**

Run: `cargo build`
Expected: Builds without errors

Run: `cargo test backfill::tests::test_ac3`
Expected: Merge tests pass

**Commit:** `refactor: run_merge accepts ResolvedTarget instead of Config + staging path`

<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Refactor run_backfill and run_reparse_audit to accept ResolvedTarget

**Verifies:** named-targets.AC6.4, named-targets.AC6.5, named-targets.AC6.6

**Files:**
- Modify: `src/backfill.rs` (`run_backfill` function, starting at line 711)
- Modify: `src/table_ops.rs` (`run_reparse_audit` function, starting at line 209)
- Modify: `src/main.rs` (backfill and reparse-audit dispatch, update call sites)

**Implementation:**

**run_backfill** — change signature from:
```rust
pub async fn run_backfill(
    config: Config,
    staging_path: Option<String>,
    backfill_from: Option<u64>,
    backfill_logs: Option<String>,
    backfill_sink: Option<String>,
    shutdown: CancellationToken,
) -> i32
```
to:
```rust
pub async fn run_backfill(
    config: Config,
    target: ResolvedTarget,
    backfill_from: Option<u64>,
    backfill_logs: Option<String>,
    backfill_sink: Option<String>,
    shutdown: CancellationToken,
) -> i32
```

Note: `config` is retained because `run_backfill` still needs:
- `config.ct_log.batch_size` for fetcher batch sizing
- `config.ct_log.state_file` for StateManager
- `config.delta_sink.batch_size` for channel buffer and writer batch size
- `config.delta_sink.flush_interval_secs` for writer timer
- CT log settings for watcher/fetcher
- Zerobus config for zerobus sink backfill

**Regarding AC6.6 ("No operation function references config.delta_sink for table paths or write settings"):** The references to `config.delta_sink.batch_size` and `config.delta_sink.flush_interval_secs` are **not** violations of AC6.6. These are runtime operational settings that control in-memory buffering behavior (how many records to accumulate before flushing, how often to flush), not per-table write settings (compression level, offline batch size) or table paths. AC6.6 targets the elimination of `config.delta_sink.table_path`, `config.delta_sink.compression_level`, `config.delta_sink.heavy_column_compression_level`, and `config.delta_sink.offline_batch_size` from operation functions — all of which move to `ResolvedTarget`.

Inside run_backfill:
- Remove `parse_table_uri(&config.delta_sink.table_path)` and `resolve_storage_options()` calls (lines 734-750). Use `target.table_path` and `target.storage_options` directly.
- The `staging_path` parameter is removed — backfill always writes to `target`. The old two-table gap detection (main + staging UNION ALL) simplifies to single-table gap detection against the target table.
- Replace `config.delta_sink.compression_level` (line 912) with `target.compression_level`
- Replace `config.delta_sink.heavy_column_compression_level` (line 913) with `target.heavy_column_compression_level`
- Pass `target.table_path` and `target.storage_options` to `detect_gaps()` and the writer task
- Simplify `detect_gaps()` signature: remove `staging_path` parameter. Gap detection reads from the target table only.

**Behavior change: dual-table gap detection removed.** Previously, `detect_gaps()` accepted both `table_path` and `staging_path` and used a UNION ALL query across both tables to avoid re-fetching records present in either table. With named targets, the staging concept is replaced by targeting a specific named table. Gap detection now queries only the target table. This means:
- If backfilling into a staging target, gap detection sees only what's in that staging target (not the main table).
- Operators who previously relied on dual-table deduplication during staging backfill should now either: (a) merge staging into main first, then backfill into staging again, or (b) backfill directly into the main target.
- This simplification is intentional: named targets treat each target as an independent table, and the resolution layer doesn't need to know about relationships between targets.

**detect_gaps() refactoring detail:**
1. Change signature from `detect_gaps(table_path: &str, staging_path: Option<&str>, logs: &[(String, u64)], backfill_from: Option<u64>, storage_options: &HashMap<String, String>)` to `detect_gaps(table_path: &str, logs: &[(String, u64)], backfill_from: Option<u64>, storage_options: &HashMap<String, String>)`
2. Remove the `staging_path` logic: the conditional that creates a UNION ALL view across main+staging tables (including the `DeltaTable::open_with_storage_options()` call for staging and the `CREATE VIEW union_all_view AS SELECT * FROM main_table UNION ALL SELECT * FROM staging_table` SQL)
3. Keep only the single-table query path: register the target table directly and query it for gaps using the existing LEAD window function logic
4. Remove the `staging_path.is_some()` fallback logic (where staging exists but main doesn't)
5. The rest of detect_gaps (gap detection SQL, work item construction) stays unchanged

Update main.rs backfill dispatch to call:
```rust
let exit_code = backfill::run_backfill(
    config,
    resolved_target,
    backfill_from,
    cli_args.backfill_logs,
    cli_args.backfill_sink,
    shutdown_token,
)
.await;
```

**run_reparse_audit** — change signature from:
```rust
pub async fn run_reparse_audit(
    config: Config,
    from_date: Option<String>,
    to_date: Option<String>,
    shutdown: CancellationToken,
) -> (i32, AuditReport)
```
to:
```rust
pub async fn run_reparse_audit(
    source: ResolvedTarget,
    from_date: Option<String>,
    to_date: Option<String>,
    shutdown: CancellationToken,
) -> (i32, AuditReport)
```

Inside run_reparse_audit:
- Remove `parse_table_uri(&config.delta_sink.table_path)` and `resolve_storage_options()` (lines 223-229). Use `source.table_path` and `source.storage_options` directly.
- Replace all `config.delta_sink.table_path` references (lines 216, 233, 241) with `source.table_path`

Update main.rs reparse-audit dispatch to call:
```rust
let (exit_code, _report) = table_ops::run_reparse_audit(
    resolved_source,
    cli_args.from_date,
    cli_args.to_date,
    shutdown_token,
)
.await;
```

Remove the transitional config clone from Phase 4.

**Verification:**

Run: `cargo build`
Expected: Builds without errors

**Commit:** `refactor: run_backfill and run_reparse_audit accept ResolvedTarget`

<!-- END_TASK_5 -->

<!-- START_TASK_6 -->
### Task 6: Update backfill, merge, and reparse-audit tests

**Verifies:** named-targets.AC6.3, named-targets.AC6.4, named-targets.AC6.5, named-targets.AC6.6

**Files:**
- Modify: `src/backfill.rs` (update backfill and merge test helpers and tests)
- Modify: `src/table_ops.rs` (update reparse-audit test helpers and tests)

**Implementation:**

Update all tests that call the refactored functions. Use the `make_test_resolved_target` helper from Task 3 (or create equivalent in each module).

**Backfill tests** — tests that called `run_backfill(config, staging_path, ...)` now call `run_backfill(config, target, ...)` where `target` is a `ResolvedTarget`. The staging_path previously passed as `Option<String>` is now always provided as the target's table_path.

Tests to update include:
- `test_ac1_1_run_backfill_no_work_items`
- `test_writer_basic_functionality`
- `test_backfill_writer_binary_as_der`
- `test_staging_write_creates_table_at_staging_path`
- Any test that calls `run_backfill` or constructs `Config` specifically for backfill's table path/compression usage

**Merge tests** — tests that called `run_merge(config, staging_path, shutdown)` now call `run_merge(source, target, shutdown)`:
- `test_ac3_1_merge_inserts_non_duplicate_records`
- `test_ac3_2_merge_skips_existing_records`
- `test_ac3_3_merge_is_idempotent`
- `test_ac3_4_staging_directory_deleted_on_success`
- `test_ac3_5_merge_returns_zero_and_logs_metrics`
- `test_merge_path_binary_as_der`

**Reparse-audit tests** — tests that called `run_reparse_audit(config, ...)` now call `run_reparse_audit(source, ...)`:
- `test_ac1_1_audit_completes_with_zero_mismatches`
- `test_ac1_2_audit_detects_field_mismatches`
- `test_ac1_3_audit_reports_multiple_mismatches`
- `test_ac1_4_audit_counts_invalid_base64_as_unparseable`
- `test_ac1_5_audit_counts_unparseable_certificates`
- `test_ac1_6_audit_exits_with_empty_date_range`
- `test_ac4_4_reparse_audit_shutdown`

**Detect_gaps tests** — if detect_gaps signature changed (staging_path removed), update tests:
- `test_ac1_3_gap_detection_with_staging_catch_up_and_historical`
- `test_ac2_1_union_all_excludes_entries_in_either_table`
- `test_ac2_2_second_staging_run_produces_fewer_work_items`
- `test_ac2_3_ceiling_caps_union_max_cert_index`
- `test_ac2_4_missing_staging_falls_back_to_main_only`

**Testing:**

Tests must verify:
- named-targets.AC6.3: Merge tests pass with source/target ResolvedTarget, merges source into target
- named-targets.AC6.4: Backfill tests pass with target ResolvedTarget, writes to target.table_path
- named-targets.AC6.5: Reparse-audit tests pass with source ResolvedTarget, reads from source.table_path
- named-targets.AC6.6: No test constructs Config to pass delta_sink table path or write settings to any refactored function

**Verification:**

Run: `cargo test`
Expected: All tests pass

**Commit:** `test: update backfill, merge, and reparse-audit tests for ResolvedTarget`

<!-- END_TASK_6 -->

<!-- END_SUBCOMPONENT_B -->
