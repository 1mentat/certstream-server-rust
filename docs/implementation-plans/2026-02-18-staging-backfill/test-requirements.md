# Test Requirements: Staging Backfill

Maps each acceptance criterion from the staging backfill design plan to specific automated tests or human verification steps.

**Source:** `/data/mentat/certstream-server-rust/docs/design-plans/2026-02-18-staging-backfill.md`
**Implementation Plans:** `/data/mentat/certstream-server-rust/.worktrees/staging-backfill/docs/implementation-plans/2026-02-18-staging-backfill/phase_01.md` through `phase_05.md`
**Generated:** 2026-02-18

---

## Automated Test Coverage

All automated tests live inline in `src/backfill.rs` within the `#[cfg(test)] mod tests` block, following the project convention of real Delta tables in `/tmp/` directories, DataFusion SQL for verification, and no mocking.

### AC1: Backfill writes to staging table

| AC ID | Criterion | Test Name | Test File | Type | What It Verifies |
|-------|-----------|-----------|-----------|------|------------------|
| AC1.1 | `--backfill --staging-path /tmp/staging` creates a Delta table at `/tmp/staging` with the standard 20-column schema partitioned by `seen_date` | `test_staging_ac1_1_creates_table_at_staging_path` | `src/backfill.rs` | Integration | Creates an mpsc channel, spawns `run_writer()` with a staging temp path, sends records, drops sender, then asserts: (1) Delta table exists at the staging path, (2) schema has exactly 20 columns, (3) table is partitioned by `seen_date` |
| AC1.2 | Fetched CT records are written to the staging table, not the main table | `test_staging_ac1_2_records_in_staging_not_main` | `src/backfill.rs` | Integration | Creates separate main and staging temp paths; spawns `run_writer()` targeting the staging path; sends records; asserts records exist in staging table via DataFusion query and main table path either does not exist or contains zero rows |
| AC1.3 | Works with both catch-up and historical modes | `test_staging_ac1_3_works_with_historical_mode` | `src/backfill.rs` | Integration | Calls `detect_gaps()` with `staging_path` in both catch-up (`backfill_from = None`) and historical (`backfill_from = Some(0)`) modes; verifies work items are generated in both cases |

### AC2: Gap detection unions main and staging

| AC ID | Criterion | Test Name | Test File | Type | What It Verifies |
|-------|-----------|-----------|-----------|------|------------------|
| AC2.1 | When staging table exists, gap detection queries both main and staging -- entries present in either table are excluded from gap list | `test_staging_ac2_1_union_excludes_entries_in_either_table` | `src/backfill.rs` | Integration | Creates main table with indices [10, 11, 15, 16], staging table with [12, 13]; calls `detect_gaps(main_path, Some(staging_path), logs_ceiling_20, None)`; asserts only gap [14, 14] is detected |
| AC2.2 | Running backfill with staging a second time produces fewer work items | `test_staging_ac2_2_second_run_fewer_work_items` | `src/backfill.rs` | Integration | Creates main table with [10, 15]; first call with no staging detects gap [11, 14]; then creates staging with [11, 12]; second call with staging detects smaller gap [13, 14]; asserts second result is a strict subset of first |
| AC2.3 | Per-log MAX(cert_index) from the union is capped at the state file ceiling | `test_staging_ac2_3_ceiling_caps_max_index` | `src/backfill.rs` | Integration | Creates main with [10, 11, 12], staging with [13, 14, 25] (25 exceeds ceiling); calls with ceiling=20; asserts no work items generated beyond index 19 |
| AC2.4 | When staging table doesn't exist yet (first run), gap detection falls back to querying main table only | `test_staging_ac2_4_missing_staging_falls_back_to_main` | `src/backfill.rs` | Integration | Creates main table with [10, 11, 15]; calls `detect_gaps(main_path, Some("/tmp/nonexistent_staging_xyz"), logs, None)`; asserts results are identical to calling with `staging_path = None` (gap [12, 14]) |

### AC3: Merge deduplicates and appends

| AC ID | Criterion | Test Name | Test File | Type | What It Verifies |
|-------|-----------|-----------|-----------|------|------------------|
| AC3.1 | `--merge --staging-path` inserts staging records not present in main | `test_merge_ac3_1_inserts_non_duplicate_records` | `src/backfill.rs` | Integration | Creates main with [10, 11, 12], staging with [13, 14, 15]; calls `run_merge()`; queries main via DataFusion; asserts main now has indices [10, 11, 12, 13, 14, 15] |
| AC3.2 | Records already in main with matching (source_url, cert_index) are skipped | `test_merge_ac3_2_skips_existing_records` | `src/backfill.rs` | Integration | Creates main with [10, 11, 12], staging with [11, 12, 13] (overlap on 11, 12); calls `run_merge()`; queries main; asserts exactly [10, 11, 12, 13] with no duplicates (count = 4) |
| AC3.3 | Merge is idempotent -- running it twice with same staging data produces identical main table | `test_merge_ac3_3_idempotent` | `src/backfill.rs` | Integration | Creates main with [10, 11], staging with [12, 13]; runs merge (staging deleted); recreates staging with same [12, 13]; runs merge again; queries main; asserts exactly [10, 11, 12, 13] with count = 4 |
| AC3.4 | Staging directory is deleted after successful merge | `test_merge_ac3_4_staging_deleted_on_success` | `src/backfill.rs` | Integration | Creates main and staging; runs `run_merge()` successfully (returns 0); asserts `std::path::Path::new(&staging_path).exists()` is false |
| AC3.5 | Merge logs metrics: records inserted, records skipped | `test_merge_ac3_5_returns_zero_on_success` | `src/backfill.rs` | Integration | Creates main and staging with known overlap; calls `run_merge()`; asserts return value is 0. See Human Verification HV1 for log content verification. |

### AC4: Error handling

| AC ID | Criterion | Test Name | Test File | Type | What It Verifies |
|-------|-----------|-----------|-----------|------|------------------|
| AC4.1 (missing) | Missing staging table exits 0 with info log | `test_error_ac4_1_missing_staging_exits_zero` | `src/backfill.rs` | Unit | Calls `run_merge()` with staging_path pointing to nonexistent directory; asserts return value is 0 |
| AC4.1 (empty) | Empty staging table exits 0 with info log | `test_error_ac4_1_empty_staging_exits_zero` | `src/backfill.rs` | Integration | Creates a staging Delta table with schema but zero records; calls `run_merge()`; asserts return value is 0 |
| AC4.2 | Missing main table is created automatically, then merge proceeds as pure inserts | `test_error_ac4_2_missing_main_auto_created` | `src/backfill.rs` | Integration | Creates staging with [10, 11, 12]; sets config main path to nonexistent directory; calls `run_merge()`; asserts return 0; opens main table and queries via DataFusion; asserts records [10, 11, 12] exist |
| AC4.3 | Merge batch failure leaves staging intact for retry, exits 1 | See HV2 | `src/backfill.rs` | Human | Difficult to simulate real MERGE INTO failure without corrupting Delta state. Code review verifies the error path returns 1 before staging cleanup. |
| AC4.4 | Process interruption (SIGINT) leaves staging intact, exits 1 | `test_error_ac4_4_cancellation_preserves_staging` | `src/backfill.rs` | Integration | Creates staging and main tables; creates a CancellationToken and cancels it before calling `run_merge()`; asserts return value is 1; asserts staging directory still exists |
| AC4.5 | `--merge` without `--staging-path` prints error and exits with non-zero code | `test_error_ac4_5_merge_requires_staging_path` | `src/backfill.rs` | Unit | Verifies the validation condition `cli_args.merge && cli_args.staging_path.is_none()` evaluates to true. See HV4 for CLI output verification. |

### AC5: Backward compatibility

| AC ID | Criterion | Test Name | Test File | Type | What It Verifies |
|-------|-----------|-----------|-----------|------|------------------|
| AC5.1 | `--backfill` without `--staging-path` writes to main table (existing behavior unchanged) | All existing `test_ac*` and `test_writer_*` tests | `src/backfill.rs` | Integration | All 15+ existing backfill tests continue to pass with `staging_path = None`. No test logic changes. |
| AC5.2 | All existing backfill tests pass without modification | `cargo test` (full suite) | All `src/*.rs` | Integration | Run `cargo test` after each phase. All 249+ existing tests must pass. |

---

## Human Verification Items

### HV1: AC3.5 -- Merge log output content

**Criterion:** Merge logs metrics: records inserted, records skipped

**Justification:** The `run_merge()` function uses `tracing::info!()` with structured fields (`total_inserted`, `total_skipped`, `total_staging_rows`). Verifying exact log content requires a tracing subscriber capture setup, which is disproportionate overhead. Return code (0) confirms successful execution, and the structured fields are validated by compilation.

**Verification steps:**
1. Run `RUST_LOG=info cargo run -- --merge --staging-path /tmp/test_staging` with prepared test data
2. Confirm log output contains a line with `merge complete`, `total_inserted`, `total_skipped`, and `total_staging_rows` fields
3. Verify the counts are numerically consistent (inserted + skipped = total_staging_rows)

### HV2: AC4.3 -- Merge batch failure preserves staging

**Criterion:** Merge batch failure leaves staging intact for retry, exits 1

**Justification:** Simulating a genuine Delta MERGE INTO failure requires corrupting the Delta transaction log or injecting I/O errors, which doesn't align with the project's real-I/O testing pattern. The cancellation path (AC4.4) exercises the same "return 1 before staging cleanup" code structure.

**Verification steps:**
1. Code review: confirm `run_merge()` returns 1 immediately on `Err(e)` from the merge `.await`, before reaching `remove_dir_all`
2. The automated test `test_error_ac4_4_cancellation_preserves_staging` covers the structural pattern (early return with staging preserved)

### HV3: AC4.4 -- Real SIGINT handling

**Criterion:** Process interruption (SIGINT) leaves staging intact, exits 1

**Justification:** The automated test uses a pre-cancelled CancellationToken. The full signal path (OS signal -> handler -> CancellationToken -> merge observes cancellation) involves the signal handler in `main.rs`.

**Verification steps:**
1. Start `cargo run -- --merge --staging-path /tmp/test_staging` with a large staging table
2. Send SIGINT (Ctrl+C) during merge execution
3. Verify process exits with code 1
4. Verify staging directory still exists

### HV4: AC4.5 -- CLI error message format

**Criterion:** `--merge` without `--staging-path` prints error and exits with non-zero code

**Justification:** The automated test verifies the condition logic. The actual stderr output and exit code require running the binary as a subprocess.

**Verification steps:**
1. Run `cargo run -- --merge` (without `--staging-path`)
2. Verify stderr contains `Error: --merge requires --staging-path <PATH>`
3. Verify exit code is 1

### HV5: AC1.3 -- End-to-end staging with both backfill modes

**Criterion:** Works with both catch-up and historical modes

**Justification:** Full end-to-end test of `run_backfill()` with `--staging-path` requires a running CT log endpoint or mock HTTP server. The component-level tests (gap detection + writer) provide sufficient coverage.

**Verification steps:**
1. Populate a state file and main Delta table with known data
2. Run `cargo run -- --backfill --staging-path /tmp/staging` (catch-up mode)
3. Verify staging table created with records filling gaps
4. Clean up staging; re-run with `--from 0` (historical mode)
5. Verify staging table created with records from index 0 to ceiling

---

## Test Infrastructure Notes

### Test Patterns

All tests follow the established patterns in `src/backfill.rs`:

- **Temp directories:** Each test creates a unique `/tmp/delta_backfill_test_<name>` directory. Cleanup via `fs::remove_dir_all()` at both the start and end of each test.
- **Real Delta tables:** Tests create actual Delta tables using `open_or_create_table()` and write real Parquet data via `DeltaOps::write()` with `SaveMode::Append`.
- **DataFusion verification:** Test assertions query Delta tables using `SessionContext`, `register_table`, and SQL.
- **No mocking:** All I/O is real filesystem operations.
- **Test records:** Use the existing `make_test_record(cert_index, source_url)` helper.

### Helpers Available

| Helper | Location | Purpose |
|--------|----------|---------|
| `make_test_record(cert_index, source_url)` | `src/backfill.rs` (test module) | Creates a `DeltaCertRecord` with test data |
| `delta_schema()` | `src/delta_sink.rs` | Returns the 20-column Arrow schema |
| `open_or_create_table(path, schema)` | `src/delta_sink.rs` | Opens or creates a Delta table |
| `records_to_batch(records, schema)` | `src/delta_sink.rs` | Converts records to Arrow RecordBatch |
| `run_writer(path, batch_size, flush_interval, rx, shutdown)` | `src/backfill.rs` | The writer task for direct testing |

### New Test Helper Needed

**Test Config builder:** Merge tests need a `Config` object. Use `DeltaSinkConfig::default()` (config.rs:299-307) and set `table_path` to the test path. Full `Config` construction will need a test helper or `Config::load()` with env var overrides.

### Test Count

| Category | New Tests | Phase |
|----------|-----------|-------|
| Staging writes (AC1) | 2-3 | Phase 2 |
| Gap detection union (AC2) | 4 | Phase 3 |
| Merge dedup/cleanup (AC3) | 5 | Phase 4 |
| Error handling (AC4) | 4-5 | Phase 5 |
| Backward compat (AC5) | 0 (existing) | All |
| **Total new** | **15-17** | |
| **Existing unchanged** | **249+** | All |
