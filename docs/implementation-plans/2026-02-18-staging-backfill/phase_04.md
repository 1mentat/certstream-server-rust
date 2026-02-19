# Staging Backfill Implementation Plan - Phase 4: Merge Command

**Goal:** `--merge` reads staging, deduplicates against main via Delta MERGE INTO, deletes staging on success

**Architecture:** `run_merge()` replaces the Phase 1 stub. It opens both tables, reads all staging records as RecordBatches via DataFusion, then performs a DeltaOps::merge() with predicate `target.source_url = source.source_url AND target.cert_index = source.cert_index` and `when_not_matched_insert()` to insert only records not already in main. Staging records are read in batches and merged batch-by-batch, each as its own transaction. On success, the staging directory is deleted. The function returns 0 on success, 1 on error.

**Tech Stack:** Rust, deltalake 0.25 (DeltaOps::merge — first use of MERGE INTO in this codebase), DataFusion, Arrow RecordBatch

**Scope:** 5 phases from original design (this is phase 4 of 5)

**Codebase verified:** 2026-02-18

**Note:** Line references throughout are approximate and should be verified against the actual file at implementation time, as prior phase changes may shift line numbers.

**API note:** The design plan mentions `when_not_matched_insert_all()` but the deltalake 0.25 Rust API does not have this method. The correct API is `when_not_matched_insert()` with explicit `.set()` calls for each column.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### staging-backfill.AC3: Merge deduplicates and appends
- **staging-backfill.AC3.1 Success:** `--merge --staging-path` inserts staging records not present in main (matched on `source_url` + `cert_index`)
- **staging-backfill.AC3.2 Success:** Records already in main with matching `(source_url, cert_index)` are skipped (not duplicated)
- **staging-backfill.AC3.3 Success:** Merge is idempotent — running it twice with same staging data produces identical main table
- **staging-backfill.AC3.4 Success:** Staging directory is deleted after successful merge
- **staging-backfill.AC3.5 Success:** Merge logs metrics: records inserted, records skipped

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Implement run_merge function (including imports)

**Files:**
- Modify: `src/backfill.rs` (replace run_merge stub with full implementation, add any missing imports)

**Implementation:**

Replace the `run_merge` stub from Phase 1 with the full implementation. Also add any missing imports at the top of the file (DeltaOps, SessionContext, etc. — check existing imports and add only what's missing).

The function:

1. Opens both the main and staging Delta tables
2. Reads all staging records as RecordBatches via DataFusion SQL
3. Converts each batch to a DataFrame via `ctx.read_batch()` (DeltaOps::merge requires a DataFrame, not Vec<RecordBatch>)
4. Merges batch-by-batch into the main table using DeltaOps::merge()
5. Deletes the staging directory on success
6. Returns exit code 0 (success) or 1 (error)

```rust
pub async fn run_merge(
    config: Config,
    staging_path: String,
    shutdown: CancellationToken,
) -> i32 {
    info!(staging_path = %staging_path, "merge mode starting");

    let schema = delta_schema();

    // Open staging table
    let staging_table = match deltalake::open_table(&staging_path).await {
        Ok(t) => t,
        Err(e) => {
            // Missing/empty staging — handled as non-error in Phase 5
            warn!(error = %e, "failed to open staging table");
            return 1;
        }
    };

    // Open or create main table
    let main_table = match open_or_create_table(&config.delta_sink.table_path, &schema).await {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "failed to open main table");
            return 1;
        }
    };

    // Read all staging records via DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("staging", std::sync::Arc::new(staging_table))
        .expect("failed to register staging table");

    let df = match ctx.sql("SELECT * FROM staging").await {
        Ok(df) => df,
        Err(e) => {
            warn!(error = %e, "failed to query staging table");
            return 1;
        }
    };

    let batches = match df.collect().await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "failed to collect staging batches");
            return 1;
        }
    };

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        info!("staging table is empty, nothing to merge");
        return 0;
    }

    let total_staging_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    info!(total_staging_rows = total_staging_rows, "read staging records");

    // Merge batch-by-batch
    let mut current_table = main_table;
    let mut total_inserted: usize = 0;
    let mut total_skipped: usize = 0;

    for (i, batch) in batches.iter().enumerate() {
        if shutdown.is_cancelled() {
            warn!("merge interrupted by shutdown signal");
            return 1;
        }

        if batch.num_rows() == 0 {
            continue;
        }

        // Convert RecordBatch to DataFrame — DeltaOps::merge() requires a DataFrame,
        // not Vec<RecordBatch>
        let merge_ctx = SessionContext::new();
        let source_df = match merge_ctx.read_batch(batch.clone()) {
            Ok(df) => df,
            Err(e) => {
                warn!(batch = i + 1, error = %e, "failed to create source DataFrame");
                return 1;
            }
        };

        let predicate = "target.source_url = source.source_url AND target.cert_index = source.cert_index";

        // Build the merge operation — when_not_matched_insert() returns a Result,
        // so use match for proper error handling instead of .expect()
        let merge_builder = match DeltaOps(current_table)
            .merge(source_df, predicate)
            .with_source_alias("source")
            .with_target_alias("target")
            .when_not_matched_insert(|insert| {
                insert
                    .set("cert_index", "source.cert_index")
                    .set("update_type", "source.update_type")
                    .set("seen", "source.seen")
                    .set("seen_date", "source.seen_date")
                    .set("source_name", "source.source_name")
                    .set("source_url", "source.source_url")
                    .set("cert_link", "source.cert_link")
                    .set("serial_number", "source.serial_number")
                    .set("fingerprint", "source.fingerprint")
                    .set("sha256", "source.sha256")
                    .set("sha1", "source.sha1")
                    .set("not_before", "source.not_before")
                    .set("not_after", "source.not_after")
                    .set("is_ca", "source.is_ca")
                    .set("signature_algorithm", "source.signature_algorithm")
                    .set("subject_aggregated", "source.subject_aggregated")
                    .set("issuer_aggregated", "source.issuer_aggregated")
                    .set("all_domains", "source.all_domains")
                    .set("as_der", "source.as_der")
                    .set("chain", "source.chain")
            }) {
                Ok(builder) => builder,
                Err(e) => {
                    warn!(batch = i + 1, error = %e, "failed to build merge operation");
                    return 1;
                }
            };

        match merge_builder.await {
            Ok((new_table, metrics)) => {
                let inserted = metrics.num_target_rows_inserted;
                let batch_rows = batch.num_rows();
                // skipped = batch_rows - inserted is valid here because we use ONLY
                // when_not_matched_insert (no updates/deletes). Every non-matched source
                // row becomes an insert; every matched source row is skipped.
                let skipped = batch_rows - inserted;
                total_inserted += inserted;
                total_skipped += skipped;

                info!(
                    batch = i + 1,
                    batch_rows = batch_rows,
                    inserted = inserted,
                    skipped = skipped,
                    "merge batch complete"
                );

                current_table = new_table;
            }
            Err(e) => {
                warn!(batch = i + 1, error = %e, "merge batch failed");
                // Leave staging intact for retry
                return 1;
            }
        }
    }

    // Log final metrics (AC3.5)
    info!(
        total_inserted = total_inserted,
        total_skipped = total_skipped,
        total_staging_rows = total_staging_rows,
        "merge complete"
    );

    // Delete staging directory on success (AC3.4)
    match std::fs::remove_dir_all(&staging_path) {
        Ok(_) => {
            info!(staging_path = %staging_path, "staging directory deleted");
        }
        Err(e) => {
            warn!(error = %e, staging_path = %staging_path, "failed to delete staging directory");
            // Non-fatal — merge succeeded, staging cleanup failed
        }
    }

    0
}
```

**Key design decisions:**
- The merge predicate matches on both `source_url` AND `cert_index` (the pair uniquely identifies entries globally)
- Only `when_not_matched_insert()` is used — no updates or deletes. Records already in main are skipped.
- **DeltaOps::merge() requires a DataFrame** (not Vec<RecordBatch>). Each batch is converted via `SessionContext::read_batch()` before passing to merge.
- Each batch merge is a separate Delta transaction, so partial progress is preserved on failure
- `when_not_matched_insert()` returns a `Result` — use `match` for proper error handling, not `.expect()` which would panic
- `when_not_matched_insert()` requires explicitly setting all 20 columns with `source.column_name` expressions
- On batch failure, returns immediately with exit code 1, leaving staging intact

**Note on MergeMetrics fields:** The exact field names may differ slightly between deltalake versions. The implementor should check the actual `MergeMetrics` struct fields at compile time (common fields: `num_target_rows_inserted`, `num_target_rows_updated`, `num_target_rows_deleted`, `num_output_rows`). If `num_target_rows_inserted` doesn't exist, use `num_output_rows - num_target_rows_deleted` or inspect the available fields.

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat(backfill): implement run_merge with Delta MERGE INTO deduplication`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Tests for merge deduplication and cleanup

**Verifies:** staging-backfill.AC3.1, staging-backfill.AC3.2, staging-backfill.AC3.3, staging-backfill.AC3.4, staging-backfill.AC3.5

**Files:**
- Modify: `src/backfill.rs` (test module)

**Implementation:**

Add tests following the existing pattern (real temp Delta tables, `make_test_record`, `DeltaOps::write`, DataFusion for verification).

Since `run_merge` takes a `Config` object, tests need to create a minimal config with `config.delta_sink.table_path` pointing to the main temp path. The existing test patterns don't create full Config objects, so tests should exercise `run_merge` through a more targeted approach — either by creating a test config or by testing the merge logic with a helper.

**Test 1: Merge inserts non-duplicate records (AC3.1)**
- Create main table with records at cert_index [10, 11, 12] for source_url "https://log.example.com"
- Create staging table with records at cert_index [13, 14, 15] for same source_url
- Call run_merge with main_path and staging_path
- Verify: main table now contains indices [10, 11, 12, 13, 14, 15]

**Test 2: Merge skips existing records (AC3.2)**
- Create main table with records at [10, 11, 12]
- Create staging table with records at [11, 12, 13] (overlap on 11 and 12)
- Call run_merge
- Verify: main table has [10, 11, 12, 13] — no duplicates of 11 or 12

**Test 3: Merge is idempotent (AC3.3)**
- Create main table with [10, 11]
- Create staging table with [12, 13]
- Run merge first time (staging gets deleted by AC3.4, so recreate it)
- Recreate staging with same data [12, 13]
- Run merge second time
- Verify: main table has [10, 11, 12, 13] — no additional rows

**Test 4: Staging directory deleted on success (AC3.4)**
- Create main and staging tables
- Run merge successfully
- Assert: staging directory no longer exists (use `std::path::Path::new(&staging_path).exists()`)

**Test 5: Merge returns 0 and logs metrics (AC3.5)**
- Create main and staging tables with known overlap
- Capture the return value of run_merge
- Assert: returns 0
- Verify metrics logged (tracing test or just verify the return code)

For creating a minimal Config in tests, use the existing `Config::load()` with env var overrides, or create a Config struct directly with the required `delta_sink` fields.

**Testing:**
Tests must verify each AC listed above:
- staging-backfill.AC3.1: Assert new records from staging are in main after merge
- staging-backfill.AC3.2: Assert records with matching (source_url, cert_index) are not duplicated
- staging-backfill.AC3.3: Assert running merge twice produces identical main table
- staging-backfill.AC3.4: Assert staging directory is removed after successful merge
- staging-backfill.AC3.5: Assert merge returns 0 on success

Follow project testing patterns — real Delta tables in `/tmp/`, DataFusion SQL for verification, manual cleanup.

**Verification:**
Run: `cargo test merge`
Expected: All new merge tests pass

Run: `cargo test`
Expected: All tests pass

**Commit:** `test(backfill): add merge deduplication and cleanup tests for AC3.1-AC3.5`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->
