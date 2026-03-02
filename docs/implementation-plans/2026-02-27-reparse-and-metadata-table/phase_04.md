# Reparse Audit & Metadata Table Implementation Plan

**Goal:** Add two new batch execution modes (`--reparse-audit` and `--extract-metadata`) for auditing stored certificates against current parsing code and extracting metadata-only Delta tables.

**Architecture:** New `src/table_ops.rs` module with two entry points (`run_reparse_audit`, `run_extract_metadata`) dispatched from the existing CLI if/else chain in `main.rs`. Both read the Delta table via DataFusion SQL, process partition-by-partition, and exit with a status code.

**Tech Stack:** Rust, Tokio, DataFusion, deltalake 0.25, Arrow

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-27

---

## Acceptance Criteria Coverage

This phase implements and tests:

### reparse-and-metadata-table.AC2: Metadata extraction produces correct metadata-only table
- **reparse-and-metadata-table.AC2.1 Success:** Output Delta table contains exactly 19 columns (all columns from source except `as_der`)
- **reparse-and-metadata-table.AC2.2 Success:** All metadata field values in output match source table exactly
- **reparse-and-metadata-table.AC2.3 Success:** Output table is partitioned by `seen_date`
- **reparse-and-metadata-table.AC2.4 Success:** Chain column is preserved with full chain certificate metadata JSON
- **reparse-and-metadata-table.AC2.5 Failure:** Missing source table exits 1 with error message
- **reparse-and-metadata-table.AC2.6 Edge:** Extraction against empty date range exits 0 with informational message

---

## Phase 4: Metadata Extraction Core

<!-- START_TASK_1 -->
### Task 1: Implement run_extract_metadata core logic

**Verifies:** reparse-and-metadata-table.AC2.1, reparse-and-metadata-table.AC2.2, reparse-and-metadata-table.AC2.3, reparse-and-metadata-table.AC2.4, reparse-and-metadata-table.AC2.5, reparse-and-metadata-table.AC2.6

**Files:**
- Modify: `src/table_ops.rs` (replace `run_extract_metadata` stub with full implementation)

**Implementation:**

Add these imports to `src/table_ops.rs` (extending existing imports as needed — some may already be present from Phase 3):

```rust
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::delta_sink::open_or_create_table;
use futures::StreamExt;
```

Replace the `run_extract_metadata` stub with the full implementation. The function:

1. Opens the source Delta table at `config.delta_sink.table_path` using `deltalake::open_table()`. If the table cannot be opened, log the error and return exit code 1 (AC2.5).

2. Creates a DataFusion `SessionContext`, registers the source Delta table as `ct_main`.

3. Builds a SQL query selecting all 19 metadata columns (everything except `as_der`):
   ```sql
   SELECT cert_index, update_type, seen, seen_date, source_name, source_url,
          cert_link, serial_number, fingerprint, sha256, sha1, not_before,
          not_after, is_ca, signature_algorithm, subject_aggregated,
          issuer_aggregated, all_domains, chain
   FROM ct_main
   ```
   Appends the date filter clause from `date_filter_clause()`. Since `date_filter_clause` returns `Result<String, String>` (Phase 2), handle the `Err` case by logging the error and returning exit code 1.

4. Executes the query via `ctx.sql(&sql).await` to get a `DataFrame`, then uses `df.execute_stream().await` to get a `SendableRecordBatchStream`. This streams batches one at a time instead of loading all data into memory. Add `use futures::StreamExt;` for `.next()` on the stream.

5. Opens or creates the output Delta table at `output_path` using `open_or_create_table(&output_path, &metadata_schema())`. This automatically configures `seen_date` partitioning (AC2.3). If creation fails, return exit code 1.

6. Builds `WriterProperties` with zstd compression from `config.delta_sink.compression_level`:
   ```rust
   let writer_props = WriterProperties::builder()
       .set_compression(Compression::ZSTD(
           ZstdLevel::try_new(config.delta_sink.compression_level)
               .expect("compression level validated at startup"),
       ))
       .build();
   ```

7. Iterates the stream using `while let Some(batch_result) = stream.next().await`. For each RecordBatch from the stream:
   - If no rows have been seen yet and this is the first batch, check if it has zero rows. After the stream is exhausted, if total rows written is 0, log "No records found in the specified date range" and return 0 (AC2.6).
   - Write the batch directly to the output table using `DeltaOps`:
     ```rust
     let new_table = DeltaOps(output_table)
         .write(vec![batch])
         .with_save_mode(SaveMode::Append)
         .with_writer_properties(writer_props.clone())
         .await?;
     output_table = new_table;
     ```
   - Log progress: `info!("Written {records} records")`.
   - Track total records written.

8. After the stream is exhausted, if total records written is 0, log "No records found in the specified date range" and return 0 (AC2.6). Otherwise log final summary: total records written across all batches. Return 0.

**Key design decisions:**
- RecordBatches from the SQL query already have the correct 19-column schema — no conversion needed. DataFusion's SELECT produces batches matching the projected columns.
- The `chain` column (List<Utf8>) is preserved as-is from the source (AC2.4). No special handling needed since the SQL SELECT includes it.
- Each batch is written in a separate Delta transaction. This keeps memory usage bounded to one batch at a time. If a write fails, the partially written output is left intact for retry.

**Error handling:**
- Source table open failure: log error, return 1
- Output table creation failure: log error, return 1
- Write failure: log error, return 1 (partial output left intact)

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat: implement metadata extraction core logic`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add tests for metadata extraction

**Verifies:** reparse-and-metadata-table.AC2.1, reparse-and-metadata-table.AC2.2, reparse-and-metadata-table.AC2.3, reparse-and-metadata-table.AC2.4, reparse-and-metadata-table.AC2.5, reparse-and-metadata-table.AC2.6

**Files:**
- Modify: `src/table_ops.rs` (add tests to existing `#[cfg(test)] mod tests` block)

**Testing:**

Tests must verify each AC listed above. Follow the project's existing Delta table test pattern: create real temp tables in `/tmp/delta_table_ops_test_*`, write test data, run extraction, then verify output table.

- **reparse-and-metadata-table.AC2.1:** Create a source table with 20-column schema, write test records, run `run_extract_metadata()`, open the output table, and verify it has exactly 19 columns with no `as_der` field. Check schema field names and types against `metadata_schema()`.

- **reparse-and-metadata-table.AC2.2:** After extraction, query the output table via DataFusion SQL and verify that all metadata field values match the source table exactly. Compare at least `cert_index`, `subject_aggregated`, `not_before`, `is_ca`, and `all_domains` values row-by-row.

- **reparse-and-metadata-table.AC2.3:** After extraction, verify the output table's metadata includes `seen_date` as a partition column. Can be verified by checking the Delta table schema or by confirming the directory structure includes `seen_date=YYYY-MM-DD/` subdirectories.

- **reparse-and-metadata-table.AC2.4:** Create source records with non-empty `chain` values (JSON strings). After extraction, verify the `chain` column in the output table preserves the exact values.

- **reparse-and-metadata-table.AC2.5:** Call `run_extract_metadata()` with a `config.delta_sink.table_path` that doesn't exist. Verify exit code 1.

- **reparse-and-metadata-table.AC2.6:** Create a source table with `seen_date = "2026-01-01"`, then run extraction with `from_date = Some("2099-01-01")`. Verify exit code 0 and informational message about no records.

Use `#[tokio::test]` for all async tests. Create helper function `make_test_record()` if not already present. Clean up temp directories after each test.

**Verification:**
Run: `cargo test --lib table_ops::tests`
Expected: All tests pass

**Commit:** `test: add metadata extraction tests for schema, data integrity, and edge cases`
<!-- END_TASK_2 -->
