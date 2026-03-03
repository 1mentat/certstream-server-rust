# Reparse Audit & Metadata Table Implementation Plan

**Goal:** Add two new batch execution modes (`--reparse-audit` and `--extract-metadata`) for auditing stored certificates against current parsing code and extracting metadata-only Delta tables.

**Architecture:** New `src/table_ops.rs` module with two entry points (`run_reparse_audit`, `run_extract_metadata`) dispatched from the existing CLI if/else chain in `main.rs`. Both read the Delta table via DataFusion SQL, process partition-by-partition, and exit with a status code.

**Tech Stack:** Rust, Tokio, DataFusion, deltalake 0.25, Arrow

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-27

---

## Acceptance Criteria Coverage

This phase implements and tests:

### reparse-and-metadata-table.AC1: Reparse audit reads and reparses stored certificates
- **reparse-and-metadata-table.AC1.1 Success:** Audit completes against a Delta table and reports zero mismatches when parsing code has not changed
- **reparse-and-metadata-table.AC1.2 Success:** Audit correctly identifies field-level mismatches when parsing code produces different output
- **reparse-and-metadata-table.AC1.3 Success:** Audit report shows per-field mismatch counts and up to 10 sample diffs
- **reparse-and-metadata-table.AC1.4 Failure:** Records with invalid base64 in `as_der` (Utf8 format) or corrupt bytes (Binary format) are counted as unparseable, not mismatches
- **reparse-and-metadata-table.AC1.5 Failure:** Records where `parse_certificate()` returns None are counted as unparseable
- **reparse-and-metadata-table.AC1.6 Edge:** Audit against an empty date range exits 0 with informational message
- **reparse-and-metadata-table.AC1.7 Success:** Audit handles both Utf8 (base64) and Binary column types for `as_der`

---

## Phase 3: Reparse Audit Core

<!-- START_TASK_1 -->
### Task 1: Implement run_reparse_audit core logic

**Verifies:** reparse-and-metadata-table.AC1.1, reparse-and-metadata-table.AC1.2, reparse-and-metadata-table.AC1.3, reparse-and-metadata-table.AC1.4, reparse-and-metadata-table.AC1.5, reparse-and-metadata-table.AC1.6, reparse-and-metadata-table.AC1.7

**Files:**
- Modify: `src/table_ops.rs` (replace `run_reparse_audit` stub with full implementation)

**Implementation:**

Add these imports to the top of `src/table_ops.rs` (extending the imports from Phase 2):

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, BooleanArray, Int64Array, ListArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use base64::{engine::general_purpose::STANDARD, Engine};
use datafusion::prelude::SessionContext;
use deltalake::open_table;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::ct::parse_certificate;
```

Replace the `run_reparse_audit` stub with the full implementation. The function:

1. Opens the Delta table at `config.delta_sink.table_path` using `deltalake::open_table()`. If the table cannot be opened, log the error and return exit code 1.

2. Creates a DataFusion `SessionContext`, registers the Delta table as `ct_main`.

3. Builds a SQL query selecting the 12 columns needed for comparison:
   ```sql
   SELECT cert_index, source_url, seen_date, as_der, subject_aggregated, issuer_aggregated,
          all_domains, signature_algorithm, is_ca, not_before, not_after, serial_number
   FROM ct_main
   ```
   Appends the date filter clause from `date_filter_clause()`. Since `date_filter_clause` returns `Result<String, String>` (Phase 2), handle the `Err` case by logging the error and returning exit code 1.

4. Executes the query via `ctx.sql(&sql).await` to get a `DataFrame`, then uses `df.execute_stream().await` to get a `SendableRecordBatchStream`. This streams batches one at a time instead of loading all data into memory.

5. Iterates the stream using `while let Some(batch_result) = stream.next().await`. Tracks whether any rows were seen. If after the stream is exhausted no rows were found, prints "No records found in the specified date range" and returns 0 (AC1.6).

6. For each RecordBatch from the stream, iterates rows. For each row:
   - Extracts `as_der` from the batch. Checks the column's data type:
     - If `DataType::Binary`: reads bytes directly via `BinaryArray` (AC1.7)
     - If `DataType::Utf8`: reads string via `StringArray`, then base64-decodes using `STANDARD.decode()` (AC1.7)
     - If the value is empty string or null, or base64 decode fails: increment `unparseable_count`, continue (AC1.4)
   - Calls `parse_certificate(&der_bytes, false)`. If returns `None`: increment `unparseable_count`, continue (AC1.5)
   - Compares the 8 parse-derived fields from the returned `LeafCert` against stored column values:
     - `subject_aggregated`: compare `leaf.subject.aggregated.as_deref().unwrap_or("")` vs stored `StringArray` value
     - `issuer_aggregated`: compare `leaf.issuer.aggregated.as_deref().unwrap_or("")` vs stored `StringArray` value
     - `all_domains`: compare `LeafCert.all_domains` (as sorted `Vec<String>`) vs stored `ListArray` values (extracted and sorted)
     - `signature_algorithm`: string equality
     - `is_ca`: bool equality via `BooleanArray`
     - `not_before`: i64 equality via `Int64Array`
     - `not_after`: i64 equality via `Int64Array`
     - `serial_number`: string equality
   - For each field mismatch: increment the field's counter in a `HashMap<String, u64>`. If total samples collected < 10, collect a sample diff struct containing cert_index, source_url, field name, stored value, reparsed value (AC1.3).
   - If any field mismatched for this row, increment `mismatch_record_count`.

7. After all batches processed, print the report to stdout in the format specified in the design:
   ```
   Reparse Audit Report
   ====================
   Records scanned: {total}
   Partitions scanned: {partition_count}
   Records with mismatches: {mismatch_count} ({percentage}%)
   Unparseable records: {unparseable}

   Field breakdown:
     subject_aggregated:  {N} mismatches
     ...

   Sample mismatches ({shown} of {total}):
     [cert_index={idx}, source={url}]
       {field}: stored="{stored}" reparsed="{reparsed}"
   ```

8. Return 0 (audit always exits 0 unless infrastructure failure).

**Key data structures to define:**

```rust
struct SampleDiff {
    cert_index: u64,
    source_url: String,
    fields: Vec<FieldDiff>,
}

struct FieldDiff {
    field_name: String,
    stored: String,
    reparsed: String,
}
```

**Extracting all_domains from ListArray:**

```rust
fn extract_domains_from_list(list_array: &ListArray, row: usize) -> Vec<String> {
    let values = list_array.value(row);
    let string_values = values.as_any().downcast_ref::<StringArray>().unwrap();
    (0..string_values.len())
        .map(|i| string_values.value(i).to_string())
        .collect()
}
```

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat: implement reparse audit core logic`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add tests for reparse audit comparison and report logic

**Verifies:** reparse-and-metadata-table.AC1.1, reparse-and-metadata-table.AC1.2, reparse-and-metadata-table.AC1.3, reparse-and-metadata-table.AC1.4, reparse-and-metadata-table.AC1.5, reparse-and-metadata-table.AC1.6

**Files:**
- Modify: `src/table_ops.rs` (add tests to existing `#[cfg(test)] mod tests` block)

**Testing:**

Tests must verify each AC listed above. Follow the project's existing test pattern: create real temp Delta tables in `/tmp/delta_table_ops_test_*`, write test data using `records_to_batch()` and `flush_buffer()` from `crate::delta_sink`, then run `run_reparse_audit()` against them.

- **reparse-and-metadata-table.AC1.1:** Create a Delta table with records whose stored metadata matches what `parse_certificate()` would produce for those DER bytes. Use `rcgen` (already in dev-deps) to generate a real test certificate, parse it with `parse_certificate()`, build a `DeltaCertRecord` from the result, write to Delta, then run audit. Verify report shows 0 mismatches.

- **reparse-and-metadata-table.AC1.2:** Create a Delta table with records where a field (e.g., `subject_aggregated`) has been manually altered to differ from what `parse_certificate()` produces. Run audit and verify the mismatch is detected for that specific field.

- **reparse-and-metadata-table.AC1.3:** Create a Delta table with multiple mismatched records (>10). Run audit and verify report output includes per-field counts and exactly 10 (or fewer if total < 10) sample diffs.

- **reparse-and-metadata-table.AC1.4:** Create a Delta table with a record where `as_der` contains invalid base64 (e.g., `"not_valid_base64!!!"`). Run audit and verify it's counted as unparseable, not a mismatch.

- **reparse-and-metadata-table.AC1.5:** Create a Delta table with a record where `as_der` is valid base64 but decodes to garbage bytes that `parse_certificate()` cannot parse. Run audit and verify it's counted as unparseable.

- **reparse-and-metadata-table.AC1.6:** Run audit with `from_date`/`to_date` that don't match any partitions. Verify exit code 0 and informational message.

Use `#[tokio::test]` for all async tests. Clean up temp directories after each test.

**Verification:**
Run: `cargo test --lib table_ops::tests`
Expected: All tests pass

**Commit:** `test: add reparse audit tests for comparison and edge cases`
<!-- END_TASK_2 -->
