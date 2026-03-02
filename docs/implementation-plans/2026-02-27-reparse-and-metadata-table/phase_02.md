# Reparse Audit & Metadata Table Implementation Plan

**Goal:** Add two new batch execution modes (`--reparse-audit` and `--extract-metadata`) for auditing stored certificates against current parsing code and extracting metadata-only Delta tables.

**Architecture:** New `src/table_ops.rs` module with two entry points (`run_reparse_audit`, `run_extract_metadata`) dispatched from the existing CLI if/else chain in `main.rs`. Both read the Delta table via DataFusion SQL, process partition-by-partition, and exit with a status code.

**Tech Stack:** Rust, Tokio, DataFusion, deltalake 0.25, Arrow

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-27

---

## Acceptance Criteria Coverage

This phase implements and tests:

### reparse-and-metadata-table.AC3: Date range filtering works for both commands
- **reparse-and-metadata-table.AC3.1 Success:** `--from-date` and `--to-date` together filter to inclusive date range
- **reparse-and-metadata-table.AC3.2 Success:** `--from-date` alone filters from that date to latest partition
- **reparse-and-metadata-table.AC3.3 Success:** `--to-date` alone filters from earliest partition to that date
- **reparse-and-metadata-table.AC3.4 Success:** Omitting both flags processes all partitions

### reparse-and-metadata-table.AC2: Metadata extraction produces correct metadata-only table (partial)
- **reparse-and-metadata-table.AC2.1 Success:** Output Delta table contains exactly 19 columns (all columns from source except `as_der`)

---

## Phase 2: Shared Infrastructure

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Add date_filter_clause helper and metadata_schema function

**Verifies:** reparse-and-metadata-table.AC3.1, reparse-and-metadata-table.AC3.2, reparse-and-metadata-table.AC3.3, reparse-and-metadata-table.AC3.4, reparse-and-metadata-table.AC2.1

**Files:**
- Modify: `src/table_ops.rs` (add helper functions after existing stub functions)

**Implementation:**

Add the following imports to the top of `src/table_ops.rs` (replacing existing minimal imports):

```rust
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::Config;
```

Add a `date_filter_clause` helper function that builds a SQL WHERE clause fragment from optional date bounds. This follows the exact pattern from `src/query.rs:321-329` where `seen_date >= '...'` and `seen_date <= '...'` are appended:

```rust
/// Validate that a date string matches the expected YYYY-MM-DD format.
/// Returns true if the format is valid, false otherwise.
fn is_valid_date_format(date: &str) -> bool {
    if date.len() != 10 {
        return false;
    }
    let bytes = date.as_bytes();
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[0..4].iter().all(|b| b.is_ascii_digit())
        && bytes[5..7].iter().all(|b| b.is_ascii_digit())
        && bytes[8..10].iter().all(|b| b.is_ascii_digit())
}

/// Build a SQL WHERE clause fragment for filtering by seen_date partition.
/// Returns an empty string if no date bounds are provided, or a string
/// starting with " WHERE " containing the appropriate conditions.
/// Returns Err if any provided date does not match YYYY-MM-DD format.
pub fn date_filter_clause(from_date: &Option<String>, to_date: &Option<String>) -> Result<String, String> {
    let mut conditions = Vec::new();

    if let Some(ref from) = from_date {
        if !is_valid_date_format(from) {
            return Err(format!("invalid --from-date format '{}', expected YYYY-MM-DD", from));
        }
        conditions.push(format!("seen_date >= '{}'", from));
    }
    if let Some(ref to) = to_date {
        if !is_valid_date_format(to) {
            return Err(format!("invalid --to-date format '{}', expected YYYY-MM-DD", to));
        }
        conditions.push(format!("seen_date <= '{}'", to));
    }

    if conditions.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(" WHERE {}", conditions.join(" AND ")))
    }
}
```

**Note on return type change:** `date_filter_clause` returns `Result<String, String>` to enforce date format validation. Callers in Phases 3-5 should handle the `Err` case by logging the error and returning exit code 1. This prevents SQL injection through malformed date strings.

Add a `metadata_schema` function that returns a 19-column Arrow schema identical to `delta_schema()` from `src/delta_sink.rs:134-169` but with the `as_der` field (field 19, between `all_domains` and `chain`) removed:

```rust
/// Return the 19-column Arrow schema for the metadata-only Delta table.
/// Identical to delta_schema() with the `as_der` column removed.
pub fn metadata_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("cert_index", DataType::UInt64, false),
        Field::new("update_type", DataType::Utf8, false),
        Field::new(
            "seen",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("seen_date", DataType::Utf8, false),
        Field::new("source_name", DataType::Utf8, false),
        Field::new("source_url", DataType::Utf8, false),
        Field::new("cert_link", DataType::Utf8, false),
        Field::new("serial_number", DataType::Utf8, false),
        Field::new("fingerprint", DataType::Utf8, false),
        Field::new("sha256", DataType::Utf8, false),
        Field::new("sha1", DataType::Utf8, false),
        Field::new("not_before", DataType::Int64, false),
        Field::new("not_after", DataType::Int64, false),
        Field::new("is_ca", DataType::Boolean, false),
        Field::new("signature_algorithm", DataType::Utf8, false),
        Field::new("subject_aggregated", DataType::Utf8, false),
        Field::new("issuer_aggregated", DataType::Utf8, false),
        Field::new(
            "all_domains",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "chain",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]))
}
```

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat: add date_filter_clause helper and metadata_schema function`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add unit tests for date_filter_clause and metadata_schema

**Verifies:** reparse-and-metadata-table.AC3.1, reparse-and-metadata-table.AC3.2, reparse-and-metadata-table.AC3.3, reparse-and-metadata-table.AC3.4, reparse-and-metadata-table.AC2.1

**Files:**
- Modify: `src/table_ops.rs` (add `#[cfg(test)] mod tests` block at end of file)

**Testing:**

Tests must verify each AC listed above:
- reparse-and-metadata-table.AC3.1: `date_filter_clause` with both `from_date` and `to_date` returns `Ok(" WHERE seen_date >= '2026-02-01' AND seen_date <= '2026-02-28'")`
- reparse-and-metadata-table.AC3.2: `date_filter_clause` with only `from_date` returns `Ok(" WHERE seen_date >= '2026-02-01'")`
- reparse-and-metadata-table.AC3.3: `date_filter_clause` with only `to_date` returns `Ok(" WHERE seen_date <= '2026-02-28'")`
- reparse-and-metadata-table.AC3.4: `date_filter_clause` with both None returns `Ok("")`
- Date format validation: `date_filter_clause` with invalid format like `"not-a-date"` or `"2026/02/01"` returns `Err` with descriptive message
- reparse-and-metadata-table.AC2.1: `metadata_schema` returns schema with exactly 19 fields, no `as_der` field present, `chain` field present, and field order matches delta_schema minus as_der

Follow the existing test pattern: inline `#[cfg(test)] mod tests` at end of file, synchronous `#[test]` for pure functions. Compare against `crate::delta_sink::delta_schema()` to verify the 19 fields match the 20-field schema minus `as_der`.

**Verification:**
Run: `cargo test --lib table_ops::tests`
Expected: All tests pass

**Commit:** `test: add unit tests for date_filter_clause and metadata_schema`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->
