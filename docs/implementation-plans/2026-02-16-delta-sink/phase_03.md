# Delta Sink Implementation Plan - Phase 3: Delta Table Management

**Goal:** Create or open a Delta table on local filesystem with the defined schema, partitioned by `seen_date`

**Architecture:** The sink needs to either open an existing Delta table or create a new one with the Arrow schema defined in Phase 2. This is done at sink startup before the receive loop begins.

**Tech Stack:** deltalake (CreateBuilder, open_table, DeltaOps)

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-sink.AC2: Delta table is created and managed correctly
- **delta-sink.AC2.1 Success:** If table doesn't exist at `table_path`, it is created with the full Arrow schema on first flush
- **delta-sink.AC2.2 Success:** If table already exists, it is opened and appended to without schema conflict

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Implement open_or_create_table function

**Files:**
- Modify: `src/delta_sink.rs`

**Implementation:**

Add an async function to `src/delta_sink.rs`:

```rust
async fn open_or_create_table(table_path: &str, schema: &Arc<Schema>) -> Result<DeltaTable, DeltaTableError>
```

This function should:

1. **Try to open existing table first:** Use `deltalake::open_table(table_path).await`. If this succeeds, return the table.

2. **If table doesn't exist, create it:** Use `CreateBuilder`:
   ```rust
   CreateBuilder::new()
       .with_location(table_path)
       .with_columns(schema_to_delta_columns(schema))
       .with_partition_columns(vec!["seen_date"])
       .await
   ```

   The `CreateBuilder` expects delta schema columns, not Arrow schema directly. Use `deltalake::kernel::Schema::try_from(schema)` to convert from Arrow schema to Delta schema, or build columns manually.

3. **Partition configuration:** The `seen_date` column must be specified as a partition column via `.with_partition_columns(["seen_date"])`.

Key imports needed:
```rust
use deltalake::{DeltaTable, DeltaTableError, DeltaOps};
use deltalake::operations::create::CreateBuilder;
use deltalake::arrow::datatypes::Schema;
use std::sync::Arc;
```

**Note:** The exact API may vary slightly between deltalake versions. The executor should check the deltalake docs for the version installed in Phase 1. The general pattern is: try open, catch "table not found" error, create with schema and partition columns.

**Verification:**

Run: `cargo build 2>&1 | tail -3`
Expected: Build succeeds

**Commit:** `feat: add open_or_create_table for Delta table lifecycle`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Tests for table creation and reopening

**Verifies:** delta-sink.AC2.1, delta-sink.AC2.2

**Files:**
- Modify: `src/delta_sink.rs` (add to existing `#[cfg(test)] mod tests`)

**Testing:**

Tests must verify each AC listed above:
- **delta-sink.AC2.1:** Call `open_or_create_table` with a temp directory path that doesn't contain a table. Assert: table is created successfully, table schema matches the expected schema (correct number of fields, field names, field types), `seen_date` is a partition column.
- **delta-sink.AC2.2:** After creating a table (from AC2.1 test), call `open_or_create_table` again with the same path. Assert: table opens successfully without error, schema matches, table version is still 0 (no new commits from just opening).

Use temp directories following the project pattern from `src/state.rs:136-145`. Create unique temp paths (e.g., `/tmp/delta_sink_test_{test_name}`) with `std::fs::create_dir_all`, and clean up with `std::fs::remove_dir_all` at the end of each test. The `tempfile` crate is not in dependencies, so use manual cleanup. Use `#[tokio::test]` for async tests.

**Verification:**

Run: `cargo test delta_sink 2>&1 | tail -10`
Expected: All delta_sink tests pass

**Commit:** `test: add tests for Delta table creation and reopening`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->
