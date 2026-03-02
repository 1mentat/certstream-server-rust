# Delta Encoding Optimization — Phase 4: Integration Verification

**Goal:** Verify all write paths produce valid tables with the new schema (Binary `as_der`) and optimized WriterProperties encoding settings through end-to-end tests.

**Architecture:** Integration tests exercise each write path (live sink via `flush_buffer`, backfill writer, merge, migration tool) and verify the output table has correct schema, compression, dictionary encoding, and readable data. The Query API is verified against a table written with the new schema. Tests use the existing Parquet metadata inspection pattern from `test_flush_buffer_writes_zstd_compression` and extend it to check per-column encoding.

**Tech Stack:** Rust, cargo test, DataFusion (read-back verification), parquet crate (SerializedFileReader for metadata inspection), deltalake

**Scope:** 4 phases from original design (phase 4 of 4)

**Codebase verified:** 2026-02-27

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-encoding-opt.AC4: Configuration
- **delta-encoding-opt.AC4.1 Success:** heavy_column_compression_level defaults to 15, configurable via CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL
- **delta-encoding-opt.AC4.2 Failure:** Invalid heavy_column_compression_level (outside 1-22) rejected at config validation

### delta-encoding-opt.AC5: No regression
- **delta-encoding-opt.AC5.1 Success:** Query API returns correct results against tables written with new schema
- **delta-encoding-opt.AC5.2 Success:** All existing tests pass after changes
- **delta-encoding-opt.AC5.3 Success:** ZeroBus sink continues to function (protobuf as_der field populated correctly)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
## Subcomponent A: WriterProperties and Schema Verification

<!-- START_TASK_1 -->
### Task 1: Verify per-column encoding in Parquet output

**Verifies:** delta-encoding-opt.AC4.1, delta-encoding-opt.AC4.2

**Files:**
- Modify: `src/delta_sink.rs` (test module, after existing `test_flush_buffer_writes_zstd_compression` at line 2056)

**Implementation:**

Add a new integration test that extends the existing `test_flush_buffer_writes_zstd_compression` pattern. The test should:

1. Create a temp directory and table with `open_or_create_table()`
2. Create and flush test records using the new `delta_writer_properties()` function (from Phase 1) — this requires updating `flush_buffer` to accept WriterProperties or using the new signature from Phase 1
3. Find the Parquet file in the table directory (reuse the `find_parquet_file` helper from `test_flush_buffer_writes_zstd_compression`, lines 2004-2023)
4. Open with `SerializedFileReader` and read metadata
5. For each column in the row group, verify:
   - Dictionary encoding columns (`update_type`, `source_name`, `source_url`, `signature_algorithm`, `issuer_aggregated`) have dictionary pages
   - High-cardinality columns (`fingerprint`, `sha256`, `sha1`, `serial_number`, `as_der`, `subject_aggregated`, `cert_link`) do NOT have dictionary pages
   - `as_der` column uses ZSTD with `heavy_column_compression_level` (15)
   - Other columns use ZSTD with `compression_level` (9)

**Parquet metadata inspection approach:**

Use the existing pattern at `src/delta_sink.rs:2028-2052`:
```rust
let reader = SerializedFileReader::new(file).expect("...");
let metadata = reader.metadata();
let row_group = metadata.row_group(0);

for col_idx in 0..row_group.num_columns() {
    let col_meta = row_group.column(col_idx);
    let col_path = col_meta.column_path().string();
    let compression = col_meta.compression();
    // Verify compression type matches expectations per column
}
```

To check dictionary encoding, inspect `col_meta.encodings()` — columns with dictionary encoding will include `Encoding::RLE_DICTIONARY` or `Encoding::PLAIN_DICTIONARY` in their encoding list.

**Testing:**

- Verify `as_der` column has ZSTD compression (separate level verification is implicit — the WriterProperties builder in Phase 1 sets it)
- Verify a dictionary-enabled column (e.g., `update_type`) has dictionary encoding in its encodings
- Verify a high-cardinality column (e.g., `fingerprint`) does NOT have dictionary encoding
- Verify column count matches the 20-column schema

**Verification:**
Run: `cargo test test_writer_properties_per_column_encoding`
Expected: Test passes

**Commit:** `test: verify per-column dictionary encoding and compression in Parquet output`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Verify Binary as_der with optimized WriterProperties round-trip

**Verifies:** delta-encoding-opt.AC5.2

**Files:**
- Modify: `src/delta_sink.rs` (test module — add or extend round-trip test)

**Implementation:**

Phase 2 Task 3 adds a basic Binary as_der round-trip test (AC2.2). This task is distinct: it verifies the round-trip works with the **full optimized pipeline** — Phase 1's `delta_writer_properties()` (dictionary encoding + per-column compression) combined with Phase 2's Binary schema. This catches any interaction between dictionary encoding settings and Binary column type.

**If Phase 2's round-trip test already uses `delta_writer_properties()`:** This task is a verification step — re-run it and confirm it passes. Mark as complete without writing new code.

**If Phase 2's round-trip test uses a plain WriterProperties (just zstd):** Extend it or add a companion test that:

1. Creates a DeltaCertRecord with known `as_der: vec![0xDE, 0xAD, 0xBE, 0xEF]`
2. Writes via `flush_buffer()` to a temp Delta table — using the new signature with both `compression_level` and `heavy_column_compression_level`
3. Re-opens table and reads back via DataFusion SQL: `SELECT as_der FROM ct_records`
4. Downcasts the result column to `BinaryArray`
5. Asserts the bytes match `[0xDE, 0xAD, 0xBE, 0xEF]`

**Testing:**

Follow the `test_flush_buffer_writes_zstd_compression` pattern (lines 1973-2056):
- Create temp table, flush records with both compression levels, read back via DataFusion, verify values

**Verification:**
Run: `cargo test test_binary_as_der`
Expected: Test passes

**Commit:** `test: verify Binary as_der round-trip with optimized WriterProperties`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->
## Subcomponent B: Write Path Integration Tests

<!-- START_TASK_3 -->
### Task 3: Verify backfill and merge paths with new schema

**Verifies:** delta-encoding-opt.AC5.2

**Files:**
- Modify: `src/backfill.rs` (test module — add integration tests)

**Implementation:**

Add integration tests that verify the backfill writer and merge paths work correctly with the new schema:

**Test 1: Backfill writer produces Binary as_der**

1. Create an mpsc channel and spawn `run_writer()` (following pattern at `src/backfill.rs:1825-1885`)
2. Send DeltaCertRecord with known `as_der: vec![1, 2, 3]` via the channel
3. Drop the sender to signal completion
4. Re-open the output table, read via DataFusion
5. Verify `as_der` column is Binary type with correct values

**Test 2: Merge path handles Binary as_der**

1. Create a main table and staging table with Binary `as_der` data (following pattern at `src/backfill.rs:2581-2655`)
2. Run `run_merge()` with the two tables
3. Verify merged table has correct data and `as_der` is still Binary type
4. Verify no data corruption — all `as_der` values match originals

These tests follow existing patterns in `src/backfill.rs` for table creation, writer testing, and merge verification.

**Testing:**

Use existing helper functions `make_test_record()` and `make_test_config()` (updated in Phase 2 to use `Vec<u8>` for `as_der`).

**Verification:**
Run: `cargo test --lib backfill`
Expected: All tests pass (existing + new)

**Commit:** `test: verify backfill writer and merge with Binary as_der schema`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Verify Query API against new-schema tables

**Verifies:** delta-encoding-opt.AC5.1

**Files:**
- Modify: `src/query.rs` (test module — add or verify integration test)

**Implementation:**

The Query API explicitly excludes `as_der` from its SELECT columns (`src/query.rs:277-279`), so it doesn't read the `as_der` column at all. The schema change from Utf8 to Binary should be transparent to the Query API. However, we need to verify this.

Add a test that:

1. Creates a Delta table with the new schema (Binary `as_der`) using `records_to_batch()` + `DeltaOps::write()`
2. Follows the existing pattern from query.rs test helpers (`create_test_cert_record` at line 638)
3. Runs a query via the Query API (domain search, issuer search, or date range)
4. Verifies results are returned correctly — correct cert_index, fingerprint, domains, etc.
5. Confirms the query did NOT fail due to schema mismatch (Binary vs Utf8 on as_der)

The key verification is that the Query API's DataFusion SQL query works correctly against a table where `as_der` is Binary, even though the query never selects `as_der`.

**Testing:**

Follow the existing test pattern in `src/query.rs:740-821` (`test_response_includes_required_fields`):
- Create table with `open_or_create_table()` + `DeltaOps::write()`
- Execute a search query
- Verify the 12 response fields are correct

**Verification:**
Run: `cargo test --lib query`
Expected: All tests pass

**Commit:** `test: verify Query API works against Binary as_der tables`
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_B -->

<!-- START_SUBCOMPONENT_C (tasks 5-6) -->
## Subcomponent C: Full Verification and Documentation

<!-- START_TASK_5 -->
### Task 5: Run full test suite and verify no regressions

**Verifies:** delta-encoding-opt.AC5.2, delta-encoding-opt.AC5.3

**Files:**
- No new files

**Implementation:**

Run the complete test suite to verify all existing tests still pass after all Phase 1-4 changes:

```bash
cargo test
```

All tests must pass. If any tests fail:
1. Identify the failure
2. If it's a test that was missed during Phase 2 (test updates for Vec<u8> as_der), fix it
3. If it's a genuine regression, fix the underlying code
4. Re-run until all tests pass

**Verification:**
Run: `cargo test`
Expected: All tests pass (0 failures)

Also verify the build is clean:
Run: `cargo build`
Expected: No warnings related to the changes (existing warnings are acceptable)

**Commit:** `test: verify full test suite passes with delta encoding optimizations`
<!-- END_TASK_5 -->

<!-- START_TASK_6 -->
### Task 6: Update CLAUDE.md with new contracts

**Verifies:** None (documentation)

**Files:**
- Modify: `CLAUDE.md` (Delta Sink Contracts section, Commands section, Project Structure section)

**Implementation:**

Update the project documentation to reflect all changes made in Phases 1-4:

1. **Delta Sink Contracts section** — update:
   - `DeltaSinkConfig` field list: add `heavy_column_compression_level`
   - `Compression` bullet: mention `heavy_column_compression_level` (i32, default 15, range 1-22) configurable via `CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL`
   - `Public helpers` bullet: add `delta_writer_properties()` function
   - `flush_buffer` signature: update to include `heavy_column_compression_level` parameter
   - `Schema` bullet: note `as_der` is `DataType::Binary` (raw DER bytes, not base64)

2. **Commands section** — add:
   - `cargo run -- --migrate --output <PATH>` - Migrate Delta table to new schema

3. **Project Structure section** — no changes needed (files stay the same)

4. **Architecture section** — add brief note that `as_der` is stored as raw binary bytes in Delta, not base64

**Verification:**
Read CLAUDE.md and confirm all new contracts are documented accurately.

**Commit:** `docs: update CLAUDE.md with delta encoding optimization contracts`
<!-- END_TASK_6 -->
<!-- END_SUBCOMPONENT_C -->
