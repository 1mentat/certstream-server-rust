# Zstd Compression Implementation Plan — Phase 2

**Goal:** Apply zstd compression to all Delta Lake write and merge operations

**Architecture:** `flush_buffer()` gains a `compression_level` parameter and constructs `WriterProperties` with `Compression::ZSTD`. `run_merge()` does the same for its merge builder. All call sites thread the value from `DeltaSinkConfig`.

**Tech Stack:** Rust, deltalake 0.25 (`DeltaOps` write/merge builders), parquet 54.x (`WriterProperties`, `Compression`, `ZstdLevel`)

**Scope:** 3 phases from original design (phases 1-3)

**Codebase verified:** 2026-02-20

---

## Acceptance Criteria Coverage

This phase implements and tests:

### zstd-compression.AC2: All Delta writes use zstd compression
- **zstd-compression.AC2.1 Success:** Live delta_sink writes Parquet files with ZSTD codec at configured level
- **zstd-compression.AC2.2 Success:** Backfill writer writes Parquet files with ZSTD codec at configured level
- **zstd-compression.AC2.3 Success:** Merge operation writes Parquet files with ZSTD codec at configured level
- **zstd-compression.AC2.4 Success:** Existing snappy-compressed files remain readable after new zstd files are written to the same table

---

<!-- START_SUBCOMPONENT_A (tasks 1-4) -->

<!-- START_TASK_1 -->
### Task 1: Add compression_level parameter to flush_buffer() and construct WriterProperties

**Files:**
- Modify: `src/delta_sink.rs` (imports at ~line 1-16, `flush_buffer()` signature at ~line 472, write builder at ~line 503-506)

**Implementation:**

**New imports** — add at the top of `src/delta_sink.rs` alongside existing deltalake imports:

```rust
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
```

**Update `flush_buffer()` signature** — add `compression_level: i32` as the last parameter. Current signature at line 472:

```rust
pub async fn flush_buffer(
    table: DeltaTable,
    buffer: &mut Vec<DeltaCertRecord>,
    schema: &Arc<Schema>,
    batch_size: usize,
    compression_level: i32,
) -> (Option<DeltaTable>, Result<usize, Box<dyn std::error::Error + Send + Sync>>)
```

**Construct WriterProperties** — add inside `flush_buffer()`, before the `DeltaOps(table).write(...)` call at line 503. Use `.expect()` since the level was validated at startup:

```rust
let writer_props = WriterProperties::builder()
    .set_compression(Compression::ZSTD(
        ZstdLevel::try_new(compression_level).expect("compression level validated at startup"),
    ))
    .build();
```

**Chain on write builder** — update the `DeltaOps` write call (currently at lines 503-506) to include `.with_writer_properties()`:

```rust
let result = DeltaOps(table)
    .write(vec![batch])
    .with_save_mode(SaveMode::Append)
    .with_writer_properties(writer_props)
    .await;
```

**Also update existing test call sites** in `src/delta_sink.rs`. There are 3 existing tests that call `flush_buffer()` (at ~lines 1310, 1364, 1526). Add `9` (or any valid level) as the `compression_level` argument to each:

```rust
// Example: update each existing flush_buffer() test call from:
flush_buffer(table, &mut buffer, &schema, batch_size).await
// to:
flush_buffer(table, &mut buffer, &schema, batch_size, 9).await
```

**Do not commit yet** — Task 2 fixes the remaining call sites in `run_delta_sink()` and `backfill.rs`.
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update all flush_buffer() call sites, run_writer() signature, and test call sites

**Verifies:** zstd-compression.AC2.1, zstd-compression.AC2.2

**Files:**
- Modify: `src/delta_sink.rs` (`run_delta_sink()` — 3 call sites at ~lines 686, 731, 755)
- Modify: `src/backfill.rs` (`run_writer()` signature at ~line 434, spawn call at ~line 695, 4 `flush_buffer()` call sites at ~lines 485, 503, 510, 526, and 9 test call sites of `run_writer()` at ~lines 1628, 1670, 1738, 1782, 1844, 1882, 1917, 2063, 3038)

**Implementation:**

**src/delta_sink.rs — run_delta_sink():**

In `run_delta_sink()`, the `config` parameter is `DeltaSinkConfig` which now has `compression_level`. Add `config.compression_level` as the last argument to all 3 `flush_buffer()` calls:

Line ~686 (batch size trigger):
```rust
let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, config.batch_size, config.compression_level).await;
```

Line ~731 (timer trigger):
```rust
let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, config.batch_size, config.compression_level).await;
```

Line ~755 (graceful shutdown):
```rust
let (table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, config.batch_size, config.compression_level).await;
```

**src/backfill.rs — run_writer() signature:**

The `run_writer()` function at ~line 434 receives `batch_size` as a parameter and passes it to `flush_buffer()`. Add `compression_level: i32` as a new parameter:

```rust
async fn run_writer(
    // ... existing params ...
    batch_size: usize,
    compression_level: i32,  // ADD THIS
    // ... remaining params ...
) -> ...
```

**src/backfill.rs — run_writer() spawn call:**

Find where `run_writer()` is called/spawned (~line 695). Add `config.delta_sink.compression_level` as the corresponding argument.

**src/backfill.rs — flush_buffer() call sites inside run_writer():**

Update all 4 `flush_buffer()` calls inside `run_writer()` to pass `compression_level`:

Line ~485 (batch size threshold):
```rust
let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size, compression_level).await;
```

Line ~503 (channel close):
```rust
let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size, compression_level).await;
```

Line ~510 (time trigger):
```rust
let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size, compression_level).await;
```

Line ~526 (graceful shutdown):
```rust
let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size, compression_level).await;
```

**src/backfill.rs — test call sites of run_writer():**

There are 9 test call sites of `run_writer()` in the test module (~lines 1628, 1670, 1738, 1782, 1844, 1882, 1917, 2063, 3038). Each needs the new `compression_level` argument. Add `9` (a valid default) at the position matching the new parameter:

Search for all calls to `run_writer(` in `src/backfill.rs` and add the `compression_level` argument (value `9`) in the correct position after `batch_size`.

**Verification:**

Run: `cargo test --no-run`
Expected: Compiles without errors (both production and test code)

**Commit:** `feat: add zstd compression to flush_buffer and all call sites`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Update run_merge() with WriterProperties

**Verifies:** zstd-compression.AC2.3

**Files:**
- Modify: `src/backfill.rs` (imports, `run_merge()` merge builder at ~lines 869-903)

**Implementation:**

**Add imports** to `src/backfill.rs` (if not already present from Task 2):

```rust
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
```

**Construct WriterProperties** inside `run_merge()`, before the merge builder chain. Access the compression level via `config.delta_sink.compression_level`:

```rust
let writer_props = WriterProperties::builder()
    .set_compression(Compression::ZSTD(
        ZstdLevel::try_new(config.delta_sink.compression_level)
            .expect("compression level validated at startup"),
    ))
    .build();
```

**Chain `.with_writer_properties()` on the merge builder.** The current code structure is:

```rust
let merge_builder = match DeltaOps(current_table)
    .merge(source_df, predicate)
    .with_source_alias("source")
    .with_target_alias("target")
    .when_not_matched_insert(|insert| { /* ... */ }) {
        Ok(builder) => builder,
        Err(e) => { /* error handling */ }
    };

match merge_builder.await { /* ... */ }
```

Add `.with_writer_properties()` in the `Ok` arm so it's applied before `.await`:

```rust
    Ok(builder) => builder.with_writer_properties(writer_props.clone()),
```

`WriterProperties` implements `Clone`. Construct `writer_props` once before the batch loop (outside the `for` loop) and use `writer_props.clone()` in each iteration, since `with_writer_properties` takes ownership.

**Verification:**

Run: `cargo check`
Expected: Compiles without errors

**Commit:** `feat: add zstd compression to merge write path`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Test verifying ZSTD codec in Parquet file metadata

**Verifies:** zstd-compression.AC2.1, zstd-compression.AC2.4

**Files:**
- Modify: `src/delta_sink.rs` (add test in existing `#[cfg(test)] mod tests` block)

**Testing:**

Add a test to the existing `mod tests` block in `src/delta_sink.rs`. The test must:

1. Create test records using the existing `make_test_record()` helper
2. Create a temp Delta table at a `/tmp/delta_zstd_test_*` path (following the existing pattern in `src/backfill.rs` tests)
3. Call `flush_buffer()` with `compression_level = 9`
4. Find the resulting Parquet file(s) in the table directory (look in the table_path for `*.parquet` files, excluding the `_delta_log/` directory)
5. Read the Parquet file metadata using `parquet::file::reader::SerializedFileReader`
6. Verify that the first row group's first column uses ZSTD compression

The test verifies AC2.1 (writes with ZSTD codec) and implicitly AC2.4 (the Delta table infrastructure handles the files — mixed-codec coexistence is a Delta Lake property, not something we need to test explicitly).

Additional imports needed in the test module:
```rust
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::basic::Compression;
use std::fs;
```

Key verification logic:
```rust
// Find parquet files in table directory (not in _delta_log)
// Open with SerializedFileReader
// Get metadata: reader.metadata()
// Check: metadata.row_group(0).column(0).compression()
// Assert it matches Compression::ZSTD(_)
```

Follow existing test patterns: use `#[tokio::test]`, cleanup temp dir before and after test, use `let _ = fs::remove_dir_all(...)` for cleanup.

**Verification:**

Run: `cargo test test_flush_buffer_writes_zstd_compression` (or whatever you name the test)
Expected: Test passes

**Commit:** `test: verify flush_buffer writes Parquet files with ZSTD codec`
<!-- END_TASK_4 -->

<!-- END_SUBCOMPONENT_A -->
