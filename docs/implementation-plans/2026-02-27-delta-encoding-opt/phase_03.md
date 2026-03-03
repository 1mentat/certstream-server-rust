# Delta Encoding Optimization — Phase 3: Table Migration Tool

**Goal:** Add a `--migrate --output <path>` CLI mode that reads an existing Delta table with old schema (base64 Utf8 `as_der`), converts `as_der` to raw Binary bytes, and writes to a new output table with the updated schema and optimized WriterProperties.

**Architecture:** The migration follows the existing CLI mode pattern (like `--merge`): a new `run_migrate()` function in `src/backfill.rs` reads the source table via DataFusion, transforms `as_der` from base64 string to raw bytes per partition, and writes each partition to the output table using `DeltaOps::write()`. The output table uses the new schema (Phase 2) and centralized WriterProperties (Phase 1). The source table is never modified.

**Tech Stack:** Rust, DataFusion (SQL queries), deltalake (DeltaOps::write), base64 (STANDARD decode), Arrow (StringArray → BinaryArray conversion)

**Scope:** 4 phases from original design (phase 3 of 4)

**Codebase verified:** 2026-02-27

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-encoding-opt.AC3: Table migration
- **delta-encoding-opt.AC3.1 Success:** --migrate --output <path> reads source table and writes new table with Binary as_der at output path
- **delta-encoding-opt.AC3.2 Success:** Migrated as_der values match STANDARD.decode() of original base64 strings
- **delta-encoding-opt.AC3.3 Success:** All non-as_der columns pass through unchanged
- **delta-encoding-opt.AC3.4 Failure:** Graceful shutdown (CancellationToken) leaves completed partitions in output table and does not corrupt data

### delta-encoding-opt.AC5: No regression
- **delta-encoding-opt.AC5.2 Success:** All existing tests pass after changes

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
## Subcomponent A: CLI Flags, Dispatch, and Migration Function

<!-- START_TASK_1 -->
### Task 1: Add --migrate and --output CLI flags

**Verifies:** delta-encoding-opt.AC3.1 (partial — CLI parsing only)

**Files:**
- Modify: `src/cli.rs:6-18` (CliArgs struct — add `migrate` and `migrate_output` fields)
- Modify: `src/cli.rs:29-32` (parse_args — add `migrate_output` variable)
- Modify: `src/cli.rs:34-46` (parse loop — add `--output` parsing)
- Modify: `src/cli.rs:48-60` (Self construction — add `migrate` and `migrate_output`)
- Modify: `src/cli.rs:89-91` (help text — add MIGRATION OPTIONS section)
- Modify: `src/cli.rs:160-341` (test module — add new tests)

**Implementation:**

**1. Add fields to CliArgs struct** (`src/cli.rs:17`, after `merge`):

```rust
pub migrate: bool,
pub migrate_output: Option<String>,
```

**2. Add variable** (`src/cli.rs:32`, after `backfill_sink`):

```rust
let mut migrate_output = None;
```

**3. Add parsing** (`src/cli.rs:44-45`, after `--sink` branch):

```rust
} else if arg == "--output" && i + 1 < args.len() {
    migrate_output = Some(args[i + 1].clone());
}
```

**4. Add to Self construction** (`src/cli.rs:59`, after `merge`):

```rust
migrate: args.iter().any(|a| a == "--migrate"),
migrate_output,
```

**5. Add help text** (`src/cli.rs:91`, after STAGING/MERGE OPTIONS `println!()` block):

```rust
println!("MIGRATION OPTIONS:");
println!("    --migrate              Migrate existing Delta table to new schema");
println!("    --output <PATH>        Output path for migrated table (required with --migrate)");
println!();
```

**Testing:**

Tests must verify:
- `--migrate` flag is parsed correctly as `migrate: true`
- `--output /tmp/output` is parsed correctly as `migrate_output: Some("/tmp/output".to_string())`
- Without `--migrate`, `migrate` field is `false`
- Without `--output`, `migrate_output` field is `None`
- Both flags together parse correctly

Follow the existing test pattern in `src/cli.rs:160-341` (e.g., `test_ac3_2_backfill_sink_none_when_not_provided`).

**Verification:**
Run: `cargo test --lib cli`
Expected: All tests pass

**Commit:** `feat: add --migrate and --output CLI flags`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement run_migrate() and add dispatch to main.rs

**Verifies:** delta-encoding-opt.AC3.1, delta-encoding-opt.AC3.2, delta-encoding-opt.AC3.3, delta-encoding-opt.AC3.4

**Files:**
- Modify: `src/backfill.rs:1-21` (add `base64` import)
- Modify: `src/backfill.rs` (add `run_migrate()` function after `run_merge()` — after line 1122)
- Modify: `src/delta_sink.rs` (make `delta_writer_properties` public if not already — depends on Phase 1 output)
- Modify: `src/main.rs:81-105` (add migrate block before merge block)

**Implementation:**

**0. Add migrate dispatch to main.rs** (`src/main.rs:81`, before merge block):

Add a new mode dispatch block before the merge check. Follow the exact pattern of the merge block (lines 81-105):

```rust
if cli_args.migrate {
    if cli_args.migrate_output.is_none() {
        eprintln!("Error: --migrate requires --output <PATH>");
        std::process::exit(1);
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.log_level)),
        )
        .init();

    let shutdown_token = CancellationToken::new();
    spawn_signal_handler(shutdown_token.clone());

    let exit_code = backfill::run_migrate(
        config,
        cli_args.migrate_output.unwrap(),
        shutdown_token,
    )
    .await;

    std::process::exit(exit_code);
}
```

**1. Add base64 import** (`src/backfill.rs`, near the top imports):

```rust
use base64::{engine::general_purpose::STANDARD, Engine};
```

**2. Add `run_migrate()` function** after `run_merge()` (after line 1122).

The function follows the `run_merge()` pattern but instead of merging, it:
1. Opens the source table at `config.delta_sink.table_path`
2. Creates or opens the output table at `output_path` with the new schema (from `delta_schema()`)
3. Queries distinct `seen_date` partitions from the source table
4. For each partition:
   - Checks CancellationToken (AC3.4)
   - Reads all records for that partition via DataFusion SQL
   - For each RecordBatch: transforms the `as_der` column from StringArray (base64) to BinaryArray (raw bytes) while passing all other columns through unchanged (AC3.3)
   - Writes the transformed batch to the output table via `DeltaOps::write()` with the centralized WriterProperties from Phase 1
5. Returns exit code 0 on success, 1 on error

**Function signature:**

```rust
pub async fn run_migrate(
    config: Config,
    output_path: String,
    shutdown: CancellationToken,
) -> i32
```

**Key implementation details:**

**Partition discovery:** Query the source table for distinct partition values:
```rust
let ctx = SessionContext::new();
ctx.register_table("source", Arc::new(source_table))?;
let partitions_df = ctx.sql("SELECT DISTINCT seen_date FROM source ORDER BY seen_date").await?;
let partition_batches = partitions_df.collect().await?;
```

**Per-partition read:**

Note: `seen_date` values come from the source table's own data (a `SELECT DISTINCT` above), not from external user input. The format is always `YYYY-MM-DD`. String interpolation is safe here — no SQL injection risk since the data is self-sourced. An alternative is to use DataFusion's DataFrame API with `col("seen_date").eq(lit(seen_date))` to avoid string interpolation entirely.

```rust
let partition_df = ctx.sql(&format!(
    "SELECT * FROM source WHERE seen_date = '{}'", seen_date
)).await?;
let batches = partition_df.collect().await?;
```

**as_der column transformation** — for each RecordBatch, find the `as_der` column index, downcast to `StringArray`, decode each base64 value to bytes, build a new `BinaryArray`, and replace the column in the batch:

```rust
let as_der_idx = batch.schema().index_of("as_der").expect("as_der column must exist");
let as_der_strings = batch.column(as_der_idx)
    .as_any()
    .downcast_ref::<StringArray>()
    .expect("source as_der should be StringArray");

let mut decode_failures: usize = 0;
let as_der_binary: BinaryArray = as_der_strings
    .iter()
    .map(|opt_val| {
        opt_val.and_then(|s| {
            match STANDARD.decode(s) {
                Ok(bytes) => Some(bytes),
                Err(_) => {
                    decode_failures += 1;
                    None // becomes empty bytes
                }
            }
        })
    })
    .collect();

if decode_failures > 0 {
    warn!(
        partition = %seen_date,
        decode_failures = decode_failures,
        "base64 decode failures in as_der column (converted to empty bytes)"
    );
}
```

Then build the new RecordBatch with the new schema (from `delta_schema()` — which uses `DataType::Binary` for `as_der` after Phase 2) and all original columns except `as_der` replaced by the binary version:

```rust
let new_schema = delta_schema();
let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.num_columns());
for (i, _field) in batch.schema().fields().iter().enumerate() {
    if i == as_der_idx {
        columns.push(Arc::new(as_der_binary.clone()));
    } else {
        columns.push(batch.column(i).clone());
    }
}
let new_batch = RecordBatch::try_new(new_schema.clone(), columns)?;
```

**Write to output table:**
```rust
use crate::delta_sink::delta_writer_properties;

let writer_props = delta_writer_properties(
    config.delta_sink.compression_level,
    config.delta_sink.heavy_column_compression_level,
);

let result = DeltaOps(output_table)
    .write(vec![new_batch])
    .with_save_mode(SaveMode::Append)
    .with_writer_properties(writer_props.clone())
    .await;
```

**CancellationToken check** (AC3.4) — between partitions:
```rust
for seen_date in &partition_dates {
    if shutdown.is_cancelled() {
        warn!("migration interrupted by shutdown signal");
        return 1;
    }
    // ... process partition ...
}
```

**Progress logging:**
```rust
info!(
    partition = %seen_date,
    rows = batch_rows,
    partition_num = idx + 1,
    total_partitions = partition_dates.len(),
    "migrated partition"
);
```

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat: add --migrate CLI mode for Delta table schema migration`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (task 3) -->
## Subcomponent B: Migration Tests

<!-- START_TASK_3 -->
### Task 3: Tests for migration tool

**Verifies:** delta-encoding-opt.AC3.1, delta-encoding-opt.AC3.2, delta-encoding-opt.AC3.3, delta-encoding-opt.AC3.4, delta-encoding-opt.AC5.2

**Files:**
- Modify: `src/backfill.rs` (test module — add migration tests)

**Testing:**

Follow the existing test patterns in `src/backfill.rs` and `src/delta_sink.rs`. The migration tests need a source table with old-schema data (Utf8 `as_der` with base64 strings).

**Test 1: Basic migration (AC3.1, AC3.2, AC3.3)**

1. Create a temp directory for source and output tables
2. Create a source Delta table with old schema (manually construct with `DataType::Utf8` for `as_der`)
3. Write a few test records with known base64 `as_der` values (e.g., `STANDARD.encode(&[0xDE, 0xAD, 0xBE, 0xEF])`)
4. Run `run_migrate()` with the source and output paths
5. Assert exit code is 0
6. Open the output table and read back all records
7. Assert:
   - `as_der` column is `DataType::Binary` (AC3.1)
   - `as_der` values match `STANDARD.decode()` of the original base64 strings (AC3.2)
   - All other columns (cert_index, update_type, fingerprint, etc.) match the source values exactly (AC3.3)

To create the old-schema source table, construct a schema with `Field::new("as_der", DataType::Utf8, false)` and use `StringArray` for `as_der` when building the RecordBatch. Write using `DeltaOps::write()` directly to the source path.

**Test 2: Migration preserves all non-as_der columns (AC3.3)**

Similar to Test 1 but with multiple records that have different values for all 20 columns. Verify every column value matches between source and output.

**Test 3: Graceful shutdown between partitions (AC3.4)**

1. Create a source table with records in at least 2 different `seen_date` partitions
2. Create a pre-cancelled CancellationToken
3. Run `run_migrate()` with the cancelled token
4. Assert exit code is 1
5. Verify no data corruption — output table either doesn't exist or has valid data for any partitions that completed before cancellation

**Test 4: Empty source table (edge case)**

1. Create an empty source Delta table (or non-existent)
2. Run `run_migrate()`
3. Assert exit code is 0 (nothing to migrate is not an error)

**Verification:**
Run: `cargo test`
Expected: All tests pass

**Commit:** `test: add migration tool tests for schema conversion and graceful shutdown`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_B -->
