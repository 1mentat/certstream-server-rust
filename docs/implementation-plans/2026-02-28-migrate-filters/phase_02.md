# Migrate Filters — Phase 2: run_migrate Signature, Filtering, and Integration Tests

**Goal:** Wire the new CLI flags through `main.rs` dispatch into `run_migrate()`, add partition date filtering, and verify with integration tests.

**Architecture:** The `run_migrate()` function gains three new parameters: `source_path` (explicit source table), `from_date` (inclusive start filter), and `to_date` (inclusive end filter). All `config.delta_sink.table_path` references inside `run_migrate()` are replaced with `source_path`. After building the `partition_dates` Vec, two `.retain()` calls apply the date filters using lexicographic string comparison (YYYY-MM-DD sorts chronologically).

**Tech Stack:** Rust, tokio, deltalake, DataFusion

**Scope:** 2 phases (phase 2 of 2)

**Codebase verified:** 2026-02-28

---

## Acceptance Criteria Coverage

This phase implements and tests:

### migrate-filters.AC2: Dispatch logic in main.rs
- **migrate-filters.AC2.1 Success:** Migrate dispatch validates `--from`/`--to` date format before calling `run_migrate()`
- **migrate-filters.AC2.2 Success:** Migrate dispatch computes `source_path` from `--source` or config fallback

### migrate-filters.AC3: Partition date filtering in run_migrate
- **migrate-filters.AC3.1 Success:** `--from <DATE>` filters out partitions with `seen_date < from`
- **migrate-filters.AC3.2 Success:** `--to <DATE>` filters out partitions with `seen_date > to`
- **migrate-filters.AC3.3 Success:** `--from` and `--to` together filter to inclusive range
- **migrate-filters.AC3.4 Success:** Source path parameter replaces `config.delta_sink.table_path`
- **migrate-filters.AC3.5 Success:** Existing migrate tests pass with updated signature

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
## Subcomponent A: main.rs Dispatch and run_migrate Signature

<!-- START_TASK_1 -->
### Task 1: Update main.rs migrate dispatch to pass new parameters

**Verifies:** migrate-filters.AC2.1, migrate-filters.AC2.2

**Files:**
- Modify: `src/main.rs:81-105` (migrate dispatch block)

**Implementation:**

Update the migrate dispatch block to validate dates, compute source_path, and pass the new parameters to `run_migrate()`:

```rust
if cli_args.migrate {
    if cli_args.migrate_output.is_none() {
        eprintln!("Error: --migrate requires --output <PATH>");
        std::process::exit(1);
    }

    // Validate --from and --to date formats if provided
    if let Some(ref from_date) = cli_args.backfill_from {
        if let Err(e) = cli::validate_date_format(from_date, "--from") {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    }
    if let Some(ref to_date) = cli_args.to {
        if let Err(e) = cli::validate_date_format(to_date, "--to") {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    }

    // Compute source path: --source flag or config fallback
    let source_path = cli_args
        .migrate_source
        .unwrap_or_else(|| config.delta_sink.table_path.clone());

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
        source_path,
        cli_args.backfill_from,
        cli_args.to,
        shutdown_token,
    )
    .await;

    std::process::exit(exit_code);
}
```

**Verification:**
Run: `cargo build`
Expected: Compilation fails because `run_migrate` signature doesn't match yet — fixed in Task 2.

**Commit:** `feat: wire --source, --from, --to through migrate dispatch`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update run_migrate signature and implementation

**Verifies:** migrate-filters.AC3.1, migrate-filters.AC3.2, migrate-filters.AC3.3, migrate-filters.AC3.4

**Files:**
- Modify: `src/backfill.rs:1137-1141` (run_migrate signature)
- Modify: `src/backfill.rs:1143` (log message — source_path)
- Modify: `src/backfill.rs:1151` (source table open — use source_path)
- Modify: `src/backfill.rs:1155` (log message — source_path)
- Modify: `src/backfill.rs:1214-1219` (add partition filtering after partition_dates built)
- Modify: `src/backfill.rs:1246` (per-partition table reopen — use source_path)
- Modify: `src/backfill.rs:1392` (final log message — source_path)

**Implementation:**

**1. Update signature** (`src/backfill.rs:1137-1141`):

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

**2. Replace `config.delta_sink.table_path` with `source_path`** (5 occurrences):

- Line 1143: `source_path = %source_path` (log message)
- Line 1151: `deltalake::open_table(&source_path)`
- Line 1155: `source_path = %source_path` (log message)
- Line 1246: `deltalake::open_table(&source_path)`
- Line 1392: `source_path = %source_path` (log message)

**3. Add partition date filtering** (after line 1214, after `partition_dates` Vec is built, before the emptiness check):

```rust
// Apply date filters
if let Some(ref from) = from_date {
    partition_dates.retain(|d| d.as_str() >= from.as_str());
}
if let Some(ref to) = to_date {
    partition_dates.retain(|d| d.as_str() <= to.as_str());
}
```

Insert these lines between the `partition_dates` extraction loop (ending at line 1214) and the emptiness check (line 1216). The log message at line 1221-1224 about total partitions will then reflect the filtered count.

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat: add source_path, from_date, to_date to run_migrate with partition filtering`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->
## Subcomponent B: Test Updates and New Integration Tests

<!-- START_TASK_3 -->
### Task 3: Update existing migrate tests for new signature

**Verifies:** migrate-filters.AC3.5

**Files:**
- Modify: `src/backfill.rs:3882` (test_migrate_basic_schema_and_data — run_migrate call)
- Modify: `src/backfill.rs:3982` (test_migrate_graceful_shutdown — run_migrate call)
- Modify: `src/backfill.rs:4001` (test_migrate_nonexistent_source_table — run_migrate call)
- Modify: `src/backfill.rs:4040` (test_migrate_empty_source_table — run_migrate call)

**Implementation:**

Update all 4 existing `run_migrate` calls to pass the source_path (same as `config.delta_sink.table_path`) and `None` for date filters:

**test_migrate_basic_schema_and_data** (line 3882):
```rust
let exit_code = run_migrate(config, output_path.clone(), source_path.clone(), None, None, shutdown).await;
```

**test_migrate_graceful_shutdown** (line 3982):
```rust
let exit_code = run_migrate(config, output_path.clone(), source_path.clone(), None, None, shutdown).await;
```

**test_migrate_nonexistent_source_table** (line 4001):
```rust
let exit_code = run_migrate(config, output_path.clone(), source_path.clone(), None, None, shutdown).await;
```

**test_migrate_empty_source_table** (line 4040):
```rust
let exit_code = run_migrate(config, output_path.clone(), source_path.clone(), None, None, shutdown).await;
```

**Verification:**
Run: `cargo test --lib backfill -- test_migrate`
Expected: All 4 existing migrate tests pass

**Commit:** `test: update existing migrate tests for new run_migrate signature`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Add integration test for date filtering and source path

**Verifies:** migrate-filters.AC3.1, migrate-filters.AC3.2, migrate-filters.AC3.3, migrate-filters.AC3.4

**Files:**
- Modify: `src/backfill.rs` (test module — add new test after existing migrate tests, around line 4046)

**Implementation:**

Add a new integration test that verifies partition date filtering:

```rust
#[tokio::test]
async fn test_migrate_with_date_filters(/* AC3.1, AC3.2, AC3.3, AC3.4 */) {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use deltalake::datafusion::prelude::*;

    let test_name = "migrate_date_filters";
    let source_path = format!("/tmp/delta_migrate_test_{}_src", test_name);
    let output_path = format!("/tmp/delta_migrate_test_{}_out", test_name);
    let _ = fs::remove_dir_all(&source_path);
    let _ = fs::remove_dir_all(&output_path);
    let _ = fs::create_dir_all(&source_path);
    let _ = fs::create_dir_all(&output_path);

    // Create source table with 3 partitions on different dates
    let old_schema = old_delta_schema();
    let b64_1 = STANDARD.encode(&[1u8, 2, 3]);
    let b64_2 = STANDARD.encode(&[4u8, 5, 6]);
    let b64_3 = STANDARD.encode(&[7u8, 8, 9]);

    let source_table = open_or_create_table(&source_path, &old_schema)
        .await
        .expect("source table creation failed");
    let batch1 = old_schema_batch(&old_schema, &[100], &[&b64_1], "2024-01-14");
    let source_table = DeltaOps(source_table)
        .write(vec![batch1])
        .with_save_mode(SaveMode::Append)
        .await
        .expect("write 1 failed");
    let batch2 = old_schema_batch(&old_schema, &[200], &[&b64_2], "2024-01-15");
    let source_table = DeltaOps(source_table)
        .write(vec![batch2])
        .with_save_mode(SaveMode::Append)
        .await
        .expect("write 2 failed");
    let batch3 = old_schema_batch(&old_schema, &[300], &[&b64_3], "2024-01-16");
    let _source_table = DeltaOps(source_table)
        .write(vec![batch3])
        .with_save_mode(SaveMode::Append)
        .await
        .expect("write 3 failed");

    // AC3.4: Use explicit source_path different from config.delta_sink.table_path
    let mut config = make_test_config("/tmp/some_other_path_not_used");
    // config.delta_sink.table_path points elsewhere — run_migrate should use source_path

    // AC3.1 + AC3.2 + AC3.3: Filter to only 2024-01-15
    let shutdown = CancellationToken::new();
    let exit_code = run_migrate(
        config,
        output_path.clone(),
        source_path.clone(),
        Some("2024-01-15".to_string()),
        Some("2024-01-15".to_string()),
        shutdown,
    )
    .await;
    assert_eq!(exit_code, 0, "migration with date filters should succeed");

    // Verify only the 2024-01-15 partition was migrated
    let output_table = deltalake::open_table(&output_path)
        .await
        .expect("output table should exist");

    let ctx = SessionContext::new();
    ctx.register_table("output", Arc::new(output_table)).expect("register failed");
    let df = ctx
        .sql("SELECT cert_index, seen_date FROM output ORDER BY cert_index")
        .await
        .expect("query failed");
    let batches = df.collect().await.expect("collect failed");

    assert_eq!(batches.len(), 1, "should have one batch");
    let result = &batches[0];
    assert_eq!(result.num_rows(), 1, "should have exactly 1 row (from 2024-01-15 partition)");

    // Verify it's the correct record (cert_index 200 from 2024-01-15)
    let cert_idx_raw = result.column_by_name("cert_index").expect("cert_index");
    let cert_idx_col = deltalake::arrow::compute::cast(cert_idx_raw, &DataType::Int64)
        .expect("castable to Int64");
    let cert_idx_arr = cert_idx_col.as_any().downcast_ref::<Int64Array>().expect("Int64");
    assert_eq!(cert_idx_arr.value(0), 200, "should be cert_index 200 from 2024-01-15");

    // Clean up
    let _ = fs::remove_dir_all(&source_path);
    let _ = fs::remove_dir_all(&output_path);
}
```

This single test covers:
- **AC3.1**: `--from 2024-01-15` excludes the `2024-01-14` partition
- **AC3.2**: `--to 2024-01-15` excludes the `2024-01-16` partition
- **AC3.3**: Combined from+to produces a single-partition result
- **AC3.4**: `source_path` is different from `config.delta_sink.table_path`, proving the explicit path is used

**Verification:**
Run: `cargo test --lib backfill -- test_migrate`
Expected: All tests pass (4 existing + 1 new)

**Commit:** `test: add integration test for migrate date filtering and source path`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Run full test suite and update CLAUDE.md

**Verifies:** migrate-filters.AC3.5

**Files:**
- No new source files
- Modify: `CLAUDE.md` (Migrate Contracts section, Commands section)

**Implementation:**

**1. Run full test suite:**

```bash
cargo test
```

All tests must pass. If any test fails due to the `backfill_from` type change (e.g., in `run_backfill` tests), fix it.

**2. Update CLAUDE.md:**

In the **Commands** section, update the migrate command:
```
- `cargo run -- --migrate --output <PATH>` - Migrate Delta table to new schema
- `cargo run -- --migrate --output <PATH> --source <SRC> --from <DATE> --to <DATE>` - Migrate with source and date filters
```

In the **Migrate Contracts** section, update:
- **CLI flags**: `--migrate --output <PATH>` activates migrate mode; `--source <PATH>` overrides source table (default: `config.delta_sink.table_path`); `--from <DATE>` start date filter (YYYY-MM-DD, inclusive); `--to <DATE>` end date filter (YYYY-MM-DD, inclusive)
- **Entry point**: `backfill::run_migrate(config, output_path, source_path, from_date, to_date, shutdown)` called from main, returns exit code (i32)
- **Source table**: reads from `source_path` parameter; defaults to `config.delta_sink.table_path` when `--source` is not provided
- **Partition filtering**: when `--from` is provided, partitions with `seen_date < from` are skipped; when `--to` is provided, partitions with `seen_date > to` are skipped; filters are inclusive

In the **Backfill Contracts** section, clarify:
- **CLI flags**: `--from <INDEX>` sets historical start (parsed as u64 integer)

**Verification:**
Run: `cargo test`
Expected: All tests pass (0 failures)

**Commit:** `docs: update CLAUDE.md with migrate filter contracts`
<!-- END_TASK_5 -->
<!-- END_SUBCOMPONENT_B -->
