# Reparse Audit & Metadata Table Implementation Plan

**Goal:** Add two new batch execution modes (`--reparse-audit` and `--extract-metadata`) for auditing stored certificates against current parsing code and extracting metadata-only Delta tables.

**Architecture:** New `src/table_ops.rs` module with two entry points (`run_reparse_audit`, `run_extract_metadata`) dispatched from the existing CLI if/else chain in `main.rs`. Both read the Delta table via DataFusion SQL, process partition-by-partition, and exit with a status code.

**Tech Stack:** Rust, Tokio, DataFusion, deltalake 0.25, Arrow

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-27

---

## Acceptance Criteria Coverage

This phase implements and tests:

### reparse-and-metadata-table.AC4: CLI integration follows existing patterns
- **reparse-and-metadata-table.AC4.1 Success:** `--reparse-audit` dispatches to reparse audit mode and exits
- **reparse-and-metadata-table.AC4.2 Success:** `--extract-metadata --output <PATH>` dispatches to metadata extraction mode and exits
- **reparse-and-metadata-table.AC4.3 Failure:** `--extract-metadata` without `--output` prints error and exits 1

---

## Phase 1: CLI and Module Scaffolding

<!-- START_TASK_1 -->
### Task 1: Add new fields to CliArgs and parse them

**Verifies:** reparse-and-metadata-table.AC4.1, reparse-and-metadata-table.AC4.2, reparse-and-metadata-table.AC4.3

**Files:**
- Modify: `src/cli.rs:5-18` (CliArgs struct)
- Modify: `src/cli.rs:27-61` (parse_args method)
- Modify: `src/cli.rs:63-101` (print_help method)

**Implementation:**

Add five new fields to the `CliArgs` struct (after the existing `merge: bool` field at line 17):

```rust
pub reparse_audit: bool,
pub extract_metadata: bool,
pub output_path: Option<String>,
pub from_date: Option<String>,
pub to_date: Option<String>,
```

In the `parse_args` method, add value-flag parsing in the `for (i, arg)` loop (after `--sink` at line 43):

```rust
} else if arg == "--output" && i + 1 < args.len() {
    output_path = Some(args[i + 1].clone());
} else if arg == "--from-date" && i + 1 < args.len() {
    from_date = Some(args[i + 1].clone());
} else if arg == "--to-date" && i + 1 < args.len() {
    to_date = Some(args[i + 1].clone());
}
```

Add the corresponding `let mut` declarations before the loop (alongside existing ones at lines 29-32):

```rust
let mut output_path = None;
let mut from_date = None;
let mut to_date = None;
```

Add boolean flags in the `Self { ... }` return block (after `merge` at line 59):

```rust
reparse_audit: args.iter().any(|a| a == "--reparse-audit"),
extract_metadata: args.iter().any(|a| a == "--extract-metadata"),
output_path,
from_date,
to_date,
```

Update `print_help()` to add new sections after the STAGING/MERGE OPTIONS block (after line 92):

```rust
println!();
println!("TABLE OPERATIONS:");
println!("    --reparse-audit        Audit stored certs against current parsing code");
println!("    --extract-metadata     Extract metadata-only Delta table (requires --output)");
println!("    --output <PATH>        Output path for metadata extraction");
println!("    --from-date <DATE>     Filter partitions from this date (YYYY-MM-DD)");
println!("    --to-date <DATE>       Filter partitions to this date (YYYY-MM-DD)");
```

**Testing:**

Tests must verify each AC listed above:
- reparse-and-metadata-table.AC4.1: Parse `--reparse-audit` flag and verify `reparse_audit` is true
- reparse-and-metadata-table.AC4.2: Parse `--extract-metadata --output /tmp/test` and verify both fields set
- reparse-and-metadata-table.AC4.3: Parse `--extract-metadata` without `--output` and verify `output_path` is None (validation happens in main.rs dispatch)
- Also: Parse `--from-date` and `--to-date` value flags, verify combined flags work together

Follow the existing test pattern in `src/cli.rs` (AC-based naming, `CliArgs::parse_args(&args)` with constructed arg vectors).

**Verification:**
Run: `cargo test --lib cli::tests`
Expected: All new and existing tests pass

**Commit:** `feat: add reparse-audit and extract-metadata CLI flags`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create table_ops module with stub functions

**Files:**
- Create: `src/table_ops.rs`
- Modify: `src/main.rs:1-17` (add mod declaration)

**Implementation:**

Create `src/table_ops.rs` with two stub async functions following the existing `run_backfill`/`run_merge` signature pattern (returns i32 exit code):

```rust
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::Config;

pub async fn run_reparse_audit(
    config: Config,
    from_date: Option<String>,
    to_date: Option<String>,
    _shutdown: CancellationToken,
) -> i32 {
    info!(
        table_path = %config.delta_sink.table_path,
        from_date = ?from_date,
        to_date = ?to_date,
        "reparse audit: stub implementation"
    );
    0
}

pub async fn run_extract_metadata(
    config: Config,
    output_path: String,
    from_date: Option<String>,
    to_date: Option<String>,
    _shutdown: CancellationToken,
) -> i32 {
    info!(
        table_path = %config.delta_sink.table_path,
        output_path = %output_path,
        from_date = ?from_date,
        to_date = ?to_date,
        "extract metadata: stub implementation"
    );
    0
}
```

Add `mod table_ops;` to `src/main.rs` module declarations (after `mod state;` at line 15, maintaining alphabetical order with the existing declarations — insert between `mod state;` and `mod websocket;`):

```rust
mod table_ops;
```

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat: add table_ops module with stub functions`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add dispatch logic in main.rs

**Files:**
- Modify: `src/main.rs:80-139` (dispatch chain, insert before `if cli_args.merge` block)

**Implementation:**

Insert two new dispatch blocks in `main.rs` **before** the existing `if cli_args.merge` block (before line 81). This follows the pattern where batch/exit modes are checked before server mode. The new blocks go after `if cli_args.export_metrics` (line 73-79) and before `if cli_args.merge` (line 81).

**Note on mutual exclusivity:** The existing dispatch chain uses sequential `if` blocks that each call `std::process::exit()`, so only the first matching mode executes. The new `--reparse-audit` and `--extract-metadata` blocks follow this same pattern. If a user passes both `--reparse-audit` and `--extract-metadata`, only `--reparse-audit` will run (since it's checked first). This matches how `--backfill` and `--merge` are handled — no explicit mutual exclusivity check, just first-match-wins via the if/else chain.

```rust
if cli_args.reparse_audit {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.log_level)),
        )
        .init();

    let shutdown_token = CancellationToken::new();
    spawn_signal_handler(shutdown_token.clone());

    let exit_code = table_ops::run_reparse_audit(
        config,
        cli_args.from_date,
        cli_args.to_date,
        shutdown_token,
    )
    .await;

    std::process::exit(exit_code);
}

if cli_args.extract_metadata {
    if cli_args.output_path.is_none() {
        eprintln!("Error: --extract-metadata requires --output <PATH>");
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

    let exit_code = table_ops::run_extract_metadata(
        config,
        cli_args.output_path.unwrap(),
        cli_args.from_date,
        cli_args.to_date,
        shutdown_token,
    )
    .await;

    std::process::exit(exit_code);
}
```

**Verification:**
Run: `cargo build`
Expected: Builds without errors

Run: `cargo run -- --reparse-audit 2>&1 | head -5`
Expected: Log line containing "reparse audit: stub implementation", exits 0

Run: `cargo run -- --extract-metadata --output /tmp/test 2>&1 | head -5`
Expected: Log line containing "extract metadata: stub implementation", exits 0

Run: `cargo run -- --extract-metadata 2>&1; echo "exit: $?"`
Expected: "Error: --extract-metadata requires --output <PATH>", exit code 1

**Commit:** `feat: wire up reparse-audit and extract-metadata dispatch in main`
<!-- END_TASK_3 -->
