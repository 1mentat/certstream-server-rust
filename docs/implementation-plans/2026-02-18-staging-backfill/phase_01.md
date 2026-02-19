# Staging Backfill Implementation Plan - Phase 1: CLI & Mode Routing

**Goal:** Add `--staging-path` and `--merge` CLI flags and route merge mode in `main.rs`

**Architecture:** Extends the existing manual `env::args()` CLI parsing in `src/cli.rs` with two new fields (`staging_path: Option<String>`, `merge: bool`). Adds a new merge mode branch in `main.rs` between the existing backfill branch and server mode, with validation that `--merge` requires `--staging-path`.

**Tech Stack:** Rust, std::env::args (manual CLI parsing, no external crate)

**Scope:** 5 phases from original design (this is phase 1 of 5)

**Codebase verified:** 2026-02-18

**Note:** Line references throughout are approximate and should be verified against the actual file at implementation time.

---

## Acceptance Criteria Coverage

This phase is infrastructure (CLI routing). It verifies:

### staging-backfill.AC4: Error handling
- **staging-backfill.AC4.5 Failure:** `--merge` without `--staging-path` prints error and exits with non-zero code

### staging-backfill.AC5: Backward compatibility
- **staging-backfill.AC5.1 Success:** `--backfill` without `--staging-path` writes to main table (existing behavior unchanged)

---

<!-- START_TASK_1 -->
### Task 1: Add staging_path and merge fields to CliArgs

**Files:**
- Modify: `src/cli.rs:5-15` (CliArgs struct)
- Modify: `src/cli.rs:18-45` (parse function)

**Implementation:**

Add two new fields to the `CliArgs` struct:
- `staging_path: Option<String>` — parsed from `--staging-path <PATH>` using the same index-based iteration pattern as `--from` and `--logs`
- `merge: bool` — parsed as a boolean flag using the same `.any()` pattern as `--backfill`

In the `parse()` function:
- Add `staging_path` to the index-based loop alongside `backfill_from` and `backfill_logs`:
  ```rust
  } else if arg == "--staging-path" && i + 1 < args.len() {
      staging_path = Some(args[i + 1].clone());
  }
  ```
- Add `merge` as a boolean flag in the struct constructor:
  ```rust
  merge: args.iter().any(|a| a == "--merge"),
  ```

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat(cli): add --staging-path and --merge flags to CliArgs`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update print_help with new flags

**Files:**
- Modify: `src/cli.rs:47-80` (print_help function)

**Implementation:**

Add a new "STAGING/MERGE OPTIONS:" section to `print_help()` after the "BACKFILL OPTIONS:" section:

```rust
println!("STAGING/MERGE OPTIONS:");
println!("    --staging-path <PATH>  Write backfill to staging Delta table at PATH");
println!("    --merge                Merge staging table into main table");
```

The staging/merge section should appear after the existing backfill options section and before the environment variables section.

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `docs(cli): add --staging-path and --merge to help output`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add merge mode routing in main.rs

**Verifies:** staging-backfill.AC4.5, staging-backfill.AC5.1

**Files:**
- Modify: `src/main.rs:72-92` (add merge mode branch before backfill branch)
- Modify: `src/backfill.rs` (add `run_merge` stub function)

**Implementation:**

In `src/main.rs`, add a new merge mode branch **before** the existing `if cli_args.backfill` block (at line 72). The merge mode:

1. Validates `--merge` requires `--staging-path` — if missing, prints error to stderr and exits with code 1
2. Initializes tracing (same pattern as backfill)
3. Creates shutdown token and signal handler (same pattern as backfill)
4. Calls `backfill::run_merge()` stub
5. Exits with the returned code

```rust
if cli_args.merge {
    if cli_args.staging_path.is_none() {
        eprintln!("Error: --merge requires --staging-path <PATH>");
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

    let exit_code = backfill::run_merge(
        config,
        cli_args.staging_path.unwrap(),
        shutdown_token,
    )
    .await;

    std::process::exit(exit_code);
}
```

In `src/backfill.rs`, add a stub `run_merge` function near the other public functions:

```rust
pub async fn run_merge(
    _config: Config,
    _staging_path: String,
    _shutdown: CancellationToken,
) -> i32 {
    info!("Merge mode stub — not yet implemented");
    0
}
```

This preserves existing `--backfill` behavior completely (staging-backfill.AC5.1) since the backfill block is unchanged and `staging_path` is not yet wired into it.

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

Run: `cargo test`
Expected: All 249 existing tests pass (backward compatibility)

**Commit:** `feat: add merge mode routing in main.rs with run_merge stub`
<!-- END_TASK_3 -->
