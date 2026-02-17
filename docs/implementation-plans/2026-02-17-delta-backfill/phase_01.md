# Delta Backfill Implementation Plan - Phase 1: CLI and Entry Point

**Goal:** Add `--backfill`, `--from`, and `--logs` CLI flags and wire up the backfill entry point in main.

**Architecture:** Extend the hand-rolled CLI parser to support value-bearing flags alongside existing boolean flags. Create a new `backfill` module with a stub entry point. Wire up an early-exit branch in main.rs following the existing pattern used by `--validate-config` and `--export-metrics`.

**Tech Stack:** Rust std library, tokio (CancellationToken), existing project patterns

**Scope:** 5 phases from original design (phase 1 of 5)

**Codebase verified:** 2026-02-17

---

## Acceptance Criteria Coverage

This phase is an infrastructure phase. It establishes the CLI interface and entry point skeleton.

**Verifies: None** - This phase creates scaffolding only. Verification is operational (stub runs and exits cleanly).

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Extend CliArgs with backfill fields and value-based flag parsing

**Files:**
- Modify: `src/cli.rs:1-55`

**Implementation:**

Add three new fields to `CliArgs`:
- `backfill: bool` - activates backfill mode
- `backfill_from: Option<u64>` - override start index for all logs
- `backfill_logs: Option<String>` - filter to specific logs by substring

The existing parser uses `.any()` for boolean flags. Value-based flags (`--from`, `--logs`) need index-based iteration to capture the next argument. Refactor `parse()` to iterate with indices for the new value-bearing flags while preserving the `.any()` pattern for existing boolean flags.

Update `print_help()` to document the new flags under a "BACKFILL OPTIONS" section.

**Verification:**

Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat(cli): add --backfill, --from, --logs CLI flags`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create backfill module with stub entry point

**Files:**
- Create: `src/backfill.rs`

**Implementation:**

Create `src/backfill.rs` with a public async function:
```rust
pub async fn run_backfill(
    config: crate::config::Config,
    backfill_from: Option<u64>,
    backfill_logs: Option<String>,
    shutdown: tokio_util::sync::CancellationToken,
) -> i32
```

The function should:
1. Log that backfill mode is starting (using `tracing::info!`)
2. Log the parameters received (from, logs filter)
3. Log that backfill is not yet implemented
4. Return exit code 0

The return type `i32` represents the process exit code (0 = success, non-zero = partial failure). This convention is established here for Phase 5 to use.

**Verification:**

Run: `cargo build`
Expected: Compiles without errors (module not yet wired into main)

**Commit:** `feat(backfill): add backfill module with stub entry point`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Wire backfill into main.rs early-exit branch

**Files:**
- Modify: `src/main.rs` (module declarations and main function)

**Implementation:**

1. Add `mod backfill;` to the module declarations at the top of `src/main.rs` (alphabetically, between `mod api;` and `mod cli;`).

2. Add an early-exit branch after the `export_metrics` check and before the server's default tracing initialization (the `tracing_subscriber::fmt()` call at line 71). The backfill branch must initialize its own tracing subscriber before calling `run_backfill()`, since it exits before the server's tracing setup would run. The branch should:
   - Initialize tracing (backfill needs logging)
   - Set up the CancellationToken and signal handler
   - Call `backfill::run_backfill()` with the config and CLI args
   - Exit with the returned exit code via `std::process::exit()`

This follows the same pattern as `--validate-config` and `--export-metrics` but needs tracing and shutdown handling since backfill is a long-running operation.

**Verification:**

Run: `cargo build`
Expected: Compiles without errors

Run: `cargo run -- --backfill`
Expected: Logs "backfill mode starting" (or similar), then exits with code 0

Run: `cargo run -- --backfill --from 100 --logs google`
Expected: Logs parameters received, then exits with code 0

Run: `cargo run -- --help`
Expected: Shows backfill options in help text

Run: `cargo test`
Expected: All tests pass (no regressions)

**Commit:** `feat(main): wire backfill early-exit branch`
<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->
