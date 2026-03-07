# Named Targets Implementation Plan — Phase 4: Main Dispatch Refactor

**Goal:** Resolve named targets in `main.rs` dispatch and pass resolved values to operations. Add required-flag validation per mode and error handling for unknown/missing target names.

**Architecture:** Each execution mode's dispatch block in `main.rs` validates required `--target`/`--source` flags, calls `config.resolve_target(name)` to produce `ResolvedTarget` values, then passes extracted fields to the existing operation function signatures. Phase 5 will change the function signatures themselves.

**Tech Stack:** Rust

**Scope:** 6 phases from original design (this is phase 4 of 6)

**Codebase verified:** 2026-03-05

---

## Acceptance Criteria Coverage

This phase implements and tests:

### named-targets.AC5: Dispatch resolution
- **named-targets.AC5.1 Failure:** `--migrate` without `--source` and `--target` exits with error
- **named-targets.AC5.2 Failure:** `--migrate --source X` without `--target` exits with error
- **named-targets.AC5.3 Failure:** `--extract-metadata` without `--source` and `--target` exits with error
- **named-targets.AC5.4 Failure:** `--merge` without `--source` and `--target` exits with error
- **named-targets.AC5.5 Failure:** `--backfill` without `--target` exits with error (Delta sink only; ZeroBus sink exempted per AC5.8)
- **named-targets.AC5.6 Failure:** `--reparse-audit` without `--source` exits with error
- **named-targets.AC5.7 Failure:** `--target nonexistent` where name not in config exits with error
- **named-targets.AC5.8 Failure:** `--target` combined with `--sink zerobus` exits with error (ZeroBus uses its own table config, so --target is both irrelevant and rejected)

---

## Codebase Verification Findings

- ✓ Dispatch structure at `src/main.rs:66-263` — 6 execution modes in if/else chain
- ✓ `validate_staging_path_uri()` at main.rs:485-492 — to be removed (replaced by resolve_target)
- ✓ Backfill zerobus validation at main.rs:230-237 — calls `cli::validate_backfill_sink_command()`
- ✓ Reparse audit at main.rs:139-159 — currently does NOT accept --source, reads from config.delta_sink.table_path
- ✓ Function signatures: run_migrate(config, output_path, source_path, from_date, to_date, shutdown), run_extract_metadata(config, output_path, from_date, to_date, shutdown), run_merge(config, staging_path, shutdown), run_backfill(config, staging_path, backfill_from, backfill_logs, backfill_sink, shutdown), run_reparse_audit(config, from_date, to_date, shutdown)
- ✓ Backfill currently doesn't require `--staging-path` — defaults to config.delta_sink.table_path. Design AC5.5 requires `--target` for backfill (behavior change).

**Transitional approach:** Phase 4 resolves targets and extracts fields to match current function signatures. For operations where the current signature doesn't accept a source/target path directly (reparse-audit, merge's target), Phase 4 modifies a config clone to set `delta_sink.table_path` before calling. Phase 5 changes the function signatures to accept `ResolvedTarget` directly, removing this workaround.

---

<!-- START_TASK_1 -->
### Task 1: Add target resolution and flag validation to main.rs dispatch

**Verifies:** named-targets.AC5.1, named-targets.AC5.2, named-targets.AC5.3, named-targets.AC5.4, named-targets.AC5.5, named-targets.AC5.6, named-targets.AC5.7, named-targets.AC5.8

**Files:**
- Modify: `src/main.rs` (refactor all dispatch branches to use resolve_target)

**Implementation:**

Add a helper function near the top of main.rs (after imports) to resolve a target name and exit on error:

```rust
fn resolve_or_exit(config: &Config, name: &str, flag: &str) -> ResolvedTarget {
    match config.resolve_target(name) {
        Ok(resolved) => resolved,
        Err(e) => {
            eprintln!("Error: {} '{}': {}", flag, name, e);
            std::process::exit(1);
        }
    }
}
```

Import `ResolvedTarget` from config module at the top of main.rs.

**MIGRATE mode** (currently lines 91-137):

Replace the validation and dispatch:
```rust
if cli_args.migrate {
    if cli_args.source.is_none() || cli_args.target.is_none() {
        eprintln!("Error: --migrate requires both --source <NAME> and --target <NAME>");
        std::process::exit(1);
    }
    // Date validation (keep existing --from and --to validation)
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
    let resolved_source = resolve_or_exit(&config, cli_args.source.as_ref().unwrap(), "--source");
    let resolved_target = resolve_or_exit(&config, cli_args.target.as_ref().unwrap(), "--target");
    // [tracing setup unchanged]
    let exit_code = backfill::run_migrate(
        config,
        resolved_target.table_path,
        resolved_source.table_path,
        cli_args.backfill_from,
        cli_args.to,
        shutdown_token,
    )
    .await;
    std::process::exit(exit_code);
}
```

**REPARSE AUDIT mode** (currently lines 139-159):

Add `--source` requirement. Use a config clone with overridden table_path (transitional — Phase 5 changes the function signature):
```rust
if cli_args.reparse_audit {
    if cli_args.source.is_none() {
        eprintln!("Error: --reparse-audit requires --source <NAME>");
        std::process::exit(1);
    }
    let resolved_source = resolve_or_exit(&config, cli_args.source.as_ref().unwrap(), "--source");
    // Transitional: override config.delta_sink.table_path with resolved source
    // Phase 5 changes run_reparse_audit to accept ResolvedTarget directly
    let mut audit_config = config.clone();
    audit_config.delta_sink.table_path = resolved_source.table_path;
    // [tracing setup unchanged]
    let (exit_code, _report) = table_ops::run_reparse_audit(
        audit_config,
        cli_args.from_date,
        cli_args.to_date,
        shutdown_token,
    )
    .await;
    std::process::exit(exit_code);
}
```

**EXTRACT-METADATA mode** (currently lines 161-187):

Add `--source` and `--target` requirement:
```rust
if cli_args.extract_metadata {
    if cli_args.source.is_none() || cli_args.target.is_none() {
        eprintln!("Error: --extract-metadata requires both --source <NAME> and --target <NAME>");
        std::process::exit(1);
    }
    let resolved_source = resolve_or_exit(&config, cli_args.source.as_ref().unwrap(), "--source");
    let resolved_target = resolve_or_exit(&config, cli_args.target.as_ref().unwrap(), "--target");
    // Transitional: override config.delta_sink.table_path with resolved source
    let mut extract_config = config.clone();
    extract_config.delta_sink.table_path = resolved_source.table_path;
    // [tracing setup unchanged]
    let exit_code = table_ops::run_extract_metadata(
        extract_config,
        resolved_target.table_path,
        cli_args.from_date,
        cli_args.to_date,
        shutdown_token,
    )
    .await;
    std::process::exit(exit_code);
}
```

**MERGE mode** (currently lines 189-215):

Add `--source` and `--target` requirement. Source is the staging table, target is the main table:
```rust
if cli_args.merge {
    if cli_args.source.is_none() || cli_args.target.is_none() {
        eprintln!("Error: --merge requires both --source <NAME> and --target <NAME>");
        std::process::exit(1);
    }
    let resolved_source = resolve_or_exit(&config, cli_args.source.as_ref().unwrap(), "--source");
    let resolved_target = resolve_or_exit(&config, cli_args.target.as_ref().unwrap(), "--target");
    // Transitional: override config.delta_sink.table_path with resolved target
    let mut merge_config = config.clone();
    merge_config.delta_sink.table_path = resolved_target.table_path;
    // [tracing setup unchanged]
    let exit_code = backfill::run_merge(
        merge_config,
        resolved_source.table_path,
        shutdown_token,
    )
    .await;
    std::process::exit(exit_code);
}
```

**BACKFILL mode** (currently lines 217-263):

Add `--target` requirement for Delta backfill, but exempt `--sink zerobus` (which uses its own table config). Check zerobus FIRST to avoid contradicting AC5.5 and AC5.8:
```rust
if cli_args.backfill {
    let is_zerobus = cli_args.backfill_sink.as_deref() == Some("zerobus");

    // ZeroBus sink uses its own table configuration, so --target is not needed (AC5.8)
    // For Delta sink, --target is required (AC5.5)
    if is_zerobus && cli_args.target.is_some() {
        eprintln!("Error: --target is not compatible with --sink zerobus (ZeroBus uses its own table configuration)");
        std::process::exit(1);
    }
    if !is_zerobus && cli_args.target.is_none() {
        eprintln!("Error: --backfill requires --target <NAME> (unless using --sink zerobus)");
        std::process::exit(1);
    }

    // Parse --from value to u64
    let backfill_from: Option<u64> = match &cli_args.backfill_from {
        Some(s) => match s.parse::<u64>() {
            Ok(v) => Some(v),
            Err(_) => {
                eprintln!("Error: --from value '{}' is not a valid integer for backfill mode", s);
                std::process::exit(1);
            }
        },
        None => None,
    };
    if let Err(error_msg) = cli::validate_backfill_sink_command(
        cli_args.backfill_sink.as_deref(),
        backfill_from,
        config.zerobus_sink.enabled,
    ) {
        eprintln!("{}", error_msg);
        std::process::exit(1);
    }

    // Resolve target only for Delta sink (zerobus doesn't use it)
    let resolved_target = if !is_zerobus {
        Some(resolve_or_exit(&config, cli_args.target.as_ref().unwrap(), "--target"))
    } else {
        None
    };

    // [tracing setup unchanged]
    let exit_code = backfill::run_backfill(
        config,
        resolved_target.map(|t| t.table_path),
        backfill_from,
        cli_args.backfill_logs,
        cli_args.backfill_sink,
        shutdown_token,
    )
    .await;
    std::process::exit(exit_code);
}
```

**Note on AC5.5/AC5.8 interaction:** AC5.5 requires `--target` for backfill, AC5.8 rejects `--target` with zerobus. These are compatible: `--target` is required for Delta-sink backfill but must be absent for ZeroBus-sink backfill. The validation checks `is_zerobus` first to route correctly.

**Remove `validate_staging_path_uri()`** — no longer called from any dispatch branch. Delete the function definition at main.rs:485-492.

**Verification:**

Run: `cargo build`
Expected: Builds without errors

Run: `cargo test`
Expected: All tests pass

**Commit:** `refactor: add named target resolution to main.rs dispatch`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add dispatch validation tests

**Verifies:** named-targets.AC5.1, named-targets.AC5.2, named-targets.AC5.3, named-targets.AC5.4, named-targets.AC5.5, named-targets.AC5.6, named-targets.AC5.7, named-targets.AC5.8

**Files:**
- Modify: `src/main.rs` (add `#[cfg(test)] mod tests` block if none exists, or add to `src/config.rs` tests)

**Implementation:**

The dispatch validation is exercised through process exits, which are difficult to test directly in unit tests. Instead, add tests that verify the `resolve_or_exit` helper and the resolve_target error paths:

Since `resolve_or_exit` calls `std::process::exit(1)` on failure (not testable in-process), test the underlying `config.resolve_target()` method instead. The AC5 criteria about "exits with error" are verified through:
1. The flag-presence checks (simple if-then-exit, verified by code review)
2. The `resolve_target()` error handling (testable)

Add tests to the existing `#[cfg(test)] mod tests` block in `src/config.rs`:

Tests must verify each AC listed above:
- named-targets.AC5.7: Call `config.resolve_target("nonexistent")` where "nonexistent" is not in config.targets. Verify it returns `Err` with a message containing "nonexistent".
- The flag-presence validation (AC5.1-AC5.6) is structural — the if-checks in dispatch are simple conditionals that call `std::process::exit(1)`. These cannot be unit-tested in-process (process::exit terminates the test runner). **These ACs receive automated coverage in Phase 6 Task 1** (integration tests that exercise the full config → CLI → resolve → operation flow). In this phase, they are verified by code review of the dispatch conditionals.
- named-targets.AC5.8: The zerobus + target interaction (zerobus rejects --target, Delta requires --target) is a conditional in backfill dispatch. **This AC also receives coverage in Phase 6 Task 1.** The logic is verified by code review in this phase.

For `resolve_or_exit`: since it's a thin wrapper over `resolve_target()` that calls `process::exit`, the meaningful test is on `resolve_target()` itself (already covered in Phase 1 tests, AC1.8).

**Verification:**

Run: `cargo test`
Expected: All tests pass

**Commit:** `test: verify dispatch resolution error paths`

<!-- END_TASK_2 -->
