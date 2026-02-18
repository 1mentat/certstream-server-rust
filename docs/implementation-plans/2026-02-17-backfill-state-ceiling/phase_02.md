# Backfill State Ceiling Implementation Plan - Phase 2

**Goal:** Update CLI help text to reflect that backfill uses the state file as its ceiling instead of HTTP tree_size fetching.

**Architecture:** Update the `print_help()` function in `src/cli.rs` to document the state file dependency in the BACKFILL OPTIONS section.

**Tech Stack:** Rust

**Scope:** 4 phases from original design (phase 2 of 4)

**Codebase verified:** 2026-02-18

---

## Acceptance Criteria Coverage

This phase is infrastructure (help text update). **Verifies: None** — verified operationally by running `--help`.

---

<!-- START_TASK_1 -->
### Task 1: Update BACKFILL OPTIONS help text

**Files:**
- Modify: `src/cli.rs:62-65` (BACKFILL OPTIONS section in `print_help()`)

**Implementation:**

Replace the current BACKFILL OPTIONS section (lines 62-65) with updated text that documents the state file dependency:

Current code:
```rust
        println!("BACKFILL OPTIONS:");
        println!("    --backfill           Activate backfill mode");
        println!("    --from <INDEX>       Override start index for all logs");
        println!("    --logs <FILTER>      Filter to specific logs by substring");
```

Replace with:
```rust
        println!("BACKFILL OPTIONS:");
        println!("    --backfill           Activate backfill mode (requires state file)");
        println!("    --from <INDEX>       Override start index for all logs");
        println!("    --logs <FILTER>      Filter to specific logs by substring");
        println!();
        println!("    Backfill uses the state file (ct_log.state_file, default:");
        println!("    certstream_state.json) as the per-log upper bound. Logs not");
        println!("    present in the state file are skipped. Run the live server");
        println!("    first to populate the state file.");
```

**Verification:**
Run: `cargo run -- --help`
Expected: Output shows updated BACKFILL OPTIONS section with state file documentation.

**Commit:** `docs: update CLI help text to document state file dependency for backfill`

<!-- END_TASK_1 -->
