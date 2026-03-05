# Named Targets Implementation Plan — Phase 3: CLI Changes

**Goal:** Replace ad-hoc `--output`, `--staging-path`, `--source <path>` CLI flags with `--target <name>` and `--source <name>` flags, and update `config.example.yaml` with a targets section.

**Architecture:** Modify `CliArgs` struct in `src/cli.rs` to remove 4 old fields and add 2 new ones, update the manual argument parser, update help text, and make minimal main.rs adjustments to keep compilation working. Phase 4 will add full dispatch refactoring.

**Tech Stack:** Rust, std::env::args

**Scope:** 6 phases from original design (this is phase 3 of 6)

**Codebase verified:** 2026-03-05

---

## Acceptance Criteria Coverage

This phase implements and tests:

### named-targets.AC4: CLI flags
- **named-targets.AC4.1 Success:** `--target foo` parses correctly into `CliArgs.target = Some("foo")`
- **named-targets.AC4.2 Success:** `--source bar` parses correctly into `CliArgs.source = Some("bar")`
- **named-targets.AC4.3 Success:** `--output`, `--staging-path` flags removed (not recognized)
- **named-targets.AC4.4 Success:** Help text shows `--target` and `--source` flags with descriptions

---

## Codebase Verification Findings

- ✓ `CliArgs` struct at `src/cli.rs:5-29` — 17 fields total, 4 to remove (`staging_path`, `migrate_output`, `migrate_source`, `output_path`), 2 to add (`target`, `source`)
- ✓ Parser at `src/cli.rs:38-96` — manual index-based parsing with `std::env::args()`, not clap
- ✓ `print_help()` at `src/cli.rs:98-152` — shows `--output`, `--staging-path`, `--source <PATH>` (old flags)
- ✓ `--output` at line 60-62 maps to BOTH `migrate_output` AND `output_path` (dual assignment)
- ✓ `--source` at line 63-64 maps to `migrate_source` only
- ✓ `--staging-path` at line 56-57 maps to `staging_path`
- ✓ 38 tests at `src/cli.rs:280-785` covering all affected flags
- ✓ `config.example.yaml` at project root — 98 lines, no `targets:` section yet
- ✓ `main.rs` references to old fields: `staging_path` (5 refs), `migrate_output` (2 refs), `migrate_source` (1 ref), `output_path` (2 refs) — all need updating for compilation
- ✓ `validate_staging_path_uri()` function in main.rs called on staging_path — must be removed or refactored (Phase 4 replaces with target resolution)

---

<!-- START_TASK_1 -->
### Task 1: Update CliArgs struct and parser

**Verifies:** named-targets.AC4.1, named-targets.AC4.2, named-targets.AC4.3

**Files:**
- Modify: `src/cli.rs:5-29` (CliArgs struct)
- Modify: `src/cli.rs:38-96` (parse_args function)

**Implementation:**

Remove these 4 fields from `CliArgs`:
- `staging_path: Option<String>` (line 15)
- `migrate_output: Option<String>` (line 19)
- `migrate_source: Option<String>` (lines 20-21)
- `output_path: Option<String>` (line 26)

Add these 2 fields to `CliArgs`:
- `target: Option<String>` — named target for write destination
- `source: Option<String>` — named target for read source

Update `parse_args()`:
- Remove `staging_path`, `migrate_output`, `output_path`, `migrate_source` local variables
- Add `target: Option<String> = None` and `source: Option<String> = None` local variables
- Replace the `--output` parsing branch (line 60-62) with `--target` parsing:
  ```rust
  } else if arg == "--target" && i + 1 < args.len() {
      target = Some(args[i + 1].clone());
  ```
- Replace the `--source` parsing branch (line 63-64) to keep the same flag name but map to the new `source` field:
  ```rust
  } else if arg == "--source" && i + 1 < args.len() {
      source = Some(args[i + 1].clone());
  ```
- Remove the `--staging-path` parsing branch (line 56-57)
- Update the `CliArgs` construction at the end of `parse_args()` to use the new field names

**Verification:**

Run: `cargo build`
Expected: May not compile yet if main.rs still references old fields — that's OK, Task 2 fixes main.rs

**Commit:** (combined with Task 2)

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update main.rs references to use new CLI field names

**Verifies:** (supports compilation, no specific AC — required for Phase 3 to compile)

**Files:**
- Modify: `src/main.rs` (update all references to removed CLI fields)

**Implementation:**

Update all 10 references to old field names in main.rs dispatch branches. **These are transitional changes that will be overwritten in Phase 4** (which adds proper `resolve_target()` resolution) **and again in Phase 5** (which changes function signatures to accept `ResolvedTarget`). The purpose of these changes is solely to keep the codebase compiling after the CLI field renames in Task 1. The dispatch code here is intentionally minimal — just swap field names to restore compilation.

**MIGRATE mode (lines 91-137):**
- Line 92: `cli_args.migrate_output.is_none()` → `cli_args.target.is_none()`
- Line 93: Error message → `"Error: --migrate requires --target <NAME>"`
- Lines 112-114: `cli_args.migrate_source.unwrap_or_else(...)` → `cli_args.source` (handle differently — see below)
- Line 128: `cli_args.migrate_output.unwrap()` → `cli_args.target.unwrap()`

For the migrate source path: the old code was `cli_args.migrate_source.unwrap_or_else(|| config.delta_sink.table_path.clone())`. With named targets, both `--source` and `--target` are required for migrate. Change the validation to require both flags:
```rust
if cli_args.target.is_none() || cli_args.source.is_none() {
    eprintln!("Error: --migrate requires both --source <NAME> and --target <NAME>");
    std::process::exit(1);
}
```

Then pass `cli_args.source.unwrap()` where `source_path` was used, and `cli_args.target.unwrap()` where `migrate_output` was used.

**EXTRACT-METADATA mode (lines 161-187):**
- Line 162: `cli_args.output_path.is_none()` → change validation to require both `--source` and `--target`:
  ```rust
  if cli_args.target.is_none() || cli_args.source.is_none() {
      eprintln!("Error: --extract-metadata requires both --source <NAME> and --target <NAME>");
      std::process::exit(1);
  }
  ```
- Line 179: `cli_args.output_path.unwrap()` → `cli_args.target.clone().unwrap()`
- Add: pass `cli_args.source.clone().unwrap()` as source path

**MERGE mode (lines 189-215):**
- Line 190: `cli_args.staging_path.is_none()` → change validation to require both `--source` and `--target`:
  ```rust
  if cli_args.target.is_none() || cli_args.source.is_none() {
      eprintln!("Error: --merge requires both --source <NAME> and --target <NAME>");
      std::process::exit(1);
  }
  ```
- Line 195: Remove `validate_staging_path_uri(cli_args.staging_path.as_ref())` call (URI validation moves to resolve_target in Phase 4)
- Line 209: `cli_args.staging_path.unwrap()` → `cli_args.source.clone().unwrap()` (merge reads from source, writes to target)

**BACKFILL mode (lines 217-263):**
- Line 240: Remove `validate_staging_path_uri(cli_args.staging_path.as_ref())` call
- Line 254: `cli_args.staging_path` → `cli_args.target.clone()` (backfill writes to target)

**Also:** Remove or comment out the `validate_staging_path_uri` helper function if it's no longer called anywhere. If it's still used elsewhere, keep it.

**Verification:**

Run: `cargo build`
Expected: Builds without errors

Run: `cargo test`
Expected: Existing tests pass. Some CLI tests will need updating (Task 4). Some main.rs-adjacent tests may fail — those are updated in later tasks.

**Commit:** `refactor: replace --output/--staging-path/--source path with --target/--source name in CLI and main.rs`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Update help text and config.example.yaml

**Verifies:** named-targets.AC4.4

**Files:**
- Modify: `src/cli.rs:98-152` (print_help function)
- Modify: `config.example.yaml` (add targets section)

**Implementation:**

Update `print_help()` to replace the old flag sections. Remove "STAGING/MERGE OPTIONS" and "MIGRATION OPTIONS" sections with separate `--output`, `--staging-path` references. Replace with a "TARGET OPTIONS" section:

```
TARGET OPTIONS:
    --target <NAME>        Named target for write destination (from config targets section)
    --source <NAME>        Named target for read source (from config targets section)

    Note: --source now accepts a target NAME (not a file path).
    The old --source <PATH>, --output <PATH>, and --staging-path <PATH> flags have been
    replaced by named targets. Define targets in config.yaml, then reference them by name.
```

Keep the BACKFILL OPTIONS, but remove `--staging-path` reference. Update MIGRATION OPTIONS to reference `--target` and `--source` instead of `--output` and `--source <PATH>`. Update TABLE OPERATIONS to reference `--target` instead of `--output`.

Add a `targets:` section to `config.example.yaml` with commented examples showing a local file target and an S3 target:

```yaml
# Named targets: reusable Delta table configurations for CLI operations
# targets:
#   main:
#     table_path: "file:///data/certstream/main"
#   staging:
#     table_path: "file:///data/certstream/staging"
#   archive:
#     table_path: "s3://my-bucket/certstream/archive"
#     compression_level: 15
#     heavy_column_compression_level: 19
#     offline_batch_size: 200000
#     storage:
#       s3:
#         endpoint: "https://s3.us-east-1.amazonaws.com"
#         region: "us-east-1"
#         access_key_id: "AKIA..."
#         secret_access_key: "..."
```

**Verification:**

Run: `cargo run -- --help`
Expected: Shows updated help text with `--target` and `--source` flags, no `--output` or `--staging-path`

**Commit:** `docs: update help text and config.example.yaml for named targets`

<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Update CLI tests for new flags

**Verifies:** named-targets.AC4.1, named-targets.AC4.2, named-targets.AC4.3, named-targets.AC4.4

**Files:**
- Modify: `src/cli.rs` (update tests in `#[cfg(test)] mod tests` block, lines 280-785)

**Implementation:**

Update existing tests that reference removed fields and add new tests for the new flags. Many existing tests reference `migrate_output`, `output_path`, `staging_path`, `migrate_source` — these must be updated to reference `target` and `source`.

**Tests to update:**
- `test_output_flag_parsed_correctly` → rename to `test_target_flag_parsed_correctly`, change args from `["--output", "path"]` to `["--target", "main"]`, assert `target == Some("main")`
- `test_output_flag_none_when_not_provided` → rename to `test_target_flag_none_when_not_provided`, assert `target.is_none()`
- `test_source_flag_parsed_correctly` → update args to `["--source", "staging"]`, assert `source == Some("staging")`
- `test_source_flag_none_when_not_provided` → assert `source.is_none()`
- `test_migrate_and_output_flags_together` → update to use `--target` instead of `--output`
- `test_migrate_with_other_flags` → update to use `--target`/`--source` instead of `--output`/`--source`
- `test_migrate_all_flags_together` → update to use `--target`/`--source`
- `test_sink_with_staging_path` → update to use `--target` instead of `--staging-path`
- `test_ac4_2_extract_metadata_with_output` → update to use `--target`
- `test_combined_extract_metadata_with_all_options` → update to use `--target`/`--source`
- `test_extract_metadata_with_only_output` → update to use `--target`
- Other tests referencing the removed fields

**New tests to add:**

Tests must verify each AC listed above:
- named-targets.AC4.1: `test_target_flag_parsed_correctly` — parse `["prog", "--target", "foo"]`, verify `cli_args.target == Some("foo".to_string())`
- named-targets.AC4.2: `test_source_flag_parsed_correctly` — parse `["prog", "--source", "bar"]`, verify `cli_args.source == Some("bar".to_string())`
- named-targets.AC4.3: `test_old_output_flag_not_recognized` — parse `["prog", "--output", "path"]`, verify `target.is_none()` (flag is ignored/unrecognized)
- named-targets.AC4.3: `test_old_staging_path_flag_not_recognized` — parse `["prog", "--staging-path", "path"]`, verify `target.is_none()`
- named-targets.AC4.4: No automated test needed (help text verified by manual `cargo run -- --help`)

**Verification:**

Run: `cargo test cli::tests`
Expected: All CLI tests pass (updated + new)

Run: `cargo test`
Expected: All tests pass

**Commit:** `test: update CLI tests for --target and --source flags`

<!-- END_TASK_4 -->
