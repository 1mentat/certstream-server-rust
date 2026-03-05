# Named Targets — Human Test Plan

## Prerequisites

- Rust toolchain installed with `cargo` available
- Working directory: project root
- `cargo test` passes (all 502 tests)
- A `config.yaml` file with at least one named target defined (see config.example.yaml)

## Phase 1: Help Text Verification (AC4.4)

| Step | Action | Expected |
|------|--------|----------|
| 1 | Run `cargo run -- --help` | Output is displayed without errors |
| 2 | Scan the "TARGET OPTIONS" section | `--target <NAME>` is listed with description "Named target for write destination (from config targets section)" |
| 3 | Scan the "TARGET OPTIONS" section | `--source <NAME>` is listed with description "Named target for read source (from config targets section)" |
| 4 | Search help text for `--output` | NOT present anywhere in the help output |
| 5 | Search help text for `--staging-path` | NOT present anywhere in the help output |
| 6 | Confirm the note about old flags | A note explaining that `--output`, `--staging-path`, and old `--source <PATH>` have been replaced by named targets is present |

## Phase 2: Dispatch Exit Validation (AC5.1-AC5.6, AC5.8)

Prepare a minimal config.yaml:
```yaml
targets:
  main:
    table_path: "file:///tmp/test-main"
  staging:
    table_path: "file:///tmp/test-staging"
ct_logs_url: "https://www.gstatic.com/ct/log_list/v3/all_logs_list.json"
```

| Step | Action | Expected |
|------|--------|----------|
| 1 | Run `cargo run -- --migrate` (no --source or --target) | Prints "Error: --migrate requires both --source <NAME> and --target <NAME>" and exits with code 1 |
| 2 | Run `cargo run -- --migrate --source main` (no --target) | Prints error about missing --target and exits 1 |
| 3 | Run `cargo run -- --migrate --target staging` (no --source) | Prints error about missing --source and exits 1 |
| 4 | Run `cargo run -- --extract-metadata` (no flags) | Prints "Error: --extract-metadata requires both --source <NAME> and --target <NAME>" and exits 1 |
| 5 | Run `cargo run -- --extract-metadata --source main` (no --target) | Prints error about missing --target and exits 1 |
| 6 | Run `cargo run -- --merge` (no flags) | Prints "Error: --merge requires both --source <NAME> and --target <NAME>" and exits 1 |
| 7 | Run `cargo run -- --merge --source main` (no --target) | Prints error about missing --target and exits 1 |
| 8 | Run `cargo run -- --backfill` (no --target, no --sink zerobus) | Prints "Error: --backfill requires --target <NAME> (unless using --sink zerobus)" and exits 1 |
| 9 | Run `cargo run -- --reparse-audit` (no --source) | Prints "Error: --reparse-audit requires --source <NAME>" and exits 1 |
| 10 | Run `cargo run -- --backfill --sink zerobus --from 0 --target main` | Prints "Error: --target is not compatible with --sink zerobus" and exits 1 |
| 11 | Run `cargo run -- --migrate --source nonexistent --target main` | Prints error about unknown target "nonexistent" listing available targets and exits 1 |

## Phase 3: No Dead Code Verification (AC7.2)

| Step | Action | Expected |
|------|--------|----------|
| 1 | Run `grep -rn 'staging_path\|migrate_output\|migrate_source\|output_path\|validate_staging_path_uri' src/` | Zero matches |
| 2 | Run `grep -rn '"--output"\|"--staging-path"' src/` | Zero matches |
| 3 | Run `cargo build 2>&1 \| grep "warning.*unused"` | No warnings about unused variables, functions, or imports related to old CLI patterns |

## Phase 4: No Operation References config.delta_sink (AC6.6)

| Step | Action | Expected |
|------|--------|----------|
| 1 | Run `grep -n 'config\.delta_sink\.table_path\|config\.delta_sink\.compression_level\|config\.delta_sink\.heavy_column_compression_level\|config\.delta_sink\.offline_batch_size' src/backfill.rs src/table_ops.rs` | Zero matches. (Note: `config.delta_sink.batch_size` and `config.delta_sink.flush_interval_secs` in `run_backfill` are allowed -- these are runtime operational settings, not path/write settings) |
| 2 | Verify `run_migrate` signature in `src/backfill.rs` | Accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` -- no `Config` parameter for table_path or compression |
| 3 | Verify `run_merge` signature in `src/backfill.rs` | Accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` -- no `Config` parameter |
| 4 | Verify `run_extract_metadata` signature in `src/table_ops.rs` | Accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` -- no `Config` parameter |
| 5 | Verify `run_reparse_audit` signature in `src/table_ops.rs` | Accepts `(source: ResolvedTarget, ...)` -- no `Config` parameter |

## Phase 5: CLAUDE.md Documentation Review (AC7.3)

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `CLAUDE.md` | File exists and is readable |
| 2 | Check the "Commands" section | Commands reference `--target` and `--source` flags (e.g., `--merge --source staging --target main`) |
| 3 | Run `grep -n 'staging-path\|--output' CLAUDE.md` | Zero matches for old flag patterns |
| 4 | Check for "Named Targets Contracts" section | Section exists and documents TargetConfig, ResolvedTarget, resolve_target(), env var pattern, --target/--source flags |
| 5 | Check "Backfill Contracts" section | References `--target <NAME>` instead of `--staging-path <PATH>` |
| 6 | Check "Merge Contracts" section | References `--merge --source <NAME> --target <NAME>` instead of `--merge --staging-path <PATH>` |
| 7 | Check "Migrate Contracts" section | References `--migrate --source <NAME> --target <NAME>` instead of `--migrate --output <PATH>` |
| 8 | Check "Reparse Audit Contracts" section | References `--reparse-audit --source <NAME>` |
| 9 | Check "Metadata Extraction Contracts" section | References `--extract-metadata --source <NAME> --target <NAME>` |
| 10 | Verify `Last verified` and `Last context update` dates | Should be updated to 2026-03-05 or later |

## End-to-End: Config -> Resolve -> Migrate Flow

**Purpose:** Validates that a user can define named targets in config.yaml, then use CLI flags to reference them for a real operation.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Create `/tmp/e2e_config.yaml` with two targets: `source` pointing to `file:///tmp/e2e_source` and `output` pointing to `file:///tmp/e2e_output`, with `delta_sink.compression_level: 12` | Config file created |
| 2 | Set `CERTSTREAM_CONFIG=/tmp/e2e_config.yaml` | Environment variable set |
| 3 | Run `cargo run -- --validate-config` | Outputs "Configuration is valid" (or similar), exits 0 |
| 4 | Run `cargo run -- --migrate --source source --target output` | If source table does not exist: exits 1 with "failed to open source table" message (expected since no data exists). This confirms the full dispatch path works: config loaded -> targets resolved -> `run_migrate` called with `ResolvedTarget` parameters. |

## End-to-End: Env Var Override for Target

**Purpose:** Validates that environment variables override YAML target config at runtime.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Create config.yaml with target `main` pointing to `file:///tmp/yaml-path` | Config file created |
| 2 | Set `CERTSTREAM_TARGETS_MAIN_TABLE_PATH=file:///tmp/env-override-path` | Env var set |
| 3 | Run `cargo run -- --reparse-audit --source main` | Error message mentions `/tmp/env-override-path` (not `/tmp/yaml-path`), confirming the env var override took effect |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 | `config::tests::test_target_config_all_fields_yaml` | -- |
| AC1.2 | `config::tests::test_target_config_only_table_path_yaml` | -- |
| AC1.3 | `config::tests::test_resolve_target_fill_compression_level` | -- |
| AC1.4 | `config::tests::test_resolve_target_fill_heavy_column_compression_level` | -- |
| AC1.5 | `config::tests::test_resolve_target_fill_offline_batch_size` | -- |
| AC1.6 | `config::tests::test_resolve_target_target_level_storage` | -- |
| AC1.7 | `config::tests::test_resolve_target_fallback_to_global_storage` | -- |
| AC1.8 | `config::tests::test_resolve_target_unknown_target` | -- |
| AC2.1 | `config::tests::test_target_env_var_table_path_override` | -- |
| AC2.2 | `config::tests::test_target_env_var_compression_level_override` | -- |
| AC2.3 | `config::tests::test_target_env_var_s3_storage_override` | -- |
| AC3.1 | `config::tests::test_validate_target_file_uri_valid` | -- |
| AC3.2 | `config::tests::test_validate_target_s3_uri_with_storage_valid` | -- |
| AC3.3 | `config::tests::test_validate_target_bare_path_fails` | -- |
| AC3.4 | `config::tests::test_validate_target_s3_uri_without_credentials_fails` + 4 tests | -- |
| AC3.5 | `config::tests::test_validate_target_compression_level_zero_fails` + 3 tests | -- |
| AC3.6 | `config::tests::test_validate_target_empty_targets_valid` | -- |
| AC4.1 | `cli::tests::test_target_flag_parsed_correctly` | -- |
| AC4.2 | `cli::tests::test_source_flag_parsed_correctly` | -- |
| AC4.3 | `cli::tests::test_old_output_flag_not_recognized` + 1 test | -- |
| AC4.4 | -- | Phase 1, Steps 1-6 |
| AC5.1 | Code review (main.rs:103) | Phase 2, Step 1 |
| AC5.2 | Code review (main.rs:103) | Phase 2, Steps 2-3 |
| AC5.3 | Code review (main.rs:177) | Phase 2, Steps 4-5 |
| AC5.4 | Code review (main.rs:208) | Phase 2, Steps 6-7 |
| AC5.5 | Code review (main.rs:245) | Phase 2, Step 8 |
| AC5.6 | Code review (main.rs:148) | Phase 2, Step 9 |
| AC5.7 | `config::tests::test_resolve_target_unknown_target` | Phase 2, Step 11 |
| AC5.8 | Code review (main.rs:241) | Phase 2, Step 10 |
| AC6.1 | `backfill::tests::test_migrate_basic_schema_and_data` | -- |
| AC6.2 | `table_ops::tests::test_ac2_1_extract_metadata_output_schema_has_19_columns_no_as_der` | -- |
| AC6.3 | `backfill::tests::test_ac3_1_merge_inserts_non_duplicate_records` | -- |
| AC6.4 | `backfill::tests::test_writer_basic_functionality` | -- |
| AC6.5 | `table_ops::tests::test_ac1_1_audit_completes_with_zero_mismatches` | -- |
| AC6.6 | -- | Phase 4, Steps 1-5 |
| AC7.1 | `config::tests::test_target_integration` + 2 variant tests | -- |
| AC7.2 | -- | Phase 3, Steps 1-3 |
| AC7.3 | -- | Phase 5, Steps 1-10 |
