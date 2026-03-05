# Named Targets — Test Requirements

This document maps each acceptance criterion from the named-targets design to its testing strategy: automated tests (unit or integration) with expected file paths, or human verification with justification.

## Summary Table

| AC ID | Description | Test Type | Phase | Test Location | Notes |
|-------|-------------|-----------|-------|---------------|-------|
| AC1.1 | TargetConfig full-field deserialization | Unit | 1 | `src/config.rs` (`test_target_config_*`) | serde_yaml deserialization |
| AC1.2 | TargetConfig minimal deserialization | Unit | 1 | `src/config.rs` (`test_target_config_*`) | Optional fields are None |
| AC1.3 | resolve_target fills compression_level | Unit | 1 | `src/config.rs` (`test_resolve_target_*`) | Fallback from delta_sink |
| AC1.4 | resolve_target fills heavy_compression | Unit | 1 | `src/config.rs` (`test_resolve_target_*`) | Fallback from delta_sink |
| AC1.5 | resolve_target fills offline_batch_size | Unit | 1 | `src/config.rs` (`test_resolve_target_*`) | Fallback from delta_sink |
| AC1.6 | resolve_target uses target-level storage | Unit | 1 | `src/config.rs` (`test_resolve_target_*`) | Per-target S3 credentials |
| AC1.7 | resolve_target falls back to global storage | Unit | 1 | `src/config.rs` (`test_resolve_target_*`) | Global config.storage used |
| AC1.8 | resolve_target errors on unknown name | Unit | 1 | `src/config.rs` (`test_resolve_target_*`) | Returns Err with name |
| AC2.1 | Env var overrides table_path | Unit | 1 | `src/config.rs` (`test_target_env_var_*`) | CERTSTREAM_TARGETS_<NAME>_TABLE_PATH |
| AC2.2 | Env var overrides compression_level | Unit | 1 | `src/config.rs` (`test_target_env_var_*`) | CERTSTREAM_TARGETS_<NAME>_COMPRESSION_LEVEL |
| AC2.3 | Env var overrides target storage | Unit | 1 | `src/config.rs` (`test_target_env_var_*`) | S3 endpoint trigger |
| AC3.1 | file:// table_path passes validation | Unit | 2 | `src/config.rs` (`test_validate_target_*`) | parse_table_uri accepts |
| AC3.2 | s3:// with full S3 config passes | Unit | 2 | `src/config.rs` (`test_validate_target_*`) | Credential completeness |
| AC3.3 | Bare path fails validation | Unit | 2 | `src/config.rs` (`test_validate_target_*`) | No URI scheme |
| AC3.4 | s3:// without S3 creds fails | Unit | 2 | `src/config.rs` (`test_validate_target_*`) | Missing endpoint/region/keys |
| AC3.5 | Out-of-range compression fails | Unit | 2 | `src/config.rs` (`test_validate_target_*`) | ZstdLevel range 1-22 |
| AC3.6 | Empty targets map passes | Unit | 2 | `src/config.rs` (`test_validate_target_*`) | No targets is valid |
| AC4.1 | --target parses correctly | Unit | 3 | `src/cli.rs` (`test_target_flag_*`) | CliArgs.target populated |
| AC4.2 | --source parses correctly | Unit | 3 | `src/cli.rs` (`test_source_flag_*`) | CliArgs.source populated |
| AC4.3 | Old flags removed | Unit | 3 | `src/cli.rs` (`test_old_*_not_recognized`) | --output, --staging-path rejected |
| AC4.4 | Help text shows new flags | **Human** | 3 | N/A | `cargo run -- --help` |
| AC5.1 | --migrate without both flags exits | Integration | 6 | `src/config.rs` (`test_target_integration_*`) | Process exit validation |
| AC5.2 | --migrate --source without --target exits | Integration | 6 | `src/config.rs` (`test_target_integration_*`) | Process exit validation |
| AC5.3 | --extract-metadata without both flags exits | Integration | 6 | `src/config.rs` (`test_target_integration_*`) | Process exit validation |
| AC5.4 | --merge without both flags exits | Integration | 6 | `src/config.rs` (`test_target_integration_*`) | Process exit validation |
| AC5.5 | --backfill without --target exits (Delta) | Integration | 6 | `src/config.rs` (`test_target_integration_*`) | ZeroBus exempted |
| AC5.6 | --reparse-audit without --source exits | Integration | 6 | `src/config.rs` (`test_target_integration_*`) | Process exit validation |
| AC5.7 | Unknown target name exits with error | Unit | 1, 4 | `src/config.rs` (`test_resolve_target_*`) | resolve_target returns Err |
| AC5.8 | --target with --sink zerobus exits | Integration | 6 | `src/config.rs` (`test_target_integration_*`) | Incompatible flags |
| AC6.1 | run_migrate accepts ResolvedTarget | Unit | 5 | `src/backfill.rs` (`test_migrate_*`) | Writes to target.table_path |
| AC6.2 | run_extract_metadata accepts ResolvedTarget | Unit | 5 | `src/table_ops.rs` (`test_ac2_*`) | Writes to target.table_path |
| AC6.3 | run_merge accepts ResolvedTarget | Unit | 5 | `src/backfill.rs` (`test_ac3_*`) | Merges source into target |
| AC6.4 | run_backfill accepts ResolvedTarget | Unit | 5 | `src/backfill.rs` (`test_ac1_*`, `test_writer_*`) | Writes to target.table_path |
| AC6.5 | run_reparse_audit accepts ResolvedTarget | Unit | 5 | `src/table_ops.rs` (`test_ac1_*`) | Reads from source.table_path |
| AC6.6 | No operation refs config.delta_sink | **Human** + Grep | 5, 6 | N/A | Code review + grep verification |
| AC7.1 | End-to-end config->resolve->operate flow | Integration | 6 | `src/config.rs` (`test_target_integration_*`) | Full round-trip |
| AC7.2 | No dead code from old patterns | **Human** + Grep | 6 | N/A | grep verification |
| AC7.3 | CLAUDE.md updated | **Human** | 6 | N/A | Documentation review |

---

## Detailed Test Specifications

### AC1: TargetConfig deserializes and resolves correctly

**Phase:** 1
**Test file:** `src/config.rs` (in existing `#[cfg(test)] mod tests` block)
**Test type:** Unit

| AC | Test Name | What It Verifies |
|----|-----------|-----------------|
| AC1.1 | `test_target_config_all_fields_deserialize` | Deserialize a YAML TargetConfig with all fields (table_path, storage, compression_level, heavy_column_compression_level, offline_batch_size) and assert each value matches the input. |
| AC1.2 | `test_target_config_minimal_deserialize` | Deserialize a YAML TargetConfig with only `table_path`. Assert `storage` is `None`, `compression_level` is `None`, `heavy_column_compression_level` is `None`, `offline_batch_size` is `None`. |
| AC1.3 | `test_resolve_target_fills_compression_level` | Construct Config with a target missing `compression_level` and `delta_sink.compression_level = 12`. Call `resolve_target()`. Assert resolved `compression_level == 12`. |
| AC1.4 | `test_resolve_target_fills_heavy_compression_level` | Construct Config with a target missing `heavy_column_compression_level` and `delta_sink.heavy_column_compression_level = 18`. Call `resolve_target()`. Assert resolved `heavy_column_compression_level == 18`. |
| AC1.5 | `test_resolve_target_fills_offline_batch_size` | Construct Config with a target missing `offline_batch_size` and `delta_sink.offline_batch_size = 50000`. Call `resolve_target()`. Assert resolved `offline_batch_size == 50000`. |
| AC1.6 | `test_resolve_target_uses_target_storage` | Construct Config with a target that has its own `StorageConfig` with S3 credentials pointing to `s3://` table_path. Set global `config.storage` to different S3 credentials. Call `resolve_target()`. Assert `storage_options` map contains the target-level S3 endpoint, not the global one. |
| AC1.7 | `test_resolve_target_falls_back_to_global_storage` | Construct Config with a target that has no `storage` field and an `s3://` table_path. Set global `config.storage` with S3 credentials. Call `resolve_target()`. Assert `storage_options` map contains the global S3 endpoint. |
| AC1.8 | `test_resolve_target_unknown_name_returns_error` | Construct Config with empty `targets`. Call `resolve_target("nonexistent")`. Assert it returns `Err` and the error message contains `"nonexistent"`. |

---

### AC2: Env var override for targets

**Phase:** 1
**Test file:** `src/config.rs` (in existing `#[cfg(test)] mod tests` block)
**Test type:** Unit

| AC | Test Name | What It Verifies |
|----|-----------|-----------------|
| AC2.1 | `test_target_env_var_table_path_override` | Construct a TargetConfig with `table_path = "file:///original"`. Set env var `CERTSTREAM_TARGETS_MAIN_TABLE_PATH=file:///override`. Apply the env var overlay logic. Assert `table_path == "file:///override"`. Clean up env var. |
| AC2.2 | `test_target_env_var_compression_level_override` | Construct a TargetConfig with `compression_level = Some(9)`. Set env var `CERTSTREAM_TARGETS_MAIN_COMPRESSION_LEVEL=15`. Apply the env var overlay logic. Assert `compression_level == Some(15)`. Clean up env var. |
| AC2.3 | `test_target_env_var_storage_s3_override` | Construct a TargetConfig with YAML S3 storage. Set `CERTSTREAM_TARGETS_MAIN_STORAGE_S3_ENDPOINT` and related S3 env vars to different values. Apply env var overlay. Assert the target's storage S3 config uses the env var values. Clean up all env vars. |

---

### AC3: Validation

**Phase:** 2
**Test file:** `src/config.rs` (in existing `#[cfg(test)] mod tests` block)
**Test type:** Unit

| AC | Test Name | What It Verifies |
|----|-----------|-----------------|
| AC3.1 | `test_validate_target_file_uri_passes` | Config with target `table_path: "file:///tmp/test"`. Call `validate()`. Assert `Ok(())`. |
| AC3.2 | `test_validate_target_s3_with_full_creds_passes` | Config with target `table_path: "s3://bucket/path"` and complete per-target S3 storage. Call `validate()`. Assert `Ok(())`. |
| AC3.3 | `test_validate_target_bare_path_fails` | Config with target `table_path: "/tmp/bare-path"`. Call `validate()`. Assert `Err` with error field containing `targets.<name>.table_path`. |
| AC3.4 | `test_validate_target_s3_missing_creds_fails` | Config with target `table_path: "s3://bucket/path"` but no S3 storage. Call `validate()`. Assert `Err` with error field containing `storage.s3`. Also: test with empty individual credentials. |
| AC3.5 | `test_validate_target_compression_out_of_range_fails` | Config with target `compression_level: Some(0)` and `Some(23)`. Call `validate()`. Assert errors for both. Also test `heavy_column_compression_level` out of range. |
| AC3.6 | `test_validate_target_empty_map_passes` | Config with `targets: HashMap::new()`. Call `validate()`. Assert `Ok(())`. |

---

### AC4: CLI flags

**Phase:** 3
**Test file:** `src/cli.rs` (in existing `#[cfg(test)] mod tests` block)
**Test type:** Unit (AC4.1-AC4.3), Human (AC4.4)

| AC | Test Name | What It Verifies |
|----|-----------|-----------------|
| AC4.1 | `test_target_flag_parsed_correctly` | Parse `["prog", "--target", "foo"]`. Assert `cli_args.target == Some("foo".to_string())`. |
| AC4.2 | `test_source_flag_parsed_correctly` | Parse `["prog", "--source", "bar"]`. Assert `cli_args.source == Some("bar".to_string())`. |
| AC4.3 | `test_old_output_flag_not_recognized` | Parse `["prog", "--output", "path"]`. Assert `cli_args.target.is_none()`. |
| AC4.3 | `test_old_staging_path_flag_not_recognized` | Parse `["prog", "--staging-path", "path"]`. Assert `cli_args.target.is_none()`. |
| AC4.4 | N/A (human) | See Human Verification section below. |

---

### AC5: Dispatch resolution

**Phase:** 4 (code), 6 (tests)
**Test file:** `src/config.rs` (in existing `#[cfg(test)] mod tests` block)
**Test type:** Unit (AC5.7), Integration (AC5.1-AC5.6, AC5.8)

| AC | Test Name | What It Verifies |
|----|-----------|-----------------|
| AC5.1-AC5.6 | *Code review + Phase 6 integration* | Dispatch conditionals check for missing required flags and call `process::exit(1)`. Verified by code review (simple if-then-exit patterns) and Phase 6 integration tests. |
| AC5.7 | `test_resolve_target_unknown_name_returns_error` | Call `config.resolve_target("nonexistent")`. Assert `Err`. (Covered by AC1.8 test.) |
| AC5.8 | *Code review + Phase 6 integration* | Backfill dispatch checks `is_zerobus && cli_args.target.is_some()` and exits. |

**Why AC5.1-AC5.6 and AC5.8 are not standard unit tests:** The dispatch logic in `main.rs` calls `std::process::exit(1)` for validation failures, which terminates the entire process and kills the test runner. The underlying `resolve_target()` is fully unit-tested (AC1.8/AC5.7), and the flag-presence conditionals are simple 2-3 line patterns verified by code review.

---

### AC6: Operation function signatures

**Phase:** 5
**Test files:** `src/backfill.rs` and `src/table_ops.rs` (in existing `#[cfg(test)] mod tests` blocks)
**Test type:** Unit

| AC | Test Name(s) | What It Verifies |
|----|-------------|-----------------|
| AC6.1 | `test_migrate_*` (updated) | `run_migrate` accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` and writes to `target.table_path`. |
| AC6.2 | `test_ac2_*` (updated) | `run_extract_metadata` accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` and writes to `target.table_path`. |
| AC6.3 | `test_ac3_*` (updated) | `run_merge` accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` and merges source into target. |
| AC6.4 | `test_ac1_*`, `test_writer_*` (updated) | `run_backfill` accepts `(config, target: ResolvedTarget, ...)` and writes to `target.table_path`. |
| AC6.5 | `test_ac1_*` in table_ops.rs (updated) | `run_reparse_audit` accepts `(source: ResolvedTarget, ...)` and reads from `source.table_path`. |
| AC6.6 | See Human Verification section | No operation function references `config.delta_sink` for table paths or write settings. |

**Note on detect_gaps refactoring:** Tests that previously exercised dual-table (main + staging UNION ALL) gap detection are updated or removed since `staging_path` is removed from `detect_gaps()`.

---

### AC7: Integration and cleanup

**Phase:** 6
**Test file:** `src/config.rs` (AC7.1), N/A (AC7.2, AC7.3)
**Test type:** Integration (AC7.1), Human (AC7.2, AC7.3)

| AC | Test Name | What It Verifies |
|----|-----------|-----------------|
| AC7.1 | `test_target_integration_full_resolve_flow` | Construct Config with targets, call `resolve_target()`, verify resolved values, optionally create Delta table with resolved values. |
| AC7.2 | N/A (human + grep) | See Human Verification section. |
| AC7.3 | N/A (human) | See Human Verification section. |

---

## Human Verification Section

### AC4.4: Help text shows --target and --source flags with descriptions

**Verification approach:**
1. Run `cargo run -- --help`
2. Verify the output contains `--target <NAME>` and `--source <NAME>` with descriptions
3. Verify `--output`, `--staging-path` do NOT appear in the help text

### AC5.1-AC5.6, AC5.8: Dispatch exit validation

**Verification approach:**
1. **Code review:** Inspect each dispatch block in `src/main.rs` for the flag-presence conditionals
2. **Optional CLI smoke test:** Run the binary with missing flags and verify error messages

### AC6.6: No operation function references config.delta_sink for table paths or write settings

**Verification approach:**
```
grep -n "config\.delta_sink\.table_path\|config\.delta_sink\.compression_level\|config\.delta_sink\.heavy_column_compression_level\|config\.delta_sink\.offline_batch_size" src/backfill.rs src/table_ops.rs
```
Expected: Zero matches. (`config.delta_sink.batch_size` and `config.delta_sink.flush_interval_secs` in `run_backfill` are NOT violations — these are runtime operational settings.)

### AC7.2: No dead code from old patterns remains

**Verification approach:**
```
grep -rn "staging_path\|migrate_output\|migrate_source\|output_path\|validate_staging_path_uri" src/
grep -rn '"\-\-output"\|"\-\-staging-path"' src/
```
Expected: Zero matches.

### AC7.3: CLAUDE.md updated

**Verification approach:**
1. Review CLAUDE.md for updated commands, architecture, and contracts sections
2. Run `grep -n "staging-path\|--output" CLAUDE.md` — expected zero matches for old flag patterns

---

## Test Execution Plan

| Phase | Command | Expected New/Updated Tests |
|-------|---------|---------------------------|
| 1 | `cargo test config::tests::test_target` | ~11 tests (AC1.1-AC1.8, AC2.1-AC2.3) |
| 2 | `cargo test config::tests::test_validate_target` | ~6 tests (AC3.1-AC3.6) |
| 3 | `cargo test cli::tests` | ~4 new + ~10 updated tests (AC4.1-AC4.3) |
| 4 | `cargo test` | No new tests; AC5.7 covered by Phase 1 |
| 5 | `cargo test` | ~20 updated tests (AC6.1-AC6.5) |
| 6 | `cargo test config::tests::test_target_integration` | ~1 integration test (AC7.1) |

**Full test suite:** `cargo test` after each phase to verify no regressions.

**Total coverage:** ~42 automated tests covering 28 of 31 AC cases. 3 criteria (AC4.4, AC7.2, AC7.3) plus the process-exit subset of AC5.1-AC5.6/AC5.8 and the negative assertion AC6.6 require human verification or grep-based checks.
