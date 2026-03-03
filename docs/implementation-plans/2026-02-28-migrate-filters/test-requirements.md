# Migrate Filters -- Test Requirements

This document maps every acceptance criterion (MF1.1 through MF3.2) to specific
automated tests.

---

## Automated Tests

### migrate-filters.AC1: CLI parsing for new flags

| AC | Criterion Text | Test Type | Test Location | Description |
|----|---------------|-----------|---------------|-------------|
| AC1.1 | `--from` stores raw string instead of parsing to u64 inline; backfill dispatch parses to u64 at call site | unit | `src/cli.rs` (test module) | Tests that `CliArgs::parse_args` with `--from 12345` stores `Some("12345".to_string())` in `backfill_from`. Tests that `--from 2024-01-15` also stores the string. Existing tests updated from `Some(12345u64)` to `Some("12345".to_string())`. |
| AC1.2 | `--source <PATH>` is correctly parsed and stored in `migrate_source` | unit | `src/cli.rs` (test module) | Tests that `--source /data/old-table` stores `Some("/data/old-table".to_string())` in `migrate_source`. Tests that omitting `--source` leaves `migrate_source` as `None`. |
| AC1.3 | `--to <DATE>` is correctly parsed and stored in `to` field | unit | `src/cli.rs` (test module) | Tests that `--to 2024-06-30` stores `Some("2024-06-30".to_string())` in `to`. Tests that omitting `--to` leaves `to` as `None`. |
| AC1.4 | `validate_date_format()` accepts YYYY-MM-DD and rejects invalid formats | unit | `src/cli.rs` (test module) | Tests valid dates (`2024-01-15`, `2025-12-31`), invalid formats (`01-15-2024`, `2024/01/15`, `2024-1-5`, `abc`, `2024-13-01`, `2024-01-32`). |
| AC1.5 | Help text includes `--source`, `--from <DATE>`, `--to <DATE>` under MIGRATION OPTIONS | visual | N/A | Structural verification: read `print_help()` and confirm the three flags are listed. |

### migrate-filters.AC2: Dispatch logic in main.rs

| AC | Criterion Text | Test Type | Test Location | Description |
|----|---------------|-----------|---------------|-------------|
| AC2.1 | Migrate dispatch validates `--from`/`--to` date format before calling `run_migrate()` | integration | `src/backfill.rs` (test module, via run_migrate args) | Invalid date format passed to `run_migrate` is caught at the main.rs dispatch level. Since main.rs is not unit-tested directly, the date validation function is tested in cli.rs (AC1.4) and correct wiring is verified by code review. |
| AC2.2 | Migrate dispatch computes `source_path` from `--source` or falls back to `config.delta_sink.table_path` | integration | `src/backfill.rs` (test module) | Test that `run_migrate` with an explicit `source_path` reads from that path, not from `config.delta_sink.table_path`. Test that when `source_path` equals `config.delta_sink.table_path`, behavior is unchanged. |
| AC2.3 | Backfill dispatch parses `backfill_from` from String to u64; exits with error if not a valid integer | unit | `src/cli.rs` (test module) | `validate_backfill_sink_command` still works with `Option<u64>` after the String is parsed at the dispatch site. Main.rs code review confirms the parse-to-u64 step. |

### migrate-filters.AC3: Partition date filtering in run_migrate

| AC | Criterion Text | Test Type | Test Location | Description |
|----|---------------|-----------|---------------|-------------|
| AC3.1 | `--from <DATE>` filters out partitions with `seen_date < from` | integration | `src/backfill.rs` (test module) | Creates a source table with partitions on `2024-01-15` and `2024-01-16`. Runs `run_migrate` with `from_date = Some("2024-01-16")`. Verifies only the `2024-01-16` partition appears in output. |
| AC3.2 | `--to <DATE>` filters out partitions with `seen_date > to` | integration | `src/backfill.rs` (test module) | Creates a source table with partitions on `2024-01-15` and `2024-01-16`. Runs `run_migrate` with `to_date = Some("2024-01-15")`. Verifies only the `2024-01-15` partition appears in output. |
| AC3.3 | `--from` and `--to` together filter to an inclusive range | integration | `src/backfill.rs` (test module) | Creates a source table with 3 partitions (`2024-01-14`, `2024-01-15`, `2024-01-16`). Runs with `from_date = Some("2024-01-15")` and `to_date = Some("2024-01-15")`. Verifies only `2024-01-15` partition appears. |
| AC3.4 | Source path parameter replaces `config.delta_sink.table_path` in all `run_migrate` codepaths | integration | `src/backfill.rs` (test module) | Test uses a `source_path` different from `config.delta_sink.table_path`. Verifies migration reads from `source_path` and writes to `output_path`. |
| AC3.5 | Existing migrate tests pass with updated signature | integration | `src/backfill.rs` (test module) | All 4 existing migrate tests updated to pass `source_path`, `None`, `None` and continue to pass. |

---

## Test Summary by File

| Test File | ACs Covered | New Tests | Modified Tests |
|-----------|-------------|-----------|----------------|
| `src/cli.rs` | AC1.1, AC1.2, AC1.3, AC1.4, AC1.5, AC2.3 | 8-10 new (--source parsing, --to parsing, --from as string, validate_date_format valid/invalid, combined flags) | 3-4 modified (backfill_from assertions change from `Some(12345u64)` to `Some("12345".to_string())`; validate_backfill_sink_command tests stay u64 since validation function is unchanged) |
| `src/backfill.rs` | AC2.2, AC3.1, AC3.2, AC3.3, AC3.4, AC3.5 | 2-3 new (date filter test, source path test, combined from+to test) | 4 modified (existing migrate tests updated to pass `source_path`, `None`, `None`) |
| `src/main.rs` | AC2.1, AC2.2 | 0 | 1 modified (migrate dispatch wiring) |

---

## Phase-to-Test Mapping

| Phase | Tasks | ACs Tested | New/Modified Tests |
|-------|-------|------------|-------------------|
| Phase 1 | Tasks 1-4 | AC1.1, AC1.2, AC1.3, AC1.4, AC1.5, AC2.3 | CLI struct changes, --from as String, --source/--to parsing, date validation function and tests |
| Phase 2 | Tasks 1-3 | AC2.1, AC2.2, AC3.1-AC3.5 | main.rs dispatch updates, run_migrate signature change, partition filtering, integration tests |
