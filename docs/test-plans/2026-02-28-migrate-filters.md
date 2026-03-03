# Human Test Plan: Migrate Filters

## Prerequisites

- Rust toolchain installed (edition 2024)
- `cargo test` passes (397 tests, 0 failures)
- A Delta table with the old schema (as_der as base64 Utf8) available for testing, or the ability to create one using the existing test fixtures
- The `certstream-server-rust` binary built via `cargo build`

## Phase 1: CLI Flag Parsing and Help Text

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Run `cargo run -- --help` | Help output displays a "MIGRATION OPTIONS:" section listing `--migrate`, `--output <PATH>`, `--source <PATH>`, `--from <DATE>`, and `--to <DATE>`. The note about `--from` being shared between backfill and migrate is present. |
| 1.2 | Run `cargo run -- --migrate` (without `--output`) | Process exits with error message `Error: --migrate requires --output <PATH>` and exit code 1. |
| 1.3 | Run `cargo run -- --migrate --output /tmp/test_out --from bad-date` | Process exits with an error message indicating `--from` value `bad-date` is not a valid date, mentioning expected format `YYYY-MM-DD`. |
| 1.4 | Run `cargo run -- --migrate --output /tmp/test_out --to 13-2024-01` | Process exits with an error message indicating `--to` value is not a valid date. |
| 1.5 | Run `cargo run -- --migrate --output /tmp/test_out --from 2024-13-01` | Process exits with error about invalid month. |
| 1.6 | Run `cargo run -- --backfill --from not-a-number` | Process exits with error message `Error: --from value 'not-a-number' is not a valid integer for backfill mode`. |

## Phase 2: Source Path Resolution

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Create a source Delta table at `/tmp/test_source` with old-schema data. Run `cargo run -- --migrate --output /tmp/test_out --source /tmp/test_source` | Migration reads from `/tmp/test_source` regardless of what `delta_sink.table_path` is set to in config.yaml. Exit code 0. |
| 2.2 | Set `delta_sink.table_path: /tmp/test_source` in config.yaml. Run `cargo run -- --migrate --output /tmp/test_out` (without `--source`) | Migration reads from `/tmp/test_source` (the config fallback). Exit code 0. |
| 2.3 | Run `cargo run -- --migrate --output /tmp/test_out --source /nonexistent/path` | Migration exits with code 1 and a log warning about failing to open the source table. |

## Phase 3: Date Filtering

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Create a source table with partitions on `2024-01-14`, `2024-01-15`, and `2024-01-16`. Run `cargo run -- --migrate --output /tmp/test_out --source /tmp/test_source --from 2024-01-15` | Output table contains only records from `2024-01-15` and `2024-01-16` partitions. The `2024-01-14` partition is excluded. |
| 3.2 | Same source table. Run `cargo run -- --migrate --output /tmp/test_out --source /tmp/test_source --to 2024-01-15` | Output table contains only records from `2024-01-14` and `2024-01-15` partitions. The `2024-01-16` partition is excluded. |
| 3.3 | Same source table. Run `cargo run -- --migrate --output /tmp/test_out --source /tmp/test_source --from 2024-01-15 --to 2024-01-15` | Output table contains only records from the `2024-01-15` partition (exactly 1 partition). |
| 3.4 | Same source table. Run `cargo run -- --migrate --output /tmp/test_out --source /tmp/test_source --from 2025-01-01` | All partitions are filtered out. Migration exits with code 0 and a log message "source table is empty or no partitions match filters, nothing to migrate". |

## End-to-End: Full Migration with Filters from Separate Source

**Purpose:** Validate that a user can migrate a subset of an existing Delta table from a custom source path to a new output path, with date filtering, and the resulting table has the correct schema (Binary `as_der` instead of Utf8).

**Steps:**

1. Create an old-schema source table at `/tmp/e2e_source` with 3 partitions (`2024-01-14`, `2024-01-15`, `2024-01-16`), each containing records with base64-encoded `as_der` values.
2. Run: `cargo run -- --migrate --output /tmp/e2e_output --source /tmp/e2e_source --from 2024-01-15 --to 2024-01-16`
3. Verify exit code is 0.
4. Open `/tmp/e2e_output` as a Delta table (e.g., using Python `deltalake` or Rust test harness).
5. Verify the output table has exactly 2 partitions (`2024-01-15` and `2024-01-16`).
6. Verify `as_der` column is `DataType::Binary` (not `DataType::Utf8`).
7. Verify the binary values match the STANDARD base64 decode of the original source values.
8. Verify all other columns (cert_index, fingerprint, update_type, etc.) are unchanged.

## End-to-End: Backfill Mode Still Works After --from Change to String

**Purpose:** Validate that the shared `--from` flag works correctly in backfill mode (parsed as integer) after the refactor that changed it to store as String internally.

**Steps:**

1. Ensure a valid `certstream_state.json` exists with at least one log entry.
2. Ensure `delta_sink.enabled = true` and `delta_sink.table_path` points to a valid location.
3. Run: `cargo run -- --backfill --from 0 --logs "some_known_log_substring"`
4. Verify the process starts backfill mode and attempts to fetch from index 0.
5. Verify exit code is 0 (or 1 if CT log connectivity fails, but the key assertion is that `--from 0` was correctly parsed as integer).

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|------------|-------|
| AC1.5 Help text formatting | Visual inspection of terminal output formatting and section grouping | Run `cargo run -- --help` and verify that the MIGRATION OPTIONS section is visually distinct, properly indented, and lists all three flags (`--source`, `--from <DATE>`, `--to <DATE>`) with descriptions. Verify the note about `--from` being shared between backfill and migrate is present and clear. |
| AC2.1 Dispatch wiring | Main.rs dispatch is not unit-testable; requires code review or end-to-end run | Review `src/main.rs` lines 81-127 to confirm: (a) `--from` and `--to` are validated via `validate_date_format()` before `run_migrate()` is called, (b) `source_path` is computed from `--source` or config fallback, (c) all parameters are passed to `run_migrate()` in the correct order. |
| AC2.3 Parse-to-u64 wiring in backfill | The String-to-u64 parse happens in main.rs which is not unit-tested | Review `src/main.rs` lines 155-165 to confirm: (a) `backfill_from` is parsed from `Option<String>` to `Option<u64>` via `.parse::<u64>()`, (b) parse failure exits with a descriptive error, (c) the resulting `Option<u64>` is passed to both `validate_backfill_sink_command` and `run_backfill`. |
| Log output quality | Subjective assessment of logging for production readiness | Run a migration with date filters and review log output for: clear start/end messages, per-partition progress, total rows migrated, decode failure counts. Verify log levels are appropriate (info for progress, warn for failures). |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 --from stores raw string | `test_from_stores_integer_string`, `test_from_stores_date_string` | -- |
| AC1.2 --source parsed | `test_source_flag_parsed_correctly`, `test_source_flag_none_when_not_provided` | -- |
| AC1.3 --to parsed | `test_to_flag_parsed_correctly`, `test_to_flag_none_when_not_provided` | -- |
| AC1.4 validate_date_format | `test_validate_date_format_valid`, `test_validate_date_format_invalid`, `test_validate_date_format_known_limitation` | -- |
| AC1.5 Help text | -- (structural) | Phase 1 Step 1.1 |
| AC2.1 Date validation dispatch | `test_validate_date_format_*` (validates function) | Phase 1 Steps 1.3-1.5 |
| AC2.2 Source path fallback | `test_migrate_with_date_filters` | Phase 2 Steps 2.1-2.2 |
| AC2.3 --from String to u64 parse | `validate_backfill_sink_command` tests | Phase 1 Step 1.6 |
| AC3.1 --from filters partitions | `test_migrate_with_date_filters` | Phase 3 Step 3.1 |
| AC3.2 --to filters partitions | `test_migrate_with_date_filters` | Phase 3 Step 3.2 |
| AC3.3 --from + --to inclusive range | `test_migrate_with_date_filters` | Phase 3 Step 3.3 |
| AC3.4 Source path replaces config | `test_migrate_with_date_filters` | Phase 2 Step 2.1 |
| AC3.5 Existing tests pass | 4 existing migrate tests updated | -- (cargo test confirms) |
