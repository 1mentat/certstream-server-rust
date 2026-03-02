# Human Test Plan: Reparse Audit & Metadata Table

## Prerequisites

- Rust toolchain installed, project builds: `cargo build`
- All automated tests pass: `cargo test table_ops::tests` (29 passing) and `cargo test cli::tests` (22 passing)
- A populated Delta table exists at `config.delta_sink.table_path` (either from live server operation or from running backfill)
- Config file at `config.yaml` or default location, with `delta_sink.enabled = true` and a valid `delta_sink.table_path`

## Phase 1: CLI Dispatch Verification

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Run `cargo run -- --reparse-audit 2>&1; echo "exit: $?"` with a valid Delta table configured | Output includes "Reparse Audit Report" header, field statistics, and `exit: 0` on the last line |
| 1.2 | Run `cargo run -- --extract-metadata 2>&1; echo "exit: $?"` (without --output) | stderr contains `Error: --extract-metadata requires --output <PATH>` and last line is `exit: 1` |
| 1.3 | Run `cargo run -- --extract-metadata --output /tmp/human_test_metadata 2>&1; echo "exit: $?"` with valid source table | Output includes "Metadata extraction completed successfully" and `exit: 0`. Verify `/tmp/human_test_metadata/_delta_log/` directory exists. |
| 1.4 | Run `cargo run -- --help` | Help output includes "TABLE OPERATIONS:" section listing `--reparse-audit`, `--extract-metadata`, `--output <PATH>`, `--from-date <DATE>`, `--to-date <DATE>` |

## Phase 2: Date Filtering Verification

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Run `cargo run -- --reparse-audit --from-date 2026-02-01 --to-date 2026-02-28 2>&1` (adjust dates to match data in your table) | Report shows "Records scanned:" with a count reflecting only the date-filtered partitions. "Partitions scanned:" count should be less than or equal to the total partition count. |
| 2.2 | Run `cargo run -- --reparse-audit --from-date 2099-01-01 2>&1; echo "exit: $?"` | Output contains "No records found in the specified date range" and exits 0 |
| 2.3 | Run `cargo run -- --reparse-audit --from-date not-a-date 2>&1; echo "exit: $?"` | Log contains `invalid --from-date format 'not-a-date', expected YYYY-MM-DD` and exits 1 |
| 2.4 | Run `cargo run -- --extract-metadata --output /tmp/human_test_filtered --from-date 2099-01-01 2>&1; echo "exit: $?"` | Exits 0 with informational message about no records found |

## Phase 3: Reparse Audit Content Verification

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Run `cargo run -- --reparse-audit 2>&1` against a table with production data | Report header "Reparse Audit Report" followed by "Records scanned:", "Partitions scanned:", "Records with mismatches: N (X.X%)", "Unparseable records: N" |
| 3.2 | If mismatches > 0, verify "Field breakdown:" section lists field names with counts | Each listed field should be one of: subject_aggregated, issuer_aggregated, all_domains, signature_algorithm, is_ca, not_before, not_after, serial_number |
| 3.3 | If mismatches > 10, verify "Sample mismatches (10 of N):" header appears | Exactly 10 `[cert_index=..., source=...]` blocks should follow, each with field-level stored vs reparsed values |
| 3.4 | Verify "Unparseable records" count is plausible | Any records with corrupt `as_der` or that fail `parse_certificate()` should be counted here, not as mismatches |

## Phase 4: Metadata Extraction Content Verification

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Run `cargo run -- --extract-metadata --output /tmp/human_test_meta_full 2>&1` | Exits 0. Output directory `/tmp/human_test_meta_full/` contains `_delta_log/` and `seen_date=YYYY-MM-DD/` partition subdirectories |
| 4.2 | Inspect output table schema (e.g., `import deltalake; dt = deltalake.DeltaTable("/tmp/human_test_meta_full"); print(dt.schema())`) | Schema has exactly 19 columns. `as_der` is NOT present. All other columns from the source table are present. |
| 4.3 | Compare a sample record between source and output tables | For any given `cert_index`/`source_url` pair, all 19 field values in the output must exactly match the corresponding values in the source table (except `as_der` which is absent) |
| 4.4 | Check `chain` column in output | Values should be non-empty lists of JSON strings for records that had chain data in the source. JSON content should be identical to source. |

## Phase 5: Signal Handling (Graceful Shutdown)

| Step | Action | Expected |
|------|--------|----------|
| 5.1 | Start a long-running audit: `cargo run -- --reparse-audit &` (against a table with many records/partitions) | Audit begins processing and logs batch progress |
| 5.2 | In another terminal, send SIGINT: `kill -INT $(pgrep certstream)` | Process prints "Reparse Audit Report (Partial - Interrupted)" with partial statistics, then exits with code 1 |
| 5.3 | Start a long-running extraction: `cargo run -- --extract-metadata --output /tmp/human_test_interrupted &` | Extraction begins |
| 5.4 | Send SIGTERM: `kill -TERM $(pgrep certstream)` | Process logs "metadata extraction interrupted by shutdown signal", exits with code 1. Partial data may exist at the output path. |

## End-to-End: Full Audit-Then-Extract Pipeline

| Step | Action | Expected |
|------|--------|----------|
| E2E.1 | Run reparse audit to baseline: `cargo run -- --reparse-audit 2>&1 \| tee /tmp/audit_report.txt` | Report generated successfully. Note the "Records scanned" count. |
| E2E.2 | Run metadata extraction: `cargo run -- --extract-metadata --output /tmp/e2e_metadata 2>&1` | Exits 0. Log shows total records written matching (approximately) the count from E2E.1. |
| E2E.3 | Verify output table record count matches source | Using Delta tooling: `SELECT COUNT(*) FROM metadata_table` should equal the source record count. |
| E2E.4 | Run extraction with date filter: `cargo run -- --extract-metadata --output /tmp/e2e_filtered --from-date 2026-02-01 --to-date 2026-02-28` | Exits 0. Record count should be less than or equal to the full extraction count. |
| E2E.5 | Verify no `as_der` data leaked into output | Query the output table: `as_der` column should not exist. No raw DER certificate bytes should be present anywhere in the output. |

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|------------|-------|
| AC4.3 (dispatch) | `std::process::exit()` terminates the process, preventing in-process test assertions | See Phase 1, Step 1.2 |
| AC4.4 (signals) | OS-level signal delivery cannot be reliably tested in `#[tokio::test]` | See Phase 5, Steps 5.1-5.4 |
| AC1.7 (Binary path) | Constructing a Delta table with Binary schema requires custom Arrow construction | Code review: verify `match as_der_col.data_type()` has `DataType::Binary` and `DataType::Utf8` arms in `src/table_ops.rs` |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 | `test_ac1_1_audit_completes_with_zero_mismatches` | Phase 3, Step 3.1 |
| AC1.2 | `test_ac1_2_audit_detects_field_mismatches` | Phase 3, Steps 3.2-3.3 |
| AC1.3 | `test_ac1_3_audit_reports_multiple_mismatches` | Phase 3, Step 3.3 |
| AC1.4 | `test_ac1_4_audit_counts_invalid_base64_as_unparseable` | Phase 3, Step 3.4 |
| AC1.5 | `test_ac1_5_audit_counts_unparseable_certificates` | Phase 3, Step 3.4 |
| AC1.6 | `test_ac1_6_audit_exits_with_empty_date_range` | Phase 2, Step 2.2 |
| AC1.7 | `test_ac1_1_*` (Utf8 path) | Human Verification: code review |
| AC2.1 | `test_metadata_schema_*` + `test_ac2_1_*` | Phase 4, Step 4.2 |
| AC2.2 | `test_ac2_2_extract_metadata_preserves_all_field_values` | Phase 4, Step 4.3 |
| AC2.3 | `test_ac2_3_extract_metadata_output_is_partitioned_by_seen_date` | Phase 4, Step 4.1 |
| AC2.4 | `test_ac2_4_extract_metadata_preserves_chain_column` | Phase 4, Step 4.4 |
| AC2.5 | `test_ac2_5_extract_metadata_fails_with_missing_source_table` | -- |
| AC2.6 | `test_ac2_6_extract_metadata_empty_date_range_exits_zero` | Phase 2, Step 2.4 |
| AC3.1-AC3.4 | `test_date_filter_clause_*` (4 tests) | Phase 2, Steps 2.1-2.2 |
| AC4.1 | `test_ac4_1_reparse_audit_flag` | Phase 1, Step 1.1 |
| AC4.2 | `test_ac4_2_extract_metadata_with_output` | Phase 1, Step 1.3 |
| AC4.3 | `test_ac4_3_extract_metadata_without_output` (parsing) | Phase 1, Step 1.2 (dispatch) |
| AC4.4 | `test_ac4_4_*_shutdown` (2 tests) | Phase 5, Steps 5.1-5.4 |
