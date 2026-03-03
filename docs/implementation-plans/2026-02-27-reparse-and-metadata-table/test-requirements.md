# Reparse Audit & Metadata Table: Test Requirements

**Generated:** 2026-02-27
**Source:** Design plan `docs/design-plans/2026-02-27-reparse-and-metadata-table.md` and implementation phases `phase_01.md` through `phase_05.md`.

---

## Conventions

- **Test location:** All tests live in inline `#[cfg(test)] mod tests` blocks within the source file they test, matching the existing codebase pattern (see `src/cli.rs`, `src/backfill.rs`, `src/delta_sink.rs`).
- **Test naming:** Tests are prefixed with the AC identifier they verify (e.g., `test_ac1_1_...`), following the pattern established in `src/cli.rs` and `src/backfill.rs`.
- **Delta table tests:** Async tests that create real temp Delta tables in `/tmp/delta_table_ops_test_*`, using `records_to_batch()` and `flush_buffer()` from `crate::delta_sink`, matching the `src/backfill.rs` test pattern.
- **Cleanup:** Each test removes its temp directory on completion.
- **Test runner:** `cargo test --lib` for unit tests; `cargo test` for full suite.

---

## AC1: Reparse audit reads and reparses stored certificates

### AC1.1: Audit completes and reports zero mismatches when parsing code has not changed

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 3, Task 2 |
| **Test name** | `test_ac1_1_audit_zero_mismatches_with_consistent_data` |
| **Approach** | Generate a real X.509 certificate with `rcgen` (already in dev-deps). Parse it with `parse_certificate()` to get a `LeafCert`. Build a `DeltaCertRecord` from the `LeafCert` fields (subject_aggregated, issuer_aggregated, all_domains, signature_algorithm, is_ca, not_before, not_after, serial_number) and set `as_der` to the base64-encoded DER bytes. Write to a temp Delta table. Call `run_reparse_audit()` against it. Assert exit code is 0. Capture stdout and verify "Records with mismatches: 0" appears in the report output. |
| **Implementation rationale** | The implementation uses streaming execution (`df.execute_stream().await` + `stream.next().await`), so the test validates the full streaming pipeline end-to-end. The test uses a real certificate to ensure `parse_certificate()` round-trips correctly -- synthetic field values would not exercise the actual comparison logic against live parsing code. |

### AC1.2: Audit correctly identifies field-level mismatches when parsing code produces different output

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 3, Task 2 |
| **Test name** | `test_ac1_2_audit_detects_field_level_mismatches` |
| **Approach** | Generate a real certificate with `rcgen`, parse with `parse_certificate()`, build a `DeltaCertRecord`, then manually alter one or more stored fields (e.g., set `subject_aggregated` to a different value, add an extra domain to `all_domains`). Write to a temp Delta table. Call `run_reparse_audit()`. Assert exit code is 0 (mismatches are not infrastructure failures). Verify the report contains "Records with mismatches:" with a count > 0, and that the altered field name (e.g., `subject_aggregated`) appears in the "Field breakdown" section. |
| **Implementation rationale** | The comparison logic compares 8 parse-derived fields individually. This test validates that per-field diffing correctly flags mismatches without false positives on untouched fields. The implementation uses `HashMap<String, u64>` for per-field counters, and this test ensures the map is keyed by the correct field names. |

### AC1.3: Audit report shows per-field mismatch counts and up to 10 sample diffs

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 3, Task 2 |
| **Test name** | `test_ac1_3_audit_report_caps_samples_at_10` |
| **Approach** | Create a Delta table with more than 10 records where `subject_aggregated` has been altered to differ from what `parse_certificate()` produces (e.g., 15 records). Call `run_reparse_audit()`. Capture the report output. Verify: (a) "Field breakdown:" section lists `subject_aggregated` with count 15, (b) "Sample mismatches (10 of 15):" header appears, (c) exactly 10 sample blocks are printed (count occurrences of `[cert_index=` in output). |
| **Implementation rationale** | The `SampleDiff` collection in the implementation is capped at 10 entries. This test exercises the cap boundary. The per-field `HashMap<String, u64>` counters are verified to count all mismatches even after the sample cap is reached. |

### AC1.4: Records with invalid base64 in `as_der` (Utf8) or corrupt bytes (Binary) are counted as unparseable, not mismatches

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 3, Task 2 |
| **Test name** | `test_ac1_4_invalid_base64_counted_as_unparseable` |
| **Approach** | Create a Delta table with a record where `as_der` contains `"not_valid_base64!!!"` (invalid base64 that will fail `STANDARD.decode()`). Call `run_reparse_audit()`. Assert exit code 0. Verify the report contains "Unparseable records: 1" and "Records with mismatches: 0" (the invalid record is not counted as a mismatch). |
| **Implementation rationale** | The implementation checks the `as_der` column type at runtime: for `DataType::Utf8`, it base64-decodes; for `DataType::Binary`, it reads bytes directly. Invalid base64 or corrupt binary bytes both increment `unparseable_count` and `continue` the row loop, never reaching the comparison logic. This test validates the Utf8/base64 failure path. Binary corrupt bytes are harder to test without manually constructing Arrow arrays with the Binary column type, but the code path is symmetric -- both go to the same `unparseable_count += 1` branch. |

### AC1.5: Records where `parse_certificate()` returns None are counted as unparseable

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 3, Task 2 |
| **Test name** | `test_ac1_5_unparseable_der_counted_as_unparseable` |
| **Approach** | Create a Delta table with a record where `as_der` is valid base64 but decodes to garbage bytes (e.g., `STANDARD.encode(b"this is not a certificate")`). `parse_certificate()` will return `None` for non-DER data. Call `run_reparse_audit()`. Assert exit code 0. Verify "Unparseable records: 1" in the report. |
| **Implementation rationale** | This tests the second failure path after successful base64 decode: `parse_certificate(&der_bytes, false)` returns `None`. The implementation increments `unparseable_count` and continues, never reaching field comparison. Distinct from AC1.4 which tests decode failure. |

### AC1.6: Audit against an empty date range exits 0 with informational message

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 3, Task 2 (initial); 5, Task 3 (hardened) |
| **Test name** | `test_ac1_6_empty_date_range_exits_zero` |
| **Approach** | Create a Delta table with records having `seen_date = "2026-01-01"`. Call `run_reparse_audit()` with `from_date = Some("2099-01-01".to_string())` (no matching partitions). Assert exit code 0. Verify log output contains "No records found in the specified date range". |
| **Implementation rationale** | The streaming execution model (`execute_stream()`) yields zero batches when no rows match the WHERE clause. The implementation tracks whether any rows were seen; if the stream exhausts with zero rows, it logs the informational message and returns 0. Phase 5 Task 2 hardens this by ensuring the empty-stream check works correctly with the streaming pattern. |

### AC1.7: Audit handles both Utf8 (base64) and Binary column types for `as_der`

| Property | Value |
|---|---|
| **Type** | Automated -- unit |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 3, Task 2 |
| **Test name** | `test_ac1_7_handles_both_as_der_column_types` |
| **Approach** | This is primarily verified by AC1.1 (which uses a Utf8 `as_der` column, since `DeltaCertRecord.as_der` is a `String` written to a Utf8 column). To additionally test the Binary path, construct a Delta table using a modified schema where `as_der` is `DataType::Binary` and write raw DER bytes directly (no base64). Run audit and verify it succeeds with 0 mismatches. Alternatively, if constructing a Binary-schema Delta table is too complex, the Binary path can be verified via code review (the runtime `match` on `column.data_type()` is straightforward). |
| **Human verification fallback** | If the Binary-column test proves impractical to construct (the existing `delta_schema()` uses Utf8 for `as_der`), verify via code review that the `DataType::Binary` arm reads via `BinaryArray` and the `DataType::Utf8` arm base64-decodes via `StringArray`. The branching is a simple match statement with no shared mutable state between arms. |

---

## AC2: Metadata extraction produces correct metadata-only table

### AC2.1: Output Delta table contains exactly 19 columns (all columns from source except `as_der`)

| Property | Value |
|---|---|
| **Type** | Automated -- unit + integration |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 2, Task 2 (unit); 4, Task 2 (integration) |
| **Test name (unit)** | `test_ac2_1_metadata_schema_has_19_fields` |
| **Test name (integration)** | `test_ac2_1_output_table_has_19_columns` |
| **Approach (unit)** | Call `metadata_schema()`. Assert it has exactly 19 fields. Assert no field is named `as_der`. Assert all 19 field names match `delta_schema()` fields minus `as_der` in the same order. Compare field data types against `delta_schema()`. |
| **Approach (integration)** | Create a source Delta table with the full 20-column schema and test records. Call `run_extract_metadata()`. Open the output Delta table. Verify its schema has exactly 19 columns, no `as_der` field, and field names/types match `metadata_schema()`. |
| **Implementation rationale** | The `metadata_schema()` function is a manually constructed 19-field `Arc<Schema>`. The unit test guards against field omissions or additions. The integration test validates that the DataFusion `SELECT` of 19 named columns produces batches whose schema matches `metadata_schema()` and that `open_or_create_table()` uses the correct schema for the output table. |

### AC2.2: All metadata field values in output match source table exactly

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 4, Task 2 |
| **Test name** | `test_ac2_2_output_values_match_source` |
| **Approach** | Create a source Delta table with known test records (using `make_test_record()` helper). Run `run_extract_metadata()`. Open both source and output tables via DataFusion. Query both for the same records. Compare at least `cert_index`, `subject_aggregated`, `issuer_aggregated`, `not_before`, `not_after`, `is_ca`, `all_domains`, `serial_number`, `fingerprint`, `sha256` values row-by-row. All must be identical. |
| **Implementation rationale** | The implementation directly projects columns via SQL SELECT without any transformation -- RecordBatches flow from DataFusion query to Delta write unchanged. This test validates the no-transformation property. The design explicitly states "DataFusion's SELECT produces batches matching the projected columns" and "no conversion needed." |

### AC2.3: Output table is partitioned by `seen_date`

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 4, Task 2 |
| **Test name** | `test_ac2_3_output_table_partitioned_by_seen_date` |
| **Approach** | Create a source table with records having different `seen_date` values (e.g., "2026-01-01" and "2026-01-02"). Run extraction. Verify the output table's Delta metadata lists `seen_date` as a partition column (check via `DeltaTable::metadata()` or by verifying `seen_date=YYYY-MM-DD/` subdirectories exist in the output path). |
| **Implementation rationale** | Partitioning is configured by `open_or_create_table()`, which the implementation reuses from `delta_sink`. The function applies `seen_date` partitioning based on the schema convention. This test validates the output table inherits the correct partitioning. |

### AC2.4: Chain column is preserved with full chain certificate metadata JSON

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 4, Task 2 |
| **Test name** | `test_ac2_4_chain_column_preserved` |
| **Approach** | Create source records with non-empty `chain` values (the `make_test_json_bytes()` helper in `delta_sink.rs` includes an intermediate CA chain cert producing a JSON string containing "Intermediate CA"). Write to source table. Run extraction. Query the output table's `chain` column and verify the exact JSON string values match the source. |
| **Implementation rationale** | The `chain` column has type `List<Utf8>` in both schemas. The SQL SELECT includes `chain` and passes it through unchanged. The design notes "Chain metadata in the metadata table is preserved as-is." This test catches any accidental filtering or transformation of the chain column. |

### AC2.5: Missing source table exits 1 with error message

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 4, Task 2 (initial); 5, Task 2 (hardened with specific DeltaTableError variants) |
| **Test name** | `test_ac2_5_missing_source_table_exits_one` |
| **Approach** | Create a `Config` with `delta_sink.table_path` pointing to `/tmp/delta_table_ops_test_nonexistent_<uuid>` (a path that does not exist). Call `run_extract_metadata()`. Assert exit code is 1. |
| **Implementation rationale** | Phase 5 Task 2 hardens the error handling to match specific `DeltaTableError` variants (`NotATable`, `InvalidTableLocation`) following the pattern from `src/backfill.rs:295-298` and `src/query.rs:235`. The test validates that the specific variant matching correctly identifies a missing table and produces exit code 1 rather than panicking. |

### AC2.6: Extraction against empty date range exits 0 with informational message

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, real Delta table) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 4, Task 2 (initial); 5, Task 3 (hardened) |
| **Test name** | `test_ac2_6_empty_date_range_exits_zero` |
| **Approach** | Create a source Delta table with `seen_date = "2026-01-01"`. Call `run_extract_metadata()` with `from_date = Some("2099-01-01".to_string())`. Assert exit code 0. Verify log output contains "No records found in the specified date range". |
| **Implementation rationale** | Same streaming pattern as AC1.6. The implementation checks if total records written is 0 after the stream exhausts. The streaming execution (`execute_stream()`) yields zero batches for non-matching WHERE clauses. |

---

## AC3: Date range filtering works for both commands

### AC3.1: `--from-date` and `--to-date` together filter to inclusive date range

| Property | Value |
|---|---|
| **Type** | Automated -- unit |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 2, Task 2 |
| **Test name** | `test_ac3_1_both_dates_produce_inclusive_range` |
| **Approach** | Call `date_filter_clause(&Some("2026-02-01".to_string()), &Some("2026-02-28".to_string()))`. Assert result is `Ok(" WHERE seen_date >= '2026-02-01' AND seen_date <= '2026-02-28'")`. The `>=` and `<=` operators ensure inclusive bounds. |
| **Implementation rationale** | The `date_filter_clause` function returns `Result<String, String>` (not plain `String`) to enforce YYYY-MM-DD format validation. This was a deliberate design decision in Phase 2 to prevent SQL injection through malformed date strings. The test validates both the SQL generation and the Ok path. |

### AC3.2: `--from-date` alone filters from that date to latest partition

| Property | Value |
|---|---|
| **Type** | Automated -- unit |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 2, Task 2 |
| **Test name** | `test_ac3_2_from_date_only_filters_lower_bound` |
| **Approach** | Call `date_filter_clause(&Some("2026-02-01".to_string()), &None)`. Assert result is `Ok(" WHERE seen_date >= '2026-02-01'")`. No upper bound means all partitions from the given date onward are included. |
| **Implementation rationale** | The function builds conditions via `Vec::push()` and joins with ` AND `. With only one condition, the WHERE clause contains a single predicate. DataFusion applies partition pruning on the `seen_date` partition column, so only partitions >= the date are scanned. |

### AC3.3: `--to-date` alone filters from earliest partition to that date

| Property | Value |
|---|---|
| **Type** | Automated -- unit |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 2, Task 2 |
| **Test name** | `test_ac3_3_to_date_only_filters_upper_bound` |
| **Approach** | Call `date_filter_clause(&None, &Some("2026-02-28".to_string()))`. Assert result is `Ok(" WHERE seen_date <= '2026-02-28'")`. No lower bound means all partitions up to and including the given date are included. |
| **Implementation rationale** | Symmetric to AC3.2. The absence of a `from` condition means no lower bound is added to the Vec, and only the upper bound predicate appears. |

### AC3.4: Omitting both flags processes all partitions

| Property | Value |
|---|---|
| **Type** | Automated -- unit |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 2, Task 2 |
| **Test name** | `test_ac3_4_no_dates_produces_empty_clause` |
| **Approach** | Call `date_filter_clause(&None, &None)`. Assert result is `Ok("")` (empty string). The SQL query will have no WHERE clause, scanning all partitions. |
| **Implementation rationale** | When the conditions Vec is empty, the function returns an empty string. The caller appends this to the SQL query, resulting in no filtering. |

### AC3 supplemental: Date format validation

| Property | Value |
|---|---|
| **Type** | Automated -- unit |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 2, Task 2 |
| **Test name** | `test_ac3_date_format_validation_rejects_invalid` |
| **Approach** | Call `date_filter_clause` with malformed dates: `"not-a-date"`, `"2026/02/01"`, `"26-02-01"`, `""`. Assert each returns `Err` with a descriptive message containing the invalid input and "expected YYYY-MM-DD". |
| **Implementation rationale** | The `is_valid_date_format()` helper validates length == 10, hyphens at positions 4 and 7, and digits elsewhere. Returning `Result<String, String>` rather than panicking was a deliberate Phase 2 design decision -- callers handle `Err` by logging and returning exit code 1. This prevents SQL injection via crafted date strings like `"'; DROP TABLE --"`. |

---

## AC4: CLI integration follows existing patterns

### AC4.1: `--reparse-audit` dispatches to reparse audit mode and exits

| Property | Value |
|---|---|
| **Type** | Automated -- unit |
| **File** | `src/cli.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 1, Task 1 |
| **Test name** | `test_ac4_1_reparse_audit_flag_parsed` |
| **Approach** | Construct arg vector `["certstream", "--reparse-audit"]`. Call `CliArgs::parse_args(&args)`. Assert `parsed.reparse_audit == true`. Assert other mode flags (`backfill`, `merge`, `extract_metadata`) are `false`. |
| **Implementation rationale** | The CLI uses `args.iter().any(|a| a == "--reparse-audit")` for boolean flag parsing, matching the existing pattern for `--backfill` and `--merge`. Dispatch happens in `main.rs` via `if cli_args.reparse_audit { ... std::process::exit(exit_code); }`, following the first-match-wins sequential if/else chain pattern. The dispatch itself is not unit-testable (it calls `std::process::exit`), but the flag parsing is. |

### AC4.2: `--extract-metadata --output <PATH>` dispatches to metadata extraction mode and exits

| Property | Value |
|---|---|
| **Type** | Automated -- unit |
| **File** | `src/cli.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 1, Task 1 |
| **Test name** | `test_ac4_2_extract_metadata_with_output_parsed` |
| **Approach** | Construct arg vector `["certstream", "--extract-metadata", "--output", "/tmp/test"]`. Call `CliArgs::parse_args(&args)`. Assert `parsed.extract_metadata == true` and `parsed.output_path == Some("/tmp/test".to_string())`. |
| **Implementation rationale** | `--output` is a value-bearing flag parsed with index-based iteration (`if arg == "--output" && i + 1 < args.len()`), matching the pattern for `--from`, `--logs`, `--staging-path`, and `--sink`. The test validates both the boolean flag and the value capture. |

### AC4.3: `--extract-metadata` without `--output` prints error and exits 1

| Property | Value |
|---|---|
| **Type** | Automated -- unit (parsing) + human verification (dispatch) |
| **File** | `src/cli.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 1, Task 1 (parsing); 1, Task 3 (dispatch) |
| **Test name** | `test_ac4_3_extract_metadata_without_output_has_none` |
| **Approach (unit)** | Construct arg vector `["certstream", "--extract-metadata"]`. Call `CliArgs::parse_args(&args)`. Assert `parsed.extract_metadata == true` and `parsed.output_path == None`. This verifies the parsing layer correctly captures the missing `--output`. |
| **Human verification** | The actual validation (printing error and calling `std::process::exit(1)`) happens in `main.rs` dispatch: `if cli_args.output_path.is_none() { eprintln!("Error: --extract-metadata requires --output <PATH>"); std::process::exit(1); }`. This dispatch code calls `std::process::exit()` which cannot be unit-tested. Verify manually by running `cargo run -- --extract-metadata 2>&1; echo "exit: $?"` and confirming the error message and exit code 1. |
| **Justification for human verification** | The `std::process::exit()` call terminates the process, making it impossible to assert from within a `#[test]` function. The parsing logic (output_path is None) is tested automatically. The dispatch logic is a 3-line if/eprintln/exit block that can be verified by code review and a single manual invocation. |

### AC4.4: Both commands respond to SIGINT/SIGTERM by stopping cleanly between partitions

| Property | Value |
|---|---|
| **Type** | Automated -- integration (async, CancellationToken) |
| **File** | `src/table_ops.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 5, Task 3 |
| **Test name (reparse)** | `test_ac4_4_reparse_audit_exits_on_cancellation` |
| **Test name (extract)** | `test_ac4_4_extract_metadata_exits_on_cancellation` |
| **Approach** | Create a `CancellationToken`, cancel it immediately, then call `run_reparse_audit()` / `run_extract_metadata()` with the cancelled token and a real Delta table. Assert both return exit code 1. For metadata extraction, verify that the output path either does not exist or contains only partial data (no assertion on completeness). |
| **Implementation rationale** | The implementation checks `shutdown.is_cancelled()` at the top of each iteration in the streaming `while let` loop (`while let Some(batch_result) = stream.next().await`). A pre-cancelled token causes the check to fire on the first batch, resulting in early exit with code 1. The reparse audit additionally prints a partial report on cancellation. Testing with a pre-cancelled token is sufficient because the check is `if shutdown.is_cancelled()` -- there is no timing dependency; a pre-cancelled token triggers the same code path as a mid-execution signal. |
| **Human verification supplement** | To verify that actual SIGINT/SIGTERM handling works end-to-end (signal handler -> CancellationToken -> graceful exit), run a long reparse audit against a large table and send `kill -INT <pid>` during execution. Verify partial report is printed and process exits with code 1. This tests the signal handler wiring in `main.rs` (`spawn_signal_handler(shutdown_token.clone())`), which is not covered by the unit test. |
| **Justification for human verification** | Signal handling involves OS-level process signals that cannot be reliably tested in `#[tokio::test]`. The CancellationToken propagation is tested automatically; the signal-to-token bridge is a single line in `main.rs` reusing the existing `spawn_signal_handler` function that is already proven by the backfill and merge modes. |

---

## AC4 supplemental: Combined flag parsing

| Property | Value |
|---|---|
| **Type** | Automated -- unit |
| **File** | `src/cli.rs` (`#[cfg(test)] mod tests`) |
| **Phase** | 1, Task 1 |
| **Test name** | `test_ac4_combined_flags_parsed_together` |
| **Approach** | Construct arg vector with all new flags: `["certstream", "--reparse-audit", "--from-date", "2026-01-01", "--to-date", "2026-01-31"]`. Assert all fields are correctly populated. Repeat for `--extract-metadata --output /tmp/out --from-date 2026-01-01 --to-date 2026-01-31`. |
| **Implementation rationale** | Value-bearing flags (`--from-date`, `--to-date`, `--output`) are parsed via index-based iteration in the same loop as existing flags. This test ensures no parsing conflicts with existing flags. |

---

## Summary Matrix

| AC | Test Name | Type | File | Phase |
|---|---|---|---|---|
| AC1.1 | `test_ac1_1_audit_zero_mismatches_with_consistent_data` | Integration | `src/table_ops.rs` | 3 |
| AC1.2 | `test_ac1_2_audit_detects_field_level_mismatches` | Integration | `src/table_ops.rs` | 3 |
| AC1.3 | `test_ac1_3_audit_report_caps_samples_at_10` | Integration | `src/table_ops.rs` | 3 |
| AC1.4 | `test_ac1_4_invalid_base64_counted_as_unparseable` | Integration | `src/table_ops.rs` | 3 |
| AC1.5 | `test_ac1_5_unparseable_der_counted_as_unparseable` | Integration | `src/table_ops.rs` | 3 |
| AC1.6 | `test_ac1_6_empty_date_range_exits_zero` | Integration | `src/table_ops.rs` | 3, 5 |
| AC1.7 | `test_ac1_7_handles_both_as_der_column_types` | Integration | `src/table_ops.rs` | 3 |
| AC2.1 | `test_ac2_1_metadata_schema_has_19_fields` | Unit | `src/table_ops.rs` | 2 |
| AC2.1 | `test_ac2_1_output_table_has_19_columns` | Integration | `src/table_ops.rs` | 4 |
| AC2.2 | `test_ac2_2_output_values_match_source` | Integration | `src/table_ops.rs` | 4 |
| AC2.3 | `test_ac2_3_output_table_partitioned_by_seen_date` | Integration | `src/table_ops.rs` | 4 |
| AC2.4 | `test_ac2_4_chain_column_preserved` | Integration | `src/table_ops.rs` | 4 |
| AC2.5 | `test_ac2_5_missing_source_table_exits_one` | Integration | `src/table_ops.rs` | 4, 5 |
| AC2.6 | `test_ac2_6_empty_date_range_exits_zero` | Integration | `src/table_ops.rs` | 4, 5 |
| AC3.1 | `test_ac3_1_both_dates_produce_inclusive_range` | Unit | `src/table_ops.rs` | 2 |
| AC3.2 | `test_ac3_2_from_date_only_filters_lower_bound` | Unit | `src/table_ops.rs` | 2 |
| AC3.3 | `test_ac3_3_to_date_only_filters_upper_bound` | Unit | `src/table_ops.rs` | 2 |
| AC3.4 | `test_ac3_4_no_dates_produces_empty_clause` | Unit | `src/table_ops.rs` | 2 |
| AC3 supp. | `test_ac3_date_format_validation_rejects_invalid` | Unit | `src/table_ops.rs` | 2 |
| AC4.1 | `test_ac4_1_reparse_audit_flag_parsed` | Unit | `src/cli.rs` | 1 |
| AC4.2 | `test_ac4_2_extract_metadata_with_output_parsed` | Unit | `src/cli.rs` | 1 |
| AC4.3 | `test_ac4_3_extract_metadata_without_output_has_none` | Unit | `src/cli.rs` | 1 |
| AC4.4 | `test_ac4_4_reparse_audit_exits_on_cancellation` | Integration | `src/table_ops.rs` | 5 |
| AC4.4 | `test_ac4_4_extract_metadata_exits_on_cancellation` | Integration | `src/table_ops.rs` | 5 |
| AC4 supp. | `test_ac4_combined_flags_parsed_together` | Unit | `src/cli.rs` | 1 |

---

## Human Verification Items

| AC | What to Verify | Why Not Automated | Verification Steps |
|---|---|---|---|
| AC4.3 (dispatch) | `--extract-metadata` without `--output` prints error to stderr and exits with code 1 | `std::process::exit()` terminates the process, preventing in-process assertions | Run: `cargo run -- --extract-metadata 2>&1; echo "exit: $?"`. Confirm stderr contains "Error: --extract-metadata requires --output <PATH>" and exit code is 1. |
| AC4.4 (signals) | SIGINT/SIGTERM during execution triggers graceful shutdown via signal handler | OS-level signal delivery cannot be reliably tested in `#[tokio::test]`; requires a running process | 1. Start a long audit: `cargo run -- --reparse-audit --from-date 2026-01-01 --to-date 2026-12-31`. 2. In another terminal: `kill -INT <pid>`. 3. Verify partial report is printed and process exits with code 1. Repeat for `--extract-metadata`. |
| AC1.7 (Binary path) | `as_der` column with `DataType::Binary` is read correctly without base64 decoding | Constructing a Delta table with a non-standard schema (Binary instead of Utf8 for `as_der`) requires custom Arrow array construction outside the normal `records_to_batch()` helper | Code review: verify the `match column.data_type()` in `run_reparse_audit` has a `DataType::Binary` arm that reads via `BinaryArray` directly. If a Binary-schema table can be constructed in the test, this becomes automated (see AC1.7 entry above). |

---

## Test Execution Commands

```bash
# Run all unit tests for the new modules
cargo test --lib table_ops::tests
cargo test --lib cli::tests

# Run the full test suite (verifies no regressions)
cargo test

# Expected test count after all phases: existing 359+ tests plus ~26 new tests
```
