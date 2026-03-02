# Reparse Audit & Metadata Table Design

## Summary

The Delta table written by the live certstream server stores every CT log entry including the full raw DER-encoded certificate bytes (`as_der`). Over time, the metadata fields derived from those bytes — subject, issuer, domain list, signature algorithm, and others — may drift from what the current parsing code would produce, because `parse_certificate()` is updated independently of the stored records. The reparse audit command (`--reparse-audit`) addresses this by reading the full Delta table, decoding each record's `as_der`, running it through the current `parse_certificate()` implementation, and comparing the freshly derived metadata against what is stored. The result is a non-destructive report showing per-field mismatch counts and representative sample diffs, giving operators visibility into parsing drift without altering any data.

The metadata extraction command (`--extract-metadata`) addresses a complementary need: once the raw table is no longer needed for long-term querying, operators want a compact, queryable table that contains all metadata columns but omits the bulky `as_der` column. The command reads from the main Delta table and writes a new 19-column Delta table at a caller-specified output path, using the same `seen_date` partitioning and zstd compression as the rest of the pipeline. Both commands are implemented as additional execution modes in the existing binary, reusing the established CLI dispatch, DataFusion query, Delta read/write, and graceful-shutdown patterns from `--backfill` and `--merge`.

## Definition of Done

1. **Reparse audit CLI command** (`--reparse-audit`): A batch tool that reads the full Delta table (or specific partitions), decodes each record's `as_der`, reparses with the current `parse_certificate()` code, compares parsed fields against stored metadata, and outputs a summary report of mismatches (count, field-level diffs, sample records). It is report-only and non-destructive.

2. **Metadata extraction CLI command** (`--extract-metadata`): A batch tool that reads the full Delta table (or specific partitions), writes a new Delta table containing all columns except `as_der`. The metadata table uses the same `seen_date` partitioning and can serve as the long-term queryable table after the raw table is archived.

3. Both tools operate as additional execution modes in the existing binary, following the established pattern of `--backfill` and `--merge`.

## Acceptance Criteria

### reparse-and-metadata-table.AC1: Reparse audit reads and reparses stored certificates
- **reparse-and-metadata-table.AC1.1 Success:** Audit completes against a Delta table and reports zero mismatches when parsing code has not changed
- **reparse-and-metadata-table.AC1.2 Success:** Audit correctly identifies field-level mismatches when parsing code produces different output
- **reparse-and-metadata-table.AC1.3 Success:** Audit report shows per-field mismatch counts and up to 10 sample diffs
- **reparse-and-metadata-table.AC1.4 Failure:** Records with invalid base64 in `as_der` (Utf8 format) or corrupt bytes (Binary format) are counted as unparseable, not mismatches
- **reparse-and-metadata-table.AC1.5 Failure:** Records where `parse_certificate()` returns None are counted as unparseable
- **reparse-and-metadata-table.AC1.6 Edge:** Audit against an empty date range exits 0 with informational message
- **reparse-and-metadata-table.AC1.7 Success:** Audit handles both Utf8 (base64) and Binary column types for `as_der`

### reparse-and-metadata-table.AC2: Metadata extraction produces correct metadata-only table
- **reparse-and-metadata-table.AC2.1 Success:** Output Delta table contains exactly 19 columns (all columns from source except `as_der`)
- **reparse-and-metadata-table.AC2.2 Success:** All metadata field values in output match source table exactly
- **reparse-and-metadata-table.AC2.3 Success:** Output table is partitioned by `seen_date`
- **reparse-and-metadata-table.AC2.4 Success:** Chain column is preserved with full chain certificate metadata JSON
- **reparse-and-metadata-table.AC2.5 Failure:** Missing source table exits 1 with error message
- **reparse-and-metadata-table.AC2.6 Edge:** Extraction against empty date range exits 0 with informational message

### reparse-and-metadata-table.AC3: Date range filtering works for both commands
- **reparse-and-metadata-table.AC3.1 Success:** `--from-date` and `--to-date` together filter to inclusive date range
- **reparse-and-metadata-table.AC3.2 Success:** `--from-date` alone filters from that date to latest partition
- **reparse-and-metadata-table.AC3.3 Success:** `--to-date` alone filters from earliest partition to that date
- **reparse-and-metadata-table.AC3.4 Success:** Omitting both flags processes all partitions

### reparse-and-metadata-table.AC4: CLI integration follows existing patterns
- **reparse-and-metadata-table.AC4.1 Success:** `--reparse-audit` dispatches to reparse audit mode and exits
- **reparse-and-metadata-table.AC4.2 Success:** `--extract-metadata --output <PATH>` dispatches to metadata extraction mode and exits
- **reparse-and-metadata-table.AC4.3 Failure:** `--extract-metadata` without `--output` prints error and exits 1
- **reparse-and-metadata-table.AC4.4 Success:** Both commands respond to SIGINT/SIGTERM by stopping cleanly between partitions

## Glossary

- **DER (Distinguished Encoding Rules)**: A binary serialization format for X.509 certificates. The `as_der` column stores raw certificate bytes in this format.
- **`as_der`**: The Delta table column holding raw certificate bytes. In transition from Utf8 (base64-encoded string) to native Binary column type; the reparse audit handles both.
- **`parse_certificate()`**: The Rust function in `src/ct/parser.rs` that takes raw DER bytes and returns parsed metadata as a `LeafCert` struct. Single source of truth for metadata derivation.
- **`LeafCert`**: The parsed certificate struct produced by `parse_certificate()`, containing metadata fields (subject, issuer, domains, etc.) that are stored in the Delta table.
- **Delta Lake**: An open-source ACID table format layered on Parquet files, used via the `deltalake` Rust crate as the primary storage layer for CT records.
- **DataFusion**: An in-process SQL query engine (Apache Arrow project) used to execute SQL against Delta tables without an external database.
- **`seen_date` partition**: The Delta table is partitioned by date string (`YYYY-MM-DD`) when a certificate was first observed. Allows date-range filtering without full table scans.
- **`metadata_schema()`**: New function returning the 19-column Arrow schema — identical to `delta_schema()` with `as_der` removed.
- **Parsing drift**: When stored metadata no longer matches what current `parse_certificate()` code would produce, because parsing logic was updated after records were written.
- **`DeltaCertRecord`**: Rust struct mirroring the 20-column Delta table schema, used as the canonical in-memory representation of a stored certificate record.
- **`CancellationToken`**: A Tokio utility propagating graceful shutdown signals (SIGINT/SIGTERM) through async tasks.
- **CT log (Certificate Transparency log)**: A public, append-only ledger of X.509 certificates under RFC 6962. The certstream server watches these logs and streams new entries.

## Architecture

Two new execution modes added to the binary, following the existing `--backfill` / `--merge` pattern. Both are batch commands that read from the Delta table via DataFusion SQL, process partition-by-partition, and exit with a status code.

### CLI Interface

**Reparse Audit:**
```
cargo run -- --reparse-audit [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD]
```
Reads from `config.delta_sink.table_path`. Prints summary report to stdout. Exit code 0 if audit completes (even with mismatches), 1 on infrastructure failure.

**Metadata Extraction:**
```
cargo run -- --extract-metadata --output <PATH> [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD]
```
Reads from `config.delta_sink.table_path`. Writes 19-column Delta table (all columns except `as_der`) to `--output` path. Exit code 0 on success, 1 on errors.

**Shared flags:** `--from-date` and `--to-date` filter `seen_date` partitions (inclusive). Both optional — omit for all partitions. One-sided bounds supported.

### Reparse Audit Data Flow

1. Open Delta table, create DataFusion `SessionContext`, register as `ct_main`
2. Query: `SELECT cert_index, source_url, seen_date, as_der, subject_aggregated, issuer_aggregated, all_domains, signature_algorithm, is_ca, not_before, not_after, serial_number FROM ct_main` with optional `WHERE seen_date` filter
3. For each RecordBatch:
   - For each row: extract DER bytes from `as_der` (supports both Binary and Utf8/base64 column types for backwards compatibility) → call `parse_certificate(der_bytes, false)` → compare `LeafCert` fields against stored columns
   - Track per-field mismatch counters, collect up to 10 sample diffs
4. Print summary report to stdout

**Fields compared** (parse-derived, can change with code updates): `subject_aggregated`, `issuer_aggregated`, `all_domains`, `signature_algorithm`, `is_ca`, `not_before`, `not_after`, `serial_number`

**Fields NOT compared** (hash-derived from DER bytes, deterministic): `sha256`, `sha1`, `fingerprint`

**Unparseable records** (base64 decode failure or `parse_certificate()` returns `None`) are counted separately, not treated as mismatches.

**Report format:**
```
Reparse Audit Report
====================
Partitions scanned: 2026-02-17 to 2026-02-20
Records scanned: 12,345,678
Records with mismatches: 1,234 (0.01%)
Unparseable records: 3

Field breakdown:
  subject_aggregated:  456 mismatches
  all_domains:         890 mismatches
  signature_algorithm: 12 mismatches

Sample mismatches (5 of 1,234):
  [cert_index=42, source=https://ct.example.com]
    subject_aggregated: stored="/CN=example.com" reparsed="/CN=example.com/O=Org"
    all_domains: stored=["example.com"] reparsed=["example.com","www.example.com"]
```

### Metadata Extraction Data Flow

1. Open source Delta table, create DataFusion `SessionContext`
2. Query: `SELECT cert_index, update_type, seen, seen_date, source_name, source_url, cert_link, serial_number, fingerprint, sha256, sha1, not_before, not_after, is_ca, signature_algorithm, subject_aggregated, issuer_aggregated, all_domains, chain FROM ct_main` with optional `WHERE seen_date` filter
3. Open or create output Delta table at `--output` with 19-column schema and `seen_date` partitioning
4. Write each RecordBatch to output table using zstd compression from `config.delta_sink.compression_level`
5. Report partitions processed and records written

**Schema:** New `metadata_schema()` function — identical to `delta_schema()` with the `as_der` field removed. The `chain` column stays (chain certs already have `as_der = None`, so chain data is already metadata-only JSON).

**Memory management:** DataFusion processes partition-by-partition. Each partition's metadata columns are estimated at 3-5 GB in Arrow memory (vs ~20 GB for full partition including `as_der`).

**Idempotency:** Re-running with overlapping dates appends records. For dedup, the user can apply the existing `--merge` pattern.

### Error Handling

**Reparse audit:**
- Missing/unopenable Delta table: exit 1 with error message
- Individual record parse failures: counted in report, not fatal
- Exit code: 0 if audit completes, 1 only on infrastructure failure

**Metadata extraction:**
- Missing source table: exit 1
- Write failures: exit 1, partially written output left intact for retry
- Empty date range (no matching partitions): exit 0 with info message

**Graceful shutdown:** Both commands check `CancellationToken` between partition processing. On cancellation, reparse audit prints partial report; metadata extraction leaves partial output intact. Both exit 1.

### Progress Reporting

Batch tools report progress via log messages (no prometheus metrics needed):
- `info!("Processing partition seen_date={date} ({current}/{total})")`
- `info!("Partition complete: {records} records in {duration}")`
- Final summary at completion

## Existing Patterns

Both commands follow established patterns from the existing codebase:

**CLI dispatch** (from `src/cli.rs` and `src/main.rs`): Boolean flag in `CliArgs`, index-based parsing for value flags, if/else dispatch chain in main before server mode. New modes add `reparse_audit: bool`, `extract_metadata: bool`, `output_path: Option<String>`, `from_date: Option<String>`, `to_date: Option<String>` to `CliArgs`.

**Delta table reading** (from `src/backfill.rs`): `SessionContext::new()` → register table → SQL query → collect batches. Date range filtering uses `WHERE seen_date >= '...' AND seen_date <= '...'` matching the partition column.

**Delta table writing** (from `src/delta_sink.rs`): Reuses `open_or_create_table()` for creating the output metadata table. Write path adapts the `flush_buffer()` pattern for the 19-column schema.

**Exit code convention** (from `src/backfill.rs`): 0 on success, 1 on failure. Async function signature `pub async fn run_X(...) -> i32`.

**Shutdown handling** (from `src/main.rs`): `CancellationToken` created in main, signal handler spawned, token passed to execution function.

**Certificate parsing** (from `src/ct/parser.rs`): `parse_certificate(der_bytes, include_der)` is the existing entry point for DER-to-metadata conversion. Reparse audit calls this with `include_der = false`.

**Divergence:** Both new commands live in a new `src/table_ops.rs` module rather than extending `src/backfill.rs`. Backfill is focused on gap-filling from CT logs; these commands operate on the stored Delta table itself. The new module imports from `delta_sink` and `ct::parser`.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: CLI and Module Scaffolding
**Goal:** Add new CLI flags, dispatch logic, and empty module structure

**Components:**
- `src/cli.rs` — add `reparse_audit`, `extract_metadata`, `output_path`, `from_date`, `to_date` fields to `CliArgs`; parse logic; validation (extract-metadata requires --output)
- `src/main.rs` — dispatch blocks for `--reparse-audit` and `--extract-metadata` before server mode
- `src/table_ops.rs` — new module with stub `pub async fn run_reparse_audit(...) -> i32` and `pub async fn run_extract_metadata(...) -> i32`
- `src/lib.rs` or `src/main.rs` — register `mod table_ops`

**Dependencies:** None

**Done when:** `cargo build` succeeds; `cargo run -- --reparse-audit` and `cargo run -- --extract-metadata --output /tmp/test` dispatch to stub functions that log a message and exit 0; invalid flag combinations (e.g., `--extract-metadata` without `--output`) print error and exit 1
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Shared Infrastructure
**Goal:** Date range filtering and metadata schema

**Components:**
- `src/table_ops.rs` — shared helper to build `WHERE seen_date` SQL clause from `from_date`/`to_date` options; `metadata_schema()` returning 19-column Arrow schema (delta_schema minus as_der)
- `src/delta_sink.rs` — no changes, but `delta_schema()`, `open_or_create_table()` are imported by table_ops

**Dependencies:** Phase 1

**Done when:** `metadata_schema()` returns correct 19-column schema; date filter helper generates correct SQL for all combinations (both dates, one date, neither); unit tests pass for both
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Reparse Audit Core
**Goal:** Read Delta table, decode DER, reparse, compare fields, produce report

**Components:**
- `src/table_ops.rs` — `run_reparse_audit()` implementation: open table, DataFusion query with date filter, iterate batches, extract DER bytes from `as_der` (handle both Binary and Utf8/base64 column types), call `parse_certificate()`, compare 8 parse-derived fields against stored values, accumulate mismatch counters and sample diffs
- Comparison logic for each field type: String equality for `subject_aggregated`/`issuer_aggregated`/`signature_algorithm`/`serial_number`, Vec<String> set-equality for `all_domains`, i64/bool equality for `not_before`/`not_after`/`is_ca`
- Report formatting: summary stats + up to 10 sample diffs to stdout

**Dependencies:** Phase 2

**Done when:** Reparse audit runs against the live Delta table at `/home/mentat/work/certstream`; produces correct summary report; handles unparseable records gracefully; tests verify comparison logic for matching records, mismatched records, and unparseable records
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Metadata Extraction Core
**Goal:** Read Delta table, project all columns except as_der, write to new Delta table

**Components:**
- `src/table_ops.rs` — `run_extract_metadata()` implementation: open source table, DataFusion query selecting 19 columns with date filter, open or create output table with `metadata_schema()`, write batches with zstd compression
- Write path: adapt flush pattern from `delta_sink` for 19-column schema, partition by `seen_date`

**Dependencies:** Phase 2

**Done when:** Metadata extraction produces a valid Delta table at the output path; output table has 19 columns (no `as_der`); `seen_date` partitioning works correctly; data round-trips correctly (query API can read the metadata table); tests verify schema correctness, partition filtering, and data integrity for a sample partition
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Graceful Shutdown and Edge Cases
**Goal:** CancellationToken integration, empty partition handling, missing table errors

**Components:**
- `src/table_ops.rs` — check `CancellationToken` between partition processing in both commands; handle missing source table (exit 1); handle empty date range (exit 0 with message); reparse audit prints partial report on cancellation

**Dependencies:** Phases 3, 4

**Done when:** Both commands respond to SIGINT/SIGTERM by stopping cleanly; missing table produces clear error; empty date range exits cleanly; tests verify shutdown behavior and error cases
<!-- END_PHASE_5 -->

## Additional Considerations

**Chain DER is not stored:** Chain certificates have `as_der = None` in the data model (`parse_chain_from_bytes` calls `parse_certificate(cert_bytes, false)`). Reparse audit only validates leaf certificate metadata. Chain metadata in the metadata table is preserved as-is without the ability to reverify.

**Reparse audit is eventually consistent with parsing code:** The audit compares against whatever version of `parse_certificate()` is compiled into the binary. Running after a parsing code change will show mismatches for all records affected by that change. This is the intended use case — run audit, see what changed, decide whether to re-extract metadata.

**as_der column type transition:** There is an in-flight change converting `as_der` from Utf8 (base64-encoded string) to native Binary. The reparse audit handles both: if the column is Binary, read bytes directly; if Utf8, base64-decode first. The metadata extraction simply projects whatever column type exists — no special handling needed.
