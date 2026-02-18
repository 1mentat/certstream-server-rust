# Staging Backfill Design

## Summary

Backfilling historical CT log records into a live Delta table today requires taking the service
offline for the duration of the fetch — which can span hours for large logs. This design adds a
staging workflow to eliminate that window. When `--backfill --staging-path <PATH>` is used, fetched
records are written to a completely separate Delta table at the specified path, leaving the
production table untouched while the live service continues serving clients. Gap detection is
extended to treat both tables as a single logical dataset, so a second staging run skips records
already staged. Once staging finishes, `--merge --staging-path <PATH>` atomically copies only
missing records into the main table using Delta Lake's MERGE INTO operation and then deletes the
staging directory. This reduces the required service downtime from the full backfill duration to
the much shorter merge duration.

The implementation is deliberately minimal: it reuses the existing writer task, `flush_buffer`, and
`open_or_create_table` helpers from `delta_sink.rs`, extends `detect_gaps` in `backfill.rs` with an
optional UNION ALL view across both tables, and introduces `DeltaOps::merge()` as the sole new
pattern. The `--merge` flag becomes a third binary mode alongside server and backfill, validated in
`main.rs` before dispatch. Backward compatibility is preserved: running `--backfill` without
`--staging-path` behaves exactly as before.

## Definition of Done
Backfill can run concurrently with the live service by writing to a separate staging Delta table, with a dedicated merge command that atomically moves staged records into the main table with deduplication — reducing service downtime from hours (full backfill duration) to seconds/minutes (merge duration only).

**Success criteria:**
- `--backfill --staging-path <PATH>` writes to an isolated staging Delta table while the live service continues
- Gap detection queries both main and staging tables to avoid redundant fetches
- `--merge --staging-path <PATH>` reads staging, deduplicates against main on `(source_url, cert_index)`, appends missing records, and deletes the staging directory
- Works with both catch-up and historical (`--from`) backfill modes
- Existing `--backfill` behavior without `--staging-path` is unchanged (backward compatible)

**Out of scope:**
- True concurrent writers via S3/MinIO (future consideration)
- Changes to the live delta sink
- Config.yaml integration for staging path

## Acceptance Criteria

### staging-backfill.AC1: Backfill writes to staging table
- **staging-backfill.AC1.1 Success:** `--backfill --staging-path /tmp/staging` creates a Delta table at `/tmp/staging` with the standard 20-column schema partitioned by `seen_date`
- **staging-backfill.AC1.2 Success:** Fetched CT records are written to the staging table, not the main table
- **staging-backfill.AC1.3 Success:** Works with both catch-up (`--backfill --staging-path`) and historical (`--backfill --from N --staging-path`) modes

### staging-backfill.AC2: Gap detection unions main and staging
- **staging-backfill.AC2.1 Success:** When staging table exists, gap detection queries both main and staging — entries present in either table are excluded from gap list
- **staging-backfill.AC2.2 Success:** Running backfill with staging a second time produces fewer work items (previously staged entries not re-fetched)
- **staging-backfill.AC2.3 Success:** Per-log MAX(cert_index) from the union is capped at the state file ceiling
- **staging-backfill.AC2.4 Edge:** When staging table doesn't exist yet (first run), gap detection falls back to querying main table only

### staging-backfill.AC3: Merge deduplicates and appends
- **staging-backfill.AC3.1 Success:** `--merge --staging-path` inserts staging records not present in main (matched on `source_url` + `cert_index`)
- **staging-backfill.AC3.2 Success:** Records already in main with matching `(source_url, cert_index)` are skipped (not duplicated)
- **staging-backfill.AC3.3 Success:** Merge is idempotent — running it twice with same staging data produces identical main table
- **staging-backfill.AC3.4 Success:** Staging directory is deleted after successful merge
- **staging-backfill.AC3.5 Success:** Merge logs metrics: records inserted, records skipped

### staging-backfill.AC4: Error handling
- **staging-backfill.AC4.1 Edge:** Missing/empty staging table exits 0 with info log (nothing to merge)
- **staging-backfill.AC4.2 Edge:** Missing main table is created automatically, then merge proceeds as pure inserts
- **staging-backfill.AC4.3 Failure:** Merge batch failure leaves staging intact for retry, exits 1
- **staging-backfill.AC4.4 Failure:** Process interruption (SIGINT) leaves staging intact, exits 1
- **staging-backfill.AC4.5 Failure:** `--merge` without `--staging-path` prints error and exits with non-zero code

### staging-backfill.AC5: Backward compatibility
- **staging-backfill.AC5.1 Success:** `--backfill` without `--staging-path` writes to main table (existing behavior unchanged)
- **staging-backfill.AC5.2 Success:** All existing backfill tests pass without modification

## Glossary

- **Certificate Transparency (CT)**: A public logging system for TLS certificates defined in RFC 6962 and its Static CT successor. Certificate authorities submit issued certificates to append-only CT logs, enabling detection of misissued certificates.
- **CT log**: An append-only, publicly auditable log operated by an independent party (e.g., Google, Cloudflare) that stores TLS leaf certificates and their issuer chains. Each entry is identified by a monotonically increasing integer index (`cert_index`).
- **cert_index**: The zero-based integer position of a certificate entry within a specific CT log. The pair `(source_url, cert_index)` uniquely identifies an entry globally across all logs.
- **Delta Lake**: An open-source ACID table format built on top of Parquet files. Each write is a transaction recorded in the `_delta_log/` directory. Provides atomic writes, schema enforcement, and time travel.
- **DeltaOps**: The high-level Rust API from the `deltalake` crate. `DeltaOps::write()` appends Parquet batches; `DeltaOps::merge()` executes a SQL MERGE INTO transaction.
- **MERGE INTO**: A SQL operation that compares rows from a source dataset against a target table on a match predicate and performs conditional inserts, updates, or deletes in a single atomic transaction. Used here to insert staging records not already present in the main table.
- **DataFusion**: An in-process SQL query engine (part of the Apache Arrow ecosystem) embedded in the `deltalake` crate. Used to register Delta tables as queryable views and run SQL gap-detection logic without a separate database process.
- **UNION ALL view**: A virtual table combining two real tables with `SELECT ... UNION ALL SELECT ...`. Used so downstream gap-detection SQL queries treat main and staging records as a single dataset without copying data.
- **LEAD window function**: A SQL analytic function that returns the value of a column from the next row in an ordered partition. Used in `find_internal_gaps()` to detect non-consecutive `cert_index` values.
- **staging table**: A temporary Delta table, separate from the production table, written to during a backfill run. Accumulates fetched records before they are merged into the main table.
- **main table**: The production Delta table that the live delta sink writes to and that consumers query. Referenced as `config.delta_sink.table_path`.
- **state file** (`certstream_state.json`): A JSON file maintained by the live service recording the most recently observed `cert_index` (`current_index`) for each CT log URL. Backfill reads this to determine the per-log upper boundary (ceiling).
- **ceiling**: The per-log upper bound for a backfill run, taken from the state file's `current_index`. Gap detection caps `MAX(cert_index)` at this value to prevent fetching beyond the live service's progress.
- **catch-up mode**: Backfill mode without `--from`. Fills only internal gaps within the index range already present in the Delta table.
- **historical mode**: Backfill mode with `--from N`. Backfills from index N up to the state file ceiling.
- **gap detection** (`detect_gaps`): Function in `backfill.rs` that queries the Delta table via DataFusion, computes per-log statistics, and uses the LEAD window function to find contiguous ranges of missing `cert_index` values.
- **mpsc channel**: A multi-producer, single-consumer async channel from `tokio::sync::mpsc`. N fetcher tasks send records; one writer task receives and flushes.
- **CancellationToken**: A `tokio_util` primitive allowing one task to signal cancellation to many others. Checked at the start of each batch iteration.
- **RecordBatch**: An Apache Arrow columnar in-memory data structure — a fixed collection of equal-length Arrow arrays sharing a schema. The Delta writer and DataFusion both operate on `RecordBatch` objects.

## Architecture

The staging backfill introduces a two-phase workflow: **stage** (write to isolated table while live service runs) and **merge** (atomically move staged records into main table during brief downtime).

### Binary Modes

The binary gains a third mode (`--merge`) alongside existing server and backfill modes:

- **Server mode** (default): unchanged
- **Backfill mode** (`--backfill`): gains optional `--staging-path <PATH>` flag. When present, the writer task writes to the staging Delta table instead of the main table. When absent, existing behavior is preserved.
- **Merge mode** (`--merge --staging-path <PATH>`): new mode. Reads staging table, deduplicates against main using Delta MERGE INTO, deletes staging directory on success.

### Data Flow

**Staging (backfill with `--staging-path`):**

```
State file (ceilings) ──► detect_gaps() ──► work items ──► N fetcher tasks
                              │                                    │
                    UNION ALL of main +                       mpsc channel
                    staging tables                                 │
                                                            1 writer task
                                                                   │
                                                         staging Delta table
```

The only change from existing backfill is (1) gap detection unions both tables and (2) the writer targets the staging path instead of the main path.

**Merge (`--merge`):**

```
staging Delta table ──► DataFusion read ──► RecordBatches
                                                │
main Delta table ◄── DeltaOps::merge() ◄────────┘
                        │
                   dedup on (source_url, cert_index)
                   when_not_matched_insert_all()
                        │
                   delete staging directory
```

### Merge Strategy: Native Delta MERGE INTO

The merge uses `DeltaOps::merge()` from the `deltalake` crate (v0.25), which provides atomic MERGE INTO semantics. The predicate matches on `(source_url, cert_index)`. Only `when_not_matched_insert_all()` is used — records already in main are skipped, new records are inserted. Each merge call is a single Delta transaction.

Staging records are read in batches via DataFusion and merged batch-by-batch. Each batch merge is its own transaction, so partial progress is preserved if interrupted. Re-running merge is idempotent — dedup prevents duplicate inserts.

### Gap Detection Union

When `--staging-path` is provided and the staging table exists, `detect_gaps()` registers both tables in DataFusion and creates a UNION ALL view:

```sql
CREATE VIEW ct_records AS
  SELECT cert_index, source_url FROM ct_main
  UNION ALL
  SELECT cert_index, source_url FROM ct_staging
```

Downstream queries (`query_log_states()`, `find_internal_gaps()`) query `"ct_records"` unchanged. The per-log `MAX(cert_index)` returned by `query_log_states()` is capped at the corresponding state file ceiling via `min(max_idx, ceiling)` in the calling code, preventing inflation from live service writes or stale staging data.

## Existing Patterns

**Patterns followed:**
- CLI flag parsing: same manual `env::args()` iteration pattern used for `--backfill`, `--from`, `--logs`
- Table operations: reuses `open_or_create_table()` and `delta_schema()` from `src/delta_sink.rs` for staging table creation
- DataFusion queries: follows existing `SessionContext` + `register_table` + SQL pattern from `detect_gaps()` in `src/backfill.rs`
- Exit codes: 0 success, 1 error (same as existing backfill)
- Graceful shutdown: `CancellationToken` propagated to all tasks
- Writer task: same structure and `flush_buffer()` reuse as existing backfill writer

**New pattern introduced:**
- `DeltaOps::merge()` — first use of Delta MERGE INTO in this codebase. All existing Delta writes use `DeltaOps::write()` with `SaveMode::Append`. The merge operation is limited to the `--merge` command and does not affect the live delta sink or normal backfill writes.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: CLI & Mode Routing

**Goal:** Add `--staging-path` and `--merge` flags, route to new merge mode in `main.rs`

**Components:**
- `src/cli.rs` — add `staging_path: Option<String>` and `merge: bool` to `CliArgs`, parse `--staging-path <PATH>` and `--merge` flags
- `src/main.rs` — add merge mode branch: validate `--merge` requires `--staging-path`, call `run_merge()` (stubbed), exit with result code

**Dependencies:** None

**Done when:** `--merge --staging-path /tmp/staging` routes to stub function and exits. `--merge` without `--staging-path` prints error. Existing `--backfill` behavior unchanged. `--help` shows new flags.
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Staging Writes

**Goal:** Backfill writes to staging table when `--staging-path` is provided

**Components:**
- `src/backfill.rs` — `run_backfill()` gains `staging_path: Option<String>` parameter; writer task uses `staging_path.unwrap_or(config.delta_sink.table_path)` as write target
- `src/main.rs` — pass `cli_args.staging_path` to `run_backfill()`

**Dependencies:** Phase 1

**Done when:** `--backfill --staging-path /tmp/staging` creates a Delta table at the staging path with correct schema and partition. `--backfill` without `--staging-path` writes to main table as before. Tests verify staging table is created at the specified path and contains expected records.

**ACs covered:** `staging-backfill.AC1.1`, `staging-backfill.AC1.2`, `staging-backfill.AC1.3`, `staging-backfill.AC5.1`, `staging-backfill.AC5.2`
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Gap Detection Union

**Goal:** Gap detection queries both main and staging tables when staging path is provided

**Components:**
- `src/backfill.rs` — `detect_gaps()` gains `staging_path: Option<&str>` parameter; registers UNION ALL view when staging table exists; `query_log_states()` results capped at per-log ceiling

**Dependencies:** Phase 2

**Done when:** Running `--backfill --staging-path` a second time detects fewer gaps (entries already in staging are excluded). Tests verify UNION ALL behavior with both tables populated, staging-only table, main-only table, and neither table existing.

**ACs covered:** `staging-backfill.AC2.1`, `staging-backfill.AC2.2`, `staging-backfill.AC2.3`, `staging-backfill.AC2.4`
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Merge Command

**Goal:** `--merge` reads staging, deduplicates against main via Delta MERGE INTO, deletes staging on success

**Components:**
- `src/backfill.rs` — `run_merge(config: Config, staging_path: String, shutdown: CancellationToken) -> i32` function: opens both tables, reads staging via DataFusion, merges batch-by-batch with `DeltaOps::merge()`, deletes staging directory on success
- `src/main.rs` — replace merge stub with call to `run_merge()`

**Dependencies:** Phase 2 (staging table must exist to merge)

**Done when:** `--merge` reads staging records, inserts non-duplicates into main, skips existing records, deletes staging directory. Tests verify dedup correctness, idempotency (running merge twice produces same result), and staging cleanup.

**ACs covered:** `staging-backfill.AC3.1`, `staging-backfill.AC3.2`, `staging-backfill.AC3.3`, `staging-backfill.AC3.4`, `staging-backfill.AC3.5`
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Error Handling & Edge Cases

**Goal:** Robust handling of missing tables, interruptions, and failure recovery

**Components:**
- `src/backfill.rs` — error paths in `run_merge()`: missing staging (exit 0), missing main (create via `open_or_create_table()`), merge failure (leave staging intact, exit 1), cancellation (skip cleanup, exit 1)

**Dependencies:** Phase 4

**Done when:** All error scenarios handled gracefully. Tests verify: missing staging exits 0, missing main creates table then merges, merge failure preserves staging, SIGINT leaves staging intact.

**ACs covered:** `staging-backfill.AC4.1`, `staging-backfill.AC4.2`, `staging-backfill.AC4.3`, `staging-backfill.AC4.4`, `staging-backfill.AC4.5`
<!-- END_PHASE_5 -->

## Additional Considerations

**Schema evolution:** If the Delta schema changes between when staging is written and when merge runs (e.g., a column is added in a new release), the merge will fail. This is the safe default — a silent schema mismatch could corrupt data. Future work could add schema compatibility checks with a clear error message suggesting re-running backfill with the updated binary.

**Future path to zero downtime:** An S3-compatible object store (e.g., MinIO) with `aws_conditional_put: "etag"` would enable Delta Lake's optimistic concurrency control, allowing true concurrent writers. The staging approach is an incremental step that requires no infrastructure changes.
