# Streaming Migration Design

## Summary

The `run_migrate` function exists to convert an existing Delta Lake table from an older schema — where the certificate DER encoding (`as_der`) was stored as a base64 text string — to the current schema, where `as_der` is stored as raw binary bytes with optimized per-column compression. The problem being solved is that the current implementation loads an entire date partition into memory at once using DataFusion's `.collect()` API. Typical partitions contain 25–36 million rows and occupy roughly 97 GB uncompressed, which, combined with intermediate transformation allocations, exceeds available memory on the target host.

The fix replaces `.collect().await` with DataFusion's `.execute_stream().await`, which returns an async stream of `RecordBatch` objects — each roughly 8,000 rows and 5–20 MB in size. The transformation (base64 decode, column alignment) and Delta write are applied to each batch individually before the next is fetched, keeping peak memory in the tens of megabytes regardless of partition size. Each streamed batch produces a separate Delta commit; post-migration compaction via Delta OPTIMIZE is expected to consolidate the resulting small files. The change is confined entirely to `run_migrate` in `src/backfill.rs` and does not affect any other DataFusion consumer in the codebase.

## Definition of Done
The `run_migrate` function processes partitions by streaming RecordBatches one at a time instead of collecting them all into memory. Each streamed batch is transformed (as_der base64→binary conversion, column alignment) and written to the output Delta table immediately, then freed from memory before the next batch is processed. Peak memory usage stays well under 64 GB for any single partition (currently ~97 GB uncompressed for a ~25-36M row partition). All existing migrate functionality (as_der conversion, column alignment, date filtering, source path override, graceful shutdown) is preserved. Existing and new tests pass.

## Acceptance Criteria

### migrate-streaming.AC1: Streaming batch processing
- **migrate-streaming.AC1.1 Success:** run_migrate processes partition data via execute_stream() instead of collect(), yielding one RecordBatch at a time
- **migrate-streaming.AC1.2 Success:** Each batch is transformed and written before the next batch is fetched (no accumulation)
- **migrate-streaming.AC1.3 Success:** Stream errors (DataFusion batch read failures) are handled gracefully with exit code 1

### migrate-streaming.AC2: Existing functionality preserved
- **migrate-streaming.AC2.1 Success:** as_der base64-to-binary conversion produces identical output to the collect-based implementation
- **migrate-streaming.AC2.2 Success:** Column alignment by name with type casting works identically per batch
- **migrate-streaming.AC2.3 Success:** Date filtering (--from, --to), source path override (--source), and graceful shutdown all work unchanged
- **migrate-streaming.AC2.4 Success:** Existing migrate tests pass without modification (test_migrate_basic_schema_and_data, test_migrate_graceful_shutdown, test_migrate_nonexistent_source_table, test_migrate_empty_source_table, test_migrate_with_date_filters)

## Glossary

- **RecordBatch**: An Apache Arrow in-memory columnar data structure holding a fixed number of rows across typed columns. The fundamental unit of data transfer in DataFusion and Arrow-based systems.
- **DataFrame**: A DataFusion abstraction representing a lazy query plan over Arrow data. Calling `.collect()` executes the plan and materializes all results; calling `.execute_stream()` executes it as an async iterator.
- **execute_stream() / SendableRecordBatchStream**: A DataFusion API that executes a query and returns results as an async stream of `RecordBatch` objects rather than collecting all rows into memory at once.
- **collect()**: The existing DataFusion API that executes a query and materializes all `RecordBatch` objects into a `Vec` in memory. The source of the OOM condition this design eliminates.
- **StreamExt**: A trait from the `futures` crate that adds async iteration methods (notably `.next().await`) to Rust streams. Required to drive a `SendableRecordBatchStream`.
- **Delta Lake**: An open-source storage layer built on Parquet that adds ACID transactions, schema enforcement, and time-travel. Used here as the on-disk format for archived certificate records.
- **Delta OPTIMIZE**: A Delta Lake compaction operation that rewrites small Parquet files into larger ones. Expected post-migration step to consolidate the many small commits produced by per-batch writes.
- **Partition / `seen_date`**: The Delta table is partitioned by `seen_date` (YYYY-MM-DD). Each partition is one day's certificate records, processed sequentially in `run_migrate`.
- **as_der**: The certificate's DER-encoded binary representation. Old schema: base64-encoded UTF-8 string. New schema: raw bytes (`DataType::Binary`).
- **WriterProperties**: A Parquet/Delta writer configuration controlling per-column compression codec, level, and dictionary encoding. Applied to all writes.
- **CancellationToken**: A Tokio primitive for cooperative graceful shutdown. Checked between partitions.
- **DataFusion**: An in-process SQL query engine built on Apache Arrow, used to query Delta Lake tables and transform RecordBatch data.

## Architecture

The `run_migrate` function in `src/backfill.rs` currently loads an entire partition into memory via `partition_df.collect().await`, which materializes all RecordBatches at once (~97 GB for a typical 25-36M row partition). Combined with the transformation clone and write buffer, this exceeds 128 GB.

The fix replaces `.collect().await` with `.execute_stream().await`, which returns a `SendableRecordBatchStream`. This stream yields one RecordBatch at a time (~8K rows, ~5-20 MB each). Each batch is transformed in place (base64→binary for `as_der`, column alignment by name), written to the output Delta table via `DeltaOps.write()`, and dropped before the next batch is fetched. Peak memory drops from ~97 GB to ~20-50 MB.

DataFusion controls batch size internally (default ~8K rows). No additional configuration is needed. The `futures::stream::StreamExt` trait provides the `.next()` async iterator — the `futures` crate is already in Cargo.toml.

Each streamed batch produces a separate Delta commit. A typical partition produces hundreds of small commits. Post-migration compaction (Delta OPTIMIZE) is expected to consolidate these.

No recovery tracking is implemented. On failure, the user deletes the output table and re-runs. This matches the current behavior and is acceptable since migration is a one-time operation.

## Existing Patterns

Investigation found that the codebase uniformly uses `.collect().await` for all DataFusion queries — in `run_migrate`, `run_merge`, `detect_gaps`, and `query_certs`. No streaming patterns exist.

This design introduces the first use of `DataFrame::execute_stream()` and `StreamExt::next()` for Arrow data. The pattern is localized to `run_migrate` and does not affect other DataFusion consumers.

The per-batch write pattern follows the existing `run_merge` approach in `src/backfill.rs:1014-1100`, which iterates collected batches and writes each individually via `DeltaOps.write()`. The only difference is that batches come from a stream instead of a pre-collected Vec.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Replace collect with execute_stream in run_migrate

**Goal:** Eliminate OOM by streaming RecordBatches one at a time instead of collecting the entire partition into memory.

**Components:**
- `src/backfill.rs` `run_migrate` function (~lines 1254-1398) — replace `partition_df.collect().await` with `partition_df.execute_stream().await` and `while let Some(batch_result) = stream.next().await` loop
- Add `use futures::stream::StreamExt;` import to `src/backfill.rs`

**Dependencies:** None (first phase)

**Covers:** migrate-streaming.AC1.1, migrate-streaming.AC1.2, migrate-streaming.AC1.3, migrate-streaming.AC2.1, migrate-streaming.AC2.2, migrate-streaming.AC2.3

**Done when:** `cargo build` succeeds. Existing migrate tests pass (`cargo test -- test_migrate`). A new test verifies streaming processes batches individually without collecting all into memory.
<!-- END_PHASE_1 -->

## Additional Considerations

**Commit count:** A partition with ~350-450 source Parquet files will produce hundreds of Delta commits. The `_delta_log/` directory for the output table will grow proportionally. This is acceptable for a one-time migration; running Delta OPTIMIZE afterward consolidates both data files and transaction log.

**DataFusion batch size:** The default DataFusion batch size (~8K rows) is not configurable through this design. If needed in the future, `SessionContext::new_with_config(SessionConfig::new().with_batch_size(N))` can override it. This is intentionally omitted to keep the change minimal.
