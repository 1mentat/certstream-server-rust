# Delta Log Checkpoint on Startup Design

## Summary

This design adds automatic Delta log checkpointing to server startup. Delta Lake maintains a transaction log as a sequence of JSON commit files in the `_delta_log/` directory. As this log grows unboundedly, table opens become slower because the Delta library must replay every commit to reconstruct the current table state. A checkpoint compacts the current state into a single Parquet file so that subsequent table opens need only read that file plus any commits after it, rather than the full log history. On a long-running server writing thousands of certificate records per day, this log can grow to tens of thousands of entries, making checkpointing a meaningful operational concern.

The implementation introduces a single new public helper, `checkpoint_table()`, in `src/delta_sink.rs`. This function opens (or creates) the Delta table using the same `open_or_create_table()` path already used by the live sink, backfill, and merge operations, then calls `create_checkpoint()` and `cleanup_metadata()` from the `deltalake-core` checkpoints API. The helper is called once during server mode startup — after config validation but before the live `delta_sink` task is spawned — and is treated as a fatal gate: if the table cannot be opened or checkpointed, the server exits with code 1 rather than starting in a potentially degraded state. Log file cleanup after the checkpoint is non-fatal and logs a warning on failure. No new configuration fields or CLI flags are introduced; checkpointing is entirely automatic when `delta_sink.enabled = true`.

## Definition of Done
1. **When `delta_sink.enabled = true` and the server starts in server mode**, a Delta log checkpoint is created for the `delta_sink.table_path` table before any other delta operations begin.
2. **If checkpointing fails, the server exits with an error** (exit code 1) — it does not proceed with a potentially degraded table.
3. **No new configuration fields** — checkpointing is automatic when delta_sink is enabled.
4. **Out of scope:** periodic checkpointing, named targets, batch/offline modes, log file vacuum/cleanup.

## Acceptance Criteria

### delta-checkpoint.AC1: Checkpoint created on server startup
- **delta-checkpoint.AC1.1 Success:** When `delta_sink.enabled = true`, server creates a checkpoint parquet file in `_delta_log/` at the table's current version before spawning the delta_sink task
- **delta-checkpoint.AC1.2 Success:** Checkpoint is idempotent — calling on a table that already has a checkpoint at the current version succeeds without error
- **delta-checkpoint.AC1.3 Edge:** New table with zero commits gets created by `open_or_create_table()` and checkpointed without error

### delta-checkpoint.AC2: Fatal exit on checkpoint failure
- **delta-checkpoint.AC2.1 Failure:** If `open_or_create_table()` fails (e.g., invalid URI, permissions), server logs error and exits with code 1
- **delta-checkpoint.AC2.2 Failure:** If `create_checkpoint()` fails (e.g., corrupt log), server logs error and exits with code 1

### delta-checkpoint.AC3: Log cleanup after checkpoint
- **delta-checkpoint.AC3.1 Success:** After successful checkpoint, `cleanup_metadata()` removes expired log files (older than `logRetentionDuration`, default 30 days)
- **delta-checkpoint.AC3.2 Failure:** If `cleanup_metadata()` fails, server logs a warning and continues startup (non-fatal)

### delta-checkpoint.AC4: No configuration, server mode only
- **delta-checkpoint.AC4.1 Success:** When `delta_sink.enabled = false`, no checkpoint operation runs
- **delta-checkpoint.AC4.2 Success:** Backfill, merge, reparse-audit, and extract-metadata modes do not trigger checkpointing
- **delta-checkpoint.AC4.3 Success:** No new config fields or CLI flags are required

## Glossary

- **Delta Lake**: An open-source storage format that adds ACID transactions, versioning, and schema enforcement on top of Parquet files. Tables are stored as a directory of Parquet data files plus a `_delta_log/` transaction log.
- **`_delta_log/`**: The transaction log directory inside a Delta table. Contains one JSON file per commit recording the operations performed (file additions, deletions, metadata changes). Replaying this log reconstructs the current table state.
- **Checkpoint (Delta)**: A Parquet snapshot file written into `_delta_log/` that captures the complete table state at a specific version number (e.g., `00000000000001000.checkpoint.parquet`). Allows the Delta library to skip replaying older JSON commit files.
- **`create_checkpoint()`**: A function from `deltalake_core::protocol::checkpoints` that writes a checkpoint Parquet file for the current table version.
- **`cleanup_metadata()`**: A function from `deltalake_core::protocol::checkpoints` that removes expired JSON commit files older than the table's `logRetentionDuration` property (default 30 days). Requires a checkpoint to exist first so history is not lost.
- **`logRetentionDuration`**: A Delta table property controlling how long old transaction log entries are retained before `cleanup_metadata()` is permitted to delete them. Defaults to 30 days.
- **`open_or_create_table()`**: An existing helper in `src/delta_sink.rs` that opens a Delta table if it exists or creates it with the certstream schema if it does not. Used by the live sink, backfill, merge, and extract-metadata modes.
- **delta_sink**: The optional live-streaming component (`src/delta_sink.rs`) that receives certificate records from the broadcast channel and writes them to the Delta table in batches. Disabled by default; enabled via `delta_sink.enabled = true`.
- **`parse_table_uri()`**: A config helper (`src/config.rs`) that validates and parses a table path string into a `TableLocation` enum (`Local` for `file://`, `S3` for `s3://`). Rejects bare paths without a URI scheme.
- **`resolve_storage_options()`**: A config helper that produces the `HashMap<String, String>` of AWS/S3 credentials needed by the Delta builder. Returns an empty map for local tables.
- **Server mode**: The default execution mode of the binary. Starts the WebSocket/SSE server and live CT log watchers. Distinct from backfill, merge, reparse-audit, and extract-metadata modes, which are offline batch operations.
- **deltalake / delta-rs**: The Rust implementation of the Delta Lake protocol (`deltalake` crate, version 0.25 in this project). Provides `DeltaTable`, `DeltaOps`, and the checkpoints API used by this design.

## Architecture

Startup checkpointing runs as a blocking step in server mode, between config loading and delta_sink task spawning. A new public helper `checkpoint_table()` in `src/delta_sink.rs` encapsulates the checkpoint and cleanup logic.

**Startup sequence (server mode only):**

```
Config loaded
  → delta_sink.enabled == true?
  → Parse delta_sink.table_path via parse_table_uri()
  → Resolve storage_options via resolve_storage_options()
  → Load table via open_or_create_table()
  → create_checkpoint(&table, None)          [fatal on error → exit(1)]
  → cleanup_metadata(&table, None)           [warn on error → continue]
  → Log version, duration, cleanup count
  → Spawn delta_sink task (existing, unchanged)
```

**`checkpoint_table()` contract:**

```rust
pub async fn checkpoint_table(
    config: &DeltaSinkConfig,
    storage: &StorageConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
```

- Parses `config.table_path` via `parse_table_uri()`
- Resolves storage options via `resolve_storage_options()`
- Opens table via `open_or_create_table()`
- Calls `create_checkpoint(&table, None)` from `deltalake_core::protocol::checkpoints`
- Calls `cleanup_metadata(&table, None)` — failure logged as warning, does not propagate
- Returns `Ok(())` on success, `Err` on checkpoint or table-open failure
- Records metrics: checkpoint duration, cleanup count

**Error handling:**

| Operation | Failure behavior |
|-----------|-----------------|
| `open_or_create_table()` | Fatal — error propagated, server exits |
| `create_checkpoint()` | Fatal — error propagated, server exits |
| `cleanup_metadata()` | Non-fatal — warning logged, startup continues |

**No shared state:** The delta_sink task opens the table independently inside `run_delta_sink()`. The checkpoint step and the writer task do not share a `DeltaTable` handle.

**Metrics:**
- `certstream_delta_checkpoint_duration_seconds` (histogram) — time for checkpoint + cleanup
- `certstream_delta_checkpoint_logs_cleaned` (gauge) — expired log files removed

## Existing Patterns

This design follows established patterns from the codebase:

- **`open_or_create_table()`** (`src/delta_sink.rs:247`): Reused directly for table loading. Same function used by delta_sink, backfill, merge, and extract-metadata.
- **`parse_table_uri()` + `resolve_storage_options()`** (`src/config.rs:501, 533`): Standard URI parsing and S3 credential resolution. Used by all Delta table operations.
- **Public helpers in `delta_sink.rs`**: `checkpoint_table()` joins existing public helpers (`delta_schema()`, `open_or_create_table()`, `flush_buffer()`, `delta_writer_properties()`).
- **Structured logging with tracing**: Uses `info!`, `warn!`, `error!` with context fields, matching all existing log points.
- **Metrics naming**: `certstream_delta_*` prefix matches existing delta sink metrics.

**Divergence from existing pattern:** The delta_sink task treats startup failure as non-fatal (task exits, server continues). Checkpoint failure is fatal by design — the user explicitly chose this because a table that can't be checkpointed may indicate corruption or permissions issues that should block startup.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Checkpoint Helper Function

**Goal:** Add `checkpoint_table()` to `src/delta_sink.rs` and integrate into server startup in `src/main.rs`.

**Components:**
- `checkpoint_table()` in `src/delta_sink.rs` — loads table, creates checkpoint, cleans up expired logs, records metrics
- Startup integration in `src/main.rs` — call `checkpoint_table()` when `delta_sink.enabled`, before spawning delta_sink task; exit(1) on failure

**Dependencies:** None (first phase)

**Acceptance Criteria covered:** delta-checkpoint.AC1.1, delta-checkpoint.AC1.2, delta-checkpoint.AC1.3, delta-checkpoint.AC2.1, delta-checkpoint.AC2.2, delta-checkpoint.AC3.1, delta-checkpoint.AC3.2, delta-checkpoint.AC4.1, delta-checkpoint.AC4.2, delta-checkpoint.AC4.3

**Done when:** Server creates checkpoint on startup when delta_sink is enabled, exits on failure, logs results. Tests verify checkpoint file creation, error propagation, cleanup behavior, and edge cases.
<!-- END_PHASE_1 -->

## Additional Considerations

**Import path uncertainty:** The checkpoint functions live in `deltalake_core::protocol::checkpoints`. The `deltalake` 0.25 crate re-exports `protocol` (confirmed by existing `use deltalake::protocol::SaveMode`), but whether the `checkpoints` submodule is accessible needs verification at implementation time. If not re-exported, `deltalake-core` must be added as a direct dependency in `Cargo.toml`.

**Fresh table edge case:** `create_checkpoint()` on a newly created table (version 0) should produce a trivial checkpoint. If delta-rs errors on this, the implementation should skip checkpointing for new tables (no commits to compact).

**Future extensibility:** The `checkpoint_table()` helper could be called from other modes (backfill, merge) in the future without architectural changes. The function signature takes `&DeltaSinkConfig` and `&StorageConfig`, which are available in all modes.
