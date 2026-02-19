# certstream-server-rust

Last verified: 2026-02-18
Last context update: 2026-02-18

## Tech Stack
- Language: Rust (edition 2024)
- Async runtime: Tokio
- Web framework: Axum 0.8 (WebSocket + SSE)
- Metrics: metrics + prometheus exporter
- Storage: Delta Lake (via deltalake 0.25) for optional CT record archival
- Testing: cargo test (unit + integration)

## Commands
- `cargo build` - Build the project
- `cargo test` - Run all tests
- `cargo run` - Start server (reads config.yaml or env vars)
- `cargo run -- --validate-config` - Validate configuration without starting
- `cargo run -- --backfill` - Run delta backfill mode (catch-up gaps)
- `cargo run -- --backfill --from 0` - Run historical backfill from index 0
- `cargo run -- --backfill --logs "google"` - Backfill only logs matching filter
- `cargo run -- --backfill --staging-path /tmp/staging` - Backfill into staging table
- `cargo run -- --merge --staging-path /tmp/staging` - Merge staging into main table

## Project Structure
- `src/main.rs` - Entry point, server startup, task orchestration
- `src/config.rs` - All configuration structs, YAML + env var loading, validation
- `src/ct/` - Certificate Transparency log fetching and watching
- `src/ct/fetch.rs` - Shared fetch functions for RFC 6962 and Static CT logs
- `src/backfill.rs` - Delta backfill mode: gap detection, fetcher tasks, writer task, staging merge
- `src/models/` - Data models (CertificateMessage, PreSerializedMessage)
- `src/websocket/` - WebSocket stream handlers
- `src/sse.rs` - SSE stream handler
- `src/delta_sink.rs` - Delta Lake storage sink (optional, disabled by default)
- `src/dedup.rs` - Cross-log certificate deduplication filter
- `src/api.rs` - REST API endpoints
- `src/middleware.rs` - Auth and rate limiting middleware
- `src/rate_limit.rs` - Rate limiter implementation
- `src/health.rs` - Health check endpoints
- `src/hot_reload.rs` - Config hot-reload via file watcher
- `src/state.rs` - Shared server state management
- `src/cli.rs` - CLI argument parsing

## Architecture
All CT log entries flow through a single `broadcast::channel<Arc<PreSerializedMessage>>`.
Consumers (WebSocket, SSE, delta_sink) each subscribe independently via `tx.subscribe()`.
The delta_sink is spawned as an optional tokio task and does not affect other consumers.

The binary has three execution modes selected in main.rs:
1. **Server mode** (default): starts the WebSocket/SSE server and live CT log watchers
2. **Backfill mode** (`--backfill`): runs gap detection against the Delta table, spawns per-log fetcher tasks and a single writer task, then exits with code 0 (success) or 1 (errors). With `--staging-path`, writes to a separate staging table instead of the main table.
3. **Merge mode** (`--merge --staging-path <PATH>`): merges a staging Delta table into the main table using Delta MERGE INTO with deduplication, then deletes the staging directory on success

## Key Conventions
- Config structs use serde Deserialize with defaults; env vars override YAML
- Env var pattern: `CERTSTREAM_<SECTION>_<FIELD>` (e.g., `CERTSTREAM_DELTA_SINK_ENABLED`)
- All optional features use an `enabled: bool` field (default false)
- Graceful shutdown via CancellationToken propagated to all tasks

## Delta Sink Contracts
- **Disabled by default** (`delta_sink.enabled = false`)
- **Config**: `DeltaSinkConfig { enabled, table_path, batch_size, flush_interval_secs }`
- **Entry point**: `delta_sink::run_delta_sink(config, rx, shutdown)` spawned in main
- **Schema**: 20-column Arrow schema, partitioned by `seen_date` (YYYY-MM-DD)
- **Flush triggers**: batch_size threshold OR flush_interval_secs timer OR graceful shutdown
- **Buffer overflow**: if buffer > 2x batch_size, drops oldest half
- **Error recovery**: failed writes retain buffer for retry; table handle reopened
- **Non-fatal startup**: if table creation fails, task exits without crashing server
- **Metrics**: `certstream_delta_*` (records_written, flushes, write_errors, buffer_size, flush_duration_seconds, messages_lagged)
- **Public helpers**: `delta_schema()`, `open_or_create_table()`, `flush_buffer()`, `records_to_batch()`, `DeltaCertRecord::from_message()` are public for reuse by backfill

## Backfill Contracts
- **CLI flags**: `--backfill` activates backfill mode; `--from <INDEX>` sets historical start; `--logs <FILTER>` filters logs by substring; `--staging-path <PATH>` writes to staging table instead of main table
- **Entry point**: `backfill::run_backfill(config, staging_path, backfill_from, backfill_logs, shutdown)` called from main, returns exit code (i32)
- **State file dependency**: backfill loads `StateManager` from `config.ct_log.state_file` (default: `certstream_state.json`) and uses each log's `current_index` as the per-log ceiling. Logs not in the state file are skipped with a warning. If no logs have state file entries, backfill exits with code 0.
- **Two modes**: catch-up (no `--from`, fills internal gaps within existing Delta data) and historical (`--from N`, backfills from index N to ceiling)
- **Gap detection**: `detect_gaps(table_path, staging_path, logs, backfill_from)` queries Delta table(s) via DataFusion SQL, finds internal gaps only (LEAD window function). When staging_path is provided and the staging table exists, both tables are queried via a UNION ALL view using `COUNT(DISTINCT cert_index)` to avoid double-counting duplicates. The second element of the `logs` tuple is the ceiling (from state file `current_index`).
- **Gap detection fallback**: if main table does not exist but staging table does, gap detection uses staging alone; if neither exists, falls back to original mode-based logic (empty work items for catch-up, full range for historical)
- **Staging write path**: when `--staging-path` is provided, the writer task writes to the staging table path instead of `config.delta_sink.table_path`; gap detection still reads both tables to avoid re-fetching already-staged records
- **Catch-up rules**: only backfills logs already present in Delta table; logs not in table are skipped; only internal gaps are filled (no frontier gaps)
- **Historical rules**: backfills all logs from `--from` index to ceiling; logs not in Delta get full range from `--from` to ceiling
- **Architecture**: mpsc channel from N fetcher tasks to 1 writer task; fetchers send `DeltaCertRecord`; writer flushes on batch_size, timer, channel close, or shutdown
- **Fetcher retry**: rate-limit errors get exponential backoff (up to 10 retries, max 60s); HTTP/parse errors get up to 3 retries
- **Writer reuses**: `delta_sink::flush_buffer()` and `delta_sink::open_or_create_table()` from the live sink
- **Exit code**: 0 if all fetchers and writer succeed, 1 if any errors occurred
- **Graceful shutdown**: CancellationToken checked per batch in fetchers; writer flushes remaining buffer on cancellation

## Merge Contracts
- **CLI flags**: `--merge --staging-path <PATH>` activates merge mode; `--merge` without `--staging-path` exits with error
- **Entry point**: `backfill::run_merge(config, staging_path, shutdown)` called from main, returns exit code (i32)
- **Merge predicate**: `target.source_url = source.source_url AND target.cert_index = source.cert_index` (deduplication key)
- **Merge behavior**: uses `DeltaOps::merge()` with `when_not_matched_insert` only (no updates, no deletes); matched source rows are silently skipped
- **Batch-by-batch**: staging records are read via DataFusion SQL, then each RecordBatch is merged individually into the main table
- **Missing staging table**: if staging table does not exist (NotATable or InvalidTableLocation), exits with code 0 (not an error)
- **Empty staging table**: if all batches have zero rows, exits with code 0
- **Staging cleanup**: on successful merge, the staging directory is deleted via `remove_dir_all`; cleanup failure is non-fatal (logged as warning)
- **Error handling**: any failure during merge leaves staging intact for retry and exits with code 1
- **Graceful shutdown**: CancellationToken checked between batch merges; if cancelled, exits with code 1 (staging left intact)

## CT Fetch Contracts
- **Module**: `ct::fetch` (public module)
- **Error type**: `FetchError { HttpError, InvalidResponse, RateLimited(u16), NotAvailable(u16) }`
- **RFC 6962**: `get_tree_size(client, base_url, timeout)` and `fetch_entries(client, base_url, start, end, source, timeout)`
- **Static CT**: `get_checkpoint_tree_size(client, base_url, timeout)` and `fetch_tile_entries(client, base_url, tile_index, partial_width, offset_in_tile, source, timeout, issuer_cache)`
- **Shared by**: watcher, static_ct poller, and backfill fetchers
- **Parse failures**: skipped with debug log and metrics counter increment, not treated as errors

## Boundaries
- Safe to edit: `src/`, `config.example.yaml`
- Never manually edit: `Cargo.lock`
- Immutable once deployed: Delta table `_delta_log/` transaction logs
