# Delta Backfill Mode Design

## Summary

This design introduces a **backfill mode** for certstream-server-rust that populates historical Certificate Transparency (CT) log entries into the Delta Lake storage layer. While the server normally streams CT logs in real-time to WebSocket and SSE clients (and optionally archives them to Delta), backfill mode runs as a standalone batch job that queries CT log servers directly, identifies missing ranges in the existing Delta table, fetches those entries, and writes them to storage before exiting. This allows operators to fill gaps caused by downtime, expand storage backward to earlier log entries, or seed a new Delta table with historical data without disrupting live streaming.

The implementation follows a three-stage pipeline: **gap detection** (query Delta table via DataFusion to find missing cert_index ranges per log), **concurrent fetchers** (one tokio task per CT log server, each fetching entries in batches with rate limiting and exponential backoff), and a **single writer task** (receives records from all fetchers and flushes them to Delta in batches). The pipeline is controlled via CLI flags (`--backfill`, `--from`, `--logs`) and supports two modes: **catch-up mode** (default, fills gaps from the earliest existing record forward to current) and **historical mode** (`--from 0`, backfills from the beginning of the log). Interruption is handled gracefully — re-running backfill after Ctrl+C automatically resumes where it left off by re-querying the Delta table for remaining gaps.

## Definition of Done

1. A `--backfill` CLI mode that fetches CT log entries and writes them directly to the delta table, then exits
2. **Catch-up mode** (default): queries the delta table for the latest `cert_index` per log, fetches everything from there to current `tree_size`
3. **Historical mode** (e.g., `--from 0`): fetches from a specified starting index forward
4. Works with the existing delta table schema and partitioning
5. Handles interruption gracefully (can resume where it left off)

**Out of scope:** live streaming integration, broadcast channel, WebSocket/SSE consumers, delta replay/time-travel

## Acceptance Criteria

### delta-backfill.AC1: CLI mode that fetches and writes directly to delta
- **delta-backfill.AC1.1 Success:** `--backfill` runs, fetches entries from configured CT logs, writes to delta table, and exits with code 0
- **delta-backfill.AC1.2 Success:** `--logs <filter>` limits backfill to matching logs only
- **delta-backfill.AC1.3 Success:** `delta_sink.enabled = false` in config does not prevent backfill from writing to delta

### delta-backfill.AC2: Catch-up mode (default)
- **delta-backfill.AC2.1 Success:** With no `--from` flag, lower bound per log is `MIN(cert_index)` from delta table for that source
- **delta-backfill.AC2.2 Success:** Internal gaps (dropped records) between MIN and MAX are detected and backfilled
- **delta-backfill.AC2.3 Success:** Frontier gap (MAX to current `tree_size`) is detected and backfilled
- **delta-backfill.AC2.4 Edge:** Log present in config but not in delta table is skipped in catch-up mode

### delta-backfill.AC3: Historical mode (`--from`)
- **delta-backfill.AC3.1 Success:** `--from 0` backfills from index 0 to current `tree_size`, including ranges before the existing MIN in delta
- **delta-backfill.AC3.2 Success:** `--from <N>` overrides the lower bound for all logs, gaps detected from N forward

### delta-backfill.AC4: Works with existing delta table schema and partitioning
- **delta-backfill.AC4.1 Success:** Backfilled records use the same 20-column Arrow schema and `seen_date` partitioning as the live delta sink
- **delta-backfill.AC4.2 Success:** Records written by backfill are indistinguishable from records written by the live sink (same schema, same field population)
- **delta-backfill.AC4.3 Edge:** Backfill into an empty/nonexistent table creates it with the correct schema and partitioning

### delta-backfill.AC5: Handles interruption gracefully
- **delta-backfill.AC5.1 Success:** Ctrl+C triggers graceful shutdown — fetchers stop, writer flushes buffered records, process exits
- **delta-backfill.AC5.2 Success:** Re-running `--backfill` after interruption picks up where it left off (gap detection re-queries delta, only missing ranges are fetched)
- **delta-backfill.AC5.3 Failure:** Permanently unreachable log does not block other logs — fetcher for that log exits after retry exhaustion, others continue
- **delta-backfill.AC5.4 Success:** Exit code reflects whether all logs completed (0) or some failed (non-zero)

### delta-backfill.AC6: Cross-Cutting Behaviors
- **delta-backfill.AC6.1:** Progress is logged periodically per log (entries fetched / total, gaps remaining)
- **delta-backfill.AC6.2:** Rate limit errors (HTTP 429/503) are handled with exponential backoff per log
- **delta-backfill.AC6.3:** Completion summary reports total records written, logs completed/failed, elapsed time

## Glossary

- **Certificate Transparency (CT)**: A protocol requiring certificate authorities to publish TLS certificates to append-only, cryptographically-verifiable public logs. Each log is identified by a URL and exposes an API to fetch entries.
- **Delta Lake**: An open-source storage layer that brings ACID transactions and time-travel capabilities to data lakes. Used here to archive CT records in a columnar format (Parquet) with partitioning and transaction logs.
- **cert_index**: The sequential index of a certificate entry within a CT log (0-based). Used as the primary ordering key for detecting gaps and resuming backfill.
- **tree_size**: The current total number of entries in a CT log, obtained via the `get-sth` (Signed Tree Head) API call.
- **source_url**: The URL of the CT log server, stored in each Delta record to distinguish entries from different logs.
- **DataFusion**: An in-memory SQL query engine used by the Delta Rust library to query Delta tables. Gap detection uses DataFusion to run windowing queries over cert_index values.
- **gap detection**: The process of querying the Delta table to identify missing ranges of cert_index values, either within the existing data (internal gaps) or between the maximum recorded index and the current tree_size (frontier gap).
- **catch-up mode**: The default backfill mode that finds the minimum cert_index per log in the Delta table and backfills from there to the current tree_size, without expanding backward to earlier entries.
- **historical mode**: A backfill mode activated by `--from <index>` that overrides the lower bound and backfills from a specified starting index (e.g., `--from 0` for the entire log history).
- **Static CT protocol** (RFC 9162): A tile-based, CDN-friendly alternative to the original CT API. Fetches entries via immutable, cacheable "tile" files instead of dynamic `get-entries` calls. Also known as Sunlight.
- **CancellationToken** (tokio): A Tokio primitive for propagating graceful shutdown signals across async tasks. Backfill tasks check this token to stop fetching and flush buffers on Ctrl+C.
- **mpsc channel** (tokio): Multi-producer, single-consumer channel. Used to send DeltaCertRecord instances from concurrent fetcher tasks to the single writer task.
- **exponential backoff**: A retry strategy that increases wait time exponentially after each failure (e.g., 1s, 2s, 4s, 8s). Applied when CT log servers return rate limit errors (HTTP 429/503).
- **broadcast channel** (tokio): Multi-producer, multi-consumer channel used in the live server to distribute CT records to all connected WebSocket/SSE clients. Not used in backfill mode.
- **DeltaCertRecord**: The Rust struct representing a single CT log entry in the schema expected by the Delta table (20-column Arrow schema with fields like source_url, cert_index, seen_date, etc.).
- **seen_date**: The partition key for the Delta table, formatted as YYYY-MM-DD. Backfilled records use the current date at write time, not the timestamp when the cert was originally logged.

## Architecture

Backfill is a separate CLI mode (`--backfill`) that runs as a batch job and exits on completion. It bypasses the broadcast channel, WebSocket, and SSE systems entirely — fetching CT log entries directly from log servers and writing them to the delta table.

The mode operates as a three-stage pipeline:

1. **Gap detection** (startup, one-time): Opens the delta table and queries it via DataFusion to find the min/max `cert_index` per `source_url`. Then identifies missing ranges within `[min_index, tree_size]` using a window function that detects discontinuities. Combined with a `get-sth` call per log to determine the current `tree_size`, this produces a list of `(source_url, start_index, end_index)` work items.

2. **Concurrent fetchers** (one tokio task per log): Each fetcher processes its work items sequentially, calling `get-entries` in configurable batch sizes. Entries are parsed via the existing `parse_leaf_input()` function and converted to `DeltaCertRecord`. Records are sent through a shared `mpsc` channel to the writer. Each fetcher manages its own rate limiting with exponential backoff.

3. **Single writer task**: Receives `DeltaCertRecord`s from all fetchers, buffers them, and flushes to the delta table in batches using the existing `records_to_batch()` and `flush_buffer()` functions. Single writer avoids delta table write contention.

**CLI interface:**

- `--backfill` — Activates backfill mode
- `--from <index>` — Override start index (default: `MIN(cert_index)` per log from delta). Overrides gap detection lower bound.
- `--logs <filter>` — Optional filter to backfill specific logs by name or URL substring (default: all configured logs)

**Resume semantics:**

- **Catch-up (default):** Lower bound = `MIN(cert_index)` per source from delta. Finds internal gaps and the frontier-to-current gap. Does not expand backward.
- **`--from <index>`:** Overrides lower bound. `--from 0` means backfill from the very beginning of the log.
- **Log not in delta:** In catch-up mode, skipped (nothing to catch up on). With `--from`, backfilled from the specified index.
- **Interruption recovery:** Re-run `--backfill`. Gap detection re-queries the delta table and produces work items for whatever is still missing.

The `delta_sink.enabled` config flag is ignored in backfill mode — running `--backfill` implies intent to write to delta.

## Existing Patterns

**CLI parsing** (`src/cli.rs`): Hand-rolled flag parser using `env::args()`. Boolean flags checked via `.any()`. Backfill flags follow this same pattern. The `--from` and `--logs` flags use positional value parsing (`.next()` after flag match).

**Early-exit modes** (`src/main.rs`): `--validate-config`, `--export-metrics`, `--version` all branch early after config loading and exit. Backfill follows the same pattern — branch after config load, call `backfill::run_backfill()`, exit.

**CancellationToken shutdown** (throughout): All long-running tasks accept a `CancellationToken` and check `shutdown.is_cancelled()` or `shutdown.cancelled()` in `tokio::select!`. Backfill tasks follow this exactly.

**Delta write functions** (`src/delta_sink.rs`): `open_or_create_table()`, `delta_schema()`, `records_to_batch()`, `flush_buffer()`, and `DeltaCertRecord::from_json()` are all reusable without modification.

**CT entry fetching** (`src/ct/watcher.rs`, `src/ct/static_ct.rs`): Entry fetching is currently coupled to broadcasting, dedup, and stats. The fetch + parse logic needs to be extracted into a shared function that both the live watchers and backfill can call. This is the one divergence from existing patterns — the extraction creates a new shared utility where none existed before.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: CLI and Entry Point

**Goal:** Add `--backfill`, `--from`, and `--logs` CLI flags and wire up the backfill entry point in main.

**Components:**
- `src/cli.rs` — Add `backfill: bool`, `backfill_from: Option<u64>`, `backfill_logs: Option<String>` fields and parsing
- `src/main.rs` — Early-exit branch that calls `backfill::run_backfill()` (stub)
- `src/backfill.rs` — New module with `pub async fn run_backfill(config: Config, shutdown: CancellationToken) -> Result<()>` stub

**Dependencies:** None

**Done when:** `cargo run -- --backfill` invokes the stub and exits cleanly. `--help` documents the new flags.
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Extract Shared Fetch Logic

**Goal:** Extract CT entry fetching from the watcher into a reusable function that both live watchers and backfill can call.

**Components:**
- `src/ct/mod.rs` or `src/ct/fetch.rs` — New shared function: fetches a range of entries from a CT log URL, parses them via `parse_leaf_input()`, returns `Vec<CertificateMessage>`
- `src/ct/watcher.rs` — Refactored to call the shared fetch function instead of inlining the logic
- `src/ct/static_ct.rs` — Refactored similarly for the tile-based protocol

**Dependencies:** Phase 1

**Done when:** Existing live streaming works identically after refactor (no behavior change). Shared fetch function is callable from backfill. Tests verify fetch returns parsed entries for both protocols.

**Acceptance criteria covered:** delta-backfill.AC4.1
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Delta Table Gap Detection

**Goal:** Query the delta table to determine what needs backfilling.

**Components:**
- `src/delta_sink.rs` — New functions: `query_log_state(table_path) -> HashMap<String, (u64, u64)>` (min/max cert_index per source_url) and `find_gaps(table_path, source_url, lower_bound, upper_bound) -> Vec<(u64, u64)>` using DataFusion SQL
- `src/backfill.rs` — Gap detection orchestration: for each configured log, determine work items based on delta state, `--from` override, and current `tree_size`

**Dependencies:** Phase 1

**Done when:** Gap detection correctly identifies missing ranges in a delta table with known gaps. Tests verify catch-up mode (uses min from delta), `--from` override, empty table, and log-not-in-delta cases.

**Acceptance criteria covered:** delta-backfill.AC2.1, delta-backfill.AC2.2, delta-backfill.AC2.3, delta-backfill.AC2.4, delta-backfill.AC3.1, delta-backfill.AC3.2
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Concurrent Fetcher Tasks

**Goal:** Spawn per-log fetcher tasks that process work items and send records to a channel.

**Components:**
- `src/backfill.rs` — Fetcher task: iterates work items for a single log, calls shared fetch function, converts to `DeltaCertRecord`, sends via `mpsc::Sender`. Per-log rate limiting with exponential backoff. Progress logging.
- `src/backfill.rs` — Orchestrator: spawns one fetcher task per log (filtered by `--logs` if specified), manages the `mpsc` channel

**Dependencies:** Phase 2, Phase 3

**Done when:** Multiple logs can be backfilled concurrently. Rate limiting handles 429/503 responses with backoff. Progress is logged periodically. `CancellationToken` stops fetchers cleanly. Tests verify concurrent execution and backoff behavior.

**Acceptance criteria covered:** delta-backfill.AC1.1, delta-backfill.AC1.2, delta-backfill.AC1.3, delta-backfill.AC5.1, delta-backfill.AC5.2
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Writer Task and End-to-End Integration

**Goal:** Single writer task that receives records from fetchers and flushes to delta. Wire up the full pipeline.

**Components:**
- `src/backfill.rs` — Writer task: receives from `mpsc::Receiver`, buffers records, flushes to delta via existing `records_to_batch()` / `flush_buffer()`. Flushes on batch size threshold, periodic timer, or channel close (all fetchers done).
- `src/backfill.rs` — Completion logic: waits for all fetchers to finish, writer to drain, reports summary (logs completed, total records, failures). Exit code reflects success/partial failure.

**Dependencies:** Phase 4

**Done when:** Full pipeline works end-to-end: gap detection -> concurrent fetch -> single writer -> delta table. Interrupted backfill can be resumed by re-running. Tests verify records are written correctly, interruption preserves progress, and exit codes are correct.

**Acceptance criteria covered:** delta-backfill.AC4.2, delta-backfill.AC4.3, delta-backfill.AC5.3, delta-backfill.AC5.4, delta-backfill.AC6.1, delta-backfill.AC6.2, delta-backfill.AC6.3
<!-- END_PHASE_5 -->

## Additional Considerations

**Rate limit variability:** CT log operators have wildly different batch size limits (Google: 1024, Let's Encrypt: 200, DigiCert: as low as 32). The backfill uses the configured `ct_log.batch_size` as a starting point. Operators that return fewer entries than requested are handled naturally (the fetcher advances by however many entries were returned). Explicit batch size discovery is not included — the existing retry/backoff config handles rate limit errors.

**Scale:** Individual CT logs can have hundreds of millions of entries. Full historical backfill of a large log could take hours or days. The gap detection query (window function over all cert_index values) may be slow on very large tables. If this becomes a problem, chunk-based gap detection can be added as an optimization without changing the architecture.

**Static CT protocol:** The tile-based Static CT API (RFC 9162 / Sunlight) is CDN-friendly and has different rate limit characteristics than RFC 6962 `get-entries`. The shared fetch function in Phase 2 must handle both protocols. The tile-based approach may be more efficient for historical backfill since tiles are cacheable static resources.
