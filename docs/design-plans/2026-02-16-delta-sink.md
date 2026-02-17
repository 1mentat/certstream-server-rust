# Delta Sink Design

## Summary

This design implements a persistent storage layer for certificate transparency (CT) entries using Delta Lake, a columnar table format optimized for analytics. A new `delta_sink` module subscribes to the existing real-time broadcast channel as a parallel consumer — the same channel that feeds WebSocket and SSE clients — and writes CT data to a local Delta table. This write-aside pattern ensures that the new storage functionality has zero impact on the existing real-time streaming pipeline.

The sink deserializes pre-serialized JSON messages from the broadcast channel, converts them into columnar Apache Arrow record batches, and periodically flushes them to disk using the delta-rs library. Flushing occurs when either a size threshold (default 10,000 records) or time threshold (default 30 seconds) is reached. The Delta table captures the full certificate metadata, including DER-encoded certificate bytes and chain data that are not exposed via the lightweight streaming formats, making the data suitable for historical analysis and querying. The feature is disabled by default and isolated — failures in the Delta sink do not affect real-time streaming.

## Definition of Done
CT entries flowing through the existing broadcast channel are also written to a local Delta table via delta-rs, using micro-batch appends. The schema includes full metadata plus leaf cert DER and chain. The existing WebSocket/SSE streaming pipeline continues working exactly as-is — the Delta writer is a parallel consumer with no impact on real-time latency.

## Acceptance Criteria

### delta-sink.AC1: CT entries are written to Delta table with correct schema
- **delta-sink.AC1.1 Success:** `full` JSON bytes from broadcast channel deserialize into `DeltaCertRecord` with all fields populated
- **delta-sink.AC1.2 Success:** Arrow RecordBatch contains correct column types (Timestamp for `seen`, List(Utf8) for `all_domains`, Boolean for `is_ca`, etc.)
- **delta-sink.AC1.3 Success:** `as_der` and `chain` fields are present in the Delta table (base64 string and JSON-serialized list respectively)
- **delta-sink.AC1.4 Success:** Records are partitioned by `seen_date` (YYYY-MM-DD derived from `seen` timestamp)
- **delta-sink.AC1.5 Edge:** Messages that fail deserialization are skipped and logged, not causing the sink to crash or stall

### delta-sink.AC2: Delta table is created and managed correctly
- **delta-sink.AC2.1 Success:** If table doesn't exist at `table_path`, it is created with the full Arrow schema on first flush
- **delta-sink.AC2.2 Success:** If table already exists, it is opened and appended to without schema conflict

### delta-sink.AC3: Batching and flush behavior
- **delta-sink.AC3.1 Success:** Buffer flushes when it reaches `batch_size` records
- **delta-sink.AC3.2 Success:** Buffer flushes when `flush_interval_secs` elapses, even if buffer is below `batch_size`
- **delta-sink.AC3.3 Success:** Graceful shutdown flushes remaining buffered records before exit
- **delta-sink.AC3.4 Failure:** Delta write failure retains buffer and retries on next flush cycle; buffer exceeding 2x `batch_size` drops oldest half

### delta-sink.AC4: No disruption to existing streaming
- **delta-sink.AC4.1 Success:** WebSocket and SSE streams continue operating normally when delta sink is enabled
- **delta-sink.AC4.2 Success:** Delta sink disabled by default (`enabled: false`); application behavior unchanged when disabled
- **delta-sink.AC4.3 Success:** Broadcast channel `Lagged` errors in the sink are logged and metriced, not propagated
- **delta-sink.AC4.4 Success:** Delta table creation failure on startup does not prevent the rest of the application from running

## Glossary

- **Delta Lake**: An open-source storage layer that provides ACID transactions, schema enforcement, and time travel capabilities on top of Parquet files. The `delta-rs` Rust implementation is used here.
- **Apache Arrow**: A columnar in-memory data format with efficient cross-language interoperability. Delta tables store data in Arrow-compatible Parquet format.
- **RecordBatch**: An Arrow data structure representing a collection of equal-length arrays (columns) with a shared schema. Used to convert row-oriented cert records into columnar format for Delta writes.
- **Write-aside pattern**: An architectural pattern where writes to a secondary storage system (Delta table) occur in parallel with the primary operation (real-time streaming), without blocking or modifying the primary path.
- **Broadcast channel**: Tokio's `broadcast::channel` — a multi-producer, multi-consumer channel where each receiver sees all messages. Used to fan out CT entries to multiple consumers (WebSocket, SSE, Delta sink).
- **PreSerializedMessage**: An application-specific struct containing pre-serialized JSON byte fields (`full`, `lite`, `domains_only`). The `full` field includes DER and chain data needed for Delta storage.
- **Micro-batch**: A small buffer of records accumulated before a bulk write operation. Reduces write overhead compared to single-record writes.
- **DER (Distinguished Encoding Rules)**: A binary encoding format for X.509 certificates. Stored as base64-encoded strings in the Delta table.
- **Lagged error**: A Tokio broadcast channel error indicating the receiver fell behind and missed messages because the channel buffer overflowed. Handled by logging and continuing.
- **Partition column**: A column used to physically organize data into separate directories (here, `seen_date` in YYYY-MM-DD format), enabling efficient time-range queries.
- **Certificate Transparency (CT)**: A framework for monitoring and auditing TLS certificates, providing cryptographically verifiable public logs of all issued certificates.
- **Graceful shutdown**: Application shutdown pattern where in-flight work completes cleanly before exit. Here, the sink flushes buffered records before terminating.

## Architecture

Write-aside pattern: a new `delta_sink` module subscribes to the existing `broadcast::channel<Arc<PreSerializedMessage>>` as a parallel consumer alongside WebSocket and SSE handlers. The sink deserializes the `full` JSON variant back into a purpose-built `DeltaCertRecord` struct, accumulates records in an in-memory buffer, and periodically flushes them as Arrow `RecordBatch` appends to a local Delta table via delta-rs.

```
broadcast_cert() -> Arc<PreSerializedMessage> -> broadcast channel
                                                      |
                                    +-----------------+-----------------+
                                    |                 |                 |
                              WS clients        SSE clients      Delta Sink Task
                                                                 |-- deserialize full JSON
                                                                 |-- convert to Arrow arrays
                                                                 |-- accumulate in buffer
                                                                 +-- flush to Delta table
                                                                     (timer OR size threshold)
```

**Data flow:** The sink reads pre-serialized JSON bytes from the broadcast channel's `full` variant (the only one containing DER and chain data). It deserializes into `DeltaCertRecord` — a flat struct mapping directly to the Arrow schema — then converts to columnar Arrow arrays for Delta writes.

**Lifecycle:**
- Startup: open existing Delta table or create with schema at configured path
- Running: receive messages, buffer, flush on dual triggers (size or time)
- Shutdown: flush remaining buffer via graceful shutdown signal, then exit

**Failure isolation:** Delta write failures never block the real-time pipeline. If the sink falls behind, the broadcast channel's `Lagged` error causes skipped messages (logged + metriced, not recovered). Write failures retain the buffer and retry on the next flush cycle.

### Delta Table Schema

| Column | Arrow Type | Source |
|--------|-----------|--------|
| `cert_index` | UInt64 | `data.cert_index` |
| `update_type` | Utf8 | `data.update_type` |
| `seen` | Timestamp(Microsecond, UTC) | `data.seen` (f64 seconds -> microseconds) |
| `seen_date` | Utf8 | Partition column, derived `YYYY-MM-DD` from `seen` |
| `source_name` | Utf8 | `data.source.name` |
| `source_url` | Utf8 | `data.source.url` |
| `cert_link` | Utf8 | `data.cert_link` |
| `serial_number` | Utf8 | `leaf_cert.serial_number` |
| `fingerprint` | Utf8 | `leaf_cert.fingerprint` |
| `sha256` | Utf8 | `leaf_cert.sha256` |
| `sha1` | Utf8 | `leaf_cert.sha1` |
| `not_before` | Int64 | `leaf_cert.not_before` |
| `not_after` | Int64 | `leaf_cert.not_after` |
| `is_ca` | Boolean | `leaf_cert.is_ca` |
| `signature_algorithm` | Utf8 | `leaf_cert.signature_algorithm` |
| `subject_aggregated` | Utf8 | `leaf_cert.subject.aggregated` |
| `issuer_aggregated` | Utf8 | `leaf_cert.issuer.aggregated` |
| `all_domains` | List(Utf8) | `leaf_cert.all_domains` |
| `as_der` | Utf8 | `leaf_cert.as_der` (base64, kept as string) |
| `chain` | List(Utf8) | `data.chain` (JSON-serialized per chain cert) |

Partitioned by `seen_date` for efficient time-range queries.

### Configuration Contract

```yaml
delta_sink:
  enabled: false                    # Off by default
  table_path: "./data/certstream"   # Local filesystem path
  batch_size: 10000                 # Records before size-triggered flush
  flush_interval_secs: 30           # Max seconds between flushes
```

Environment variable overrides: `CERTSTREAM_DELTA_SINK_ENABLED`, `CERTSTREAM_DELTA_SINK_TABLE_PATH`, `CERTSTREAM_DELTA_SINK_BATCH_SIZE`, `CERTSTREAM_DELTA_SINK_FLUSH_INTERVAL_SECS`.

### Batching Strategy

Dual-trigger flush — whichever fires first:
- **Size:** buffer reaches `batch_size` records (default 10,000)
- **Time:** `flush_interval_secs` elapsed since last flush (default 30s)

On flush: convert `Vec<DeltaCertRecord>` to columnar Arrow arrays, build `RecordBatch`, append to Delta table, clear buffer. On write failure: retain buffer, log error, retry next cycle. If buffer exceeds 2x `batch_size` from repeated failures, drop oldest half and warn.

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `certstream_delta_records_written` | Counter | Total records flushed |
| `certstream_delta_flushes` | Counter | Total flush operations |
| `certstream_delta_write_errors` | Counter | Failed flush attempts |
| `certstream_delta_buffer_size` | Gauge | Current buffer occupancy |
| `certstream_delta_messages_lagged` | Counter | Skipped broadcast messages |
| `certstream_delta_flush_duration_seconds` | Histogram | Time per flush |

## Existing Patterns

Investigation found the following patterns this design follows:

**Broadcast channel subscription:** WS handlers (`src/websocket/server.rs:88`) and SSE handler (`src/sse.rs:40`) both call `state.tx.subscribe()` to get an independent `broadcast::Receiver<Arc<PreSerializedMessage>>`. The Delta sink follows the same pattern. Lagged handling matches the WS handler at `server.rs:206-208`.

**Optional feature gating:** Features like REST API (`config.protocols.api`) and SSE (`config.protocols.sse`) use a bool config flag checked in `main.rs:build_router()` before conditionally adding routes/spawning tasks. Delta sink follows this pattern with `config.delta_sink.enabled`.

**Configuration structure:** Nested config structs with `#[serde(default)]` for YAML, environment variable overrides checked in `Config::load()`. `DeltaSinkConfig` follows the same pattern as `ConnectionLimitConfig` and `AuthConfig`.

**Module organization:** Single-file modules (e.g., `src/sse.rs`, `src/dedup.rs`, `src/state.rs`) for self-contained features with one public entry point. `src/delta_sink.rs` follows this pattern.

**No divergence from existing patterns.**

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Dependencies and Configuration
**Goal:** Add delta-rs/Arrow dependencies, define `DeltaSinkConfig`, wire into config loading

**Components:**
- `Cargo.toml` — add `deltalake` and `arrow` crate dependencies with appropriate features
- `DeltaSinkConfig` struct in `src/config.rs` — `enabled`, `table_path`, `batch_size`, `flush_interval_secs` with defaults and env var overrides
- Config loading in `Config::load()` — YAML parsing + env var overrides for delta_sink section

**Dependencies:** None (first phase)

**Done when:** `cargo build` succeeds with new dependencies, config parses `delta_sink` section from YAML and environment variables
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Arrow Schema and Record Conversion
**Goal:** Define the Arrow schema, `DeltaCertRecord` deserialization struct, and conversion from JSON bytes to `RecordBatch`

**Components:**
- `DeltaCertRecord` struct in `src/delta_sink.rs` — flat serde-deserializable struct matching the Arrow schema columns
- Arrow schema definition in `src/delta_sink.rs` — `Arc<Schema>` with all columns from the schema table above
- `records_to_batch(records: &[DeltaCertRecord]) -> Result<RecordBatch>` — converts a Vec of records into columnar Arrow arrays and builds a RecordBatch

**Dependencies:** Phase 1 (Arrow crate available)

**Covers:** delta-sink.AC1.1, delta-sink.AC1.2, delta-sink.AC1.3

**Done when:** Unit tests pass that deserialize sample `full` JSON into `DeltaCertRecord` and convert to a valid `RecordBatch` with correct column types and values
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Delta Table Management
**Goal:** Create or open a Delta table on local filesystem with the defined schema

**Components:**
- Table initialization in `src/delta_sink.rs` — open existing table at `table_path` or create new one with Arrow schema if it doesn't exist
- Partition configuration — `seen_date` as partition column

**Dependencies:** Phase 2 (schema definition)

**Covers:** delta-sink.AC2.1, delta-sink.AC2.2

**Done when:** Integration test passes that creates a new Delta table, verifies schema, closes and reopens it, and confirms the table is readable with correct schema
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Sink Task with Batching and Flush
**Goal:** Implement the core async task that receives from broadcast channel, buffers, and flushes to Delta

**Components:**
- `spawn_delta_sink()` in `src/delta_sink.rs` — public entry point, spawns the async task, returns `JoinHandle`
- Receive loop — `tokio::select!` over broadcast receiver, flush timer, and shutdown signal
- Buffer management — `Vec<DeltaCertRecord>` with dual-trigger flush (size + time)
- Delta append — write `RecordBatch` to table via delta-rs append API
- Error handling — retry on write failure, drop oldest half if buffer exceeds 2x batch_size

**Dependencies:** Phase 3 (table management)

**Covers:** delta-sink.AC1.4, delta-sink.AC1.5, delta-sink.AC3.1, delta-sink.AC3.2, delta-sink.AC3.3, delta-sink.AC3.4

**Done when:** Integration test passes that sends messages through a broadcast channel, verifies records appear in the Delta table after flush, and confirms graceful shutdown flushes remaining buffer
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Metrics and Main Integration
**Goal:** Add Prometheus metrics, wire sink into `main.rs` with config gating

**Components:**
- Metrics instrumentation in `src/delta_sink.rs` — all six metrics from the metrics table
- `mod delta_sink;` declaration in `src/main.rs`
- Conditional spawn in `main.rs` — check `config.delta_sink.enabled`, subscribe to broadcast, spawn task, log confirmation
- Graceful shutdown integration — sink task respects existing shutdown signal

**Dependencies:** Phase 4 (sink task)

**Covers:** delta-sink.AC4.1, delta-sink.AC4.2, delta-sink.AC4.3, delta-sink.AC4.4

**Done when:** Full application starts with `delta_sink.enabled: true`, certificates flow to Delta table, Prometheus metrics are emitted, graceful shutdown flushes buffer, and existing WS/SSE streaming is unaffected
<!-- END_PHASE_5 -->

## Additional Considerations

**Error handling:** Deserialization failures for individual messages are logged and skipped (should be rare — we're deserializing JSON we just serialized). Table creation failure on startup prevents the sink from spawning but does not affect the rest of the application.

**Memory bound:** The 2x `batch_size` cap on the buffer prevents unbounded growth during sustained write failures. At default settings (10,000 batch_size) with ~2KB per record, worst case buffer is ~40MB.

**Future TODOs (out of scope):**
1. Approach A optimization — dedicated mpsc channel for structured `CertificateMessage` to avoid deserialize round-trip
2. Backfill from CT logs to Delta table
3. Zerobus support
4. S3/cloud storage backend via delta-rs object store
5. REST API for historical certificate queries from Delta table
