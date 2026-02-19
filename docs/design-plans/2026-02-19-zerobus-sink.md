# ZeroBus Sink Design

## Summary

This document describes the design for `zerobus_sink`, a new optional sink module that streams Certificate Transparency (CT) certificate records from certstream-server-rust into a Databricks-managed Delta table via the ZeroBus Ingest SDK. The existing `delta_sink` writes records to a local Delta Lake table on disk; `zerobus_sink` provides an alternative path for teams who want CT data ingested directly into a cloud-managed Databricks environment without managing local storage or running a separate ETL pipeline. Both sinks can run simultaneously in server mode -- each subscribes independently to the same internal broadcast channel, so enabling ZeroBus does not affect the local Delta sink or any other consumer.

The implementation follows the established `delta_sink` pattern closely: an `enabled: bool` config flag, `CERTSTREAM_ZEROBUS_*` environment variable overrides, a long-lived async task that subscribes to the broadcast channel, and graceful shutdown via `CancellationToken`. The one structurally new element is Protobuf serialization: rather than writing Arrow `RecordBatch` objects to a local file system, records are encoded using a `.proto` schema compiled by `prost-build` and forwarded one at a time to the ZeroBus SDK, which handles internal microbatching and delivery. Backfill mode gains a new `--sink zerobus` CLI flag that routes historical CT records through the same Protobuf path instead of the local Delta writer, subject to a constraint that catch-up gap detection is not supported for remote Databricks tables (historical mode with `--from N` only).

## Definition of Done

1. **New `zerobus_sink` module** that streams CT certificate records to a Databricks-managed Delta table via the ZeroBus Ingest SDK (`databricks-zerobus-ingest-sdk` crate), following the same architectural pattern as the existing `delta_sink` (broadcast channel subscriber, `enabled: bool` config, graceful shutdown).
2. **Full 20-column schema** matching the existing Delta table format (`DeltaCertRecord`), serialized as Protobuf via a `.proto` definition and `prost`.
3. **Backfill mode support** so `--backfill` can write historical records to Databricks via ZeroBus as an alternative to local Delta writes.
4. **Config + metrics** following existing conventions (`CERTSTREAM_ZEROBUS_*` env vars, `certstream_zerobus_*` metrics).

**Out of scope:** Replacing delta_sink, Query API changes, schema evolution, merge mode for ZeroBus.

## Acceptance Criteria

### zerobus-sink.AC1: Live sink streams records to Databricks
- **zerobus-sink.AC1.1 Success:** Records from the broadcast channel are ingested into the Databricks Delta table via ZeroBus SDK
- **zerobus-sink.AC1.2 Success:** Sink shuts down gracefully — flushes pending records and closes stream on CancellationToken
- **zerobus-sink.AC1.3 Success:** Retryable SDK errors trigger stream recovery via `recreate_stream()` and resume ingestion
- **zerobus-sink.AC1.4 Failure:** Non-retryable SDK errors skip the record, log a warning, and continue processing
- **zerobus-sink.AC1.5 Failure:** Stream creation failure at startup exits the sink task without crashing the server

### zerobus-sink.AC2: Full 20-column schema as Protobuf
- **zerobus-sink.AC2.1 Success:** All 20 fields of `DeltaCertRecord` are represented in the protobuf `CertRecord` message and round-trip correctly
- **zerobus-sink.AC2.2 Success:** `DescriptorProto` is constructable from the compiled proto and accepted by `TableProperties`

### zerobus-sink.AC3: Backfill writes historical records via ZeroBus
- **zerobus-sink.AC3.1 Success:** `--backfill --sink zerobus --from N` ingests records from index N to state file ceiling into Databricks
- **zerobus-sink.AC3.2 Success:** `--backfill` without `--sink` uses the delta writer (backwards-compatible)
- **zerobus-sink.AC3.3 Failure:** `--sink zerobus` without `--from` exits with a clear error (historical mode required)
- **zerobus-sink.AC3.4 Failure:** `--sink zerobus` when `zerobus_sink.enabled = false` exits with a clear error
- **zerobus-sink.AC3.5 Failure:** `--sink invalidname` exits with a clear error listing valid sink names

### zerobus-sink.AC4: Config and metrics follow conventions
- **zerobus-sink.AC4.1 Success:** `ZerobusSinkConfig` loads from YAML with serde defaults and is disabled by default
- **zerobus-sink.AC4.2 Success:** Env vars (`CERTSTREAM_ZEROBUS_*`) override YAML values for all config fields
- **zerobus-sink.AC4.3 Failure:** Validation rejects enabled config with empty endpoint, unity_catalog_url, table_name, client_id, or client_secret
- **zerobus-sink.AC4.4 Success:** Metrics (`certstream_zerobus_records_ingested`, `_ingest_errors`, `_stream_recoveries`, `_messages_lagged`, `_records_skipped`) increment correctly during operation

## Glossary

- **broadcast::channel**: A Tokio multi-producer, multi-consumer channel where every active subscriber receives every message. Used here so delta_sink, zerobus_sink, WebSocket, and SSE consumers can each independently receive the same CT records without coordination.
- **CancellationToken**: A `tokio_util` primitive passed to every long-lived task. When the server initiates shutdown, it cancels the token; tasks observe it and flush pending work before exiting.
- **CertRecord**: The Protobuf message type generated by `prost-build` from `proto/cert_record.proto`. Mirrors the 20-column `DeltaCertRecord` layout and is the wire format sent to the ZeroBus SDK.
- **CT (Certificate Transparency)**: An Internet standard (RFC 6962 and the newer Static CT API) that requires certificate authorities to log every TLS certificate they issue to publicly auditable logs. certstream-server-rust watches these logs and rebroadcasts new entries in real time.
- **DataFusion**: An Apache Arrow-based SQL query engine embedded in the `deltalake` crate. Used by backfill gap detection to query existing Delta table contents via SQL without a running database server.
- **Databricks**: A cloud data platform built on Apache Spark and Delta Lake. ZeroBus is Databricks' managed streaming-ingest service; records sent via the SDK appear in a Unity Catalog Delta table.
- **Delta Lake**: An open-source storage layer that brings ACID transactions and versioned history to Parquet files. Used by both the existing `delta_sink` (local) and the Databricks-managed table that ZeroBus writes to.
- **DeltaCertRecord**: The canonical flattened Rust struct (defined in `src/delta_sink.rs`) that maps a nested `CertificateMessage` to the 20 columns of the Delta schema. Reused by the new sink as the intermediate representation before Protobuf encoding.
- **DescriptorProto**: A Protobuf runtime type that describes a message schema. The ZeroBus SDK's `TableProperties` requires one so Databricks knows the column layout of incoming records.
- **mpsc channel**: A Tokio multi-producer, single-consumer channel. In backfill mode, N fetcher tasks send `DeltaCertRecord` values through an mpsc channel to a single writer task, keeping IO concerns separated from fetch concerns.
- **prost / prost-build**: A Rust Protobuf library. `prost-build` is a build-time code generator (`build.rs`) that compiles `.proto` files into Rust structs with `prost::Message` derive; `prost` provides the runtime `encode_to_vec()` serialization.
- **PreSerializedMessage**: An internal model struct that holds pre-serialized JSON bytes for a certificate event. The broadcast channel carries `Arc<PreSerializedMessage>` so JSON encoding happens once and is shared across all subscribers.
- **Protobuf (Protocol Buffers)**: A language-neutral binary serialization format defined by `.proto` schema files. Compact and schema-enforced; required by the ZeroBus SDK as the wire format for records.
- **Unity Catalog**: Databricks' unified governance layer for data assets. A Unity Catalog table name has the form `catalog.schema.table` (two dots), which is why the config validation checks for exactly two dots in `table_name`.
- **ZeroBus / ZeroBus Ingest SDK**: Databricks' managed streaming-ingest service and its corresponding Rust SDK crate (`databricks-zerobus-ingest-sdk`). Accepts Protobuf-encoded records one at a time via `ingest_record_offset()`, handles internal microbatching, and commits writes to a Databricks Delta table every 1-5 seconds.
- **ZerobusStream**: The long-lived connection object returned by `sdk.create_stream()`. Represents an open ingest session to a specific Databricks table; can be recovered via `sdk.recreate_stream()` on transient errors.

## Architecture

Parallel sink module alongside existing `delta_sink`. Both can run simultaneously in server mode (independent broadcast subscribers). In backfill mode, an explicit `--sink` CLI flag selects which writer backend to use.

**Data flow (live):**
```
broadcast::channel<Arc<PreSerializedMessage>>
  ├─ delta_sink (if enabled)  ──► local Delta table
  └─ zerobus_sink (if enabled) ──► Databricks managed Delta table
```

**Data flow (backfill):**
```
fetcher tasks ──mpsc──► writer task
                         ├─ run_writer()          (--sink delta, default)
                         └─ run_zerobus_writer()  (--sink zerobus)
```

**Serialization path:**
```
PreSerializedMessage (JSON bytes)
  → DeltaCertRecord::from_json()
  → CertRecord::from_delta_cert()  (generated prost struct)
  → record.encode_to_vec()         (protobuf binary)
  → stream.ingest_record_offset()  (ZeroBus SDK)
```

No client-side buffering or batching. The ZeroBus SDK manages internal microbatching, inflight buffering via `max_inflight_records`, and server-side batch writes every 1-5 seconds. The sink forwards records one at a time.

**Stream lifecycle:** One long-lived `ZerobusStream` per sink instance. On retryable errors, `sdk.recreate_stream()` recovers the connection. Non-retryable errors skip the record. If stream recreation fails, the task exits (non-fatal to server).

**Backfill constraint:** ZeroBus backfill only supports historical mode (`--from N`). Catch-up gap detection requires querying the target table via DataFusion SQL, which is not possible for remote Databricks tables. `--sink zerobus` without `--from` exits with an error.

## Existing Patterns

This design follows the `delta_sink` pattern established in `src/delta_sink.rs`:

- **Config struct** with `enabled: bool` default false, serde defaults, env var overrides (`src/config.rs:288-320`)
- **Conditional spawn** in `main.rs` via `tx.subscribe()` (`src/main.rs:181-191`)
- **Non-fatal startup** — if sink initialization fails, log and exit task without crashing server
- **Graceful shutdown** via `CancellationToken`, 30-second timeout on join handle
- **Metrics naming** — `certstream_<sink>_*` counters and gauges
- **Lagged message handling** — log warning, increment counter, continue
- **DeltaCertRecord** as the canonical flattened record type, reused from `src/delta_sink.rs:18-127`

New pattern introduced: **Protobuf serialization** via `.proto` file + `prost-build` in `build.rs`. This is new to the codebase but standard for the ZeroBus SDK ecosystem. The `.proto` file mirrors the existing `delta_schema()` 20-column definition.

New pattern introduced: **`--sink <name>` CLI flag** for backfill writer target selection in `src/cli.rs`. Existing backfill flags remain unchanged.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Protobuf Schema & Build Pipeline

**Goal:** Define the certificate record protobuf schema and set up compile-time code generation.

**Components:**
- `proto/cert_record.proto` — protobuf message definition with all 20 fields matching `DeltaCertRecord`
- `build.rs` — prost-build script to compile `.proto` into Rust types
- `Cargo.toml` — add `prost`, `prost-types` dependencies and `prost-build` build dependency
- Conversion function `CertRecord::from_delta_cert(&DeltaCertRecord)` in `src/zerobus_sink.rs`
- Function to build `DescriptorProto` from the compiled proto for `TableProperties`

**Dependencies:** None (first phase)

**Done when:** `cargo build` succeeds, generated `CertRecord` struct exists with `prost::Message` derive, conversion from `DeltaCertRecord` works, `DescriptorProto` can be constructed. Tests verify field mapping between `DeltaCertRecord` and `CertRecord` round-trips correctly.

**Covers:** zerobus-sink.AC2.1, zerobus-sink.AC2.2
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Configuration

**Goal:** Add `ZerobusSinkConfig` and wire it into config loading, env var overrides, and validation.

**Components:**
- `ZerobusSinkConfig` struct in `src/config.rs` — `enabled`, `endpoint`, `unity_catalog_url`, `table_name`, `client_id`, `client_secret`, `max_inflight_records`
- `Default` impl with sensible defaults (enabled: false, max_inflight_records: 10000)
- `zerobus_sink: ZerobusSinkConfig` field added to `Config` struct (`src/config.rs:369`)
- Env var overrides: `CERTSTREAM_ZEROBUS_ENABLED`, `CERTSTREAM_ZEROBUS_ENDPOINT`, etc. in `Config::load()`
- YAML deserialization support in `YamlConfig` (`src/config.rs:391`)
- Validation: non-empty endpoint/uc_url/table_name/credentials when enabled; table_name contains two dots
- `config.example.yaml` — add commented `zerobus_sink` section

**Dependencies:** None (independent of Phase 1)

**Done when:** Config loads from YAML and env vars, validation catches invalid configs, `--validate-config` reports zerobus_sink status. Tests verify defaults, env var overrides, and validation rules.

**Covers:** zerobus-sink.AC4.1, zerobus-sink.AC4.2, zerobus-sink.AC4.3
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Live Sink

**Goal:** Implement the streaming ZeroBus sink that subscribes to the broadcast channel and forwards records to Databricks.

**Components:**
- `src/zerobus_sink.rs` — `pub async fn run_zerobus_sink(config, rx, shutdown)` main task function
- SDK initialization: `ZerobusSdk::new()` → `create_stream()` with `StreamConfigurationOptions`
- `tokio::select!` loop: broadcast recv → deserialize → convert → encode → `ingest_record_offset()`
- Shutdown branch: `stream.flush()` → `stream.close()`
- Error recovery: `is_retryable()` check → `sdk.recreate_stream()` on retryable, skip on non-retryable
- Lagged message handling matching delta_sink pattern
- Conditional spawn in `src/main.rs` alongside delta_sink

**Dependencies:** Phase 1 (protobuf types), Phase 2 (config)

**Done when:** Sink starts when enabled, connects to ZeroBus, forwards records from broadcast channel, recovers from transient errors, shuts down gracefully. Tests verify startup, record forwarding, error recovery, lagged handling, and shutdown behavior.

**Covers:** zerobus-sink.AC1.1, zerobus-sink.AC1.2, zerobus-sink.AC1.3, zerobus-sink.AC1.4, zerobus-sink.AC1.5
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Metrics

**Goal:** Add observability metrics for the ZeroBus sink.

**Components:**
- Metric instrumentation in `src/zerobus_sink.rs`:
  - `certstream_zerobus_records_ingested` (counter)
  - `certstream_zerobus_ingest_errors` (counter)
  - `certstream_zerobus_stream_recoveries` (counter)
  - `certstream_zerobus_messages_lagged` (counter)
  - `certstream_zerobus_records_skipped` (counter)

**Dependencies:** Phase 3 (live sink exists to instrument)

**Done when:** All metrics increment correctly during normal operation, errors, recovery, and lagged events. Tests verify metric values after specific operations.

**Covers:** zerobus-sink.AC4.4
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Backfill Writer & CLI

**Goal:** Add ZeroBus as a backfill write target with explicit `--sink` CLI flag.

**Components:**
- `--sink <name>` flag parsing in `src/cli.rs` — new `backfill_sink: Option<String>` field in `CliArgs`
- `run_zerobus_writer()` function in `src/backfill.rs` — receives `DeltaCertRecord` from mpsc, converts to protobuf, calls `ingest_record_offset()`, flushes on channel close/shutdown
- Dispatch logic in `run_backfill()` — match on `backfill_sink` to spawn correct writer
- Validation: `--sink zerobus` requires both `zerobus_sink.enabled = true` in config AND `--from N` flag (historical mode only — no catch-up gap detection for remote tables)
- `run_backfill()` signature updated to accept `backfill_sink: Option<String>`, plumbed from `main.rs`
- Gap detection skipped when `--sink zerobus`: work items built directly from `--from N` to state file ceiling per log (same logic as historical mode in existing backfill)

**Dependencies:** Phase 1 (protobuf types), Phase 2 (config), Phase 3 (SDK patterns established)

**Done when:** `--backfill --sink zerobus --from N` spawns ZeroBus writer with correct range, `--backfill` alone uses delta writer (backwards-compatible), `--sink zerobus` without `--from` exits with clear error, invalid sink name errors clearly. Tests verify writer dispatch, historical-only enforcement, record forwarding, and error handling.

**Covers:** zerobus-sink.AC3.1, zerobus-sink.AC3.2, zerobus-sink.AC3.3, zerobus-sink.AC3.4, zerobus-sink.AC3.5
<!-- END_PHASE_5 -->

<!-- START_PHASE_6 -->
### Phase 6: Integration Testing

**Goal:** End-to-end validation with a real Databricks workspace.

**Components:**
- Integration test in `tests/` that connects to a test Databricks workspace
- Verifies: SDK connection, record ingestion, data visible in target table
- Gated behind `#[cfg(feature = "integration")]` or env var to avoid running in CI without credentials
- Documents manual test procedure for local verification

**Dependencies:** All prior phases

**Done when:** Records ingested via both live sink and backfill paths appear in the Databricks Delta table. Manual test procedure documented.

**Covers:** zerobus-sink.AC1.1, zerobus-sink.AC3.1 (end-to-end verification)
<!-- END_PHASE_6 -->

## Additional Considerations

**Credentials security:** `client_id` and `client_secret` should be provided via env vars (`CERTSTREAM_ZEROBUS_CLIENT_ID`, `CERTSTREAM_ZEROBUS_CLIENT_SECRET`) rather than YAML config files to avoid secrets in version control. The YAML fields exist for flexibility but documentation should recommend env vars.

**SDK maturity:** The `databricks-zerobus-ingest-sdk` crate is v0.1 (Public Preview). Minor version updates may include backwards-incompatible changes. Pin the exact version in `Cargo.toml` and note this in documentation.

**Dual sink operation:** Both `delta_sink` and `zerobus_sink` can run simultaneously in server mode. Each subscribes independently to the broadcast channel. This allows writing to both local Delta and Databricks concurrently if desired.
