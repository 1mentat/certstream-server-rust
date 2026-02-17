# certstream-server-rust

Last verified: 2026-02-16

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

## Project Structure
- `src/main.rs` - Entry point, server startup, task orchestration
- `src/config.rs` - All configuration structs, YAML + env var loading, validation
- `src/ct/` - Certificate Transparency log fetching and watching
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

## Boundaries
- Safe to edit: `src/`, `config.example.yaml`
- Never manually edit: `Cargo.lock`
- Immutable once deployed: Delta table `_delta_log/` transaction logs
