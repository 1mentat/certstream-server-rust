# Human Test Plan: Delta Sink

**Implementation Plan:** `docs/implementation-plans/2026-02-16-delta-sink/`
**Automated Tests:** 35 delta_sink tests (all passing)
**Generated:** 2026-02-16

---

## Prerequisites

- A built release or debug binary of certstream-server-rust with delta sink support
- `cargo test delta_sink` passes (35 tests, 0 failures)
- A writable directory for Delta table output (e.g., `./data/certstream_test`)
- Tools installed: `websocat` (or another WebSocket client), `curl`, `jq` (optional, for JSON parsing)
- At least one reachable CT log (or use `static_logs` configuration to point to a known log)
- Access to view application logs (stdout/stderr)

---

## Phase 1: Delta Sink Enabled with Live Streams

| Step | Action | Expected |
|------|--------|----------|
| 1 | Create a `config.yaml` with `delta_sink.enabled: true`, `delta_sink.table_path: "./data/certstream_test"`, `delta_sink.batch_size: 100`, `delta_sink.flush_interval_secs: 10`. Enable WebSocket and SSE protocols. Configure at least one static CT log or allow RFC6962 log discovery. | Config file is valid. Run `cargo run -- --validate-config` to confirm "Configuration is valid" and "Delta sink enabled: true" is printed. |
| 2 | Start the application: `cargo run` | Logs show: "Delta sink task started" with the configured `table_path`, `batch_size`, and `flush_interval_secs`. No error logs related to table creation. |
| 3 | Connect a WebSocket client: `websocat ws://localhost:8080/full-stream` | WebSocket client begins receiving JSON certificate messages. Messages flow at a normal rate (multiple per second once CT logs are being polled). |
| 4 | In a separate terminal, connect an SSE client: `curl -N http://localhost:8080/sse` (if SSE is enabled in config) | SSE client begins receiving `data:` lines containing JSON certificate messages. |
| 5 | Wait at least 30 seconds for certificate data to accumulate. | Both WS and SSE clients continue to receive messages without interruption. |
| 6 | Check the Delta table directory: `ls -la ./data/certstream_test/` | Directory contains `_delta_log/` subdirectory and at least one `seen_date=YYYY-MM-DD/` partition directory. |
| 7 | Inspect the Delta table log: `ls ./data/certstream_test/_delta_log/` | At least one `.json` commit file exists (e.g., `00000000000000000000.json`, `00000000000000000001.json`). |
| 8 | Inspect a partition directory: `ls ./data/certstream_test/seen_date=*/` | Contains `.parquet` files. |
| 9 | Check application logs for any broadcast channel contention errors. | No errors like "broadcast channel full" or "send error". No warnings about lagged messages under normal load. |
| 10 | Stop the application with Ctrl+C. | Logs show "Graceful shutdown initiated, flushing remaining buffer" followed by "Shutdown flush completed" and "Delta sink task stopped". No error logs during shutdown. |

## Phase 2: Delta Sink Disabled (Default Behavior)

| Step | Action | Expected |
|------|--------|----------|
| 1 | Create a `config.yaml` without any `delta_sink` section, or with `delta_sink.enabled: false`. | Config file is valid. |
| 2 | Start the application: `cargo run` | Logs show "delta sink disabled". No "Delta sink task started" message appears. |
| 3 | Connect a WebSocket client: `websocat ws://localhost:8080/full-stream` | WebSocket client receives messages normally. |
| 4 | Check the filesystem for any Delta table directory at the default path `./data/certstream`. | No Delta table directory is created. No `_delta_log/` or `.parquet` files exist. |
| 5 | Stop the application with Ctrl+C. | Clean shutdown. No delta-sink related log messages during shutdown. |

## Phase 3: Invalid Table Path (Startup Failure Resilience)

| Step | Action | Expected |
|------|--------|----------|
| 1 | Set `delta_sink.enabled: true` and `delta_sink.table_path: "/root/no_access/delta"` (or another unwritable path). | Config file is valid (path validation does not happen at config load time). |
| 2 | Start the application: `cargo run` | Logs show an error like "Failed to open or create Delta table at startup, delta-sink task exiting" with the invalid path. The application continues running -- it does NOT exit or crash. |
| 3 | Verify the health endpoint: `curl http://localhost:8080/health` | Returns `200 OK` (or the expected health response). |
| 4 | Connect a WebSocket client: `websocat ws://localhost:8080/full-stream` | WebSocket client receives certificate messages normally despite delta sink failure. |
| 5 | Connect an SSE client (if enabled): `curl -N http://localhost:8080/sse` | SSE client receives messages normally. |
| 6 | Check logs for any CT log watcher activity. | CT log watchers are running and fetching entries (look for "found CT logs" or certificate processing messages). |
| 7 | Stop the application with Ctrl+C. | Clean shutdown. No panic or abnormal exit. |

## End-to-End: Sustained Load with Delta Sink

**Purpose:** Validate that the delta sink operates correctly under sustained real-world load without degrading WebSocket/SSE performance or causing memory growth from unbounded buffers.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Configure `delta_sink.enabled: true`, `batch_size: 1000`, `flush_interval_secs: 30`. Enable all protocols. Point to multiple CT logs (either via RFC6962 discovery or multiple `static_logs`). | Configuration valid. |
| 2 | Start the application and let it run for at least 5 minutes. | Delta sink flushes occur at regular intervals. Check logs for "Timer-triggered flush completed" or "Batch flush completed" messages. |
| 3 | Monitor the `./data/certstream_test/_delta_log/` directory over time. | New commit files appear periodically (roughly every 30 seconds or when 1000 records accumulate, whichever comes first). |
| 4 | During the 5-minute run, periodically check the `/metrics` endpoint: `curl http://localhost:8080/metrics \| grep certstream_delta` | Metrics present: `certstream_delta_records_written` (increasing), `certstream_delta_flushes` (increasing), `certstream_delta_write_errors` (should be 0), `certstream_delta_buffer_size` (fluctuating between 0 and batch_size), `certstream_delta_flush_duration_seconds` (populated). |
| 5 | Verify no `certstream_delta_messages_lagged` counter increments under normal load. | Counter is 0 or not present if no lagging occurred. |
| 6 | Connect a WebSocket client during the run and verify it receives messages at the same rate as when delta sink is disabled. | No noticeable latency increase or message loss on the WebSocket stream. |
| 7 | Stop with Ctrl+C. Verify final flush. | "Graceful shutdown initiated, flushing remaining buffer" appears. Final records are written to the table. |

---

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|------------|-------|
| **AC4.1** - WebSocket and SSE streams continue operating normally when delta sink is enabled | Automated tests use mock consumers, not real WS/SSE protocol handlers with real network connections. Full verification requires actual protocol-level clients connecting to the Axum server. | 1. Start the application with `delta_sink.enabled: true` and a valid `table_path`. 2. Connect a WebSocket client (`websocat ws://localhost:8080/full-stream`). 3. Connect an SSE client (`curl -N http://localhost:8080/sse`). 4. Wait for certificate entries to flow (30+ seconds). 5. Verify WS and SSE clients receive messages at normal rate. 6. Verify Delta table directory contains `.parquet` files. 7. Confirm no error logs related to broadcast channel contention. |
| **AC4.4** - Delta table creation failure on startup does not prevent the rest of the application from running | Automated test verifies the sink task exits cleanly, but cannot verify the full application (WS, SSE, CT log watchers, health endpoints) continues running since it cannot spin up `main()`. | 1. Configure `delta_sink.enabled: true` with `table_path` set to an unwritable path (e.g., `/root/no_access/delta`). 2. Start the application. 3. Verify startup log shows the delta sink error but the application continues. 4. Confirm health endpoint responds: `curl http://localhost:8080/health`. 5. Confirm WebSocket connections work normally. 6. Confirm CT log watchers are running (check logs for certificate entries). |

---

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 - Full JSON deserializes into DeltaCertRecord | `test_from_json_deserializes_all_fields`, `test_from_json_derives_seen_date`, `test_from_json_serializes_chain_to_json_strings`, `test_from_json_with_empty_chain`, `test_from_json_with_empty_as_der`, `test_from_json_with_empty_domains` | -- |
| AC1.2 - RecordBatch correct Arrow column types | `test_delta_schema_has_correct_field_count`, `test_delta_schema_field_types`, `test_records_to_batch_creates_correct_row_count`, `test_records_to_batch_creates_correct_column_count`, `test_records_to_batch_seen_timestamp`, `test_records_to_batch_is_ca_boolean_values` | -- |
| AC1.3 - as_der and chain in table | `test_records_to_batch_contains_as_der_string`, `test_records_to_batch_contains_chain_as_list`, `test_records_to_batch_with_empty_domains_and_chain` | -- |
| AC1.4 - Partitioned by seen_date | `test_ac1_4_records_partitioned_by_seen_date` | -- |
| AC1.5 - Bad messages skipped | `test_ac1_5_malformed_json_skipped`, `test_malformed_json_in_sink_task` | -- |
| AC2.1 - New table created with schema | `test_open_or_create_table_creates_new_table`, `test_open_or_create_table_preserves_seen_date_partition` | -- |
| AC2.2 - Existing table opened without conflict | `test_open_or_create_table_reopens_existing_table` | -- |
| AC3.1 - Flush at batch_size | `test_ac3_1_size_triggered_flush` | -- |
| AC3.2 - Flush at time interval | `test_ac3_2_time_triggered_flush` | -- |
| AC3.3 - Graceful shutdown flush | `test_ac3_3_graceful_shutdown_flush` | -- |
| AC3.4 - Overflow drops oldest; failure retains buffer | `test_buffer_overflow_drops_oldest_half`, `test_buffer_overflow_no_drop_at_threshold`, `test_buffer_overflow_no_drop_below_threshold`, `test_ac3_4_buffer_overflow_drop_oldest` | -- |
| AC4.1 - WS/SSE unaffected | `test_ac4_1_no_interference_with_ws_sse_consumers`, `test_ac4_integration_full_workflow` | Phase 1 Steps 3-9 |
| AC4.2 - Disabled by default | `test_ac4_2_disabled_by_default` | Phase 2 Steps 1-4 |
| AC4.3 - Lagged errors handled | `test_ac4_3_lagged_messages_handled` | -- |
| AC4.4 - Table creation failure non-fatal | `test_ac4_4_startup_failure_non_fatal` | Phase 3 Steps 1-7 |
