# ZeroBus Sink Human Test Plan

## Prerequisites

- Rust toolchain installed (edition 2024)
- All automated tests passing: `cargo test` (350 tests, 0 failures)
- For live Databricks tests: OAuth M2M service principal credentials, ZeroBus gRPC endpoint (`https://<workspace-id>.zerobus.<region>.cloud.databricks.com`), workspace base URL (`https://<instance>.cloud.databricks.com`), and a managed Delta table in `catalog.schema.table` format (catalog must have explicit managed storage location, not default workspace storage)
- For integration tests: `ZEROBUS_TEST_ENDPOINT` (gRPC endpoint), `ZEROBUS_TEST_UC_URL` (workspace base URL), `ZEROBUS_TEST_TABLE_NAME`, `ZEROBUS_TEST_CLIENT_ID`, `ZEROBUS_TEST_CLIENT_SECRET`

## Phase 1: Configuration Validation (AC4.1, AC4.2, AC4.3)

These are covered by automated tests but benefit from a quick manual sanity check via `--validate-config`.

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Create a `config.yaml` with an empty `zerobus_sink:` block (no fields). Run `cargo run -- --validate-config` | Output shows ZeroBus sink configuration section with `enabled: false` and all defaults. No validation errors for ZeroBus fields. |
| 1.2 | Set `CERTSTREAM_ZEROBUS_ENABLED=true` and `CERTSTREAM_ZEROBUS_ENDPOINT=https://test.example.com`. Run `cargo run -- --validate-config` | Output shows `enabled: true` and `endpoint: https://test.example.com`. Validation errors appear for missing `unity_catalog_url`, `table_name`, `client_id`, `client_secret`. |
| 1.3 | Set all 7 `CERTSTREAM_ZEROBUS_*` env vars to valid values (endpoint, uc_url, `catalog.schema.table`, client_id, client_secret, max_inflight_records=5000). Run `cargo run -- --validate-config` | No ZeroBus validation errors. All overridden values shown correctly. |

## Phase 2: CLI Error Cases (AC3.3, AC3.4, AC3.5)

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Run `cargo run -- --backfill --sink zerobus` (no `--from`, with `CERTSTREAM_ZEROBUS_ENABLED=true` in env) | Process exits with code 1. Stderr contains: "Error: --sink zerobus requires --from" |
| 2.2 | Run `cargo run -- --backfill --sink zerobus --from 0` with `CERTSTREAM_ZEROBUS_ENABLED=false` (or unset) | Process exits with code 1. Stderr contains: "Error: --sink zerobus requires zerobus_sink.enabled = true" |
| 2.3 | Run `cargo run -- --backfill --sink badname --from 0` | Process exits with code 1. Stderr contains: "Error: unknown sink 'badname'. Valid sinks: delta, zerobus" |
| 2.4 | Run `cargo run -- --backfill --from 0` (no `--sink` flag) | Process does not error on sink validation. Proceeds to backfill using delta writer (may fail for other reasons like missing state file, which is expected). |

## Phase 3: Live Sink Startup and Shutdown (AC1.1, AC1.2, AC1.5)

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Set all `CERTSTREAM_ZEROBUS_*` env vars with valid Databricks credentials. Run `cargo run` | Log output contains "zerobus sink started, streaming to Databricks" with the configured table name. Server starts normally. WebSocket endpoint at `ws://localhost:8080/` is accessible. |
| 3.2 | Wait 30-60 seconds for CT records to flow | Log output shows certificate processing. No ZeroBus error messages. |
| 3.3 | Query the Databricks table (via SQL or UI): `SELECT COUNT(*) FROM catalog.schema.table` | Row count is greater than 0. Records contain valid certificate data (cert_index, source_url, seen, all_domains populated). |
| 3.4 | Send SIGINT (Ctrl+C) to the running server | Log output contains "zerobus sink shutting down, flushing pending records" followed by "zerobus sink shutdown complete" with a record count. No error messages during shutdown. |
| 3.5 | Set `CERTSTREAM_ZEROBUS_ENDPOINT` to `https://invalid.endpoint.example.com` (intentionally bad). Start server with `cargo run` | Log output contains "failed to create ZeroBus SDK, sink will not run" or "failed to create ZeroBus stream, sink will not run". Server continues running. WebSocket and SSE endpoints remain functional. Other sinks (if enabled) continue operating. |

## Phase 4: Stream Recovery (AC1.3, AC1.4)

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Start the server with valid ZeroBus config. After records begin flowing, temporarily disrupt network access to the ZeroBus endpoint (e.g., firewall rule, DNS block, or container network disconnect) | Log output contains "retryable ingest error, recreating stream". After network is restored, log output contains "zerobus stream recovered". `GET /metrics` shows `certstream_zerobus_stream_recoveries` counter > 0. |
| 4.2 | During normal operation, observe any non-retryable SDK errors | If they occur, log output contains "non-retryable ingest error, skipping record". The sink continues processing subsequent records without stopping. `certstream_zerobus_records_skipped` counter increments. |

## Phase 5: Backfill via ZeroBus (AC3.1)

| Step | Action | Expected |
|------|--------|----------|
| 5.1 | Ensure `certstream_state.json` exists with at least one log entry (run the live server briefly if needed) | State file contains JSON with log URLs and `current_index` values. |
| 5.2 | Run `cargo run -- --backfill --sink zerobus --from 0` with valid ZeroBus credentials | Backfill begins. Log output shows fetcher tasks starting and records being ingested. Process eventually completes. |
| 5.3 | Check the exit code: `echo $?` | Exit code is 0 (success). |
| 5.4 | Query Databricks table for records with low cert_index values: `SELECT * FROM catalog.schema.table WHERE cert_index < 100 LIMIT 10` | Records exist from the backfill range. Fields are populated correctly. |

## Phase 6: Metrics Verification (AC4.4)

| Step | Action | Expected |
|------|--------|----------|
| 6.1 | Start the server with ZeroBus enabled. After records begin flowing, request `GET http://localhost:8080/metrics` | Response body contains Prometheus-format metrics. |
| 6.2 | Search for `certstream_zerobus_records_ingested` in the metrics output | Counter exists and has a value greater than 0 (incrementing as records are ingested). |
| 6.3 | Search for `certstream_zerobus_ingest_errors` | Counter exists. Value is 0 during normal operation. |
| 6.4 | Search for `certstream_zerobus_stream_recoveries` | Counter exists. Value is 0 during normal operation. |
| 6.5 | Search for `certstream_zerobus_messages_lagged` | Counter exists. Value is 0 unless broadcast channel overflows. |
| 6.6 | Search for `certstream_zerobus_records_skipped` | Counter exists. Value is 0 during normal operation. |

## End-to-End: Full Lifecycle Test

**Purpose:** Validate the complete data path from CT log fetch through ZeroBus ingestion to Databricks table, including both live and backfill modes.

1. Start with a clean state (no `certstream_state.json`, empty or new Databricks table).
2. Set all `CERTSTREAM_ZEROBUS_*` env vars with valid credentials.
3. Run `cargo run` and let the live server operate for 2-3 minutes.
4. Verify records appear in Databricks: `SELECT COUNT(*), MIN(cert_index), MAX(cert_index) FROM catalog.schema.table`.
5. Send SIGINT. Verify clean shutdown logs.
6. Note the `current_index` values from `certstream_state.json`.
7. Run `cargo run -- --backfill --sink zerobus --from 0`. This should backfill from index 0 up to the ceiling from the state file.
8. Verify exit code 0.
9. Query Databricks: `SELECT COUNT(*) FROM catalog.schema.table`. Row count should be significantly higher than after step 4.
10. Verify no duplicate records: `SELECT cert_index, source_url, COUNT(*) as cnt FROM catalog.schema.table GROUP BY cert_index, source_url HAVING cnt > 1`. Should return 0 rows.

## End-to-End: CLI Validation Guards

**Purpose:** Confirm that all invalid `--sink` combinations are caught before any work begins.

1. Run `cargo run -- --backfill --sink zerobus` (missing `--from`). Verify error exits immediately (< 1 second) with clear message.
2. Run `CERTSTREAM_ZEROBUS_ENABLED=false cargo run -- --backfill --sink zerobus --from 0`. Verify error exits immediately.
3. Run `cargo run -- --backfill --sink invalid --from 0`. Verify error lists valid sink names.
4. Run `cargo run -- --backfill --from 0` (no `--sink`). Verify it proceeds to delta writer path (no sink validation error).

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 | Integration: `test_zerobus_ingest_record` | Phase 3: 3.1-3.3 |
| AC1.2 | -- | Phase 3: 3.4 |
| AC1.3 | -- | Phase 4: 4.1 |
| AC1.4 | `test_delta_cert_record_from_json_invalid` (partial) | Phase 4: 4.2 |
| AC1.5 | -- | Phase 3: 3.5 |
| AC2.1 | `test_from_delta_cert_all_fields`, `test_cert_record_round_trip` | -- |
| AC2.2 | `test_cert_record_descriptor_proto` | -- |
| AC3.1 | `test_zerobus_work_items_generated_from_range`, `test_zerobus_gap_detection_skipped` | Phase 5: 5.1-5.4 |
| AC3.2 | `test_ac3_2_backfill_sink_none_when_not_provided`, `test_ac3_2_backwards_compatible_delta_default` | E2E CLI Guards: step 4 |
| AC3.3 | `test_ac3_3_zerobus_without_from_is_error`, `test_ac3_3_zerobus_with_from_is_valid` | Phase 2: 2.1 |
| AC3.4 | `test_ac3_4_zerobus_disabled_is_error`, `test_ac3_4_delta_sink_always_valid` | Phase 2: 2.2 |
| AC3.5 | `test_ac3_5_parse_sink_invalid`, `test_ac3_5_unknown_sink_is_error` | Phase 2: 2.3 |
| AC4.1 | `test_zerobus_sink_config_defaults`, `test_zerobus_sink_config_yaml_empty` | Phase 1: 1.1 |
| AC4.2 | `test_zerobus_sink_config_env_var_*` (7 tests) | Phase 1: 1.2-1.3 |
| AC4.3 | `test_zerobus_sink_validation_*` (10 tests) | Phase 1: 1.2 |
| AC4.4 | -- | Phase 6: 6.1-6.6 |
