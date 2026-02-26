# ZeroBus Sink Test Requirements

This document maps each acceptance criterion from the ZeroBus Sink design to its verification method: automated test, integration test (feature-gated), or documented human verification.

## Constraints

The following constraints inform the verification strategy:

1. **Binary-only crate**: The project has no `lib.rs`, so `tests/` directory integration tests cannot import crate types. All automated tests live in inline `#[cfg(test)] mod tests` blocks within source files.
2. **No SDK mock**: The `databricks-zerobus-ingest-sdk` v0.1 provides no mock or test harness. Functions that call the SDK (`run_zerobus_sink()`, `run_zerobus_writer()`) cannot be unit tested in isolation.
3. **Global metrics recorder**: The `metrics` crate uses a global recorder that conflicts with parallel test execution. Metric counter tests are not feasible as unit tests, matching the existing `delta_sink` pattern (no metrics unit tests).
4. **Integration test gate**: Tests requiring a live Databricks connection are gated behind `#[cfg(all(test, feature = "integration"))]` inside source files and require environment variables for credentials.

## Verification Summary

| AC ID | Description | Verification | Type | File |
|-------|-------------|-------------|------|------|
| AC1.1 | Live sink streams records to Databricks | Integration test + Manual | Integration (feature-gated) + Manual | `src/zerobus_sink.rs` integration_tests module |
| AC1.2 | Graceful shutdown flushes and closes stream | Manual | Manual | N/A (documented procedure) |
| AC1.3 | Retryable errors trigger stream recovery | Manual | Manual | N/A (documented procedure) |
| AC1.4 | Non-retryable errors skip record and continue | Automated (partial) + Manual | Unit + Manual | `src/zerobus_sink.rs` tests module |
| AC1.5 | Stream creation failure exits sink without crash | Manual | Manual | N/A (documented procedure) |
| AC2.1 | All 20 fields round-trip via protobuf | Automated | Unit | `src/zerobus_sink.rs` tests module |
| AC2.2 | DescriptorProto constructable with 20 fields | Automated | Unit | `src/zerobus_sink.rs` tests module |
| AC3.1 | --backfill --sink zerobus --from N ingests records | Integration test + Manual | Integration (feature-gated) + Manual | `src/zerobus_sink.rs` integration_tests module |
| AC3.2 | --backfill without --sink uses delta writer | Automated | Unit | `src/cli.rs` tests module |
| AC3.3 | --sink zerobus without --from exits with error | Automated | Unit | `src/cli.rs` tests module, `src/backfill.rs` tests module |
| AC3.4 | --sink zerobus when disabled exits with error | Automated | Unit | `src/backfill.rs` tests module |
| AC3.5 | --sink invalidname exits with error | Automated | Unit | `src/cli.rs` tests module |
| AC4.1 | ZerobusSinkConfig loads with serde defaults | Automated | Unit | `src/config.rs` tests module |
| AC4.2 | Env vars override YAML values | Automated | Unit | `src/config.rs` tests module |
| AC4.3 | Validation rejects empty required fields | Automated | Unit | `src/config.rs` tests module |
| AC4.4 | Metrics increment correctly | Manual | Manual | N/A (documented procedure via /metrics endpoint) |

## Detailed Requirements

### zerobus-sink.AC1: Live sink streams records to Databricks

#### AC1.1: Records from the broadcast channel are ingested into the Databricks Delta table via ZeroBus SDK

- **Verification**: Integration test (feature-gated) + Manual
- **Type**: Integration with `#[cfg(all(test, feature = "integration"))]` + Manual human verification
- **Test file**: `src/zerobus_sink.rs` -- `integration_tests::test_zerobus_ingest_record`
- **What the integration test verifies**: Creates a ZeroBus SDK connection using test credentials from environment variables, converts a `DeltaCertRecord` to a protobuf `CertRecord`, encodes it, calls `ingest_record()` on a live stream, awaits the acknowledgement future, then flushes and closes the stream. Confirms the SDK accepts the record without error.
- **What the manual test verifies**: Start the server with ZeroBus enabled, observe CT records flowing through logs, query the Databricks table to confirm records appear. This validates the full live data path including broadcast channel subscription, JSON deserialization, protobuf conversion, and SDK ingestion.
- **Run integration**: `cargo test --features integration` (with `ZEROBUS_TEST_*` env vars set)
- **Manual procedure**: `docs/implementation-plans/2026-02-19-zerobus-sink/manual-test-procedure.md`, Test 1

#### AC1.2: Sink shuts down gracefully -- flushes pending records and closes stream on CancellationToken

- **Verification**: Manual
- **Type**: Manual human verification
- **Test file**: N/A
- **Why not automated**: `run_zerobus_sink()` requires a live SDK connection. The shutdown path calls `stream.flush()` and `stream.close()` which are SDK network operations. No mock is available.
- **What the manual test verifies**: Start the server with ZeroBus enabled, let records flow, send SIGINT. Verify log output contains "zerobus sink shutting down, flushing pending records" followed by "zerobus sink shutdown complete". Verify no error logs during shutdown sequence.
- **Manual procedure**: `docs/implementation-plans/2026-02-19-zerobus-sink/manual-test-procedure.md`, Test 1 step 5

#### AC1.3: Retryable SDK errors trigger stream recovery via recreate_stream() and resume ingestion

- **Verification**: Manual
- **Type**: Manual human verification
- **Test file**: N/A
- **Why not automated**: Retryable errors originate from the SDK's network layer. There is no way to inject a retryable error without a live connection or mock, and the SDK v0.1 provides neither.
- **What the manual test verifies**: During a long-running server session, if a transient network error occurs, verify log output contains "retryable ingest error, recreating stream" followed by "zerobus stream recovered". Verify the `certstream_zerobus_stream_recoveries` counter increments on the `/metrics` endpoint. Alternatively, simulate by temporarily disrupting network access to the ZeroBus endpoint.
- **Manual procedure**: `docs/implementation-plans/2026-02-19-zerobus-sink/manual-test-procedure.md` (observed during extended run)

#### AC1.4: Non-retryable SDK errors skip the record, log a warning, and continue processing

- **Verification**: Automated (partial) + Manual
- **Type**: Unit test (deserialization error path) + Manual (SDK error path)
- **Test file**: `src/zerobus_sink.rs` -- `tests::test_from_json_malformed`
- **What the automated test verifies**: Tests `DeltaCertRecord::from_json()` with malformed JSON input, confirming it returns `Err`. This validates the deserialization-error skip path used by the sink (the sink logs a debug message and calls `continue` on deserialization failure).
- **What the manual test verifies**: The SDK-level non-retryable error path (where `is_retryable()` returns false) can only be triggered by actual SDK errors during operation. Verify via logs that a "non-retryable ingest error, skipping record" warning appears and the sink continues processing subsequent records.
- **Manual procedure**: Observed during operation if SDK returns non-retryable errors

#### AC1.5: Stream creation failure at startup exits the sink task without crashing the server

- **Verification**: Manual
- **Type**: Manual human verification
- **Test file**: N/A
- **Why not automated**: Stream creation requires calling the live SDK. The non-fatal exit behavior is a `return` from `run_zerobus_sink()` after logging an error, which does not crash the server because the sink is spawned as an independent tokio task. Testing this would require a live SDK that fails on purpose (e.g., invalid credentials).
- **What the manual test verifies**: Configure ZeroBus with invalid credentials or endpoint. Start the server. Verify log output contains "failed to create ZeroBus stream, sink will not run" or "failed to create ZeroBus SDK, sink will not run". Verify the server continues running (WebSocket, SSE, other sinks still operational).
- **Manual procedure**: Set `CERTSTREAM_ZEROBUS_ENDPOINT` to an invalid URL, start server, verify it continues without the ZeroBus sink

---

### zerobus-sink.AC2: Full 20-column schema as Protobuf

#### AC2.1: All 20 fields of DeltaCertRecord are represented in the protobuf CertRecord message and round-trip correctly

- **Verification**: Automated
- **Type**: Unit test
- **Test file**: `src/zerobus_sink.rs` -- `tests` module
- **Tests**:
  1. `test_cert_record_from_delta_cert_all_fields`: Creates a `DeltaCertRecord` with known values for all 20 fields, converts to `CertRecord` via `from_delta_cert()`, and asserts every field matches using `assert_eq!`. Covers scalar types (`u64`, `f64`, `i64`, `bool`), `String` fields, and `Vec<String>` fields (`all_domains`, `chain`).
  2. `test_cert_record_round_trip`: Converts a `DeltaCertRecord` to `CertRecord`, encodes via `prost::Message::encode_to_vec()`, decodes back via `CertRecord::decode()`, and asserts all 20 fields match the original. Validates that protobuf serialization preserves all data.

#### AC2.2: DescriptorProto is constructable from the compiled proto and accepted by TableProperties

- **Verification**: Automated
- **Type**: Unit test
- **Test file**: `src/zerobus_sink.rs` -- `tests` module
- **Tests**:
  1. `test_descriptor_proto_name_and_fields`: Calls `cert_record_descriptor_proto()`, asserts the returned `DescriptorProto` has `name == Some("CertRecord")` and contains exactly 20 fields in its `field` vector. Validates that the compiled proto descriptor matches the expected schema.

---

### zerobus-sink.AC3: Backfill writes historical records via ZeroBus

#### AC3.1: --backfill --sink zerobus --from N ingests records from index N to state file ceiling into Databricks

- **Verification**: Integration test (feature-gated) + Manual
- **Type**: Integration with `#[cfg(all(test, feature = "integration"))]` + Manual human verification
- **Test file**: `src/zerobus_sink.rs` -- `integration_tests::test_zerobus_ingest_record`
- **What the integration test verifies**: The same integration test as AC1.1 validates that the protobuf encoding and SDK ingestion path works. The `run_zerobus_writer()` function uses the identical conversion and ingestion code path.
- **What the manual test verifies**: Run `cargo run -- --backfill --sink zerobus --from 0` with valid credentials and a state file. Verify records are ingested (check Databricks table). Verify exit code 0.
- **Manual procedure**: `docs/implementation-plans/2026-02-19-zerobus-sink/manual-test-procedure.md`, Test 2

#### AC3.2: --backfill without --sink uses the delta writer (backwards-compatible)

- **Verification**: Automated
- **Type**: Unit test
- **Test file**: `src/cli.rs` -- `tests` module
- **Tests**:
  1. `test_backfill_without_sink_flag`: Parses `["--backfill", "--from", "0"]` via `CliArgs::parse()`, asserts `backfill_sink` is `None`. When `backfill_sink` is `None`, the dispatch logic in `run_backfill()` defaults to the delta writer, preserving backwards compatibility.
- **What this verifies**: The CLI parsing layer correctly produces `None` for the sink field when `--sink` is not provided, which causes the backfill dispatch to use the existing delta writer path.

#### AC3.3: --sink zerobus without --from exits with a clear error (historical mode required)

- **Verification**: Automated
- **Type**: Unit test
- **Test file**: `src/cli.rs` -- `tests` module, `src/backfill.rs` -- `tests` module
- **Tests**:
  1. `test_sink_zerobus_parsed_correctly`: Parses `["--backfill", "--sink", "zerobus"]` via `CliArgs::parse()`, asserts `backfill_sink == Some("zerobus")` and `backfill_from == None`. Validates that the CLI layer parses correctly; the validation check that rejects this combination lives in `main.rs`.
  2. Validation logic test (in `src/backfill.rs` or tested via the validation function): Asserts that when `backfill_sink == Some("zerobus")` and `backfill_from == None`, the validation produces an error message containing "requires --from".
- **What this verifies**: The combination of `--sink zerobus` without `--from` is rejected with a clear error before any backfill work begins, enforcing the historical-mode-only constraint for remote tables.

#### AC3.4: --sink zerobus when zerobus_sink.enabled = false exits with a clear error

- **Verification**: Automated
- **Type**: Unit test
- **Test file**: `src/backfill.rs` -- `tests` module
- **Tests**:
  1. Validation logic test: Creates a `Config` with `zerobus_sink.enabled = false`, simulates the validation check for `backfill_sink == Some("zerobus")`, and asserts the error message contains "requires zerobus_sink.enabled = true".
- **What this verifies**: The validation in `main.rs` correctly rejects `--sink zerobus` when the ZeroBus sink is not enabled in configuration, preventing confusing SDK connection failures.

#### AC3.5: --sink invalidname exits with a clear error listing valid sink names

- **Verification**: Automated
- **Type**: Unit test
- **Test file**: `src/cli.rs` -- `tests` module
- **Tests**:
  1. `test_sink_unknown_name_parsed`: Parses `["--backfill", "--sink", "badname"]` via `CliArgs::parse()`, asserts `backfill_sink == Some("badname")`. The CLI layer accepts any string; validation in `main.rs` rejects unknown names.
  2. Validation logic test: Asserts that when `backfill_sink == Some("badname")`, the validation produces an error message containing "unknown sink" and lists "delta, zerobus" as valid options.
- **What this verifies**: Invalid sink names are caught with a clear, actionable error message before any backfill processing begins.

---

### zerobus-sink.AC4: Config and metrics follow conventions

#### AC4.1: ZerobusSinkConfig loads from YAML with serde defaults and is disabled by default

- **Verification**: Automated
- **Type**: Unit test
- **Test file**: `src/config.rs` -- `tests` module
- **Tests**:
  1. `test_zerobus_sink_config_defaults`: Asserts `ZerobusSinkConfig::default()` returns `enabled: false`, empty strings for `endpoint`, `unity_catalog_url`, `table_name`, `client_id`, `client_secret`, and `max_inflight_records: 10000`.
  2. `test_zerobus_sink_yaml_empty_block`: Tests that YAML deserialization of an empty `zerobus_sink:` block produces the same defaults as `ZerobusSinkConfig::default()`.
- **What this verifies**: The ZeroBus sink is disabled by default and all fields have safe initial values, matching the `DeltaSinkConfig` pattern.

#### AC4.2: Env vars (CERTSTREAM_ZEROBUS_*) override YAML values for all config fields

- **Verification**: Automated
- **Type**: Unit test
- **Test file**: `src/config.rs` -- `tests` module
- **Tests**:
  1. `test_zerobus_env_var_overrides`: Sets each `CERTSTREAM_ZEROBUS_*` environment variable (`ENABLED`, `ENDPOINT`, `UNITY_CATALOG_URL`, `TABLE_NAME`, `CLIENT_ID`, `CLIENT_SECRET`, `MAX_INFLIGHT_RECORDS`), loads config, and asserts each field was overridden. Cleans up env vars after test.
- **What this verifies**: All seven ZeroBus config fields can be overridden via environment variables, following the `CERTSTREAM_<SECTION>_<FIELD>` naming convention. Credentials can be provided securely via env vars rather than YAML.
- **Note**: Env var tests modify global state (`std::env::set_var`) and should not run in parallel with other config tests. Use `#[serial]` or ensure test isolation.

#### AC4.3: Validation rejects enabled config with empty endpoint, unity_catalog_url, table_name, client_id, or client_secret

- **Verification**: Automated
- **Type**: Unit test
- **Test file**: `src/config.rs` -- `tests` module
- **Tests**:
  1. `test_zerobus_validation_empty_endpoint`: Sets `enabled: true` with empty `endpoint`, calls `config.validate()`, asserts error contains field `"zerobus_sink.endpoint"`.
  2. `test_zerobus_validation_empty_uc_url`: Sets `enabled: true` with empty `unity_catalog_url`, asserts error for `"zerobus_sink.unity_catalog_url"`.
  3. `test_zerobus_validation_empty_table_name`: Sets `enabled: true` with empty `table_name`, asserts error for `"zerobus_sink.table_name"`.
  4. `test_zerobus_validation_bad_table_name_format`: Sets `enabled: true` with `table_name: "no_dots"`, asserts error about Unity Catalog format.
  5. `test_zerobus_validation_one_dot_table_name`: Sets `enabled: true` with `table_name: "schema.table"`, asserts error (needs exactly two dots).
  6. `test_zerobus_validation_valid_table_name`: Sets `enabled: true` with `table_name: "catalog.schema.table"`, asserts no table_name validation error.
  7. `test_zerobus_validation_empty_client_id`: Sets `enabled: true` with empty `client_id`, asserts error for `"zerobus_sink.client_id"`.
  8. `test_zerobus_validation_empty_client_secret`: Sets `enabled: true` with empty `client_secret`, asserts error for `"zerobus_sink.client_secret"`.
  9. `test_zerobus_validation_disabled_skips`: Sets `enabled: false` with all fields empty, calls `config.validate()`, asserts no ZeroBus-related validation errors (disabled sinks are not validated).
  10. `test_zerobus_validation_all_valid`: Sets `enabled: true` with all required fields populated (valid endpoint, uc_url, three-part table_name, client_id, client_secret), asserts `config.validate()` returns `Ok`.
- **What this verifies**: All five required fields are validated when the sink is enabled, table_name format is enforced, and disabled sinks skip validation entirely.

#### AC4.4: Metrics (certstream_zerobus_records_ingested, _ingest_errors, _stream_recoveries, _messages_lagged, _records_skipped) increment correctly during operation

- **Verification**: Manual
- **Type**: Manual human verification (code review + runtime observation)
- **Test file**: N/A
- **Why not automated**: The `metrics` crate uses a global recorder. Installing a test recorder conflicts with parallel test execution and with any other test that touches metrics. The existing `delta_sink` module has zero metrics unit tests for the same reason. This is an accepted project-wide constraint.
- **What the manual test verifies**:
  1. **Code review**: Verify each `metrics::counter!("certstream_zerobus_*").increment(N)` call is placed at the correct code point in `run_zerobus_sink()` (Phase 4 Task 1 specifies exact placement).
  2. **Runtime verification**: Start the server with ZeroBus enabled, let records flow, then query `GET /metrics`. Verify the following Prometheus counters appear:
     - `certstream_zerobus_records_ingested` -- increments as records are ingested
     - `certstream_zerobus_ingest_errors` -- zero during normal operation
     - `certstream_zerobus_stream_recoveries` -- zero during normal operation
     - `certstream_zerobus_messages_lagged` -- zero unless broadcast channel overflows
     - `certstream_zerobus_records_skipped` -- zero during normal operation
  3. **Error path verification**: Introduce a transient error (e.g., temporary network disruption) and verify `_ingest_errors` and `_stream_recoveries` increment.
- **Manual procedure**: `docs/implementation-plans/2026-02-19-zerobus-sink/manual-test-procedure.md`, Test 1 (with `/metrics` endpoint checks)

---

## Test Execution Summary

### Automated tests (run with `cargo test`)

| Test Location | AC Coverage | Count |
|---------------|-------------|-------|
| `src/zerobus_sink.rs` tests module | AC2.1, AC2.2, AC1.4 (partial) | 4 tests |
| `src/config.rs` tests module | AC4.1, AC4.2, AC4.3 | 12+ tests |
| `src/cli.rs` tests module | AC3.2, AC3.3, AC3.5 | 3+ tests |
| `src/backfill.rs` tests module | AC3.3, AC3.4 | 2+ tests |

**Total automated**: ~21+ unit tests covering 10 of 16 ACs (fully or partially)

### Integration tests (run with `cargo test --features integration`)

| Test Location | AC Coverage | Count |
|---------------|-------------|-------|
| `src/zerobus_sink.rs` integration_tests module | AC1.1, AC3.1 | 1 test |

**Total integration**: 1 test covering 2 ACs (requires live Databricks credentials)

### Manual verification (documented procedure)

| AC | What to verify |
|----|----------------|
| AC1.1 | Records visible in Databricks table after server run |
| AC1.2 | Graceful shutdown logs: flush + close on SIGINT |
| AC1.3 | Stream recovery logs after transient error |
| AC1.4 | Non-retryable error skip logs during operation |
| AC1.5 | Server continues running after sink startup failure |
| AC3.1 | Records visible in Databricks after backfill run |
| AC4.4 | Metrics visible on /metrics endpoint with correct values |

**Total manual**: 7 ACs require manual verification (all in AC1 and AC4.4)

### ACs with no automated coverage (manual only)

- **AC1.2** (graceful shutdown): Requires live SDK for flush/close
- **AC1.3** (stream recovery): Requires injecting transient SDK errors
- **AC1.5** (startup failure): Requires intentionally failing SDK connection
- **AC4.4** (metrics): Global recorder conflicts with parallel tests

These are accepted gaps given the SDK v0.1 constraints and the project's existing patterns.
