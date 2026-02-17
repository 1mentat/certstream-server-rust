# Delta Sink - Test Requirements

**Design:** `docs/design-plans/2026-02-16-delta-sink.md`
**Date:** 2026-02-16
**Total Acceptance Criteria:** 15 (across 4 groups)
**Test Location:** All tests are inline `#[cfg(test)] mod tests` in `src/delta_sink.rs` (project convention)

---

## Coverage Summary

| Group | Criteria | Automated | Human Verification |
|-------|----------|-----------|-------------------|
| AC1: Correct schema | 5 | 5 | 0 |
| AC2: Table management | 2 | 2 | 0 |
| AC3: Batching and flush | 4 | 4 | 0 |
| AC4: No disruption | 4 | 2 | 2 |
| **Total** | **15** | **13** | **2** |

---

## AC1: CT entries are written to Delta table with correct schema

### delta-sink.AC1.1

| Field | Value |
|-------|-------|
| **AC Text** | `full` JSON bytes from broadcast channel deserialize into `DeltaCertRecord` with all fields populated |
| **Test Type** | Unit |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 2, Task 3 |
| **Description** | Constructs a sample `CertificateMessage` (reusing the `make_test_message()` pattern from `src/models/certificate.rs`), serializes it to JSON bytes, deserializes via `DeltaCertRecord::from_json()`, and asserts every field is populated correctly: `cert_index`, `update_type`, `seen`, `seen_date`, `source_name`, `source_url`, `cert_link`, `serial_number`, `fingerprint`, `sha256`, `sha1`, `not_before`, `not_after`, `is_ca`, `signature_algorithm`, `subject_aggregated`, `issuer_aggregated`, `all_domains`, `as_der`. Also covers edge cases: `None` values for `as_der` (maps to empty string), `None` for `chain` (maps to empty vec), empty `all_domains`. |

---

### delta-sink.AC1.2

| Field | Value |
|-------|-------|
| **AC Text** | Arrow RecordBatch contains correct column types (Timestamp for `seen`, List(Utf8) for `all_domains`, Boolean for `is_ca`, etc.) |
| **Test Type** | Unit |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 2, Task 3 |
| **Description** | Creates a batch of `DeltaCertRecord` values, calls `records_to_batch()`, and asserts: the batch has the correct number of rows, the schema field count matches (20 columns), and specific column types are verified -- `seen` is `Timestamp(Microsecond, UTC)`, `all_domains` is `List(Utf8)`, `chain` is `List(Utf8)`, `is_ca` is `Boolean`, `cert_index` is `UInt64`, `not_before` is `Int64`, `not_after` is `Int64`, and string columns are `Utf8`. |

---

### delta-sink.AC1.3

| Field | Value |
|-------|-------|
| **AC Text** | `as_der` and `chain` fields are present in the Delta table (base64 string and JSON-serialized list respectively) |
| **Test Type** | Unit |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 2, Task 3 |
| **Description** | Creates a `DeltaCertRecord` with `as_der` set to a known base64 string (e.g., `"MIIB..."`) and `chain` populated with JSON-serialized chain certificate strings. Converts to `RecordBatch` and reads back from the `StringArray` (for `as_der`) and `ListArray` (for `chain`) columns, asserting the values round-trip correctly. |

---

### delta-sink.AC1.4

| Field | Value |
|-------|-------|
| **AC Text** | Records are partitioned by `seen_date` (YYYY-MM-DD derived from `seen` timestamp) |
| **Test Type** | Integration |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 4, Task 3 |
| **Description** | Writes records with different `seen` timestamps (spanning multiple dates) through the sink to a Delta table in a temp directory. After flush, inspects the filesystem for `seen_date=YYYY-MM-DD/` partition directories and confirms records are physically split by date. Uses `#[tokio::test]` with manual temp directory cleanup. |

---

### delta-sink.AC1.5

| Field | Value |
|-------|-------|
| **AC Text** | Messages that fail deserialization are skipped and logged, not causing the sink to crash or stall |
| **Test Type** | Integration |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 4, Task 3 |
| **Description** | Sends a `PreSerializedMessage` with malformed/invalid JSON in the `full` field through the broadcast channel to the sink task. Then sends valid messages after the malformed one. Asserts the sink task does not crash or panic, and that subsequent valid messages are successfully written to the Delta table. |

---

## AC2: Delta table is created and managed correctly

### delta-sink.AC2.1

| Field | Value |
|-------|-------|
| **AC Text** | If table doesn't exist at `table_path`, it is created with the full Arrow schema on first flush |
| **Test Type** | Integration |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 3, Task 2 |
| **Description** | Calls `open_or_create_table` with a temp directory path that does not contain an existing table. Asserts: the call succeeds, the returned table's schema matches the expected Arrow schema (correct number of fields, field names, field data types), and `seen_date` is configured as a partition column. Uses `#[tokio::test]` with manual temp directory creation/cleanup. |

---

### delta-sink.AC2.2

| Field | Value |
|-------|-------|
| **AC Text** | If table already exists, it is opened and appended to without schema conflict |
| **Test Type** | Integration |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 3, Task 2 |
| **Description** | First creates a table via `open_or_create_table` (from AC2.1 setup), then calls `open_or_create_table` again with the same path. Asserts: table opens successfully without error, schema still matches, and table version is still 0 (no new commits from just opening). Verifies no schema conflicts arise from reopening. |

---

## AC3: Batching and flush behavior

### delta-sink.AC3.1

| Field | Value |
|-------|-------|
| **AC Text** | Buffer flushes when it reaches `batch_size` records |
| **Test Type** | Integration |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 4, Task 3 |
| **Description** | Configures the sink with a small `batch_size` (e.g., 5) and a long `flush_interval_secs` (e.g., 300, so timer does not fire). Sends exactly `batch_size` messages through the broadcast channel. After a short delay, reads back the Delta table and asserts that all records have been flushed -- verifying the size-trigger caused the flush, not the timer. |

---

### delta-sink.AC3.2

| Field | Value |
|-------|-------|
| **AC Text** | Buffer flushes when `flush_interval_secs` elapses, even if buffer is below `batch_size` |
| **Test Type** | Integration |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 4, Task 3 |
| **Description** | Configures the sink with a large `batch_size` (e.g., 10000) and a short `flush_interval_secs` (e.g., 1 second). Sends fewer than `batch_size` messages. Waits for the flush interval to elapse plus a small margin. Reads back the Delta table and asserts that all records were flushed by the timer despite being below the size threshold. |

---

### delta-sink.AC3.3

| Field | Value |
|-------|-------|
| **AC Text** | Graceful shutdown flushes remaining buffered records before exit |
| **Test Type** | Integration |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 4, Task 3 |
| **Description** | Configures the sink with a large `batch_size` and long `flush_interval_secs` (so neither trigger fires naturally). Sends a few messages. Cancels the `CancellationToken` to trigger graceful shutdown. Waits for the sink task to complete. Reads back the Delta table and asserts that all buffered records were flushed before exit. |

---

### delta-sink.AC3.4

| Field | Value |
|-------|-------|
| **AC Text** | Delta write failure retains buffer and retries on next flush cycle; buffer exceeding 2x `batch_size` drops oldest half |
| **Test Type** | Unit + Integration |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 4, Task 3 |
| **Description** | Two-part test. **(a) Unit test for buffer overflow:** Extracts the overflow check into a testable helper function `check_buffer_overflow(buffer, batch_size)`. Tests that a buffer exceeding 2x `batch_size` has its oldest half dropped. Tests that a buffer at exactly 2x `batch_size` or below is not modified. **(b) Integration test for retry:** Verifies that on Delta write failure the buffer contents are retained (not cleared), allowing the next flush cycle to attempt the write again. This may use a deliberately invalid table state or a path that becomes unwritable. |

---

## AC4: No disruption to existing streaming

### delta-sink.AC4.1

| Field | Value |
|-------|-------|
| **AC Text** | WebSocket and SSE streams continue operating normally when delta sink is enabled |
| **Test Type** | **Human Verification** |
| **Test File** | N/A |
| **Phase / Task** | Phase 5, Task 3 (partial automated verification) |
| **Description** | A partial automated test creates a full broadcast channel, spawns both a mock WS-like consumer and the delta sink, sends messages, and asserts both consumers receive messages without interference. |

**Human Verification Required**

| Field | Value |
|-------|-------|
| **Justification** | Full end-to-end verification requires a running application with real WebSocket/SSE clients connected while the delta sink is active. The automated test uses mock consumers, not actual WS/SSE protocol handlers with real network connections. |
| **Verification Approach** | 1. Start the application with `delta_sink.enabled: true` and a valid `table_path`. 2. Connect a WebSocket client (e.g., `websocat ws://localhost:8080/`) and an SSE client (e.g., `curl -N http://localhost:8080/events`). 3. Wait for certificate entries to flow. 4. Verify WS and SSE clients receive messages at normal rate. 5. Verify Delta table directory is being written to (check for `.parquet` files). 6. Confirm no error logs related to broadcast channel contention. |

---

### delta-sink.AC4.2

| Field | Value |
|-------|-------|
| **AC Text** | Delta sink disabled by default (`enabled: false`); application behavior unchanged when disabled |
| **Test Type** | Unit |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 5, Task 3 |
| **Description** | Asserts that `DeltaSinkConfig::default().enabled` is `false`. This is a config-level unit test verifying the default value. The `main.rs` gating logic (`if config.delta_sink.enabled { ... }`) ensures `run_delta_sink` is never called when disabled -- this is verified by code inspection and the fact that no delta table directory is created when the feature is off. |

---

### delta-sink.AC4.3

| Field | Value |
|-------|-------|
| **AC Text** | Broadcast channel `Lagged` errors in the sink are logged and metriced, not propagated |
| **Test Type** | Integration |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 5, Task 3 |
| **Description** | Creates a broadcast channel with a small buffer capacity. Sends enough messages to overflow the buffer and cause a `RecvError::Lagged` on the sink's receiver. Asserts the sink task continues processing after the lag (does not crash or exit), and that subsequent valid messages are still written to the Delta table. Metrics verification (counter increment) is implicit through the metrics crate instrumentation. |

---

### delta-sink.AC4.4

| Field | Value |
|-------|-------|
| **AC Text** | Delta table creation failure on startup does not prevent the rest of the application from running |
| **Test Type** | Integration + **Human Verification** |
| **Test File** | `src/delta_sink.rs` (`#[cfg(test)] mod tests`) |
| **Phase / Task** | Phase 5, Task 3 |
| **Description** | Automated test: calls `run_delta_sink` with an invalid/unwritable `table_path` (e.g., `/nonexistent/deeply/nested/path/delta`). Asserts the task returns without panicking and does not affect the calling code. |

**Human Verification Required**

| Field | Value |
|-------|-------|
| **Justification** | The automated test verifies the sink task itself exits cleanly on table creation failure. However, verifying that "the rest of the application" (WS, SSE, CT log watchers, health endpoints) continues running normally requires a full running application -- the integration test cannot spin up the entire `main()` function. |
| **Verification Approach** | 1. Configure `delta_sink.enabled: true` with `table_path` set to an unwritable path (e.g., `/root/no_access/delta`). 2. Start the application. 3. Verify startup log shows the delta sink error but the application continues. 4. Confirm health endpoint responds (`curl http://localhost:8080/health`). 5. Confirm WebSocket and SSE connections work normally. 6. Confirm CT log watchers are running (check logs for certificate entries). |

---

## Test Execution Matrix

### Automated Tests by Phase

| Phase | Task | Test Count | AC Coverage | Test Runner Command |
|-------|------|------------|-------------|---------------------|
| Phase 2 | Task 3 | 3+ | AC1.1, AC1.2, AC1.3 | `cargo test delta_sink` |
| Phase 3 | Task 2 | 2+ | AC2.1, AC2.2 | `cargo test delta_sink` |
| Phase 4 | Task 3 | 6+ | AC1.4, AC1.5, AC3.1, AC3.2, AC3.3, AC3.4 | `cargo test delta_sink` |
| Phase 5 | Task 3 | 4+ | AC4.1 (partial), AC4.2, AC4.3, AC4.4 (partial) | `cargo test delta_sink` |

### Manual Verification Checklist

| AC | Verification | Tester | Status |
|----|-------------|--------|--------|
| AC4.1 | Full WS/SSE with delta sink enabled | [ ] | [ ] Pass / [ ] Fail |
| AC4.4 | App runs normally with invalid table_path | [ ] | [ ] Pass / [ ] Fail |

---

## Test Dependencies and Environment

| Requirement | Details |
|-------------|---------|
| **Runtime** | `#[tokio::test]` for all async tests |
| **Temp Directories** | Integration tests create temp directories under `/tmp/delta_sink_test_*`; manual cleanup with `std::fs::remove_dir_all` (no `tempfile` crate) |
| **Test Data** | Sample `CertificateMessage` constructed using `make_test_message()` pattern from `src/models/certificate.rs:272-308` |
| **Broadcast Channel** | Tests create their own `tokio::sync::broadcast::channel` instances |
| **Shutdown Signal** | Tests use `tokio_util::sync::CancellationToken` for graceful shutdown testing |
| **Filesystem** | Integration tests require write access to `/tmp` for Delta table creation |

---

## Traceability Matrix

| AC ID | AC Text (abbreviated) | Type | Phase | Automated | Human |
|-------|----------------------|------|-------|-----------|-------|
| AC1.1 | Full JSON deserializes into DeltaCertRecord | Unit | P2/T3 | Yes | -- |
| AC1.2 | RecordBatch has correct Arrow column types | Unit | P2/T3 | Yes | -- |
| AC1.3 | as_der and chain present in table | Unit | P2/T3 | Yes | -- |
| AC1.4 | Partitioned by seen_date | Integration | P4/T3 | Yes | -- |
| AC1.5 | Bad messages skipped, sink continues | Integration | P4/T3 | Yes | -- |
| AC2.1 | New table created with schema | Integration | P3/T2 | Yes | -- |
| AC2.2 | Existing table opened without conflict | Integration | P3/T2 | Yes | -- |
| AC3.1 | Flush at batch_size threshold | Integration | P4/T3 | Yes | -- |
| AC3.2 | Flush at time threshold | Integration | P4/T3 | Yes | -- |
| AC3.3 | Graceful shutdown flushes buffer | Integration | P4/T3 | Yes | -- |
| AC3.4 | Write failure retains buffer; overflow drops oldest | Unit + Integration | P4/T3 | Yes | -- |
| AC4.1 | WS/SSE unaffected by delta sink | Integration | P5/T3 | Partial | Yes |
| AC4.2 | Disabled by default | Unit | P5/T3 | Yes | -- |
| AC4.3 | Lagged errors logged, not propagated | Integration | P5/T3 | Yes | -- |
| AC4.4 | Table creation failure non-fatal | Integration | P5/T3 | Partial | Yes |
