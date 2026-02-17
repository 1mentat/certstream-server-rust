# Delta Backfill Test Requirements

This document maps every acceptance criterion from the [delta backfill design](../../../docs/design-plans/2026-02-17-delta-backfill.md) to either an automated test or a documented human verification procedure.

**Convention:** All automated tests live in `#[cfg(test)]` modules within the source file that contains the code under test. The project does not use a separate `tests/` directory for unit tests. Integration-style tests that touch real Delta tables use `/tmp/` directories and clean up after themselves.

---

## Summary

| Category | Automated | Human Verification |
|----------|-----------|-------------------|
| AC1: CLI mode | 2 | 1 |
| AC2: Catch-up mode | 4 | 0 |
| AC3: Historical mode | 2 | 0 |
| AC4: Schema compatibility | 3 | 0 |
| AC5: Interruption handling | 3 | 1 |
| AC6: Cross-cutting behaviors | 1 | 2 |
| **Total** | **15** | **4** |

---

## AC1: CLI mode that fetches and writes directly to delta

| AC ID | Criterion | Test Type | Test Location | Description |
|-------|-----------|-----------|---------------|-------------|
| AC1.1 | `--backfill` runs, fetches, writes, exits code 0 | Unit | `src/backfill.rs` `#[cfg(test)]` | Test that `run_backfill()` returns exit code 0 when invoked in catch-up mode against an empty delta table (no work items). Validates the orchestrator flow completes cleanly without requiring live CT log servers. |
| AC1.2 | `--logs <filter>` limits to matching logs | Unit | `src/backfill.rs` `#[cfg(test)]` | Test the log filtering logic directly: given a list of `CtLog` entries with various descriptions and URLs, verify that applying a filter string (e.g., "google") retains only logs whose description or URL contains the substring (case-insensitive). |
| AC1.3 | `delta_sink.enabled=false` does not prevent backfill writing | Human | N/A | See [Human Verification: AC1.3](#human-ac13) below. |

---

## AC2: Catch-up mode (default)

| AC ID | Criterion | Test Type | Test Location | Description |
|-------|-----------|-----------|---------------|-------------|
| AC2.1 | No `--from` flag: lower bound = MIN(cert_index) per log | Integration | `src/backfill.rs` `#[cfg(test)]` | Create a delta table with records for a source at cert_index [100, 101, 102]. Call `detect_gaps()` with `backfill_from: None` and `tree_size=105`. Verify the generated work items start at 103 (after MAX), not at 0. Confirms lower bound is derived from the table's MIN, and no work items exist below 100. |
| AC2.2 | Internal gaps detected and backfilled | Integration | `src/backfill.rs` `#[cfg(test)]` | Create a delta table with cert_index values [10, 11, 12, 15, 16, 20] for one source. Call `detect_gaps()` with `tree_size=21`. Verify work items include gap (13, 14) and gap (17, 19). |
| AC2.3 | Frontier gap (MAX to tree_size) detected | Integration | `src/backfill.rs` `#[cfg(test)]` | Create a delta table with cert_index values [0, 1, 2] for one source. Call `detect_gaps()` with `tree_size=10`. Verify work items include the frontier gap (3, 9). |
| AC2.4 | Log in config but not in delta: skipped in catch-up mode | Integration | `src/backfill.rs` `#[cfg(test)]` | Create a delta table with records for "https://log-a.example.com" only. Call `detect_gaps()` in catch-up mode (`backfill_from: None`) with logs including both "log-a" (tree_size=100) and "log-b" (tree_size=200). Verify "log-b" produces zero work items. |

---

## AC3: Historical mode (`--from`)

| AC ID | Criterion | Test Type | Test Location | Description |
|-------|-----------|-----------|---------------|-------------|
| AC3.1 | `--from 0` backfills from 0 to tree_size | Integration | `src/backfill.rs` `#[cfg(test)]` | Create a delta table with cert_index values [50, 51, 52] for one source. Call `detect_gaps()` with `backfill_from: Some(0)` and `tree_size=55`. Verify work items include the pre-existing range (0, 49) and the frontier (53, 54). |
| AC3.2 | `--from N` overrides lower bound | Integration | `src/backfill.rs` `#[cfg(test)]` | Same delta table as AC3.1. Call `detect_gaps()` with `backfill_from: Some(40)` and `tree_size=55`. Verify work items start at 40 (range 40..49) rather than 50, plus the frontier gap (53, 54). |

---

## AC4: Works with existing delta table schema

| AC ID | Criterion | Test Type | Test Location | Description |
|-------|-----------|-----------|---------------|-------------|
| AC4.1 | Same 20-column schema and seen_date partitioning | Unit | `src/ct/fetch.rs` `#[cfg(test)]` and `src/delta_sink.rs` `#[cfg(test)]` | Two tests: (1) In `fetch.rs`, test that `build_certificate_message()` produces a `CertificateMessage` with correct `cert_index`, `source`, `cert_link`, `seen`, and `message_type` fields from a synthetic `ParsedEntry`. (2) In `delta_sink.rs`, test that `DeltaCertRecord::from_message()` produces field-identical output to `DeltaCertRecord::from_json()` for the same input message. Together these confirm the shared parsing path produces schema-compatible records. |
| AC4.2 | Records indistinguishable from live sink records | Integration | `src/backfill.rs` `#[cfg(test)]` | Create `DeltaCertRecord` instances via `from_message()`, send through an mpsc channel to the writer task, then read back from the delta table using DataFusion SQL. Verify the written records have all 20 columns populated and field values match the input. Compare against a record written by the live sink's `from_json()` path to confirm equivalence. |
| AC4.3 | Empty/nonexistent table created with correct schema | Integration | `src/backfill.rs` `#[cfg(test)]` | Run the writer task against a non-existent table path in `/tmp/`. Verify it creates the table. Read back table metadata (column count, column names, partition columns) to confirm the 20-column schema and `seen_date` partitioning are correct. |

---

## AC5: Handles interruption gracefully

| AC ID | Criterion | Test Type | Test Location | Description |
|-------|-----------|-----------|---------------|-------------|
| AC5.1 | Ctrl+C: graceful shutdown, flush buffered records | Unit | `src/backfill.rs` `#[cfg(test)]` | Create a fetcher task with a `CancellationToken`. Cancel the token after a short delay (e.g., 10ms). Verify the fetcher exits promptly without processing all remaining work items. Separately, verify the writer flushes its buffer when receiving the cancellation signal by checking that records sent before cancellation appear in the delta table. |
| AC5.2 | Re-running picks up where left off | Human | N/A | See [Human Verification: AC5.2](#human-ac52) below. |
| AC5.3 | Unreachable log does not block others | Unit | `src/backfill.rs` `#[cfg(test)]` | Spawn multiple fetcher tasks where one sends to a closed receiver (simulating immediate failure). Verify the remaining fetchers complete independently by checking their return values. The orchestrator collects all results, and the test asserts the failed fetcher is counted separately from successful ones. |
| AC5.4 | Exit code reflects success/failure | Unit | `src/backfill.rs` `#[cfg(test)]` | Test the exit code logic: verify `run_backfill()` returns 0 when all fetchers report success and returns non-zero when at least one fetcher reports failure. Tested with mock work items that immediately complete (success) vs immediately error (failure). |

---

## AC6: Cross-Cutting Behaviors

| AC ID | Criterion | Test Type | Test Location | Description |
|-------|-----------|-----------|---------------|-------------|
| AC6.1 | Progress logged periodically per log | Human | N/A | See [Human Verification: AC6.1](#human-ac61) below. |
| AC6.2 | Rate limit errors handled with exponential backoff | Human | N/A | See [Human Verification: AC6.2](#human-ac62) below. |
| AC6.3 | Completion summary reports records, logs, elapsed time | Unit | `src/backfill.rs` `#[cfg(test)]` | Verify the `WriterResult` struct returned by the writer task contains correct `total_records_written` and `write_errors` counts. The completion summary log line is constructed from these values and the fetcher results. The test validates the data inputs to the summary rather than the log output itself. |

---

## Human Verification Procedures

### <a id="human-ac13"></a>AC1.3: delta_sink.enabled=false does not prevent backfill writing

**Justification:** This criterion validates that the backfill code path reads `delta_sink.table_path` from config regardless of the `delta_sink.enabled` flag. Testing this end-to-end requires a real config file and a running backfill process. The behavior is a single conditional branch in `run_backfill()` that ignores the `enabled` field, which is more clearly verified by reading the code path than by constructing an elaborate integration test.

**Verification approach:**
1. Create a `config.yaml` with `delta_sink.enabled: false` and `delta_sink.table_path: /tmp/test-backfill-ac13`
2. Run: `cargo run -- --backfill --from 0 --logs "<known-small-log>"`
3. Verify the process writes records to `/tmp/test-backfill-ac13` (directory exists and contains `_delta_log/` and Parquet files)
4. Verify the process exits with code 0

### <a id="human-ac52"></a>AC5.2: Re-running after interruption picks up where left off

**Justification:** This criterion requires two sequential process executions with an interruption (Ctrl+C) in between. Automated testing would require spawning a child process, sending SIGINT, waiting for it to exit, then spawning another. While technically possible, this is fragile and slow. The core mechanism (gap detection re-queries the delta table) is already tested by the gap detection unit tests (AC2.1-AC2.3). What this human test adds is confirmation that the end-to-end flow works across process boundaries.

**Verification approach:**
1. Ensure a delta table exists at the configured path (or use `--from 0` to start fresh)
2. Run: `cargo run -- --backfill --from 0 --logs "<known-log>"`
3. Wait until progress logs show entries being written, then press Ctrl+C
4. Note the last logged progress count (e.g., "fetched 5000/100000 entries")
5. Re-run the same command: `cargo run -- --backfill --from 0 --logs "<known-log>"`
6. Verify gap detection reports fewer work items than the first run (the already-written ranges are excluded)
7. Verify the process completes and the delta table contains the full range

### <a id="human-ac61"></a>AC6.1: Progress logged periodically per log

**Justification:** Progress logging is a side effect (tracing log output). Capturing and asserting on log output in Rust tests requires a custom tracing subscriber, which adds test infrastructure complexity disproportionate to the risk. The logging code is a single `tracing::info!()` call in the fetcher loop.

**Verification approach:**
1. Run: `RUST_LOG=info cargo run -- --backfill --from 0 --logs "<known-log>"`
2. Observe stdout/stderr for periodic log lines matching the pattern: `[<source_url>] fetched X/Y entries (Z% complete)`
3. Verify logs appear at regular intervals (not just at start and end)
4. Verify the log includes the source URL, current count, total count, and percentage

### <a id="human-ac62"></a>AC6.2: Rate limit errors handled with exponential backoff

**Justification:** Testing exponential backoff against real CT log servers requires either a mock HTTP server (the project does not use HTTP mocking) or deliberately triggering rate limits on production servers (unreliable and potentially harmful). The backoff logic is a standard loop with sleep duration doubling, which is straightforward to verify by code review.

**Verification approach:**
1. Code review: confirm the fetcher task contains an exponential backoff loop that:
   - Starts at a base delay (e.g., 1 second)
   - Doubles the delay on each consecutive rate limit error
   - Caps at a maximum delay (e.g., 60 seconds)
   - Resets the delay on a successful request
   - Handles both HTTP 429 and HTTP 503 status codes via `FetchError::RateLimited`
2. Optionally, run backfill against a high-volume log with an aggressive batch size to observe backoff behavior in the logs: look for increasing delays in log timestamps between retry attempts

---

## Test Matrix by Implementation Phase

This section maps each implementation phase to the tests it is responsible for delivering.

### Phase 1: CLI and Entry Point

No automated tests. Phase 1 is scaffolding only.

**Manual verification:** `cargo run -- --backfill` exits cleanly, `cargo run -- --help` shows backfill options.

### Phase 2: Extract Shared Fetch Logic

| Test | File | Verifies |
|------|------|----------|
| `build_certificate_message` produces correct CertificateMessage fields | `src/ct/fetch.rs` | AC4.1 |
| `from_message()` produces same DeltaCertRecord as `from_json()` | `src/delta_sink.rs` | AC4.1 |

### Phase 3: Delta Table Gap Detection

| Test | File | Verifies |
|------|------|----------|
| Catch-up mode lower bound is MIN(cert_index) | `src/backfill.rs` | AC2.1 |
| Internal gaps detected | `src/backfill.rs` | AC2.2 |
| Frontier gap detected | `src/backfill.rs` | AC2.3 |
| Log not in delta skipped in catch-up | `src/backfill.rs` | AC2.4 |
| `--from 0` includes pre-existing range | `src/backfill.rs` | AC3.1 |
| `--from N` overrides lower bound | `src/backfill.rs` | AC3.2 |
| Empty/non-existent table in catch-up mode yields no work items | `src/backfill.rs` | AC2.4 (edge) |
| Empty/non-existent table with `--from 0` yields full range | `src/backfill.rs` | AC3.1 (edge) |

### Phase 4: Concurrent Fetcher Tasks

| Test | File | Verifies |
|------|------|----------|
| Orchestrator returns 0 with no work items | `src/backfill.rs` | AC1.1 |
| Log filter retains only matching logs | `src/backfill.rs` | AC1.2 |
| CancellationToken stops fetcher promptly | `src/backfill.rs` | AC5.1 |

### Phase 5: Writer Task and End-to-End Integration

| Test | File | Verifies |
|------|------|----------|
| Written records match live sink records | `src/backfill.rs` | AC4.2 |
| Non-existent table created with correct schema | `src/backfill.rs` | AC4.3 |
| Failed fetcher does not block others | `src/backfill.rs` | AC5.3 |
| Exit code 0 on success, non-zero on failure | `src/backfill.rs` | AC5.4 |
| WriterResult contains correct counts | `src/backfill.rs` | AC6.3 |
