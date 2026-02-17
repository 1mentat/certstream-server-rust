# Delta Backfill - Human Test Plan

## Prerequisites

- Rust toolchain installed (edition 2024)
- A `config.yaml` file present at the project root (copy from `config.example.yaml` if needed)
- The project builds successfully: `cargo build`
- All automated tests pass: `cargo test` (243 tests, 0 failures)
- Access to a terminal with stdout/stderr visible
- At least one known CT log URL that is accessible (e.g., a Google CT log such as `https://ct.googleapis.com/logs/us1/argon2025h1/`)

## Phase 1: CLI Scaffolding Verification

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Run `cargo run -- --help` | Output includes `--backfill` flag described as activating backfill mode, `--from <INDEX>` for historical start index, and `--logs <FILTER>` for log filtering. |
| 1.2 | Run `cargo run -- --backfill` (with no delta table and default config) | Process starts, logs "backfill mode starting", attempts to fetch CT log list. If no delta table exists and no `--from` flag, the process should log "no gaps detected, nothing to backfill" and exit with code 0. Verify exit code: `echo $?` outputs `0`. |

## Phase 2: AC1.3 -- delta_sink.enabled=false Does Not Prevent Backfill Writing

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Edit `config.yaml` to set `delta_sink.enabled: false` and `delta_sink.table_path: /tmp/test-backfill-ac13` | Config file saved. |
| 2.2 | Remove any prior test table: `rm -rf /tmp/test-backfill-ac13` | Clean slate. |
| 2.3 | Run: `cargo run -- --backfill --from 0 --logs "<known-small-log-substring>"` (use a small, fast log with few entries, or interrupt after a few seconds with Ctrl+C) | Process starts, logs "backfill mode starting" and "backfill_from parameter: 0". Fetcher tasks spawn and begin fetching entries. |
| 2.4 | After the process completes (or after Ctrl+C), verify: `ls /tmp/test-backfill-ac13/` | Directory contains `_delta_log/` subdirectory and at least one `seen_date=YYYY-MM-DD/` partition directory with `.parquet` files inside. |
| 2.5 | Verify exit code: `echo $?` | Outputs `0` if the process completed normally, or `0` if Ctrl+C was handled gracefully and records were flushed. |

## Phase 3: AC5.2 -- Re-running After Interruption Picks Up Where Left Off

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Set `delta_sink.table_path: /tmp/test-backfill-ac52` in `config.yaml`. Remove prior data: `rm -rf /tmp/test-backfill-ac52` | Clean starting state. |
| 3.2 | Run: `RUST_LOG=info cargo run -- --backfill --from 0 --logs "<known-log-substring>"` | Process starts and begins fetching. Progress logs appear showing batch progress and record counts. |
| 3.3 | Wait until progress logs show entries being written (progress_percent > 0%), then press Ctrl+C | Process logs shutdown message with `total_records_written` count greater than 0, then exits. |
| 3.4 | Note the `total_records_written` value from the shutdown summary. | Record this number (e.g., 5000). |
| 3.5 | Re-run the same command: `RUST_LOG=info cargo run -- --backfill --from 0 --logs "<known-log-substring>"` | Gap detection should report fewer total work items than the first run. Already-written ranges are excluded. |
| 3.6 | Let the second run complete or interrupt again. Compare work items count. | The second run's `total_work_items` should be less than the first run. |

## Phase 4: AC6.1 -- Progress Logged Periodically Per Log

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Run: `RUST_LOG=info cargo run -- --backfill --from 0 --logs "<known-log-substring>"` | Process starts fetching entries. |
| 4.2 | Observe stdout/stderr for periodic log lines | Lines matching: `source_url=<URL> fetched=<N> batch_idx=<M> total_batches=<T> progress_percent=<P> "fetcher progress"` should appear. |
| 4.3 | Verify logs appear at regular intervals | Multiple progress log lines should appear every 10 batches or at 10% milestones. |
| 4.4 | Verify completion log appears | A final line: `source_url=<URL> total_records=<N> "fetcher complete"` should appear for each log source. |

## Phase 5: AC6.2 -- Rate Limit Errors Handled With Exponential Backoff

| Step | Action | Expected |
|------|--------|----------|
| 5.1 | **Code review**: Open `src/backfill.rs`, locate `run_fetcher` function | Function contains retry loop with backoff logic. |
| 5.2 | Verify base delay | `backoff_ms` initialized to `1000u64` (1 second). |
| 5.3 | Verify exponential doubling | `backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS)` doubles on each rate-limited retry. |
| 5.4 | Verify maximum cap | `MAX_BACKOFF_MS = 60000` (60 seconds). |
| 5.5 | Verify rate limit retry limit | `max_rate_limit_retries = 10`. After 10 consecutive rate limit errors, the batch is skipped. |
| 5.6 | Verify HTTP 429/503 handling | `FetchError::RateLimited(_)` is matched; in `ct/fetch.rs`, status codes 429 and 503 map to `FetchError::RateLimited(status)`. |
| 5.7 | Verify reset on success | After a successful fetch, the loop breaks. On the next batch iteration, counters are re-initialized fresh. |

## End-to-End: Full Backfill Lifecycle

| Step | Action | Expected |
|------|--------|----------|
| E2E.1 | Set `delta_sink.table_path: /tmp/test-backfill-e2e` in `config.yaml`. Run: `rm -rf /tmp/test-backfill-e2e` | Clean state. |
| E2E.2 | Run: `RUST_LOG=info cargo run -- --backfill --from 0 --logs "<small-known-log>"` | Logs show: "backfill mode starting", "backfilling logs" with count, "gap detection complete" with total_work_items. |
| E2E.3 | Wait for completion | Logs show: per-log "fetcher progress", per-log "fetcher complete", "Writer task completed" with total_records_written and write_errors=0, "backfill complete" summary. |
| E2E.4 | Verify exit code: `echo $?` | Outputs `0`. |
| E2E.5 | Verify delta table: `ls /tmp/test-backfill-e2e/` | Contains `_delta_log/` and `seen_date=YYYY-MM-DD/` partition directories with `.parquet` files. |
| E2E.6 | Re-run the same command | "no gaps detected, nothing to backfill" and exit code 0. |

## End-to-End: Log Filtering

| Step | Action | Expected |
|------|--------|----------|
| F.1 | Run: `RUST_LOG=info cargo run -- --backfill --from 0 --logs "nonexistent_string_xyz"` | Logs show "no logs matched the filter" and exits with code 1. |
| F.2 | Run: `RUST_LOG=info cargo run -- --backfill --from 0 --logs "google"` | Only Google CT logs appear in "backfilling logs" count and fetcher progress. |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1: Backfill runs and exits code 0 | `test_ac1_1_run_backfill_no_work_items` | E2E.4 |
| AC1.2: `--logs` filter limits to matching logs | `test_ac1_2_log_filtering` | F.1, F.2 |
| AC1.3: `delta_sink.enabled=false` does not block backfill | N/A | Phase 2 |
| AC2.1: Catch-up lower bound from MIN | `test_ac2_1_catch_up_lower_bound_from_min` | -- |
| AC2.2: Internal gaps detected | `test_ac2_2_catch_up_internal_gaps` | -- |
| AC2.3: Frontier gap detected | `test_ac2_3_catch_up_frontier_gap` | -- |
| AC2.4: Missing log skipped in catch-up | `test_ac2_4_catch_up_missing_log_skipped` | -- |
| AC3.1: `--from 0` backfills full range | `test_ac3_1_historical_pre_existing_gap` + `test_ac3_1_historical_empty_table_full_range` | -- |
| AC3.2: `--from N` overrides lower bound | `test_ac3_2_historical_from_override` | -- |
| AC4.1: 20-column schema, correct fields | `test_build_rfc6962_certificate_message` + `test_from_message_matches_from_json` | -- |
| AC4.2: Records match live sink records | `test_ac4_2_writer_records_match_schema` + `test_ac4_2_writer_respects_batch_size` | -- |
| AC4.3: Non-existent table created correctly | `test_ac4_3_writer_creates_table_with_correct_schema` | -- |
| AC5.1: Graceful shutdown flushes buffer | `test_ac5_1_fetcher_respects_cancellation_token` + `test_writer_basic_functionality` | -- |
| AC5.2: Re-run picks up where left off | N/A (gap detection unit tests cover core mechanism) | Phase 3 |
| AC5.3: Failed fetcher does not block others | `test_ac5_3_concurrent_fetchers_independent` | -- |
| AC5.4: Exit code reflects success/failure | `test_ac5_4_writer_result_tracks_errors` | -- |
| AC6.1: Progress logged periodically | N/A | Phase 4 |
| AC6.2: Rate limit exponential backoff | N/A | Phase 5 (code review) |
| AC6.3: Completion summary counts | `test_ac6_3_completion_summary_counts` | E2E.3 |
