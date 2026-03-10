# Human Test Plan: Delta Log Checkpoint on Startup

## Prerequisites

- Working checkout of the `delta-checkpoint` branch
- All automated tests passing: `cargo test -- delta_sink::tests::test_ac` (expect 5 tests pass: AC1.1, AC1.2, AC1.3, AC2.1, AC3.2)
- Full test suite passing: `cargo test` (expect 0 failures)

## Phase 1: Code Review Verification

### AC2.2: Checkpoint failure exits with code 1

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `src/delta_sink.rs` and navigate to `checkpoint_table()` (line 292). | Function returns `Result<(), Box<dyn std::error::Error + Send + Sync>>`. |
| 2 | Inspect line 309: `checkpoints::create_checkpoint(&table, None).await?;` | The `?` operator propagates `create_checkpoint()` errors as `Err`. |
| 3 | Open `src/main.rs` line 315-320. | `if let Err(e) = delta_sink::checkpoint_table(...).await { eprintln!(...); std::process::exit(1); }` |

### AC3.1: Cleanup removes expired log files

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `src/delta_sink.rs` line 311. | `checkpoints::cleanup_metadata(&table, None).await` called after `create_checkpoint()`. `None` = default 30-day retention. |
| 2 | Verify call is after `create_checkpoint()` at line 309. | Cleanup always runs on the success path. |
| 3 | Confirm `Ok(cleaned)` arm records count in `certstream_delta_checkpoint_logs_cleaned` gauge. | Metric recorded at lines 316-317. |

### AC3.2: Cleanup failure is non-fatal

| Step | Action | Expected |
|------|--------|----------|
| 1 | Inspect lines 311-337. | `cleanup_metadata()` handled by `match`, NOT `?`. |
| 2 | Inspect `Err(e)` arm at line 325. | Calls `warn!(...)`, no `return Err(...)`. |
| 3 | Inspect line 339: `Ok(())`. | Function unconditionally returns `Ok(())` after the match block. |

### AC4.1: No-op when delta_sink is disabled

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `src/main.rs` line 315. | Checkpoint call guarded by `if config.delta_sink.enabled`. |
| 2 | Search `src/main.rs` for `checkpoint_table`. | Only one call site (line 317). |
| 3 | Confirm else branch. | Sets `delta_sink_handle = None`, skipping checkpoint and sink. |

### AC4.2: Server mode only

| Step | Action | Expected |
|------|--------|----------|
| 1 | Identify offline dispatch order in `src/main.rs`. | reparse-audit (~102), extract-metadata (~131), merge (~162), backfill (~191). Each calls `std::process::exit()`. |
| 2 | Confirm `checkpoint_table()` at line 317. | Inside server-mode startup block, only reachable if no offline flag provided. |
| 3 | Verify each offline mode exits before reaching line 315. | All four dispatch blocks exit before server startup code. |

### AC4.3: No new config/CLI fields

| Step | Action | Expected |
|------|--------|----------|
| 1 | Search for "checkpoint" in `src/config.rs`. | Zero matches. |
| 2 | Search for "checkpoint" in `src/cli.rs`. | Zero matches. |
| 3 | Review `DeltaSinkConfig` struct fields. | `enabled`, `table_path`, `batch_size`, `flush_interval_secs`, `compression_level`, `heavy_column_compression_level`, `offline_batch_size` -- no checkpoint fields. |

## End-to-End: Server Startup with Delta Sink Enabled

| Step | Action | Expected |
|------|--------|----------|
| 1 | Create config with `delta_sink.enabled: true`, `table_path: "file:///tmp/e2e_checkpoint_test"`. | Config created. |
| 2 | `mkdir -p /tmp/e2e_checkpoint_test` | Directory exists. |
| 3 | `cargo run -- --validate-config` | Validation passes. |
| 4 | `cargo run` — observe startup logs. | `"creating delta log checkpoint"` and `"delta log checkpoint complete"` appear before `"delta sink started"`. |
| 5 | Check `/tmp/e2e_checkpoint_test/_delta_log/`. | `*.checkpoint.parquet` file exists. |
| 6 | Stop server (Ctrl+C), restart. | Same checkpoint messages. Server starts without error (idempotent). |
| 7 | `rm -rf /tmp/e2e_checkpoint_test` | Cleanup. |

## End-to-End: Server Startup with Delta Sink Disabled

| Step | Action | Expected |
|------|--------|----------|
| 1 | Start server with `delta_sink.enabled: false` (default). | Server starts. |
| 2 | Observe startup logs. | No "checkpoint" messages. |
| 3 | Verify no `_delta_log/` at default table path. | No Delta artifacts created. |

## End-to-End: Offline Modes Do Not Checkpoint

| Step | Action | Expected |
|------|--------|----------|
| 1 | Config: `delta_sink.enabled: true`, named target `main`. | Config created. |
| 2 | `cargo run -- --reparse-audit --source main` | No "checkpoint" messages. |
| 3 | `cargo run -- --extract-metadata --source main --target main` | No "checkpoint" messages. |
| 4 | `cargo run -- --merge --source main --target main` | No "checkpoint" messages. |
| 5 | `cargo run -- --backfill --target main` | No "checkpoint" messages. |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 - Checkpoint creates parquet file | `test_ac1_1_checkpoint_creates_parquet_file` | E2E: Enabled, step 5 |
| AC1.2 - Checkpoint is idempotent | `test_ac1_2_checkpoint_is_idempotent` | E2E: Enabled, step 6 |
| AC1.3 - New table checkpointed | `test_ac1_3_checkpoint_new_table_with_zero_commits` | E2E: Enabled, step 4 |
| AC2.1 - Invalid URI returns error | `test_ac2_1_checkpoint_invalid_uri_returns_error` | -- |
| AC2.2 - Checkpoint failure exits | -- | Phase 1: AC2.2 |
| AC3.1 - Cleanup removes expired logs | -- | Phase 1: AC3.1 |
| AC3.2 - Cleanup failure non-fatal | `test_ac3_2_cleanup_failure_does_not_fail_checkpoint` | Phase 1: AC3.2 |
| AC4.1 - No-op when disabled | -- | Phase 1: AC4.1 + E2E: Disabled |
| AC4.2 - Server mode only | -- | Phase 1: AC4.2 + E2E: Offline |
| AC4.3 - No new config/CLI | -- | Phase 1: AC4.3 |
