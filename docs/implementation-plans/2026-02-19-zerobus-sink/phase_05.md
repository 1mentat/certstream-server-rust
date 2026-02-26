# ZeroBus Sink Implementation Plan - Phase 5: Backfill Writer & CLI

**Goal:** Add ZeroBus as a backfill write target with an explicit `--sink` CLI flag, allowing historical CT records to be ingested into Databricks via ZeroBus.

**Architecture:** A new `--sink <name>` CLI flag selects the writer backend. `run_backfill()` gains a `backfill_sink` parameter and dispatches to either the existing `run_writer()` (Delta) or a new `run_zerobus_writer()` (ZeroBus). The ZeroBus writer receives `DeltaCertRecord` from the same mpsc channel, converts to protobuf, and calls `ingest_record()`. Validation enforces that `--sink zerobus` requires `--from N` (historical mode only) and `zerobus_sink.enabled = true`.

**Tech Stack:** databricks-zerobus-ingest-sdk 0.1, prost 0.13, tokio mpsc

**Scope:** 6 phases from original design (phase 5 of 6)

**Codebase verified:** 2026-02-19

---

## Acceptance Criteria Coverage

This phase implements and tests:

### zerobus-sink.AC3: Backfill writes historical records via ZeroBus
- **zerobus-sink.AC3.1 Success:** `--backfill --sink zerobus --from N` ingests records from index N to state file ceiling into Databricks
- **zerobus-sink.AC3.2 Success:** `--backfill` without `--sink` uses the delta writer (backwards-compatible)
- **zerobus-sink.AC3.3 Failure:** `--sink zerobus` without `--from` exits with a clear error (historical mode required)
- **zerobus-sink.AC3.4 Failure:** `--sink zerobus` when `zerobus_sink.enabled = false` exits with a clear error
- **zerobus-sink.AC3.5 Failure:** `--sink invalidname` exits with a clear error listing valid sink names

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Add --sink flag to CLI parsing

**Verifies:** zerobus-sink.AC3.5 (partial)

**Files:**
- Modify: `src/cli.rs` (add `backfill_sink` field and parsing logic)

**Implementation:**

1. Add field to `CliArgs` struct (after `merge: bool` at line 16):
```rust
    pub backfill_sink: Option<String>,
```

2. Add variable initialization in `parse()` (before the loop, around line 22):
```rust
        let mut backfill_sink = None;
```

3. Add parsing in the argument loop (after `--staging-path` parsing at lines 35-36):
```rust
                } else if arg == "--sink" && i + 1 < args.len() {
                    backfill_sink = Some(args[i + 1].clone());
                }
```

4. Add to the `CliArgs` return (around line 50):
```rust
            backfill_sink,
```

5. Add `--sink` to `print_help()` in the `BACKFILL OPTIONS` section (after `--logs` line at line 72):
```rust
        println!("    --sink <NAME>        Writer backend: delta (default), zerobus");
```

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Compiles without errors

**Commit:** `feat: add --sink CLI flag for backfill writer selection`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add --sink validation, update run_backfill() signature, and add writer dispatch

**Verifies:** zerobus-sink.AC3.1, zerobus-sink.AC3.2, zerobus-sink.AC3.3, zerobus-sink.AC3.4, zerobus-sink.AC3.5

**Files:**
- Modify: `src/main.rs` (add validation before `run_backfill()` call, around lines 99-117)
- Modify: `src/backfill.rs` (update `run_backfill()` signature at line 545, add writer dispatch logic around lines 687-699)

**Implementation:**

**Part A — Update `run_backfill()` signature first** (so the call site change compiles):

1. In `src/backfill.rs`, update `run_backfill()` signature (add `backfill_sink` parameter after `backfill_logs`):
```rust
pub async fn run_backfill(
    config: Config,
    staging_path: Option<String>,
    backfill_from: Option<u64>,
    backfill_logs: Option<String>,
    backfill_sink: Option<String>,
    shutdown: CancellationToken,
) -> i32 {
```

2. When `--sink zerobus` is specified, skip gap detection and build work items directly from `--from N` to state file ceiling (historical mode logic already exists). The gap detection code should be bypassed for ZeroBus since it can't query remote tables.

3. Modify the writer spawning section (around lines 687-699). Replace the unconditional `run_writer()` spawn with a dispatch:

```rust
    let writer_handle = if backfill_sink.as_deref() == Some("zerobus") {
        let zerobus_config = config.zerobus_sink.clone();
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            run_zerobus_writer(zerobus_config, rx, shutdown_clone).await
        })
    } else {
        // AC3.2: default to delta writer (backwards-compatible)
        let table_path = staging_path
            .unwrap_or_else(|| config.delta_sink.table_path.clone());
        let batch_size = config.delta_sink.batch_size;
        let flush_interval_secs = config.delta_sink.flush_interval_secs;
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            run_writer(table_path, batch_size, flush_interval_secs, rx, shutdown_clone).await
        })
    };
```

**Part B — Add validation and updated call site in `src/main.rs`:**

After the `if cli_args.backfill {` check and before calling `run_backfill()`, add validation logic:

```rust
        // Validate --sink flag
        if let Some(ref sink_name) = cli_args.backfill_sink {
            match sink_name.as_str() {
                "delta" => {} // valid, default behavior
                "zerobus" => {
                    // AC3.4: zerobus_sink must be enabled
                    if !config.zerobus_sink.enabled {
                        eprintln!("Error: --sink zerobus requires zerobus_sink.enabled = true in config");
                        std::process::exit(1);
                    }
                    // AC3.3: --from is required for zerobus sink
                    if cli_args.backfill_from.is_none() {
                        eprintln!("Error: --sink zerobus requires --from <INDEX> (historical mode only, catch-up gap detection not supported for remote tables)");
                        std::process::exit(1);
                    }
                }
                other => {
                    // AC3.5: invalid sink name
                    eprintln!("Error: unknown sink '{}'. Valid sinks: delta, zerobus", other);
                    std::process::exit(1);
                }
            }
        }
```

Pass the sink to `run_backfill()`:
```rust
        let exit_code = backfill::run_backfill(
            config,
            cli_args.staging_path,
            cli_args.backfill_from,
            cli_args.backfill_logs,
            cli_args.backfill_sink,
            shutdown_token,
        )
        .await;
```

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Compile error (`run_zerobus_writer` doesn't exist yet — fixed in Task 3)

**Commit:** `feat: add --sink validation and backfill writer dispatch`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->


<!-- START_TASK_3 -->
### Task 3: Implement run_zerobus_writer() in backfill.rs

**Verifies:** zerobus-sink.AC3.1

**Files:**
- Modify: `src/backfill.rs` (add `run_zerobus_writer()` function, add imports for ZeroBus types)

**Implementation:**

Add imports at top of `src/backfill.rs`:
```rust
use crate::config::ZerobusSinkConfig;
use crate::zerobus_sink::{cert_record_descriptor_proto, proto};
use databricks_zerobus_ingest_sdk::{
    StreamConfigurationOptions, TableProperties, ZerobusSdk,
};
use prost::Message;
```

Implement `run_zerobus_writer()` following the same pattern as `run_writer()` (lines 434-543) but using ZeroBus SDK instead of Delta:

```rust
async fn run_zerobus_writer(
    config: ZerobusSinkConfig,
    mut rx: mpsc::Receiver<DeltaCertRecord>,
    shutdown: CancellationToken,
) -> WriterResult {
    let mut result = WriterResult {
        total_records_written: 0,
        write_errors: 0,
    };

    // Build SDK
    let sdk = match ZerobusSdk::new(
        config.endpoint.clone(),
        config.unity_catalog_url.clone(),
    ) {
        Ok(sdk) => sdk,
        Err(e) => {
            error!(error = %e, "failed to create ZeroBus SDK for backfill writer");
            result.write_errors = 1;
            return result;
        }
    };

    let table_properties = TableProperties {
        table_name: config.table_name.clone(),
        descriptor_proto: cert_record_descriptor_proto(),
    };

    let options = StreamConfigurationOptions {
        max_inflight_records: config.max_inflight_records,
        ..Default::default()
    };

    let mut stream = match sdk
        .create_stream(
            table_properties,
            config.client_id.clone(),
            config.client_secret.clone(),
            Some(options),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to create ZeroBus stream for backfill writer");
            result.write_errors = 1;
            return result;
        }
    };

    info!("zerobus backfill writer started");

    loop {
        tokio::select! {
            recv_result = rx.recv() => {
                match recv_result {
                    Some(record) => {
                        let cert_record = proto::CertRecord::from_delta_cert(&record);
                        let encoded = cert_record.encode_to_vec();

                        // Two-stage: first await queues record, returns ack future
                        match stream.ingest_record(encoded).await {
                            Ok(_ack_future) => {
                                // Record queued; drop ack future (flush collects pending acks)
                                result.total_records_written += 1;
                            }
                            Err(e) => {
                                if e.is_retryable() {
                                    // recreate_stream() takes stream by value (consumes it)
                                    warn!(error = %e, "retryable error in backfill writer, recreating stream");
                                    match sdk.recreate_stream(stream).await {
                                        Ok(new_stream) => {
                                            stream = new_stream;
                                            // Retry the record
                                            let cert_record = proto::CertRecord::from_delta_cert(&record);
                                            let encoded = cert_record.encode_to_vec();
                                            match stream.ingest_record(encoded).await {
                                                Ok(_ack) => {
                                                    result.total_records_written += 1;
                                                }
                                                Err(retry_err) => {
                                                    warn!(error = %retry_err, "failed to ingest after recovery, skipping");
                                                    result.write_errors += 1;
                                                }
                                            }
                                        }
                                        Err(recreate_err) => {
                                            // Stream consumed by failed recreate — cannot continue
                                            error!(error = %recreate_err, "failed to recreate stream, exiting writer");
                                            result.write_errors += 1;
                                            // Return directly since stream is consumed
                                            info!(records = result.total_records_written, errors = result.write_errors, "zerobus backfill writer finished (stream lost)");
                                            return result;
                                        }
                                    }
                                } else {
                                    warn!(error = %e, "non-retryable error, skipping record");
                                    result.write_errors += 1;
                                }
                            }
                        }
                    }
                    None => {
                        // Channel closed: all fetchers done
                        info!("backfill channel closed, flushing zerobus stream");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                info!("backfill shutdown signal, flushing zerobus stream");
                break;
            }
        }
    }

    // Flush and close
    if let Err(e) = stream.flush().await {
        warn!(error = %e, "error flushing zerobus stream in backfill");
    }
    if let Err(e) = stream.close().await {
        warn!(error = %e, "error closing zerobus stream in backfill");
    }

    info!(records = result.total_records_written, errors = result.write_errors, "zerobus backfill writer finished");
    result
}
```

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Builds without errors

**Commit:** `feat: implement run_zerobus_writer() for backfill`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Write tests for CLI validation and writer dispatch

**Verifies:** zerobus-sink.AC3.2, zerobus-sink.AC3.3, zerobus-sink.AC3.4, zerobus-sink.AC3.5

**Files:**
- Modify: `src/cli.rs` (add tests for --sink parsing in `#[cfg(test)] mod tests` block if one exists, or create it)
- Modify: `src/backfill.rs` (add tests in existing `#[cfg(test)] mod tests` block)

**Testing:**

Tests must verify each AC listed above:
- zerobus-sink.AC3.2: Test that `backfill_sink` is `None` when `--sink` is not provided (backwards-compatible delta default)
- zerobus-sink.AC3.3: Validation test — `--sink zerobus` without `--from` should trigger error (test the validation logic, not process::exit)
- zerobus-sink.AC3.4: Validation test — `--sink zerobus` with disabled config should trigger error
- zerobus-sink.AC3.5: Test that `CliArgs::parse()` correctly parses `--sink zerobus` and `--sink delta` into `backfill_sink: Some("zerobus")` / `Some("delta")`; test that an unknown sink name is parsed (validation happens in main.rs)

For CLI tests, create a helper that builds args vectors and calls `CliArgs::parse()`. For validation tests, extract the validation logic into a testable function or test the conditions directly on CliArgs/Config structs.

**Verification:**
Run: `cargo test --bin certstream-server-rust` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: All tests pass

**Commit:** `test: add CLI --sink parsing and backfill validation tests`
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_B -->
