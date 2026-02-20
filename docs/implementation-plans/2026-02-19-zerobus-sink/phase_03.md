# ZeroBus Sink Implementation Plan - Phase 3: Live Sink

**Goal:** Implement the streaming ZeroBus sink that subscribes to the broadcast channel and forwards records to Databricks via the ZeroBus Ingest SDK.

**Architecture:** A long-lived async task (`run_zerobus_sink`) subscribes to the broadcast channel, deserializes each `PreSerializedMessage` into a `DeltaCertRecord`, converts it to a protobuf `CertRecord`, encodes it, and forwards it to the ZeroBus SDK via `ingest_record()`. Error recovery uses `sdk.recreate_stream()` for retryable errors. Graceful shutdown flushes and closes the stream.

**Tech Stack:** databricks-zerobus-ingest-sdk 0.1, prost 0.13, tokio

**Scope:** 6 phases from original design (phase 3 of 6)

**Codebase verified:** 2026-02-19

---

## Acceptance Criteria Coverage

This phase implements and tests:

### zerobus-sink.AC1: Live sink streams records to Databricks
- **zerobus-sink.AC1.1 Success:** Records from the broadcast channel are ingested into the Databricks Delta table via ZeroBus SDK
- **zerobus-sink.AC1.2 Success:** Sink shuts down gracefully -- flushes pending records and closes stream on CancellationToken
- **zerobus-sink.AC1.3 Success:** Retryable SDK errors trigger stream recovery via `recreate_stream()` and resume ingestion
- **zerobus-sink.AC1.4 Failure:** Non-retryable SDK errors skip the record, log a warning, and continue processing
- **zerobus-sink.AC1.5 Failure:** Stream creation failure at startup exits the sink task without crashing the server

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Add databricks-zerobus-ingest-sdk dependency to Cargo.toml

**Files:**
- Modify: `Cargo.toml` (add to `[dependencies]` section, after the `prost-types` line added in Phase 1)

**Implementation:**

Add to `[dependencies]`:

```toml
databricks-zerobus-ingest-sdk = "0.1"
```

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Downloads and compiles the SDK crate without errors

**Commit:** `chore: add databricks-zerobus-ingest-sdk dependency`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement run_zerobus_sink() in src/zerobus_sink.rs

**Verifies:** zerobus-sink.AC1.1, zerobus-sink.AC1.2, zerobus-sink.AC1.3, zerobus-sink.AC1.4, zerobus-sink.AC1.5

**Files:**
- Modify: `src/zerobus_sink.rs` (add the main sink function and imports)

**Implementation:**

Add imports at the top of `src/zerobus_sink.rs`:

```rust
use crate::config::ZerobusSinkConfig;
use crate::delta_sink::DeltaCertRecord;
use crate::models::PreSerializedMessage;
use databricks_zerobus_ingest_sdk::{
    StreamConfigurationOptions, TableProperties, ZerobusError, ZerobusSdk, ZerobusStream,
};
use prost::Message;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
```

Implement the main sink function following the `delta_sink::run_delta_sink()` pattern (`src/delta_sink.rs:641-770`):

```rust
pub async fn run_zerobus_sink(
    config: ZerobusSinkConfig,
    mut rx: broadcast::Receiver<Arc<PreSerializedMessage>>,
    shutdown: CancellationToken,
) {
    // Build SDK
    let sdk = match ZerobusSdk::new(
        config.endpoint.clone(),
        config.unity_catalog_url.clone(),
    ) {
        Ok(sdk) => sdk,
        Err(e) => {
            error!(error = %e, "failed to create ZeroBus SDK, sink will not run");
            return; // AC1.5: non-fatal exit
        }
    };

    // Build table properties with DescriptorProto from Phase 1
    let table_properties = TableProperties {
        table_name: config.table_name.clone(),
        descriptor_proto: cert_record_descriptor_proto(),
    };

    // Stream configuration
    let options = StreamConfigurationOptions {
        max_inflight_records: config.max_inflight_records,
        ..Default::default()
    };

    // Create initial stream
    let mut stream = match sdk
        .create_stream(
            table_properties.clone(),
            config.client_id.clone(),
            config.client_secret.clone(),
            Some(options.clone()),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to create ZeroBus stream, sink will not run");
            return; // AC1.5: non-fatal exit
        }
    };

    info!(
        table_name = %config.table_name,
        "zerobus sink started, streaming to Databricks"
    );

    let mut record_counter: i64 = 0;

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        // Deserialize JSON bytes -> DeltaCertRecord -> CertRecord -> protobuf bytes
                        let record = match DeltaCertRecord::from_json(&msg.full) {
                            Ok(r) => r,
                            Err(e) => {
                                debug!(error = %e, "failed to deserialize message, skipping");
                                continue;
                            }
                        };

                        let cert_record = proto::CertRecord::from_delta_cert(&record);
                        let encoded = cert_record.encode_to_vec();

                        // Ingest via SDK (two-stage: first await queues record, returns ack future)
                        match stream.ingest_record(encoded).await {
                            Ok(_ack_future) => {
                                // Record queued; ack arrives asynchronously via SDK internals
                                // Drop ack future — flush() at shutdown collects pending acks
                                record_counter += 1;
                            }
                            Err(e) => {
                                if e.is_retryable() {
                                    // AC1.3: retryable error -> recreate stream
                                    // Note: recreate_stream() takes stream by value (consumes it)
                                    warn!(error = %e, "retryable ingest error, recreating stream");
                                    match sdk.recreate_stream(stream).await {
                                        Ok(new_stream) => {
                                            stream = new_stream;
                                            info!("zerobus stream recovered");
                                            // Retry the record on new stream
                                            let cert_record = proto::CertRecord::from_delta_cert(&record);
                                            let encoded = cert_record.encode_to_vec();
                                            match stream.ingest_record(encoded).await {
                                                Ok(_ack) => {
                                                    record_counter += 1;
                                                }
                                                Err(retry_err) => {
                                                    warn!(error = %retry_err, "failed to ingest after stream recovery, skipping record");
                                                }
                                            }
                                        }
                                        Err(recreate_err) => {
                                            // Stream consumed by failed recreate — cannot continue
                                            error!(error = %recreate_err, "failed to recreate zerobus stream, exiting sink");
                                            return;
                                        }
                                    }
                                } else {
                                    // AC1.4: non-retryable error -> skip record
                                    warn!(error = %e, "non-retryable ingest error, skipping record");
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Match delta_sink lagged handling pattern (delta_sink.rs:712-720)
                        warn!(count = n, "zerobus sink lagged, dropped {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("broadcast channel closed, zerobus sink shutting down");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                // AC1.2: graceful shutdown
                info!("zerobus sink shutting down, flushing pending records");
                if let Err(e) = stream.flush().await {
                    warn!(error = %e, "error flushing zerobus stream during shutdown");
                }
                if let Err(e) = stream.close().await {
                    warn!(error = %e, "error closing zerobus stream during shutdown");
                }
                info!(records = record_counter, "zerobus sink shutdown complete");
                return;
            }
        }
    }

    // Channel closed path — also flush and close
    if let Err(e) = stream.flush().await {
        warn!(error = %e, "error flushing zerobus stream");
    }
    if let Err(e) = stream.close().await {
        warn!(error = %e, "error closing zerobus stream");
    }
    info!(records = record_counter, "zerobus sink finished");
}
```

Key design decisions matching delta_sink patterns:
- Non-fatal startup: if SDK or stream creation fails, log error and return (AC1.5)
- Lagged messages: log warning with count, continue (matching `delta_sink.rs:712-720`)
- Broadcast closed: break from loop, flush and close stream
- Shutdown: flush pending records, close stream (AC1.2)
- Retryable errors: recreate_stream (takes stream by value), retry the record once (AC1.3)
- Non-retryable errors: log warning, skip record, continue (AC1.4)
- `record_counter` tracks total ingested records (used by metrics in Phase 4)

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Compiles without errors

**Commit:** `feat: implement run_zerobus_sink() live sink function`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_TASK_3 -->
### Task 3: Wire conditional spawn in main.rs

**Verifies:** zerobus-sink.AC1.1, zerobus-sink.AC1.5

**Files:**
- Modify: `src/main.rs` (add conditional spawn block after delta_sink spawn at line 191, and shutdown wait after delta_sink shutdown at line 280)

**Implementation:**

1. After the delta_sink conditional spawn block (after line 191), add the zerobus_sink spawn following the exact same pattern (`src/main.rs:181-191`):

```rust
    let zerobus_sink_handle = if config.zerobus_sink.enabled {
        let zerobus_rx = tx.subscribe();
        let zerobus_config = config.zerobus_sink.clone();
        let zerobus_shutdown = shutdown_token.clone();
        let handle = tokio::spawn(zerobus_sink::run_zerobus_sink(
            zerobus_config,
            zerobus_rx,
            zerobus_shutdown,
        ));
        info!("zerobus sink enabled, streaming to: {}", config.zerobus_sink.table_name);
        Some(handle)
    } else {
        info!("zerobus sink disabled");
        None
    };
```

2. After the delta_sink graceful shutdown block (after line 280), add matching shutdown wait:

```rust
    if let Some(handle) = zerobus_sink_handle {
        match tokio::time::timeout(Duration::from_secs(30), handle).await {
            Ok(Ok(())) => info!("zerobus sink shutdown complete"),
            Ok(Err(e)) => warn!(error = %e, "zerobus sink task panicked during shutdown"),
            Err(_) => warn!("zerobus sink shutdown timed out after 30s"),
        }
    }
```

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Builds without errors

**Commit:** `feat: wire zerobus sink conditional spawn and graceful shutdown in main.rs`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Write tests for deserialization path and DeltaCertRecord::from_json

**Verifies:** zerobus-sink.AC1.1 (partial), zerobus-sink.AC1.4 (partial)

**Files:**
- Modify: `src/zerobus_sink.rs` (add tests in `#[cfg(test)] mod tests` block, extending tests from Phase 1)

**Testing:**

Tests must verify the deserialization path used by the live sink:
- zerobus-sink.AC1.1 (partial): Test `DeltaCertRecord::from_json()` with a valid JSON payload matching the `PreSerializedMessage.full` format. Verify all 20 fields are correctly deserialized, then convert to `CertRecord` via `from_delta_cert()` and encode to protobuf bytes. Assert the round-trip produces non-empty bytes.
- zerobus-sink.AC1.4 (partial): Test `DeltaCertRecord::from_json()` with malformed JSON to verify it returns `Err` (this is the deserialization error path that the sink handles by skipping with a debug log).

Note: The full `run_zerobus_sink()` function cannot be unit-tested in isolation because it requires a live ZeroBus SDK connection. The SDK interaction paths (AC1.2, AC1.3, AC1.5) are verified by the integration test in Phase 6 and by the manual test procedure.

Follow the existing `#[cfg(test)] mod tests` pattern in `src/zerobus_sink.rs` (created in Phase 1 Task 6).

**Verification:**
Run: `cargo test zerobus` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: All tests pass

**Commit:** `test: add deserialization path tests for live sink`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Verify end-to-end compilation

**Files:** None (verification only)

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Full project builds without errors

Run: `cargo test zerobus` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Phase 1 and Phase 3 tests all pass

**Commit:** No commit needed (verification only)
<!-- END_TASK_5 -->
