# Delta Sink Implementation Plan - Phase 5: Metrics and Main Integration

**Goal:** Add Prometheus metrics, wire sink into `main.rs` with config gating

**Architecture:** The delta sink task is conditionally spawned based on `config.delta_sink.enabled`. It subscribes to the same broadcast channel as WS/SSE handlers and respects the existing CancellationToken shutdown signal.

**Tech Stack:** metrics crate (already in Cargo.toml), tokio_util CancellationToken

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-sink.AC4: No disruption to existing streaming
- **delta-sink.AC4.1 Success:** WebSocket and SSE streams continue operating normally when delta sink is enabled
- **delta-sink.AC4.2 Success:** Delta sink disabled by default (`enabled: false`); application behavior unchanged when disabled
- **delta-sink.AC4.3 Success:** Broadcast channel `Lagged` errors in the sink are logged and metriced, not propagated
- **delta-sink.AC4.4 Success:** Delta table creation failure on startup does not prevent the rest of the application from running

---

<!-- START_TASK_1 -->
### Task 1: Add Prometheus metrics instrumentation to delta_sink.rs

**Files:**
- Modify: `src/delta_sink.rs`

**Implementation:**

Add metrics instrumentation to the sink task and flush function, following the project's metrics pattern. The project uses the `metrics` crate macros directly (see `src/ct/mod.rs:87`, `src/dedup.rs:35,46,65`, `src/sse.rs:123`):

```rust
// Counter example (from src/ct/mod.rs:87):
metrics::counter!("certstream_messages_sent", "log" => log_name.to_string()).increment(1);

// Gauge example (from src/dedup.rs:65):
metrics::gauge!("certstream_dedup_cache_size").set(self.seen.len() as f64);
```

Add the following metrics to the delta sink code:

| Metric | Type | Where to instrument |
|--------|------|---------------------|
| `certstream_delta_records_written` | Counter | After successful flush, increment by count of records written |
| `certstream_delta_flushes` | Counter | After each successful flush attempt |
| `certstream_delta_write_errors` | Counter | After each failed flush attempt |
| `certstream_delta_buffer_size` | Gauge | After each receive (set to current buffer.len()) |
| `certstream_delta_messages_lagged` | Counter | On `RecvError::Lagged(n)`, increment by `n` |
| `certstream_delta_flush_duration_seconds` | Histogram | Wrap the flush call with `Instant::now()` / `elapsed()` |

Usage patterns:
```rust
metrics::counter!("certstream_delta_records_written").increment(count as u64);
metrics::counter!("certstream_delta_flushes").increment(1);
metrics::counter!("certstream_delta_write_errors").increment(1);
metrics::gauge!("certstream_delta_buffer_size").set(buffer.len() as f64);
metrics::counter!("certstream_delta_messages_lagged").increment(n);
metrics::histogram!("certstream_delta_flush_duration_seconds").record(elapsed.as_secs_f64());
```

These integrate automatically with the Prometheus exporter already configured in `src/main.rs:84-86` (PrometheusBuilder).

**Verification:**

Run: `cargo build 2>&1 | tail -3`
Expected: Build succeeds

**Commit:** `feat: add Prometheus metrics to delta sink`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Wire delta sink into main.rs with config gating

**Files:**
- Modify: `src/main.rs` (verify `mod delta_sink;` present from Phase 2, add conditional spawn, update print_config_validation)

**Implementation:**

**Step 1: Verify module declaration already present**

Verify that `mod delta_sink;` is already present in `src/main.rs` (added in Phase 2, Task 1). It should be between `dedup` and `health`:

```rust
mod dedup;
mod delta_sink;
mod health;
```

If it's missing for any reason, add it now.

**Step 2: Add conditional spawn**

Add the delta sink spawn immediately after the dedup filter setup, specifically after `info!("cross-log dedup filter enabled");` (around line 127) and before the `ct_log_config` variable setup. This is outside any conditional blocks, so the delta sink is independent of other features.

```rust
if config.delta_sink.enabled {
    let delta_rx = tx.subscribe();
    let delta_config = config.delta_sink.clone();
    let delta_shutdown = shutdown_token.clone();
    tokio::spawn(delta_sink::run_delta_sink(delta_config, delta_rx, delta_shutdown));
    info!("delta sink enabled, writing to: {}", config.delta_sink.table_path);
} else {
    info!("delta sink disabled");
}
```

This follows the existing pattern for optional features:
- SSE is gated by `config.protocols.sse` (checked in `build_router`)
- REST API is gated by `config.protocols.api` (checked in `build_router`)
- The delta sink uses `config.delta_sink.enabled` (checked before spawning the task)

The `tx.subscribe()` call follows the pattern from SSE handler (`src/sse.rs:40`) and WS handlers.

The `shutdown_token.clone()` follows the pattern from watcher tasks (`src/main.rs:169`).

Note: `run_delta_sink` is an async function (not a spawner). `tokio::spawn` wraps it as a task. This matches the pattern used for other spawned tasks.

**Step 3: Update print_config_validation**

In the `print_config_validation` function (`src/main.rs`, around lines 556-585), add delta sink config display after the existing config prints:

```rust
println!("Delta sink enabled: {}", config.delta_sink.enabled);
if config.delta_sink.enabled {
    println!("  Table path: {}", config.delta_sink.table_path);
    println!("  Batch size: {}", config.delta_sink.batch_size);
    println!("  Flush interval: {}s", config.delta_sink.flush_interval_secs);
}
```

**Step 4: Ensure delta sink startup failure is non-fatal**

The `run_delta_sink` function (from Phase 4) handles table creation failure internally by logging the error and returning early. This means:
- If table creation fails, the spawned task exits silently
- The main application continues running normally (AC4.4)
- No special error handling needed in main.rs

**Verification:**

Run: `cargo build 2>&1 | tail -3`
Expected: Build succeeds

Run: `cargo test 2>&1 | tail -5`
Expected: All tests pass (existing + delta sink tests)

**Commit:** `feat: wire delta sink into main.rs with config gating`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Verify end-to-end behavior

**Verifies:** delta-sink.AC4.1, delta-sink.AC4.2, delta-sink.AC4.3, delta-sink.AC4.4

**Files:**
- Modify: `src/delta_sink.rs` (add to `#[cfg(test)] mod tests`)

**Testing:**

Tests must verify:
- **delta-sink.AC4.1:** Create a full application state with broadcast channel, spawn both a mock WS-like consumer and the delta sink. Send messages. Assert both consumers receive messages (delta sink writes to table, mock consumer receives messages). This verifies no interference.
- **delta-sink.AC4.2:** Verify `DeltaSinkConfig::default().enabled` is `false`. Verify that when disabled, `run_delta_sink` is not called (this is a main.rs logic test — verify by checking the config default).
- **delta-sink.AC4.3:** In the sink task test, send enough messages to cause `Lagged` error (by filling the broadcast channel buffer). Assert the sink continues processing after logging the lag.
- **delta-sink.AC4.4:** Call `run_delta_sink` with an invalid/unwritable `table_path` (e.g., `/nonexistent/path/delta`). Assert the task starts and exits without panicking, and does not affect the calling code.

**Verification:**

Run: `cargo test delta_sink 2>&1 | tail -10`
Expected: All delta_sink tests pass

Run: `cargo test 2>&1 | tail -5`
Expected: All 183+ tests pass (no regressions)

**Commit:** `test: add integration tests for delta sink main integration`
<!-- END_TASK_3 -->
