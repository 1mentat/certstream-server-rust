# ZeroBus Sink Implementation Plan - Phase 4: Metrics

**Goal:** Add observability metrics for the ZeroBus sink following the `certstream_delta_*` naming convention.

**Architecture:** Instrument the `run_zerobus_sink()` function from Phase 3 with five `metrics::counter!` calls at the appropriate code points. Uses the `metrics` crate (already in the project at v0.24) with the same invocation pattern as `src/delta_sink.rs`.

**Tech Stack:** metrics 0.24 (already in project)

**Scope:** 6 phases from original design (phase 4 of 6)

**Codebase verified:** 2026-02-19

---

## Acceptance Criteria Coverage

This phase implements and tests:

### zerobus-sink.AC4: Config and metrics follow conventions
- **zerobus-sink.AC4.4 Success:** Metrics (`certstream_zerobus_records_ingested`, `_ingest_errors`, `_stream_recoveries`, `_messages_lagged`, `_records_skipped`) increment correctly during operation

---

<!-- START_TASK_1 -->
### Task 1: Add metrics instrumentation to run_zerobus_sink()

**Verifies:** zerobus-sink.AC4.4

**Files:**
- Modify: `src/zerobus_sink.rs` (add `metrics::counter!` calls at five points in `run_zerobus_sink()`)

**Implementation:**

Add metric counter calls at each relevant point in the Phase 3 implementation. Follow the exact `metrics::counter!("name").increment(N)` pattern used in `src/delta_sink.rs:608-719`:

1. **`certstream_zerobus_records_ingested`** — increment by 1 after each successful `ingest_record()` call (both initial and post-recovery):
```rust
metrics::counter!("certstream_zerobus_records_ingested").increment(1);
```

2. **`certstream_zerobus_ingest_errors`** — increment by 1 on any `ingest_record()` error (retryable or not):
```rust
metrics::counter!("certstream_zerobus_ingest_errors").increment(1);
```

3. **`certstream_zerobus_stream_recoveries`** — increment by 1 after each successful `recreate_stream()`:
```rust
metrics::counter!("certstream_zerobus_stream_recoveries").increment(1);
```

4. **`certstream_zerobus_messages_lagged`** — increment by `n` on `RecvError::Lagged(n)`:
```rust
metrics::counter!("certstream_zerobus_messages_lagged").increment(n);
```

5. **`certstream_zerobus_records_skipped`** — increment by 1 when a record is skipped due to non-retryable error or failed retry after recovery:
```rust
metrics::counter!("certstream_zerobus_records_skipped").increment(1);
```

Placement in the run_zerobus_sink() function:
- On successful ingest: increment `records_ingested`
- On ingest error (before retryable check): increment `ingest_errors`
- On successful recreate_stream: increment `stream_recoveries`
- On RecvError::Lagged(n): increment `messages_lagged` by n
- On non-retryable error skip: increment `records_skipped`
- On retry failure after recovery: increment `records_skipped`
- On failed recreate_stream exit: no skip counter (task exits)

**Testing note:** Metrics correctness (counter names, placement, increment values) is verified by:
1. Compilation: `metrics::counter!` calls are validated at build time
2. Code review: counter names follow the `certstream_zerobus_*` convention matching `certstream_delta_*`
3. Manual test procedure (Phase 6): live server run verifies counters appear in `/metrics` Prometheus endpoint
4. No automated unit test for counter increment values — the `metrics` crate requires a global recorder which conflicts with parallel test execution. This matches the delta_sink pattern (no metrics unit tests in `src/delta_sink.rs` either).

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Builds without errors

Run: `cargo test zerobus` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: All existing tests still pass

**Commit:** `feat: add certstream_zerobus_* metrics to live sink`
<!-- END_TASK_1 -->
