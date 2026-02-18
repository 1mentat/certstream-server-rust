# Delta Query API Implementation Plan - Phase 5: Metrics, Timeout, Polish

**Goal:** Add Prometheus metrics, query timeout protection, config validation at startup, and ensure all error paths are covered.

**Architecture:** Prometheus metrics use the existing `metrics` crate pattern (`certstream_query_*` prefix). Query timeout wraps DataFusion execution in `tokio::time::timeout()`. Config validation logs a warning for inaccessible table paths at startup. The no-filter-params rejection (400) was already added in Phase 2 but is verified here.

**Tech Stack:** Rust, metrics crate 0.24, tokio::time::timeout, Axum

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-18

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-query-api.AC5: Configuration and operational
- **delta-query-api.AC5.3 Failure:** Inaccessible `table_path` logged as warning at startup (non-fatal)
- **delta-query-api.AC5.4 Success:** Query timeout returns 504 when DataFusion execution exceeds `query_timeout_secs`
- **delta-query-api.AC5.5 Success:** Prometheus metrics emitted (request count by status, duration, result count)

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Add Prometheus metrics to query handler

**Verifies:** delta-query-api.AC5.5

**Files:**
- Modify: `src/query.rs` (add metrics recording around the handler)

**Implementation:**

Add metrics recording following the existing `certstream_delta_*` pattern from `src/delta_sink.rs:608-617`. Use `certstream_query_*` prefix.

Three metrics:
1. `certstream_query_requests` — counter with `status` label (e.g., "200", "400", "503", "504")
2. `certstream_query_duration_seconds` — histogram recording total handler duration
3. `certstream_query_results_count` — histogram recording number of results returned

Add timing at the start of the handler:
```rust
use std::time::Instant;

async fn handle_query_certs(
    State(state): State<Arc<QueryApiState>>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let start = Instant::now();

    // ... existing handler logic ...

    // At each early return (error), record metrics:
    // metrics::counter!("certstream_query_requests", "status" => "400").increment(1);
    // metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());

    // At the successful return:
    // metrics::counter!("certstream_query_requests", "status" => "200").increment(1);
    // metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
    // metrics::histogram!("certstream_query_results_count").record(results.len() as f64);
}
```

Since the handler has multiple return points (400, 410, 503, 504, 500, 200), ensure each return point records the counter with the appropriate status label and the duration histogram. A helper or restructuring to a single return path may be useful but is not required — follow whichever approach is simpler.

**Testing:**
- delta-query-api.AC5.5: Verify metrics are recorded correctly. Two approaches (choose based on what works cleanly):
  1. **Preferred — use `metrics_util::debugging::DebuggingRecorder`:** Install a test-scoped recorder before the test, execute a query, then inspect the recorder's snapshot to verify `certstream_query_requests` counter was incremented and `certstream_query_duration_seconds` histogram was recorded. Note: `metrics-util` must be added as a dev-dependency in `Cargo.toml` (`metrics-util = "0.19"`).
  2. **Fallback — operational verification:** If the `DebuggingRecorder` approach conflicts with the global recorder used by other tests running in parallel, verify that the metrics calls compile and execute without panicking by running a query in a test. Actual Prometheus output verification is then out of scope for unit tests but can be confirmed manually.

  The key constraint is that the `metrics` crate uses a global recorder, and only one recorder can be installed at a time. If other tests in the crate also interact with metrics, use approach 2 to avoid conflicts.

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat(query-api): add Prometheus metrics for requests, duration, and result count`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add query timeout via tokio::time::timeout

**Verifies:** delta-query-api.AC5.4

**Files:**
- Modify: `src/query.rs` (wrap DataFusion execution in timeout)

**Implementation:**

Wrap the DataFusion SQL execution and RecordBatch collection in `tokio::time::timeout()`:

```rust
use std::time::Duration;
use tokio::time::timeout;

// In handle_query_certs, after building the SQL and registering the table:
let timeout_duration = Duration::from_secs(state.config.query_timeout_secs);

let batches = match timeout(timeout_duration, async {
    let df = ctx.sql(&sql).await?;
    df.collect().await
}).await {
    Ok(Ok(batches)) => batches,
    Ok(Err(e)) => {
        warn!(error = %e, "DataFusion query execution error");
        metrics::counter!("certstream_query_requests", "status" => "500").increment(1);
        metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: "Internal query error".to_string() }),
        ).into_response();
    }
    Err(_elapsed) => {
        warn!("Query timed out after {}s", state.config.query_timeout_secs);
        metrics::counter!("certstream_query_requests", "status" => "504").increment(1);
        metrics::histogram!("certstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
        return (
            StatusCode::GATEWAY_TIMEOUT,
            Json(ErrorResponse { error: "Query timed out".to_string() }),
        ).into_response();
    }
};
```

The timeout wraps only the DataFusion execution (SQL parse + query plan + data scan), not the entire handler. This ensures the table opening and response serialization don't count against the timeout.

**Testing:**
- delta-query-api.AC5.4: Test with a very short timeout (e.g., 0 seconds or 1 nanosecond) and a query that would take longer, verify 504 is returned. Alternatively, set `query_timeout_secs` to 0 in the test config.

**Verification:**
Run: `cargo test query::tests`
Expected: All query tests pass

**Commit:** `feat(query-api): add query timeout with 504 response`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add startup config validation for table_path

**Verifies:** delta-query-api.AC5.3

**Files:**
- Modify: `src/main.rs` (add table_path accessibility check when query_api is enabled)

**Implementation:**

In `src/main.rs`, after the query API router is conditionally merged (inside the `if config.query_api.enabled` block), add a non-fatal check for table_path accessibility:

```rust
if config.query_api.enabled {
    // Check table path accessibility (non-fatal warning)
    let table_path = std::path::Path::new(&config.query_api.table_path);
    if !table_path.exists() {
        warn!(
            table_path = %config.query_api.table_path,
            "Query API table path does not exist yet; queries will return 503 until data is written"
        );
    }

    let query_api_state = Arc::new(query::QueryApiState {
        config: config.query_api.clone(),
    });
    let query_router = query::query_api_router(query_api_state);
    app = app.merge(query_router);
    info!("Query API enabled");
}
```

This follows the design's intent: inaccessible table_path is logged as a warning at startup but does NOT prevent the server from starting. The query handler itself returns 503 when the table can't be opened at query time.

**Testing:**
- delta-query-api.AC5.3: This is a startup behavior test. Verify by running the server with a non-existent table_path and checking logs for the warning. In unit tests, the most practical verification is that the config validation logic doesn't panic or block startup. The existing build + test cycle confirms this.

**Verification:**
Run: `cargo build`
Expected: Builds without errors

Run: `cargo test`
Expected: All tests pass

**Commit:** `feat(query-api): add startup warning for inaccessible table path`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->
