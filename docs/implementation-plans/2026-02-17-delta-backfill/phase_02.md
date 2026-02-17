# Delta Backfill Implementation Plan - Phase 2: Extract Shared Fetch Logic

**Goal:** Extract CT entry fetching from the watcher into reusable functions that both live watchers and backfill can call.

**Architecture:** Create `src/ct/fetch.rs` with standalone functions for fetching+parsing CT log entries. These functions handle HTTP fetch, response parsing, and CertificateMessage construction. The existing watchers are refactored to call these shared functions instead of inlining the logic, while retaining their own health/backoff/dedup/broadcast responsibilities.

**Tech Stack:** Rust, reqwest, existing ct/parser.rs functions, thiserror for error types

**Scope:** 5 phases from original design (phase 2 of 5)

**Codebase verified:** 2026-02-17

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-backfill.AC4: Works with existing delta table schema and partitioning
- **delta-backfill.AC4.1 Success:** Backfilled records use the same 20-column Arrow schema and `seen_date` partitioning as the live delta sink

This AC is addressed by ensuring both live and backfill paths share the same parsing+construction code, producing identical `CertificateMessage` structures.

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Create fetch module with shared types and error handling

**Verifies:** delta-backfill.AC4.1 (shared parsing path)

**Files:**
- Create: `src/ct/fetch.rs`
- Modify: `src/ct/mod.rs:1-7` (add `pub mod fetch;`)

**Implementation:**

Create `src/ct/fetch.rs` with:

1. A `FetchError` enum using `thiserror::Error`:
   - `HttpError(reqwest::Error)` - network/HTTP failures
   - `InvalidResponse(String)` - unparseable response body
   - `RateLimited(u16)` - HTTP 429 or 503 status
   - `NotAvailable(u16)` - HTTP 400 or other client errors (log may not support the requested range)

2. Response types moved/shared (these are currently private in `watcher.rs`). Note: `Entry` from `watcher.rs` is renamed to `RawEntry` in this shared module to be more descriptive. Phase 2 Task 2 must update all references from `Entry` to `RawEntry` when removing the private structs from `watcher.rs`:
   ```rust
   #[derive(Debug, Deserialize)]
   pub(crate) struct SthResponse {
       pub tree_size: u64,
   }

   #[derive(Debug, Deserialize)]
   pub(crate) struct EntriesResponse {
       pub entries: Vec<RawEntry>,
   }

   #[derive(Debug, Deserialize)]
   pub(crate) struct RawEntry {
       pub leaf_input: String,
       pub extra_data: String,
   }
   ```

3. A `get_tree_size` function:
   ```rust
   pub async fn get_tree_size(
       client: &reqwest::Client,
       base_url: &str,
       timeout: Duration,
   ) -> Result<u64, FetchError>
   ```
   Makes HTTP GET to `{base_url}/ct/v1/get-sth`, parses `SthResponse`, returns `tree_size`.

4. A `fetch_entries` function for RFC 6962:
   ```rust
   pub async fn fetch_entries(
       client: &reqwest::Client,
       base_url: &str,
       start: u64,
       end: u64,
       source: &Arc<Source>,
       timeout: Duration,
   ) -> Result<Vec<CertificateMessage>, FetchError>
   ```
   Makes HTTP GET to `{base_url}/ct/v1/get-entries?start={start}&end={end}`. Parses JSON to `EntriesResponse`. For each entry, calls `parse_leaf_input()`. Constructs `CertificateMessage` for each successfully parsed entry (skipping unparseable ones). Sets `seen` to current UTC timestamp, `cert_index` to `start + i`, `cert_link` to the appropriate URL.

5. A `fetch_tile_entries` function for Static CT:
   ```rust
   pub async fn fetch_tile_entries(
       client: &reqwest::Client,
       base_url: &str,
       tile_index: u64,
       partial_width: u64,
       offset_in_tile: usize,
       source: &Arc<Source>,
       timeout: Duration,
       issuer_cache: &IssuerCache,
   ) -> Result<Vec<CertificateMessage>, FetchError>
   ```
   Makes HTTP GET for the tile URL (using existing `tile_url()` function). Decompresses (using existing `decompress_tile()`). Parses leaves (using existing `parse_tile_leaves()`). For each leaf starting at `offset_in_tile`, calls `parse_certificate()`, resolves chain via `fetch_issuer()`, constructs `CertificateMessage`. Returns the vector.

6. A `get_checkpoint_tree_size` function for Static CT:
   ```rust
   pub async fn get_checkpoint_tree_size(
       client: &reqwest::Client,
       base_url: &str,
       timeout: Duration,
   ) -> Result<u64, FetchError>
   ```
   Fetches `{base_url}/checkpoint` and parses the tree size from the checkpoint text format.

Add `pub mod fetch;` to `src/ct/mod.rs` module declarations, and `pub use fetch::*;` to re-export the public types.

**Verification:**

Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat(ct/fetch): add shared fetch functions for CT log entries`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Refactor RFC 6962 watcher to use shared fetch functions

**Verifies:** delta-backfill.AC4.1 (shared parsing path)

**Files:**
- Modify: `src/ct/watcher.rs`

**Implementation:**

Refactor `run_watcher_with_cache()` in `watcher.rs`:

1. Remove the private `SthResponse`, `EntriesResponse`, and `Entry` structs (now in `fetch.rs`).

2. Replace the inline get-sth call with `fetch::get_tree_size()`. The existing retry logic with `backon::ExponentialBuilder` should wrap the call to `get_tree_size()`.

3. Replace the inline get-entries HTTP call + parsing loop with a call to `fetch::fetch_entries()`. The watcher's existing health/backoff error handling wraps this call:
   - On `Ok(messages)`: iterate through messages, apply dedup filter, broadcast each
   - On `Err(FetchError::RateLimited(_))`: apply backoff via health status
   - On `Err(FetchError::NotAvailable(_))`: log warning, apply backoff
   - On `Err(FetchError::HttpError(_))`: log error, apply backoff

4. The watcher keeps ALL of its own logic: health tracking, circuit breaker, dedup, broadcast, state management, rate limiting, the main loop. Only the raw HTTP fetch+parse is delegated.

**Verification:**

Run: `cargo test`
Expected: All tests pass (no regressions) (behavior unchanged)

Run: `cargo build`
Expected: No warnings about unused code

**Commit:** `refactor(watcher): use shared fetch functions from ct/fetch`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Refactor Static CT watcher to use shared fetch functions

**Verifies:** delta-backfill.AC4.1 (shared parsing path)

**Files:**
- Modify: `src/ct/static_ct.rs`

**Implementation:**

Refactor `run_static_ct_watcher()` in `static_ct.rs`:

1. Replace the inline checkpoint fetch with `fetch::get_checkpoint_tree_size()`.

2. Replace the inline tile fetch + parse + chain resolution with `fetch::fetch_tile_entries()`. The watcher's health/backoff error handling wraps this call, same pattern as Task 2.

3. The `tile_url()`, `decompress_tile()`, `parse_tile_leaves()`, and `fetch_issuer()` functions used by `fetch_tile_entries()` are already public in `static_ct.rs`. Keep these functions in `static_ct.rs` with `pub(crate)` visibility and call them from `fetch.rs` via `super::static_ct::tile_url()`, `super::static_ct::decompress_tile()`, etc. This minimizes code movement and keeps static-CT-specific logic in its original module.

4. Static CT watcher keeps: health tracking, dedup, broadcast, state management, rate limiting, the main loop.

**Verification:**

Run: `cargo test`
Expected: All tests pass (no regressions) (behavior unchanged)

**Commit:** `refactor(static_ct): use shared fetch functions from ct/fetch`
<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 4-5) -->

<!-- START_TASK_4 -->
### Task 4: Add DeltaCertRecord::from_message() direct conversion

**Verifies:** delta-backfill.AC4.1 (identical records from backfill and live paths)

**Files:**
- Modify: `src/delta_sink.rs`

**Implementation:**

Add a `from_message()` method to `DeltaCertRecord` that converts directly from a `&CertificateMessage` without going through JSON serialization/deserialization. This is functionally equivalent to `from_json()` but avoids the round-trip:

```rust
impl DeltaCertRecord {
    pub fn from_message(msg: &CertificateMessage) -> Self {
        // Same field mapping as from_json(), but reads directly from the struct
        // seen_date derived from msg.data.seen the same way as from_json()
        // chain serialized to JSON strings the same way
        // all_domains collected the same way
    }
}
```

This method must produce byte-identical records to `from_json()` when given the same input. The backfill fetcher (Phase 4) will use this to convert fetched CertificateMessages to DeltaCertRecords without needing the broadcast channel or PreSerializedMessage.

**Testing:**

Tests must verify:
- delta-backfill.AC4.1: A CertificateMessage converted via `from_message()` produces the same DeltaCertRecord field values as the same message serialized to JSON and then passed through `from_json()`

**Verification:**

Run: `cargo test`
Expected: All tests pass including new from_message test

**Commit:** `feat(delta_sink): add DeltaCertRecord::from_message() for direct conversion`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Add tests for shared fetch function parsing

**Verifies:** delta-backfill.AC4.1

**Files:**
- Modify: `src/ct/fetch.rs` (add `#[cfg(test)]` module)

**Testing:**

Tests must verify:
- delta-backfill.AC4.1: `fetch_entries` returns correctly constructed `CertificateMessage` with proper cert_index, source, cert_link fields (test with mock HTTP response data is not feasible without an HTTP test server; instead, test the CertificateMessage construction logic that converts ParsedEntry to CertificateMessage by extracting it as a pure function and testing that)

Focus tests on the pure transformation logic (ParsedEntry + metadata → CertificateMessage) rather than the HTTP call itself. Extract a testable pure function from the fetch functions:

```rust
fn build_certificate_message(
    parsed: ParsedEntry,
    cert_index: u64,
    source: &Arc<Source>,
    base_url: &str,
    seen: f64,
) -> CertificateMessage
```

Test this function with synthetic `ParsedEntry` data (use `parse_certificate()` with a DER cert from `rcgen`, matching the test pattern in `src/ct/parser.rs:455-465`). Verify the returned `CertificateMessage` has correct `cert_index`, `source`, `cert_link`, `seen`, and `message_type` fields.

The HTTP layer is tested indirectly by the existing integration tests (all existing tests passing means the refactored watchers work correctly).

**Verification:**

Run: `cargo test`
Expected: All tests pass

**Commit:** `test(ct/fetch): add tests for shared fetch parsing logic`
<!-- END_TASK_5 -->

<!-- END_SUBCOMPONENT_B -->
