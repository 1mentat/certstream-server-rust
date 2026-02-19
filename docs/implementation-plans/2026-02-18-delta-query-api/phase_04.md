# Delta Query API Implementation Plan - Phase 4: Cursor Pagination

**Goal:** Implement stable cursor-based pagination pinned to Delta table versions, enabling multi-page result sets to read from a consistent data snapshot.

**Architecture:** Cursor is a Base64-encoded JSON object `{"v": <i64 delta version>, "k": <u64 last cert_index>}`. First request opens latest table version; subsequent requests use `deltalake::open_table_with_version()` to pin to the cursor's version. Pagination uses `WHERE cert_index > k ORDER BY cert_index LIMIT N+1` — if N+1 rows returned, `has_more: true` and the last row is excluded. Expired cursors (vacuumed versions) return 410 Gone.

**Tech Stack:** Rust, deltalake 0.25 (`open_table_with_version`), base64 0.22, serde_json

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-18

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-query-api.AC2: Stable cursor pagination pinned to Delta version
- **delta-query-api.AC2.1 Success:** First request returns results with `next_cursor` and `has_more: true` when more results exist
- **delta-query-api.AC2.2 Success:** Subsequent request with cursor returns next page from same Delta version
- **delta-query-api.AC2.3 Success:** Final page returns `has_more: false` with no `next_cursor`
- **delta-query-api.AC2.4 Failure:** Invalid cursor returns 400
- **delta-query-api.AC2.5 Failure:** Expired cursor (vacuumed version) returns 410
- **delta-query-api.AC2.6 Edge:** `limit` parameter respected, capped at `max_results_per_page`

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Add cursor encoding/decoding

**Files:**
- Modify: `src/query.rs` (add Cursor struct and encode/decode functions)

**Implementation:**

Add cursor types and encode/decode logic to `src/query.rs`:

```rust
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

#[derive(Debug, Serialize, Deserialize)]
struct Cursor {
    /// Delta table version
    v: i64,
    /// Last cert_index seen (pagination key)
    k: u64,
}

fn encode_cursor(version: i64, last_cert_index: u64) -> String {
    let cursor = Cursor { v: version, k: last_cert_index };
    let json = serde_json::to_string(&cursor).expect("cursor serialization cannot fail");
    BASE64.encode(json.as_bytes())
}

fn decode_cursor(encoded: &str) -> Result<Cursor, ()> {
    let bytes = BASE64.decode(encoded).map_err(|_| ())?;
    serde_json::from_slice(&bytes).map_err(|_| ())
}
```

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat(query-api): add cursor encoding and decoding`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement version-pinned table opening and pagination logic

**Verifies:** delta-query-api.AC2.1, delta-query-api.AC2.2, delta-query-api.AC2.3, delta-query-api.AC2.4, delta-query-api.AC2.5, delta-query-api.AC2.6

**Files:**
- Modify: `src/query.rs` (refactor handler to use cursors for table opening and pagination)

**Implementation:**

Refactor `handle_query_certs` to:

1. **Decode cursor if provided.** If cursor is invalid Base64 or JSON, return 400.

2. **Open table at correct version.** Without cursor: `deltalake::open_table()` (latest). With cursor: `deltalake::open_table_with_version(path, cursor.v)`. If version-pinned open fails (vacuumed), return 410.

3. **Add `cert_index > k` filter when cursor present.** Append to the WHERE clause.

4. **Use LIMIT N+1 for has_more detection.** Fetch `limit + 1` rows. If exactly `limit + 1` rows returned, set `has_more: true`, exclude the last row, and encode a new cursor from the last included row's `cert_index`.

Table opening logic — decode cursor once and return `(table, version, Option<Cursor>)`:

```rust
// Decode cursor once, open table, return all three values together
let (table, version, decoded_cursor) = if let Some(ref cursor_str) = params.cursor {
    let cursor = decode_cursor(cursor_str).map_err(|_| {
        (StatusCode::BAD_REQUEST,
         Json(ErrorResponse { error: "Invalid cursor".to_string() }))
    })?;

    match deltalake::open_table_with_version(&state.config.table_path, cursor.v).await {
        Ok(t) => {
            let v = t.version();
            (t, v, Some(cursor))
        }
        Err(e) => {
            // Vacuumed or missing version → 410 Gone
            warn!(version = cursor.v, error = %e, "Failed to open table at cursor version");
            return (
                StatusCode::GONE,
                Json(ErrorResponse { error: "Cursor expired, please restart query".to_string() }),
            ).into_response();
        }
    }
} else {
    // No cursor — open latest version
    match deltalake::open_table(&state.config.table_path).await {
        Ok(t) => {
            let v = t.version();
            (t, v, None)
        }
        Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse { error: "Query service unavailable".to_string() }),
            ).into_response();
        }
        Err(e) => {
            warn!(error = %e, "Failed to open delta table for query");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: "Internal query error".to_string() }),
            ).into_response();
        }
    }
};
```

Cursor filter in SQL — add after other WHERE clauses, using the already-decoded cursor:
```rust
if let Some(ref cursor) = decoded_cursor {
    sql.push_str(&format!(" AND cert_index > {}", cursor.k));
}
```

**Note on `cert_index` type:** The Delta schema defines `cert_index` as `UInt64`. The cursor encodes `k` as `u64` and embeds it as a numeric literal in SQL (`cert_index > 12345`). DataFusion handles `UInt64` columns correctly in comparisons and ORDER BY with integer literals. The `CertResult` struct uses `cert_index: u64` matching the schema type. No special handling is needed, but if issues arise during testing, verify that the literal value in SQL is being compared correctly against the `UInt64` column.

Pagination logic — fetch limit+1, detect has_more:
```rust
// Use limit + 1 for has_more detection
let fetch_limit = limit + 1;
sql.push_str(&format!(" ORDER BY cert_index LIMIT {}", fetch_limit));

// ... execute query, build results ...

let has_more = results.len() > limit;
if has_more {
    results.truncate(limit);
}

let next_cursor = if has_more {
    results.last().map(|r| encode_cursor(version, r.cert_index))
} else {
    None
};

Json(QueryResponse {
    version,
    results,
    next_cursor,
    has_more,
}).into_response()
```

**Testing:**
Tests must verify each AC listed above. Create a Delta table with enough records to span multiple pages, then exercise pagination:

- delta-query-api.AC2.1: Insert 10 records, query with `limit=3`, verify `has_more: true` and `next_cursor` is present
- delta-query-api.AC2.2: Use `next_cursor` from AC2.1's response, verify next 3 records are returned from the same version
- delta-query-api.AC2.3: Continue paginating until `has_more: false` and `next_cursor` is absent
- delta-query-api.AC2.4: Pass garbage string as cursor, verify 400
- delta-query-api.AC2.5: Encode a cursor with a non-existent version (e.g., version 999999), verify 410
- delta-query-api.AC2.6: Verify `limit` parameter is clamped to `max_results_per_page`; test with limit=1000 when max is 500

Follow the existing test pattern from `src/backfill.rs`: create temp dir, seed with records, exercise functions.

**Verification:**
Run: `cargo test query::tests`
Expected: All query tests pass

Run: `cargo test`
Expected: All tests pass

**Commit:** `feat(query-api): implement cursor pagination pinned to Delta versions`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add cursor unit tests

**Files:**
- Modify: `src/query.rs` (add unit tests for cursor encode/decode)

**Testing:**
Add unit tests for the cursor encoding/decoding functions:

- `encode_cursor(7808, 12345)` produces valid Base64 that decodes back to `{v: 7808, k: 12345}`
- `decode_cursor` with valid Base64 JSON returns correct Cursor
- `decode_cursor` with invalid Base64 returns Err
- `decode_cursor` with valid Base64 but invalid JSON returns Err
- Round-trip: encode then decode preserves values

**Verification:**
Run: `cargo test query::tests`
Expected: All query tests pass

**Commit:** `test(query-api): add cursor encode/decode unit tests`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->
