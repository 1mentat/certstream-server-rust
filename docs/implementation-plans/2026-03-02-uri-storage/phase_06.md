# URI-Based Storage Backend — Phase 6: Query API Read Path

**Goal:** Query API reads from S3-backed Delta tables using `DeltaTableBuilder` with storage options.

**Architecture:** `QueryApiState` gains a `StorageConfig` reference. The query handler resolves storage options at startup and uses `DeltaTableBuilder` instead of `deltalake::open_table()` and `deltalake::open_table_with_version()`. DataFusion `SessionContext` registration is unchanged — it receives a `DeltaTable` handle regardless of backend. The `std::path::Path::exists()` check in `main.rs` is made conditional on local paths.

**Tech Stack:** Rust, deltalake DeltaTableBuilder, std::collections::HashMap

**Scope:** Phase 6 of 7 from original design

**Codebase verified:** 2026-03-02

---

## Acceptance Criteria Coverage

This phase implements:

### uri-storage.AC3: Delta code paths work over S3
- **uri-storage.AC3.7 Success:** Query API reads and searches S3-backed Delta table with pagination
- **uri-storage.AC3.8 Success:** All `file://` paths continue to work identically to previous bare-path behavior

**Verification approach:** AC3.7 is validated in Phase 7 integration tests. AC3.8 verified by existing unit tests passing.

---

## Codebase verification findings

- ✓ `QueryApiState` at `src/query.rs:52-54` — holds `config: QueryApiConfig`
- ✓ `handle_query_certs()` at `src/query.rs:152` — takes `State(state): State<Arc<QueryApiState>>`
- ✓ `deltalake::open_table_with_version(&state.config.table_path, cursor.v)` at line 202 — cursor path
- ✓ `deltalake::open_table(&state.config.table_path)` at line 230 — latest version path
- ✓ Query API initialization in `src/main.rs:563-578` — constructs `QueryApiState` with `config.query_api.clone()`
- ✓ `std::path::Path::new(&config.query_api.table_path).exists()` check at main.rs:565-566 — only valid for local paths
- ✓ `query_api_router()` at `src/query.rs:146-150` — creates router with state
- ✓ Imports in query.rs: `use deltalake::DeltaTableError;` present, `DeltaTableBuilder` not yet imported
- ✓ ~19 test callers of `open_or_create_table` in query.rs test module (already updated in Phase 2)
- ✓ No test callers of `deltalake::open_table()` or `open_table_with_version()` in tests (tests use `open_or_create_table` directly)

---

<!-- START_TASK_1 -->
### Task 1: Update QueryApiState to carry storage options and replace table open calls

**Files:**
- Modify: `src/query.rs:1-16` — add imports for `DeltaTableBuilder`, `HashMap`
- Modify: `src/query.rs:52-54` — add `storage_options` field to `QueryApiState`
- Modify: `src/query.rs:198-240` — replace `deltalake::open_table*` calls with `DeltaTableBuilder`
- Modify: `src/main.rs:563-578` — parse URI, resolve storage options, pass to `QueryApiState`

**Implementation:**

1. Add imports at top of `src/query.rs`:
```rust
use deltalake::DeltaTableBuilder;
use std::collections::HashMap;
```

2. Update `QueryApiState` at line 52:
```rust
pub struct QueryApiState {
    pub config: QueryApiConfig,
    pub storage_options: HashMap<String, String>,
}
```

3. Replace `deltalake::open_table_with_version(&state.config.table_path, cursor.v)` at line 202 with:
```rust
DeltaTableBuilder::from_uri(&state.config.table_path)
    .with_storage_options(state.storage_options.clone())
    .with_version(cursor.v)
    .load()
    .await
```

4. Replace `deltalake::open_table(&state.config.table_path)` at line 230 with:
```rust
DeltaTableBuilder::from_uri(&state.config.table_path)
    .with_storage_options(state.storage_options.clone())
    .load()
    .await
```

5. Update `src/main.rs:563-578` to parse URI and resolve storage options:
```rust
if config.query_api.enabled {
    // Check table path accessibility (non-fatal warning) — only for local paths
    if !config.query_api.table_path.starts_with("s3://") {
        let table_path = std::path::Path::new(&config.query_api.table_path);
        if !table_path.exists() {
            warn!(
                table_path = %config.query_api.table_path,
                "Query API table path does not exist yet; queries will return 503 until data is written"
            );
        }
    }

    let query_storage_options = match parse_table_uri(&config.query_api.table_path) {
        Ok(location) => resolve_storage_options(&location, &config.storage),
        Err(e) => {
            error!(error = %e, "Invalid query_api.table_path URI");
            std::process::exit(1);
        }
    };

    let query_api_state = Arc::new(query::QueryApiState {
        config: config.query_api.clone(),
        storage_options: query_storage_options,
    });
    let query_router = query::query_api_router(query_api_state);
    app = app.merge(query_router);
    info!("Query API enabled");
}
```

6. Add imports in `src/main.rs` if not already present:
```rust
use crate::config::{parse_table_uri, resolve_storage_options};
```

7. Update test code in `src/query.rs` `mod tests` that constructs `QueryApiState` to include `storage_options: HashMap::new()`:
```rust
let state = Arc::new(QueryApiState {
    config: QueryApiConfig { ... },
    storage_options: HashMap::new(),
});
```

**Verification:**
Run: `cargo test` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All tests pass

**Commit:** `feat(query): use DeltaTableBuilder with storage options for S3 query support`
<!-- END_TASK_1 -->
