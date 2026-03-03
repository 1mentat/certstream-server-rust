# URI-Based Storage Backend — Phase 2: Cargo Features and open_or_create_table

**Goal:** Enable S3 in deltalake and update the shared table open/create function to accept storage options.

**Architecture:** Add `s3` feature to deltalake, change `open_or_create_table()` to use `DeltaTableBuilder::from_uri().with_storage_options()` for opening and `CreateBuilder` with `.with_storage_options()` for creation. All existing callers pass empty `HashMap` (no behavioral change for local paths).

**Tech Stack:** Rust, deltalake 0.25 (s3 feature), DeltaTableBuilder, std::collections::HashMap

**Scope:** Phase 2 of 7 from original design

**Codebase verified:** 2026-03-02

---

## Acceptance Criteria Coverage

This phase implements (but cannot unit-test without S3 endpoint; integration tests in Phase 7):

### uri-storage.AC3: Delta code paths work over S3
- **uri-storage.AC3.1 Success:** `open_or_create_table()` opens existing S3-backed table
- **uri-storage.AC3.2 Success:** `open_or_create_table()` creates new table at S3 location when none exists
- **uri-storage.AC3.8 Success:** All `file://` paths continue to work identically to previous bare-path behavior

**Verification approach:** Existing unit tests passing with `HashMap::new()` proves AC3.8 (backward compatibility). AC3.1 and AC3.2 are validated in Phase 7 integration tests.

---

## Codebase verification findings

- ✓ `Cargo.toml:51`: `deltalake = { version = "0.25", features = ["datafusion"] }` — need to add `"s3"`
- ✓ `open_or_create_table()` at `src/delta_sink.rs:236-257` — signature: `(table_path: &str, schema: &Arc<Schema>) -> Result<DeltaTable, DeltaTableError>`
- ✓ Uses `deltalake::open_table(table_path)` for opening (line 241)
- ✓ Uses `CreateBuilder::new().with_location(table_path)` for creation (line 247)
- ✓ Production callers: delta_sink.rs (lines 553, 571, 660), backfill.rs (lines 458, 962)
- ✓ Test callers: ~60 across delta_sink.rs, backfill.rs, query.rs — all pass `(&path_str, &schema)`
- + `DeltaTableBuilder` not currently imported — needs `use deltalake::DeltaTableBuilder;`

## External dependency findings

- ✓ `deltalake` s3 feature enables `object_store` with AWS/S3 support
- ✓ `DeltaTableBuilder::from_uri(path).with_storage_options(opts).load().await` opens tables with credentials
- ✓ `CreateBuilder::new().with_location(path).with_storage_options(opts)...await` creates tables with credentials
- ✓ Storage option keys: `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `conditional_put`, `AWS_ALLOW_HTTP`
- ✓ Empty `HashMap` works for local paths (no special configuration needed)
- ✓ `DeltaTableBuilder::from_uri()` accepts both bare paths and `s3://` URIs

---

<!-- START_TASK_1 -->
### Task 1: Add s3 feature to Cargo.toml and update open_or_create_table implementation

**Files:**
- Modify: `Cargo.toml:51` — add `"s3"` to deltalake features
- Modify: `src/delta_sink.rs:236-257` — new function signature and implementation
- Modify: `src/delta_sink.rs` imports — add `DeltaTableBuilder`, `HashMap`

**Implementation:**

1. Update `Cargo.toml` line 51:
```toml
deltalake = { version = "0.25", features = ["datafusion", "s3"] }
```

2. Add import in `src/delta_sink.rs` near the existing deltalake imports:
```rust
use deltalake::DeltaTableBuilder;
use std::collections::HashMap;
```

3. Also add a `TableLocation::as_uri()` helper to `src/config.rs` (after the `TableLocation` enum added in Phase 1):
```rust
impl TableLocation {
    /// Returns the path/URI string for use with DeltaTableBuilder::from_uri().
    /// For Local, this returns the stripped path (no file:// prefix) because
    /// DeltaTableBuilder::from_uri() accepts bare paths for local filesystem.
    /// For S3, this returns the full s3:// URI.
    pub fn as_uri(&self) -> &str {
        match self {
            TableLocation::Local { path } => path,
            TableLocation::S3 { uri } => uri,
        }
    }
}
```

4. Update `open_or_create_table()` at line 236. New signature and implementation:

```rust
pub async fn open_or_create_table(
    table_path: &str,
    schema: &Arc<Schema>,
    storage_options: HashMap<String, String>,
) -> Result<DeltaTable, DeltaTableError> {
    // First, try to open an existing table
    match DeltaTableBuilder::from_uri(table_path)
        .with_storage_options(storage_options.clone())
        .load()
        .await
    {
        Ok(table) => Ok(table),
        Err(DeltaTableError::NotATable(_)) => {
            // Table doesn't exist, create a new one
            let struct_fields = arrow_schema_to_delta_struct_fields(schema);

            let table = CreateBuilder::new()
                .with_location(table_path)
                .with_storage_options(storage_options)
                .with_columns(struct_fields)
                .with_partition_columns(vec!["seen_date"])
                .await?;

            Ok(table)
        }
        Err(e) => Err(e),
    }
}
```

Key changes from current code:
- Added `storage_options: HashMap<String, String>` parameter
- Replaced `deltalake::open_table(table_path)` with `DeltaTableBuilder::from_uri(table_path).with_storage_options(storage_options.clone()).load()`
- Added `.with_storage_options(storage_options)` to `CreateBuilder` chain

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: Compiles (tests will fail until callers are updated in Task 2)

**Commit:** Do not commit yet (callers are broken)
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update all callers to pass HashMap::new()

**Files:**
- Modify: `src/delta_sink.rs` — update 3 production callers (lines 553, 571, 660) and ~8 test callers
- Modify: `src/backfill.rs` — update 2 production callers (lines 458, 962) and ~35 test callers
- Modify: `src/query.rs` — update ~18 test callers

**Implementation:**

This is a mechanical find-and-replace. Every call to `open_or_create_table(&path, &schema)` becomes `open_or_create_table(&path, &schema, HashMap::new())`.

Ensure `use std::collections::HashMap;` is present in:
- `src/backfill.rs` — in both the main module imports AND in the `mod tests` imports
- `src/query.rs` — in the `mod tests` imports
- `src/delta_sink.rs` — already added in Task 1; also add to `mod tests` imports

**Production callers in delta_sink.rs:**
- Line 553: `open_or_create_table(&table_path, schema)` → `open_or_create_table(&table_path, schema, HashMap::new())`
- Line 571: `open_or_create_table(&table_path, schema)` → `open_or_create_table(&table_path, schema, HashMap::new())`
- Line 660: `open_or_create_table(&config.table_path, &schema)` → `open_or_create_table(&config.table_path, &schema, HashMap::new())`

**Production callers in backfill.rs:**
- Line 458: `open_or_create_table(&table_path, &schema)` → `open_or_create_table(&table_path, &schema, HashMap::new())`
- Line 962: `open_or_create_table(&config.delta_sink.table_path, &schema)` → `open_or_create_table(&config.delta_sink.table_path, &schema, HashMap::new())`

**All test callers** across delta_sink.rs, backfill.rs, query.rs follow the same pattern. Use find-and-replace:
- Find: `open_or_create_table(&` followed by two arguments closing with `)`
- Replace: same but with `, HashMap::new()` before the closing `)`

**Note:** The production callers will be updated to pass real storage options in Phases 3-6. For now, `HashMap::new()` preserves backward compatibility.

**Verification:**
Run: `cargo test` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All 359 tests pass (identical behavior — empty HashMap = no storage options = local filesystem)

**Commit:** `feat(delta): update open_or_create_table to accept storage options for S3 support`
<!-- END_TASK_2 -->
