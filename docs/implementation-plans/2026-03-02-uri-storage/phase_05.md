# URI-Based Storage Backend — Phase 5: Merge Path

**Goal:** Merge operations work with S3-backed main and staging tables, including S3-aware staging cleanup.

**Architecture:** `run_merge()` receives `StorageConfig`, parses both the main table path and staging path as URIs, resolves storage options, and passes them through to table open calls. Staging cleanup uses `object_store` APIs for S3 paths instead of `std::fs::remove_dir_all()`. All callers of `run_merge()` updated to pass `StorageConfig`.

**Tech Stack:** Rust, deltalake DeltaTableBuilder, object_store (list + delete_stream), std::collections::HashMap

**Scope:** Phase 5 of 7 from original design

**Codebase verified:** 2026-03-02

---

## Acceptance Criteria Coverage

This phase implements:

### uri-storage.AC3: Delta code paths work over S3
- **uri-storage.AC3.6 Success:** Merge reads S3 staging table and merges into S3 main table
- **uri-storage.AC3.8 Success:** All `file://` paths continue to work identically to previous bare-path behavior

**Verification approach:** AC3.6 is validated in Phase 7 integration tests. AC3.8 verified by existing unit tests passing.

---

## Codebase verification findings

- ✓ `run_merge()` at `src/backfill.rs:942-1123` — takes `(config: Config, staging_path: String, shutdown: CancellationToken) -> i32`
- ✓ `deltalake::open_table(&staging_path)` at line 952 — opens staging table
- ✓ `open_or_create_table(&config.delta_sink.table_path, &schema)` at line 965 — opens/creates main table
- ✓ `std::fs::remove_dir_all(&staging_path)` at line 1112 — staging cleanup, NOT S3-aware
- ✓ Main table path comes from `config.delta_sink.table_path`, staging path is a function parameter
- ✓ Call site in `src/main.rs:97-102` — passes `config`, `cli_args.staging_path.unwrap()`, `shutdown_token`
- ✓ `object_store` crate available via deltalake dependency — provides `list()` and `delete_stream()` for S3 cleanup
- ✓ `DeltaTable::object_store()` returns `Arc<dyn ObjectStore>` for accessing the underlying store

## External dependency findings

- ✓ `ObjectStore::list(prefix)` returns `BoxStream<'static, Result<ObjectMeta>>` — recursively lists all objects under prefix
- ✓ `ObjectStore::delete_stream(paths)` accepts stream of `Path`, uses bulk operations on S3 (batches of 1000)
- ✓ `object_store::path::Path::from("prefix")` creates a path from string
- ✓ No `delete_all` convenience method exists — must compose `list()` + `delete_stream()` manually
- ✓ S3 `delete()` returns `Ok(())` for non-existent objects (no special handling needed)

---

<!-- START_TASK_1 -->
### Task 1: Update run_merge() to use storage options for table opening

**Files:**
- Modify: `src/backfill.rs:942-970` — add storage options, replace `deltalake::open_table()` and `open_or_create_table()` calls
- Modify: `src/main.rs:97-102` — update `run_merge()` call site

**Implementation:**

1. Add imports at top of `src/backfill.rs` (if not already present from Phase 4):
```rust
use crate::config::{StorageConfig, parse_table_uri, resolve_storage_options};
use deltalake::DeltaTableBuilder;
```

2. Update `run_merge()` signature at line 942:
```rust
pub async fn run_merge(
    config: Config,
    staging_path: String,
    shutdown: CancellationToken,
) -> i32 {
```
No signature change needed — `Config` already contains `StorageConfig` after Phase 1. The storage options are resolved inside the function.

3. After `let schema = delta_schema();` (around line 949), add URI parsing and storage options resolution for BOTH the main table path and the staging path. Parse them independently to correctly handle mixed-backend cases (e.g., local staging + S3 main, or vice versa):
```rust
let main_storage_options = match parse_table_uri(&config.delta_sink.table_path) {
    Ok(location) => resolve_storage_options(&location, &config.storage),
    Err(e) => {
        error!(error = %e, "Invalid delta_sink.table_path URI");
        return 1;
    }
};

let staging_storage_options = match parse_table_uri(&staging_path) {
    Ok(location) => resolve_storage_options(&location, &config.storage),
    Err(e) => {
        error!(error = %e, "Invalid staging path URI");
        return 1;
    }
};
```

4. Replace `deltalake::open_table(&staging_path)` at line 952 with (using `staging_storage_options`):
```rust
let staging_table = match DeltaTableBuilder::from_uri(&staging_path)
    .with_storage_options(staging_storage_options.clone())
    .load()
    .await
{
    Ok(t) => t,
    Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
        info!(staging_path = %staging_path, "staging table does not exist, nothing to merge");
        return 0;
    }
    Err(e) => {
        warn!(error = %e, "failed to open staging table");
        return 1;
    }
};
```

5. Replace `open_or_create_table(&config.delta_sink.table_path, &schema)` at line 965 with:
```rust
let main_table = match open_or_create_table(&config.delta_sink.table_path, &schema, main_storage_options.clone()).await {
    Ok(t) => t,
    Err(e) => {
        warn!(error = %e, "failed to open main table");
        return 1;
    }
};
```

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: Compiles (staging cleanup still uses `remove_dir_all` until Task 2)
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add S3-aware staging cleanup and update test callers

**Files:**
- Modify: `src/backfill.rs:1112-1121` — replace `remove_dir_all` with URI-aware cleanup
- Modify: `src/backfill.rs` (test module) — update any test callers of `run_merge()` if they exist

**Implementation:**

1. Add `object_store` and `futures` imports at top of `src/backfill.rs`:
```rust
use object_store::ObjectStore as _;
use futures::TryStreamExt;
```
Note: `futures` is already a dependency (used for `StreamExt`). `object_store` is re-exported by `deltalake`.

2. Create a helper function for S3-aware cleanup, placed before `run_merge()`:
```rust
/// Remove all objects under a staging path. For local paths, uses std::fs::remove_dir_all.
/// For S3 paths, lists and deletes all objects under the prefix via object_store APIs.
async fn cleanup_staging(staging_path: &str, storage_options: &HashMap<String, String>) {
    if staging_path.starts_with("s3://") {
        // S3 cleanup: list all objects and delete them
        match DeltaTableBuilder::from_uri(staging_path)
            .with_storage_options(storage_options.clone())
            .build()
        {
            Ok(table) => {
                let store = table.object_store();
                // DeltaTableBuilder::build() returns a table whose object_store() is
                // already scoped to the table prefix within the S3 bucket.
                // Passing None lists all objects under that prefix.
                let objects: Vec<object_store::path::Path> = match store
                    .list(None)
                    .map_ok(|meta| meta.location)
                    .try_collect()
                    .await
                {
                    Ok(paths) => paths,
                    Err(e) => {
                        warn!(error = %e, staging_path = %staging_path, "failed to list staging objects for cleanup");
                        return;
                    }
                };

                if objects.is_empty() {
                    info!(staging_path = %staging_path, "no staging objects to clean up");
                    return;
                }

                let count = objects.len();
                let path_stream = futures::stream::iter(objects.into_iter().map(Ok));
                let results: Vec<_> = store
                    .delete_stream(Box::pin(path_stream))
                    .collect::<Vec<_>>()
                    .await;

                let errors: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
                if errors.is_empty() {
                    info!(staging_path = %staging_path, objects_deleted = count, "S3 staging objects deleted");
                } else {
                    warn!(
                        staging_path = %staging_path,
                        errors = errors.len(),
                        total = count,
                        "some S3 staging objects failed to delete"
                    );
                }
            }
            Err(e) => {
                warn!(error = %e, staging_path = %staging_path, "failed to build table for staging cleanup");
            }
        }
    } else {
        // Local cleanup
        match std::fs::remove_dir_all(staging_path) {
            Ok(_) => {
                info!(staging_path = %staging_path, "staging directory deleted");
            }
            Err(e) => {
                warn!(error = %e, staging_path = %staging_path, "failed to delete staging directory");
            }
        }
    }
}
```

3. Replace the `std::fs::remove_dir_all` block at line 1112 with:
```rust
cleanup_staging(&staging_path, &staging_storage_options).await;
```

4. Add `use futures::StreamExt as _;` to the imports if `StreamExt` is not already imported for the `collect()` call on the delete_stream result. Check existing imports — `use futures::stream::StreamExt;` is already present at line 14, so `collect()` should work.

5. Update test callers in `mod tests` if any tests call `run_merge()` directly — pass appropriate config with default `StorageConfig`.

**Verification:**
Run: `cargo test` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All tests pass

**Commit:** `feat(merge): pass storage options through merge path with S3-aware staging cleanup`
<!-- END_TASK_2 -->
