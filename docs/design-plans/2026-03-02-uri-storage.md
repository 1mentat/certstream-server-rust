# URI-Based Storage Backend Design

## Summary

The project currently stores Delta Lake tables using bare filesystem paths (e.g., `./data/certstream`). This design adds support for URI-based storage paths so that all Delta table operations — live writes from the delta sink, backfill reads and writes, staging, merge, and query API reads — can target either the local filesystem (`file://`) or an S3-compatible object store (`s3://`). The first validated S3 target is Tigris, a globally distributed S3-compatible storage service.

The implementation is organized around two small, shared helpers: `parse_table_uri()`, which converts a URI string into a typed `TableLocation` enum, and `resolve_storage_options()`, which maps a `TableLocation` to the key-value credential map that the `deltalake` crate's `DeltaTableBuilder` accepts. These two functions become the single point of URI interpretation for the entire codebase. The rest of the Delta code paths change only at the point where a table is opened or created, leaving all write and read logic intact. A new shared `[storage]` config section holds per-backend credentials with full env var override support, and validation at startup rejects bare paths and catches missing credentials before any data is written.

## Definition of Done

1. **All Delta table paths accept URIs** — `s3://` for S3-compatible stores, `file://` for local filesystem. Bare paths without a scheme are rejected with a clear error.
2. **A shared `[storage]` config section with typed backends** — the storage type (e.g., `s3`, `file`) determines which fields are required and validated. Designed to be extensible for future backends (ADLSv2, GCS, etc.) without redesign. Env var overrides follow existing convention.
3. **All Delta code paths work over S3** — live delta_sink writes, backfill reads/writes (including staging), merge operations, and query API reads all function against an S3 backend.
4. **Tigris validated** — the implementation is tested against a Tigris bucket as the first S3-compatible target.

## Acceptance Criteria

### uri-storage.AC1: URI scheme parsing
- **uri-storage.AC1.1 Success:** `file:///absolute/path` parses to Local with path `/absolute/path`
- **uri-storage.AC1.2 Success:** `file://./relative/path` parses to Local with path `./relative/path`
- **uri-storage.AC1.3 Success:** `s3://bucket/prefix` parses to S3 with full URI preserved
- **uri-storage.AC1.4 Failure:** Bare path `./data/certstream` rejected with error suggesting `file://` prefix
- **uri-storage.AC1.5 Failure:** Unknown scheme `gcs://bucket/path` rejected with error listing supported schemes
- **uri-storage.AC1.6 Failure:** Empty string rejected

### uri-storage.AC2: Storage config validation
- **uri-storage.AC2.1 Success:** Config loads with `storage.s3` section when S3 URIs present
- **uri-storage.AC2.2 Success:** Config loads without `storage.s3` section when only `file://` URIs used
- **uri-storage.AC2.3 Failure:** S3 URI present but `storage.s3` missing — validation error
- **uri-storage.AC2.4 Failure:** S3 URI present but `storage.s3.endpoint` empty — validation error
- **uri-storage.AC2.5 Success:** Env vars override config.yaml S3 fields (`CERTSTREAM_STORAGE_S3_ENDPOINT`, etc.)
- **uri-storage.AC2.6 Success:** `--staging-path` CLI arg validated as URI with recognized scheme

### uri-storage.AC3: Delta code paths work over S3
- **uri-storage.AC3.1 Success:** `open_or_create_table()` opens existing S3-backed table
- **uri-storage.AC3.2 Success:** `open_or_create_table()` creates new table at S3 location when none exists
- **uri-storage.AC3.3 Success:** delta_sink writes batches to S3-backed Delta table
- **uri-storage.AC3.4 Success:** Backfill gap detection queries S3-backed Delta table via DataFusion
- **uri-storage.AC3.5 Success:** Backfill writer writes records to S3-backed Delta table (including staging)
- **uri-storage.AC3.6 Success:** Merge reads S3 staging table and merges into S3 main table
- **uri-storage.AC3.7 Success:** Query API reads and searches S3-backed Delta table with pagination
- **uri-storage.AC3.8 Success:** All `file://` paths continue to work identically to previous bare-path behavior
- **uri-storage.AC3.9 Failure:** S3 connection failure handled non-fatally (delta_sink logs and exits task, backfill exits code 1, query returns 503)

### uri-storage.AC4: Tigris validation
- **uri-storage.AC4.1 Success:** Integration test creates Delta table on Tigris bucket
- **uri-storage.AC4.2 Success:** Integration test writes batch and reads it back with matching data
- **uri-storage.AC4.3 Success:** Conditional put (etag) prevents concurrent write corruption

## Glossary

- **Delta Lake**: An open-source storage layer built on top of Parquet files that adds ACID transactions, schema enforcement, and versioned table history. Used here to archive Certificate Transparency records.
- **`deltalake` crate**: The Rust client library for Delta Lake. Provides `DeltaTableBuilder`, `DeltaTable`, `DeltaOps`, and related types for opening, creating, reading, and writing Delta tables.
- **DataFusion**: An in-process SQL query engine (used via the `deltalake` crate) that executes analytical queries against Delta tables, including gap detection and Query API searches.
- **`DeltaTableBuilder`**: The builder type in the `deltalake` crate used to open or create a Delta table. Accepts a URI string and an optional map of storage options.
- **storage options**: A `HashMap<String, String>` of key-value pairs (e.g., `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`) passed to `DeltaTableBuilder::with_storage_options()` to configure the underlying object store client.
- **`object_store`**: A Rust crate (used internally by `deltalake`) that abstracts filesystem and cloud object store access behind a common async API. Relevant for S3 staging cleanup where `std::fs::remove_dir_all` cannot be used.
- **S3-compatible object store**: Any storage service that implements the Amazon S3 REST API. Enables the same client code to target AWS S3, Tigris, MinIO, Cloudflare R2, and others.
- **Tigris**: A globally distributed, S3-compatible object storage service with no egress fees. First concrete S3 target validated by this design's integration tests.
- **conditional PUT (ETag)**: An S3 operation that atomically writes an object only if the current ETag matches an expected value. Used by the `deltalake` crate to prevent two writers from corrupting the Delta transaction log simultaneously.
- **staging table**: A separate Delta table written during backfill with `--staging-path` so that in-progress backfill data is isolated from the main table until a `--merge` step completes.
- **gap detection**: The backfill step that queries existing Delta table data using a SQL window function (`LEAD`) to find ranges of certificate indexes that are missing and need to be re-fetched.
- **URI scheme**: The prefix portion of a URI before `://` (e.g., `s3` in `s3://bucket/path`, `file` in `file:///local/path`). Used here to dispatch to the correct storage backend.
- **Parquet**: The columnar binary file format that Delta Lake uses to physically store table data.

## Architecture

URI-driven storage with keyed backend configuration. The URI scheme (`s3://`, `file://`) on each table path determines the storage backend. A shared `[storage]` config section provides per-backend credentials and options. A pair of helper functions — `parse_table_uri()` and `resolve_storage_options()` — form the single point of URI interpretation used by all Delta code paths.

**Data flow:**

```
config.yaml table_path (URI string)
  → parse_table_uri() → TableLocation enum (Local | S3)
  → resolve_storage_options(location, storage_config) → HashMap<String, String>
  → DeltaTableBuilder::from_uri(path).with_storage_options(opts)
  → DeltaTable handle (used by existing write/read logic unchanged)
```

**Config model:**

`StorageConfig` in `src/config.rs` with optional per-backend sections:

```rust
struct StorageConfig {
    s3: Option<S3StorageConfig>,
    // future: adls: Option<AdlsStorageConfig>,
}

struct S3StorageConfig {
    endpoint: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    conditional_put: Option<String>,
    allow_http: Option<bool>,
}
```

`TableLocation` enum:
- `TableLocation::Local { path: String }` — bare filesystem path (scheme stripped)
- `TableLocation::S3 { uri: String }` — full `s3://bucket/path` URI preserved

**Env var convention:** `CERTSTREAM_STORAGE_S3_ENDPOINT`, `CERTSTREAM_STORAGE_S3_REGION`, `CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID`, `CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY`, `CERTSTREAM_STORAGE_S3_CONDITIONAL_PUT`.

**Validation rules:**
- All `table_path` fields and `--staging-path` CLI arg must have a recognized scheme (`file://` or `s3://`). Bare paths rejected with error suggesting `file://` prefix.
- If any path uses `s3://`, `storage.s3` must be present with non-empty `endpoint`, `region`, `access_key_id`, `secret_access_key`.
- Unknown URI schemes rejected at validation time.

**Key constraint:** Storage options are bound at table open/create time. Once a `DeltaTable` handle exists, write operations inherit the storage config. This means `flush_buffer()` requires no signature change — only functions that open or create tables change.

## Existing Patterns

Investigation found the following patterns this design follows:

**Config pattern:** Flat structs with `#[derive(Deserialize)]`, per-field `#[serde(default = "fn")]` defaults, and explicit env var overrides in `Config::load()`. The new `StorageConfig` and `S3StorageConfig` follow this pattern exactly.

**Config validation:** `Config::validate()` returns `Vec<ConfigValidationError>` with `field` and `message` strings. URI scheme validation and S3 config presence checks follow this pattern.

**Table path threading:** Config field → function parameter → deltalake call. This design preserves this pattern but replaces the raw `&str` with `TableLocation` + `HashMap` at the boundary.

**Feature gating via `enabled` flag:** Optional features use `enabled: bool`. Storage backends don't use this pattern — their presence is inferred from URI scheme usage. This is a minor divergence: instead of `storage.s3.enabled`, the S3 backend activates when any URI uses `s3://`. This avoids redundancy (why configure S3 credentials and then set enabled: false?).

**Integration test gating:** The ZeroBus integration test gates on `feature = "integration"` with required env vars. The S3 integration test follows this same pattern.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: URI Parsing and Storage Config
**Goal:** Add URI parsing, storage config structs, and validation — no Delta code path changes yet.

**Components:**
- `StorageConfig` and `S3StorageConfig` structs in `src/config.rs` — serde deserialization, defaults, env var overrides
- `parse_table_uri()` and `resolve_storage_options()` helpers in `src/config.rs` (or a new `src/storage.rs` module)
- `TableLocation` enum
- Config validation updates in `Config::validate()` — URI scheme checks, S3 config presence check
- `config.example.yaml` — new `storage` section (commented out), updated `table_path` defaults to `file://` scheme

**Dependencies:** None (first phase)

**Done when:** Config loads with `file://` and `s3://` URIs, validation rejects bare paths and missing S3 config, unit tests pass for URI parsing and validation (covers `uri-storage.AC1.*`, `uri-storage.AC2.*`)
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Cargo Features and open_or_create_table
**Goal:** Enable S3 in deltalake and update the shared table open/create function to accept storage options.

**Components:**
- `Cargo.toml` — add `s3` feature to deltalake dependency
- `open_or_create_table()` in `src/delta_sink.rs` — new signature with `TableLocation` and `HashMap<String, String>`, uses `DeltaTableBuilder::from_uri().with_storage_options()` for opening and `CreateBuilder::new().with_location().with_storage_options()` for creation

**Dependencies:** Phase 1 (URI parsing and config structs)

**Done when:** `open_or_create_table()` works with both `file://` and `s3://` locations, existing `file://` unit tests still pass (covers `uri-storage.AC3.1`, `uri-storage.AC3.2`)
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Delta Sink Write Path
**Goal:** Live delta_sink writes to S3-backed tables.

**Components:**
- `run_delta_sink()` in `src/delta_sink.rs` — parse URI at startup, pass `TableLocation` and storage options to `open_or_create_table()`
- No changes to `flush_buffer()` (inherits storage config from table handle)

**Dependencies:** Phase 2 (updated `open_or_create_table`)

**Done when:** delta_sink can write batches to an S3-backed Delta table, existing local write path unchanged (covers `uri-storage.AC3.3`)
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Backfill Read/Write Paths
**Goal:** Backfill gap detection, fetcher-writer pipeline, and staging all work with S3 URIs.

**Components:**
- `detect_gaps()` in `src/backfill.rs` — use `DeltaTableBuilder` with storage options instead of `deltalake::open_table()`, for both main and staging table paths
- Backfill writer task in `src/backfill.rs` — pass parsed location and storage options to `open_or_create_table()`
- `--staging-path` CLI arg in `src/cli.rs` — validated as URI with recognized scheme

**Dependencies:** Phase 2 (updated `open_or_create_table`)

**Done when:** Backfill can detect gaps in and write to S3-backed tables, staging path supports S3 URIs (covers `uri-storage.AC3.4`, `uri-storage.AC3.5`)
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Merge Path
**Goal:** Merge operations work with S3-backed main and staging tables.

**Components:**
- `run_merge()` in `src/backfill.rs` — parse both main table path and staging path as URIs, pass storage options to table open calls
- Staging cleanup: `remove_dir_all` only applies to `file://` paths; S3 staging cleanup uses deltalake or object_store delete APIs

**Dependencies:** Phase 2 (updated `open_or_create_table`)

**Done when:** Merge reads from S3 staging, writes to S3 main table, staging cleanup works for both local and S3 paths (covers `uri-storage.AC3.6`)
<!-- END_PHASE_5 -->

<!-- START_PHASE_6 -->
### Phase 6: Query API Read Path
**Goal:** Query API reads from S3-backed Delta tables.

**Components:**
- Query handler in `src/query.rs` — use `DeltaTableBuilder` with storage options instead of `deltalake::open_table()` and `deltalake::open_table_with_version()`
- DataFusion `SessionContext` registration unchanged (receives `DeltaTable` handle regardless of backend)

**Dependencies:** Phase 2 (updated `open_or_create_table`)

**Done when:** Query API can search certificates in S3-backed Delta table, cursor-based pagination works against S3 table versions (covers `uri-storage.AC3.7`)
<!-- END_PHASE_6 -->

<!-- START_PHASE_7 -->
### Phase 7: Integration Testing with Tigris
**Goal:** Validate full read/write cycle against a real Tigris bucket.

**Components:**
- Integration test in `tests/` or `src/` gated behind `feature = "integration"` — requires env vars `CERTSTREAM_TEST_S3_ENDPOINT`, `CERTSTREAM_TEST_S3_BUCKET`, `CERTSTREAM_TEST_S3_ACCESS_KEY_ID`, `CERTSTREAM_TEST_S3_SECRET_ACCESS_KEY`
- Test exercises: create table, write batch, read back, verify data, delete table

**Dependencies:** Phases 3-6 (all Delta code paths updated)

**Done when:** Integration test passes against Tigris endpoint, confirming write and read round-trip (covers `uri-storage.AC4.*`)
<!-- END_PHASE_7 -->

## Additional Considerations

**Staging cleanup on S3:** The current merge path uses `std::fs::remove_dir_all()` to delete staging after successful merge. For S3 paths, this must use the object_store or deltalake API to list and delete objects under the staging prefix. This is a behavioral difference worth testing explicitly.

**Credential security:** S3 credentials in `config.yaml` are plaintext. The env var override path (`CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID`, etc.) is the recommended approach for production. The config.example.yaml should show empty strings with a comment directing users to env vars.

**`file://` relative paths:** `file://./data/certstream` uses a relative path. The URI parsing helper should preserve this as-is (strip `file://`, pass `./data/certstream` to deltalake). No path canonicalization needed — deltalake handles relative paths natively.
