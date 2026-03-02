# URI-Based Storage Backend — Phase 1: URI Parsing and Storage Config

**Goal:** Add URI parsing, storage config structs, and validation — no Delta code path changes yet.

**Architecture:** A `StorageConfig` with optional per-backend sections (`S3StorageConfig`) provides credentials. A `TableLocation` enum (Local | S3) and two helper functions (`parse_table_uri()`, `resolve_storage_options()`) form the single point of URI interpretation. All existing `table_path` fields become URI strings validated at startup.

**Tech Stack:** Rust, serde (Deserialize), std::collections::HashMap

**Scope:** Phase 1 of 7 from original design

**Codebase verified:** 2026-03-02

---

## Acceptance Criteria Coverage

This phase implements and tests:

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

---

## Codebase verification findings

- ✓ `src/config.rs` exists (1538 lines) with all expected patterns: `#[derive(Deserialize)]` structs, `Config::load()` env var overrides, `Config::validate()` returning `Result<(), Vec<ConfigValidationError>>`
- ✓ `ConfigValidationError` struct at line 476 with `field: String` and `message: String`
- ✓ `DeltaSinkConfig` at line 288 with `table_path: String` (default `"./data/certstream"`)
- ✓ `QueryApiConfig` at line 330 with `table_path: String` (default `"./data/certstream"`)
- ✓ `YamlConfig` at line 436 uses `Option<SubConfig>` pattern for sub-configs
- ✓ `Config` struct at line 412 uses non-Option fields for sub-configs
- ✓ Env var pattern: `if let Ok(v) = env::var("CERTSTREAM_...") { field = v; }` (lines 607-662)
- ✓ `config.example.yaml` exists with bare paths at lines 59 and 68
- ✓ `src/cli.rs` exists with `staging_path: Option<String>` at line 15
- ✓ No existing `StorageConfig`, `S3StorageConfig`, `TableLocation`, `parse_table_uri`, or `resolve_storage_options`
- ✓ Tests are inline `#[cfg(test)] mod tests {}` with local helpers, no mocking libraries, real Delta tables in `/tmp/`
- ✓ `test_config()` helper at line 856 constructs Config with all fields

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Add StorageConfig, S3StorageConfig structs and Config integration

**Files:**
- Modify: `src/config.rs:406` — add structs after `ZerobusSinkConfig` Default impl
- Modify: `src/config.rs:436` — add `storage` field to `YamlConfig`
- Modify: `src/config.rs:412` — add `storage` field to `Config`
- Modify: `src/config.rs:607-662` — add env var overrides for storage.s3 fields
- Modify: `src/config.rs:664-685` — add `storage` to `Config` constructor
- Modify: `src/config.rs:856-878` — add `storage` to `test_config()` helper

**Implementation:**

Add two new config structs following the existing pattern (`DeltaSinkConfig` at line 288 is the model):

`S3StorageConfig`:
- Fields: `endpoint: String`, `region: String`, `access_key_id: String`, `secret_access_key: String`, `conditional_put: Option<String>`, `allow_http: Option<bool>`
- All fields `#[serde(default)]` (empty string defaults for strings, `None` for Option fields)
- `#[derive(Debug, Clone, Default, Deserialize)]`

`StorageConfig`:
- Field: `s3: Option<S3StorageConfig>`
- `#[serde(default)]` on the field
- `#[derive(Debug, Clone, Default, Deserialize)]`

Add to `YamlConfig` (line ~468, before closing brace):
```rust
#[serde(default)]
storage: Option<StorageConfig>,
```

Add to `Config` struct (line ~432, before `config_path`):
```rust
pub storage: StorageConfig,
```

Add env var overrides in `Config::load()` after zerobus overrides (after line 662), following the existing `if let Ok(v) = env::var(...)` pattern:
```rust
let mut storage = yaml_config.storage.unwrap_or_default();
if let Some(ref mut s3) = storage.s3 {
    if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_ENDPOINT") {
        s3.endpoint = v;
    }
    if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_REGION") {
        s3.region = v;
    }
    if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID") {
        s3.access_key_id = v;
    }
    if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY") {
        s3.secret_access_key = v;
    }
    if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_CONDITIONAL_PUT") {
        s3.conditional_put = Some(v);
    }
    if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_ALLOW_HTTP") {
        s3.allow_http = v.parse().ok();
    }
} else {
    // If no s3 section in YAML, check if env vars want to create one.
    // CERTSTREAM_STORAGE_S3_ENDPOINT is the trigger: if it's empty or unset,
    // no S3 config is created even if other S3 env vars are set.
    // This is intentional — endpoint is required for any S3 operation.
    let endpoint = env::var("CERTSTREAM_STORAGE_S3_ENDPOINT").unwrap_or_default();
    if !endpoint.is_empty() {
        storage.s3 = Some(S3StorageConfig {
            endpoint,
            region: env::var("CERTSTREAM_STORAGE_S3_REGION").unwrap_or_default(),
            access_key_id: env::var("CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID").unwrap_or_default(),
            secret_access_key: env::var("CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY").unwrap_or_default(),
            conditional_put: env::var("CERTSTREAM_STORAGE_S3_CONDITIONAL_PUT").ok(),
            allow_http: env::var("CERTSTREAM_STORAGE_S3_ALLOW_HTTP").ok().and_then(|v| v.parse().ok()),
        });
    }
}
```

Add `storage` to the Config constructor (in the `Self { ... }` block at line 664) and to `test_config()` (line 856) as `storage: StorageConfig::default()`.

**Verification:**
Run: `cargo test --lib` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All existing 359 tests still pass (no behavioral changes, only new structs added)

**Commit:** `feat(config): add StorageConfig and S3StorageConfig structs with env var overrides`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Tests for StorageConfig defaults and deserialization

**Verifies:** uri-storage.AC2.1, uri-storage.AC2.2, uri-storage.AC2.5

**Files:**
- Modify: `src/config.rs` — add tests to `mod tests` block (after existing tests, before closing `}`)

**Testing:**

Tests must verify each AC listed above. Add to the existing `mod tests` block in `src/config.rs`:

- uri-storage.AC2.1: Deserialize a YAML string with `storage.s3` section containing all fields, verify all fields populated correctly
- uri-storage.AC2.2: Deserialize a YAML string with no `storage` section at all, verify `StorageConfig::default()` has `s3: None`
- uri-storage.AC2.5: Test env var override pattern — set `CERTSTREAM_STORAGE_S3_ENDPOINT` env var, verify it overrides the config value (follow existing env var test pattern at lines 1134-1165 using `unsafe { env::set_var(...) }` / `env::remove_var(...)`)
- Edge case: Set `CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID` env var WITHOUT setting `CERTSTREAM_STORAGE_S3_ENDPOINT`, verify no S3 config is created (endpoint is the trigger for env-var-only S3 config creation)

Follow the existing test patterns: `test_zerobus_sink_config_defaults` (line 1090) for defaults, `test_zerobus_sink_config_yaml_partial` (line 1115) for YAML deserialization, `test_zerobus_sink_config_env_var_endpoint` (line 1149) for env var tests.

**Verification:**
Run: `cargo test storage` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All new tests pass

**Commit:** `test(config): add StorageConfig defaults, deserialization, and env var tests`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->
<!-- START_TASK_3 -->
### Task 3: Add TableLocation enum and parse_table_uri() function

**Files:**
- Modify: `src/config.rs` — add enum and function after the StorageConfig structs (added in Task 1)

**Implementation:**

Add `TableLocation` enum:
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum TableLocation {
    Local { path: String },
    S3 { uri: String },
}
```

Add `parse_table_uri()` function:
```rust
pub fn parse_table_uri(uri: &str) -> Result<TableLocation, String> {
    if uri.is_empty() {
        return Err("Table path cannot be empty".to_string());
    }

    if let Some(path) = uri.strip_prefix("file://") {
        Ok(TableLocation::Local { path: path.to_string() })
    } else if uri.starts_with("s3://") {
        Ok(TableLocation::S3 { uri: uri.to_string() })
    } else if uri.contains("://") {
        let scheme = uri.split("://").next().unwrap_or("");
        Err(format!(
            "Unsupported URI scheme '{}://'. Supported schemes: file://, s3://",
            scheme
        ))
    } else {
        Err(format!(
            "Table path '{}' must use a URI scheme. Use 'file://{}' for local filesystem paths",
            uri, uri
        ))
    }
}
```

Key behaviors:
- `file:///absolute/path` → `Local { path: "/absolute/path" }` (strips `file://` prefix)
- `file://./relative/path` → `Local { path: "./relative/path" }` (preserves relative path)
- `s3://bucket/prefix` → `S3 { uri: "s3://bucket/prefix" }` (preserves full URI)
- Bare path `./data/certstream` → error suggesting `file://` prefix
- Unknown scheme `gcs://...` → error listing supported schemes
- Empty string → error

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: Compiles without errors

**Commit:** `feat(config): add TableLocation enum and parse_table_uri() function`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Tests for parse_table_uri() and add resolve_storage_options()

**Verifies:** uri-storage.AC1.1, uri-storage.AC1.2, uri-storage.AC1.3, uri-storage.AC1.4, uri-storage.AC1.5, uri-storage.AC1.6

**Files:**
- Modify: `src/config.rs` — add `resolve_storage_options()` function after `parse_table_uri()`
- Modify: `src/config.rs` — add tests to `mod tests` block

**Implementation:**

Add `resolve_storage_options()` function:
```rust
pub fn resolve_storage_options(
    location: &TableLocation,
    storage: &StorageConfig,
) -> HashMap<String, String> {
    match location {
        TableLocation::Local { .. } => HashMap::new(),
        TableLocation::S3 { .. } => {
            let mut opts = HashMap::new();
            if let Some(ref s3) = storage.s3 {
                if !s3.endpoint.is_empty() {
                    opts.insert("AWS_ENDPOINT_URL".to_string(), s3.endpoint.clone());
                }
                if !s3.region.is_empty() {
                    opts.insert("AWS_REGION".to_string(), s3.region.clone());
                }
                if !s3.access_key_id.is_empty() {
                    opts.insert("AWS_ACCESS_KEY_ID".to_string(), s3.access_key_id.clone());
                }
                if !s3.secret_access_key.is_empty() {
                    opts.insert("AWS_SECRET_ACCESS_KEY".to_string(), s3.secret_access_key.clone());
                }
                if let Some(ref cp) = s3.conditional_put {
                    opts.insert("conditional_put".to_string(), cp.clone());
                }
                if let Some(allow) = s3.allow_http {
                    opts.insert("AWS_ALLOW_HTTP".to_string(), allow.to_string());
                }
            }
            opts
        }
    }
}
```

Add `use std::collections::HashMap;` at the top of `config.rs` if not already present.

**Testing:**

Tests must verify each AC listed above. Add to the existing `mod tests` block:

- uri-storage.AC1.1: `parse_table_uri("file:///absolute/path")` returns `Ok(TableLocation::Local { path: "/absolute/path".to_string() })`
- uri-storage.AC1.2: `parse_table_uri("file://./relative/path")` returns `Ok(TableLocation::Local { path: "./relative/path".to_string() })`
- uri-storage.AC1.3: `parse_table_uri("s3://bucket/prefix")` returns `Ok(TableLocation::S3 { uri: "s3://bucket/prefix".to_string() })`
- uri-storage.AC1.4: `parse_table_uri("./data/certstream")` returns `Err` containing text suggesting `file://` prefix
- uri-storage.AC1.5: `parse_table_uri("gcs://bucket/path")` returns `Err` containing "Unsupported URI scheme" and listing supported schemes
- uri-storage.AC1.6: `parse_table_uri("")` returns `Err` with "cannot be empty"

Also test `resolve_storage_options()`:
- Local location returns empty HashMap
- S3 location with full S3StorageConfig returns HashMap with `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- S3 location with `conditional_put` set includes it in the HashMap
- S3 location but `storage.s3` is `None` returns empty HashMap

**Verification:**
Run: `cargo test parse_table_uri` and `cargo test resolve_storage` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All new tests pass

**Commit:** `feat(config): add resolve_storage_options() and URI parsing tests`
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_B -->

<!-- START_SUBCOMPONENT_C (tasks 5-6) -->
<!-- START_TASK_5 -->
### Task 5: Add URI scheme validation to Config::validate()

**Verifies:** uri-storage.AC2.3, uri-storage.AC2.4, uri-storage.AC2.6

**Files:**
- Modify: `src/config.rs:688-813` — add validation checks to `Config::validate()`

**Implementation:**

Add the following validation checks inside `Config::validate()`, after the existing delta_sink validations (after line 756) and before the zerobus validations (line 758):

1. **Validate delta_sink.table_path is a valid URI** (when delta_sink is enabled):
```rust
if self.delta_sink.enabled {
    if let Err(e) = parse_table_uri(&self.delta_sink.table_path) {
        errors.push(ConfigValidationError {
            field: "delta_sink.table_path".to_string(),
            message: e,
        });
    }
}
```

2. **Validate query_api.table_path is a valid URI** (when query_api is enabled):
```rust
if self.query_api.enabled {
    if let Err(e) = parse_table_uri(&self.query_api.table_path) {
        errors.push(ConfigValidationError {
            field: "query_api.table_path".to_string(),
            message: e,
        });
    }
}
```

3. **Check S3 config presence when any S3 URI is used:**
```rust
let has_s3_uri = (self.delta_sink.enabled && self.delta_sink.table_path.starts_with("s3://"))
    || (self.query_api.enabled && self.query_api.table_path.starts_with("s3://"));

if has_s3_uri {
    match &self.storage.s3 {
        None => {
            errors.push(ConfigValidationError {
                field: "storage.s3".to_string(),
                message: "S3 storage configuration required when using s3:// URIs. Add a [storage.s3] section to config.yaml or set CERTSTREAM_STORAGE_S3_ENDPOINT env var.".to_string(),
            });
        }
        Some(s3) => {
            if s3.endpoint.is_empty() {
                errors.push(ConfigValidationError {
                    field: "storage.s3.endpoint".to_string(),
                    message: "S3 endpoint cannot be empty when using s3:// URIs".to_string(),
                });
            }
            if s3.region.is_empty() {
                errors.push(ConfigValidationError {
                    field: "storage.s3.region".to_string(),
                    message: "S3 region cannot be empty when using s3:// URIs".to_string(),
                });
            }
            if s3.access_key_id.is_empty() {
                errors.push(ConfigValidationError {
                    field: "storage.s3.access_key_id".to_string(),
                    message: "S3 access key ID cannot be empty when using s3:// URIs".to_string(),
                });
            }
            if s3.secret_access_key.is_empty() {
                errors.push(ConfigValidationError {
                    field: "storage.s3.secret_access_key".to_string(),
                    message: "S3 secret access key cannot be empty when using s3:// URIs".to_string(),
                });
            }
        }
    }
}
```

4. **Validate `--staging-path` CLI arg as URI in `main.rs`** (before dispatching to `run_backfill()` or `run_merge()`):

In `src/main.rs`, at the point where `staging_path` is used (before calling `run_backfill()` at the backfill dispatch and `run_merge()` at the merge dispatch), add:
```rust
if let Some(ref staging) = cli_args.staging_path {
    if let Err(e) = parse_table_uri(staging) {
        eprintln!("Error: invalid --staging-path: {}", e);
        std::process::exit(1);
    }
}
```

This validates the staging path at startup rather than deep inside `run_backfill()`, giving the user a clear error message immediately. Add `use crate::config::parse_table_uri;` to main.rs imports if not already present.

**Important:** The existing `test_config()` helper uses `DeltaSinkConfig::default()` which has `table_path: "./data/certstream"` (a bare path). Since validation only checks URIs when `enabled: true`, and `test_config()` has `enabled: false`, existing tests will not break. No change needed to the default table_path values in `DeltaSinkConfig` or `QueryApiConfig` — those defaults will be updated to use `file://` in Task 7 (config.example.yaml), but the struct defaults remain bare paths for backward compatibility until all code paths are updated in later phases.

**Verification:**
Run: `cargo test --lib` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All existing tests still pass (validation only triggers when features are enabled)

**Commit:** `feat(config): add URI scheme and S3 config validation`
<!-- END_TASK_5 -->

<!-- START_TASK_6 -->
### Task 6: Tests for Config::validate() URI validation

**Verifies:** uri-storage.AC2.1, uri-storage.AC2.2, uri-storage.AC2.3, uri-storage.AC2.4, uri-storage.AC2.6

**Files:**
- Modify: `src/config.rs` — add tests to `mod tests` block

**Testing:**

Tests must verify each AC listed above. Use the existing `test_config()` helper and override fields with `..test_config()` pattern (see existing tests at lines 1010-1034):

- uri-storage.AC2.1: Config with `delta_sink.enabled = true`, `delta_sink.table_path = "s3://bucket/path"`, and `storage.s3` populated with all required fields → `validate()` returns `Ok(())`
- uri-storage.AC2.2: Config with `delta_sink.enabled = true`, `delta_sink.table_path = "file://./data/certstream"`, and `storage.s3 = None` → `validate()` returns `Ok(())`
- uri-storage.AC2.3: Config with `delta_sink.enabled = true`, `delta_sink.table_path = "s3://bucket/path"`, and `storage.s3 = None` → `validate()` returns `Err` with error on field `"storage.s3"`
- uri-storage.AC2.4: Config with `delta_sink.enabled = true`, `delta_sink.table_path = "s3://bucket/path"`, and `storage.s3 = Some(S3StorageConfig { endpoint: "".to_string(), ... })` → `validate()` returns `Err` with error on field `"storage.s3.endpoint"`
- uri-storage.AC2.6: `parse_table_uri("file:///tmp/staging")` succeeds; `parse_table_uri("/tmp/staging")` fails with helpful error (this validates the same function used for `--staging-path`)
- Bare path validation: Config with `delta_sink.enabled = true`, `delta_sink.table_path = "./data/certstream"` (bare path) → `validate()` returns `Err` with error on field `"delta_sink.table_path"` suggesting `file://` prefix

**Verification:**
Run: `cargo test validate` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage`
Expected: All validation tests pass

**Commit:** `test(config): add URI validation tests for AC2.1-AC2.6`
<!-- END_TASK_6 -->
<!-- END_SUBCOMPONENT_C -->

<!-- START_TASK_7 -->
### Task 7: Update config.example.yaml

**Files:**
- Modify: `config.example.yaml:55-84` — add `storage` section, update `table_path` comments

**Implementation:**

1. Update the `delta_sink.table_path` comment at line 59 to show `file://` prefix:
```yaml
delta_sink:
  enabled: false                    # Off by default
  table_path: "file://./data/certstream"   # URI: file:// for local, s3:// for S3
```

2. Update the `query_api.table_path` comment at line 68:
```yaml
query_api:
  enabled: false                    # Off by default
  table_path: "file://./data/certstream"   # Same Delta table as delta_sink (URI format)
```

3. Add a new `storage` section after the `query_api` section (after line 71) and before the zerobus section (line 73):
```yaml
# Storage backend configuration
# Required when using s3:// URIs for table_path or staging_path
# Credentials should be provided via env vars: CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID, CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY
# storage:
#   s3:
#     endpoint: ""                        # S3-compatible endpoint URL
#     region: ""                          # AWS region (e.g., "auto" for Tigris)
#     access_key_id: ""                   # Access key (prefer env var)
#     secret_access_key: ""               # Secret key (prefer env var)
#     conditional_put: "etag"             # Atomic writes via ETag (required for Tigris)
#     allow_http: false                   # Allow non-HTTPS connections
```

**Verification:**
Run: `cargo run -- --validate-config` in `/data/mentat/certstream-server-rust/.worktrees/uri-storage` (with no config.yaml present, so it falls through to defaults)
Expected: No errors (defaults don't enable delta_sink or query_api, so URI validation is skipped)

**Commit:** `docs(config): update config.example.yaml with storage section and URI table paths`
<!-- END_TASK_7 -->
