# Named Targets Implementation Plan — Phase 1: TargetConfig and ResolvedTarget Structs

**Goal:** Define the config structures and resolution logic for named targets without changing any CLI or operations.

**Architecture:** Add `TargetConfig` and `ResolvedTarget` structs to `src/config.rs`, a `targets: HashMap<String, TargetConfig>` field on `Config`, a `resolve_target()` method that fills unspecified fields from `delta_sink` defaults and resolves storage options, plus YAML deserialization and env var loading for the `targets` section.

**Tech Stack:** Rust, serde, serde_yaml, std::collections::HashMap

**Scope:** 6 phases from original design (this is phase 1 of 6)

**Codebase verified:** 2026-03-05

---

## Acceptance Criteria Coverage

This phase implements and tests:

### named-targets.AC1: TargetConfig deserializes and resolves correctly
- **named-targets.AC1.1 Success:** TargetConfig with all fields specified deserializes from YAML with correct values
- **named-targets.AC1.2 Success:** TargetConfig with only `table_path` deserializes, optional fields are `None`
- **named-targets.AC1.3 Success:** `resolve_target()` fills `None` compression_level from `delta_sink.compression_level`
- **named-targets.AC1.4 Success:** `resolve_target()` fills `None` heavy_column_compression_level from `delta_sink.heavy_column_compression_level`
- **named-targets.AC1.5 Success:** `resolve_target()` fills `None` offline_batch_size from `delta_sink.offline_batch_size`
- **named-targets.AC1.6 Success:** `resolve_target()` uses target-level `StorageConfig` when present
- **named-targets.AC1.7 Success:** `resolve_target()` falls back to global `config.storage` when target has no storage config
- **named-targets.AC1.8 Failure:** `resolve_target()` returns error for unknown target name

### named-targets.AC2: Env var override for targets
- **named-targets.AC2.1 Success:** `CERTSTREAM_TARGETS_<NAME>_TABLE_PATH` overrides YAML value
- **named-targets.AC2.2 Success:** `CERTSTREAM_TARGETS_<NAME>_COMPRESSION_LEVEL` overrides YAML value
- **named-targets.AC2.3 Success:** Target storage env vars override YAML storage config

---

## Codebase Verification Findings

- ✓ `Config` struct at `src/config.rs:552-574` — no `targets` field yet, ready to add
- ✓ `YamlConfig` at `src/config.rs:576-611` — separate deserialization struct, needs matching `targets` field
- ✓ `DeltaSinkConfig` at `src/config.rs:289-319` — `compression_level` (i32, default 9), `heavy_column_compression_level` (i32, default 15), `offline_batch_size` (usize, default 100000) all confirmed
- ✓ `StorageConfig` at `src/config.rs:439-443` — `pub s3: Option<S3StorageConfig>`
- ✓ `S3StorageConfig` at `src/config.rs:423-437` — 6 fields: endpoint, region, access_key_id, secret_access_key, conditional_put, allow_http
- ✓ `parse_table_uri()` at `src/config.rs:479-504` — `pub fn parse_table_uri(uri: &str) -> Result<TableLocation, String>`
- ✓ `resolve_storage_options()` at `src/config.rs:511-545` — `pub fn resolve_storage_options(location: &TableLocation, storage: &StorageConfig) -> HashMap<String, String>`
- ✓ `TableLocation` enum at `src/config.rs:446-465` — `Local { path }` and `S3 { uri }` variants with `as_uri()` method
- ✓ Env var pattern: `CERTSTREAM_<SECTION>_<FIELD>` consistently used (e.g., `CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL`)
- ✓ `Config::validate()` at `src/config.rs:875-1070` — collects `Vec<ConfigValidationError>`, returns `Result<(), Vec<ConfigValidationError>>`
- ✓ Default pattern: `#[serde(default)]` on fields, explicit `Default` impls, helper functions like `fn default_delta_sink_compression_level() -> i32 { 9 }`
- ✓ Testing pattern: in-file `#[cfg(test)] mod tests`, `#[test]` and `#[tokio::test]`, helper function `test_config() -> Config` at config.rs:1113, struct construction for config tests, inline YAML strings for deserialization tests, env var tests with `set_var`/`remove_var`

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->

<!-- START_TASK_1 -->
### Task 1: Add TargetConfig struct and targets field to Config and YamlConfig

**Verifies:** named-targets.AC1.1, named-targets.AC1.2

**Files:**
- Modify: `src/config.rs:289` (add TargetConfig struct near DeltaSinkConfig)
- Modify: `src/config.rs:552-574` (add `targets` field to Config struct)
- Modify: `src/config.rs:576-611` (add `targets` field to YamlConfig struct)

**Implementation:**

Add `TargetConfig` struct after the `DeltaSinkConfig` block (struct + Default impl, which ends around line 345). Place it before the next config struct (`StorageConfig` at line 439). This positions it with the other sink/target config types. Follow the same `#[derive(Debug, Clone, Deserialize)]` pattern with `#[serde(default)]` on optional fields:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct TargetConfig {
    pub table_path: String,
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    #[serde(default)]
    pub compression_level: Option<i32>,
    #[serde(default)]
    pub heavy_column_compression_level: Option<i32>,
    #[serde(default)]
    pub offline_batch_size: Option<usize>,
}
```

Add `ResolvedTarget` struct immediately after `TargetConfig`:

```rust
#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub table_path: String,
    pub storage_options: HashMap<String, String>,
    pub compression_level: i32,
    pub heavy_column_compression_level: i32,
    pub offline_batch_size: usize,
}
```

Add `targets` field to `Config` struct (at `src/config.rs:552-574`):

```rust
pub targets: HashMap<String, TargetConfig>,
```

Add `targets` field to `YamlConfig` struct (at `src/config.rs:576-611`):

```rust
#[serde(default)]
targets: Option<HashMap<String, TargetConfig>>,
```

Update the `Config` construction in `Config::load()` to populate the `targets` field from `YamlConfig`. Find the block where `Config` is assembled (around line 855-873) and add:

```rust
targets: yaml_config.targets.unwrap_or_default(),
```

Ensure `HashMap` is imported from `std::collections` (it already is, used by `resolve_storage_options`).

**Verification:**

Run: `cargo build`
Expected: Builds without errors

Run: `cargo test`
Expected: All existing tests pass. Some tests that construct `Config` directly (like `test_config()` at config.rs:1113) will need the new field added — add `targets: HashMap::new()` to the `test_config()` helper and any other direct Config constructions in tests.

**Commit:** `feat: add TargetConfig and ResolvedTarget structs to config`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add resolve_target() method and TargetConfig/ResolvedTarget tests

**Verifies:** named-targets.AC1.1, named-targets.AC1.2, named-targets.AC1.3, named-targets.AC1.4, named-targets.AC1.5, named-targets.AC1.6, named-targets.AC1.7, named-targets.AC1.8

**Files:**
- Modify: `src/config.rs` (add `resolve_target()` method on Config, after `validate()`)
- Modify: `src/config.rs` (add tests to existing `#[cfg(test)] mod tests` block)

**Implementation:**

Add `resolve_target()` as a method on `Config`, placed after the `validate()` method (after line ~1070):

```rust
/// Resolve a named target from config.targets, filling unspecified fields
/// from delta_sink defaults and resolving storage options.
pub fn resolve_target(&self, name: &str) -> Result<ResolvedTarget, String> {
    let target = self
        .targets
        .get(name)
        .ok_or_else(|| format!("Unknown target '{}'. Available targets: {:?}", name, self.targets.keys().collect::<Vec<_>>()))?;

    let location = parse_table_uri(&target.table_path)?;

    // Use target-level storage if present, otherwise fall back to global config.storage
    let storage = target.storage.as_ref().unwrap_or(&self.storage);
    let storage_options = resolve_storage_options(&location, storage);

    Ok(ResolvedTarget {
        table_path: location.as_uri().to_string(),
        storage_options,
        compression_level: target.compression_level.unwrap_or(self.delta_sink.compression_level),
        heavy_column_compression_level: target.heavy_column_compression_level.unwrap_or(self.delta_sink.heavy_column_compression_level),
        offline_batch_size: target.offline_batch_size.unwrap_or(self.delta_sink.offline_batch_size),
    })
}
```

Note: `resolve_target()` calls `parse_table_uri()` which normalizes the path. For `file://` URIs, `TableLocation::Local` stores the stripped path; for `s3://` URIs, `TableLocation::S3` stores the full URI. The `as_uri()` method returns the appropriate string for `DeltaTableBuilder`.

**Testing:**

Tests go in the existing `#[cfg(test)] mod tests` block in `src/config.rs`. Follow existing patterns:
- Use the existing `test_config()` helper (adding `targets: HashMap::new()`)
- Test deserialization with inline YAML strings via `serde_yaml::from_str`
- Test `resolve_target()` by constructing Config with specific targets and delta_sink values

Tests must verify each AC listed above:
- named-targets.AC1.1: Deserialize TargetConfig YAML with all fields (table_path, storage, compression_level, heavy_column_compression_level, offline_batch_size) and verify all values are correct
- named-targets.AC1.2: Deserialize TargetConfig YAML with only `table_path` and verify optional fields are `None`
- named-targets.AC1.3: Create Config with target missing `compression_level`, call `resolve_target()`, verify it returns `delta_sink.compression_level`
- named-targets.AC1.4: Create Config with target missing `heavy_column_compression_level`, call `resolve_target()`, verify it returns `delta_sink.heavy_column_compression_level`
- named-targets.AC1.5: Create Config with target missing `offline_batch_size`, call `resolve_target()`, verify it returns `delta_sink.offline_batch_size`
- named-targets.AC1.6: Create Config with target that has its own `StorageConfig` with S3 credentials, call `resolve_target()`, verify `storage_options` contain the target-level S3 credentials (not global)
- named-targets.AC1.7: Create Config with target that has no storage config, set global `config.storage` with S3 credentials, call `resolve_target()` with an `s3://` table_path, verify `storage_options` contain the global S3 credentials
- named-targets.AC1.8: Call `resolve_target("nonexistent")` on a Config with no matching target name, verify it returns `Err` with a message containing the target name

Follow existing test naming convention: `test_target_config_*` and `test_resolve_target_*`.

**Verification:**

Run: `cargo test config::tests::test_target`
Expected: All new target-related tests pass

Run: `cargo test`
Expected: All tests pass (465 total + new tests)

**Commit:** `feat: add resolve_target() method with fallback inheritance`

<!-- END_TASK_2 -->

<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->

<!-- START_TASK_3 -->
### Task 3: Add env var loading for targets section in Config::load()

**Verifies:** named-targets.AC2.1, named-targets.AC2.2, named-targets.AC2.3

**Files:**
- Modify: `src/config.rs` (add env var overlay logic in `Config::load()`, after existing env var blocks around lines 749-848)

**Implementation:**

Add env var overlay logic for targets in `Config::load()`. This goes after the existing storage env var block (around line 848) and before the Config struct assembly.

The pattern follows the existing convention but with an additional dimension: target names are part of the env var path. Since target names come from YAML, the env var overlay iterates over already-deserialized targets and applies overrides:

```rust
// Overlay env vars for each named target
let mut targets = yaml_config.targets.unwrap_or_default();
for (name, target) in targets.iter_mut() {
    let prefix = format!("CERTSTREAM_TARGETS_{}", name.to_uppercase());

    if let Ok(v) = env::var(format!("{}_TABLE_PATH", prefix)) {
        target.table_path = v;
    }
    if let Ok(v) = env::var(format!("{}_COMPRESSION_LEVEL", prefix)) {
        if let Ok(level) = v.parse::<i32>() {
            target.compression_level = Some(level);
        }
    }
    if let Ok(v) = env::var(format!("{}_HEAVY_COLUMN_COMPRESSION_LEVEL", prefix)) {
        if let Ok(level) = v.parse::<i32>() {
            target.heavy_column_compression_level = Some(level);
        }
    }
    if let Ok(v) = env::var(format!("{}_OFFLINE_BATCH_SIZE", prefix)) {
        if let Ok(size) = v.parse::<usize>() {
            target.offline_batch_size = Some(size);
        }
    }

    // Storage S3 env var overrides for per-target storage
    let s3_prefix = format!("{}_STORAGE_S3", prefix);
    let s3_endpoint = env::var(format!("{}_ENDPOINT", s3_prefix)).unwrap_or_default();
    if !s3_endpoint.is_empty() {
        // If endpoint env var is set, create or override S3 config
        let s3 = S3StorageConfig {
            endpoint: s3_endpoint,
            region: env::var(format!("{}_REGION", s3_prefix)).unwrap_or_default(),
            access_key_id: env::var(format!("{}_ACCESS_KEY_ID", s3_prefix)).unwrap_or_default(),
            secret_access_key: env::var(format!("{}_SECRET_ACCESS_KEY", s3_prefix)).unwrap_or_default(),
            conditional_put: env::var(format!("{}_CONDITIONAL_PUT", s3_prefix)).ok(),
            allow_http: env::var(format!("{}_ALLOW_HTTP", s3_prefix)).ok().and_then(|v| v.parse().ok()),
        };
        target.storage = Some(StorageConfig { s3: Some(s3) });
    } else if let Some(ref mut storage) = target.storage {
        // If no endpoint trigger but target has YAML storage, overlay individual non-endpoint fields
        // Note: endpoint overlay is intentionally omitted here — we already know the endpoint env var
        // is empty/unset (that's why we're in the else branch), so overlaying it would be a no-op.
        if let Some(ref mut s3) = storage.s3 {
            if let Ok(v) = env::var(format!("{}_REGION", s3_prefix)) {
                s3.region = v;
            }
            if let Ok(v) = env::var(format!("{}_ACCESS_KEY_ID", s3_prefix)) {
                s3.access_key_id = v;
            }
            if let Ok(v) = env::var(format!("{}_SECRET_ACCESS_KEY", s3_prefix)) {
                s3.secret_access_key = v;
            }
            if let Ok(v) = env::var(format!("{}_CONDITIONAL_PUT", s3_prefix)) {
                s3.conditional_put = Some(v);
            }
            if let Ok(v) = env::var(format!("{}_ALLOW_HTTP", s3_prefix)) {
                s3.allow_http = v.parse().ok();
            }
        }
    }
}
```

Then update the Config assembly to use the pre-processed `targets` variable instead of unwrapping from yaml_config:

```rust
targets,
```

**Verification:**

Run: `cargo build`
Expected: Builds without errors

Run: `cargo test`
Expected: All tests pass

**Commit:** `feat: add env var overlay for named targets`

<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Add env var override tests for targets

**Verifies:** named-targets.AC2.1, named-targets.AC2.2, named-targets.AC2.3

**Files:**
- Modify: `src/config.rs` (add tests to existing `#[cfg(test)] mod tests` block)

**Implementation:**

Add tests for env var overrides following the existing pattern from tests like `test_zerobus_sink_config_env_var_enabled` (which uses `unsafe { env::set_var(...) }` and `unsafe { env::remove_var(...) }`).

Note: These tests cannot use `Config::load()` directly (it reads from a real YAML file). Instead, follow the existing pattern: construct a `TargetConfig` manually, then simulate the env var overlay logic inline. Alternatively, test at the integration level by writing a temp YAML file. Follow whichever pattern is simpler and matches existing tests.

**Testing:**

Tests must verify each AC listed above:
- named-targets.AC2.1: Set `CERTSTREAM_TARGETS_MAIN_TABLE_PATH` env var, construct a target config with a different table_path, apply the env var overlay pattern, verify the env var value wins
- named-targets.AC2.2: Set `CERTSTREAM_TARGETS_MAIN_COMPRESSION_LEVEL` env var to "15", construct a target config with compression_level `Some(9)`, apply the env var overlay pattern, verify the env var value (15) wins
- named-targets.AC2.3: Set `CERTSTREAM_TARGETS_MAIN_STORAGE_S3_ENDPOINT` and related S3 env vars, construct a target config with different S3 storage, apply the env var overlay pattern, verify the env var S3 credentials are used

Use the existing test naming convention: `test_target_env_var_*`.

Important: Always clean up env vars with `remove_var` in each test (even if the test panics — the existing pattern accepts this risk since tests run in separate threads).

**Verification:**

Run: `cargo test config::tests::test_target_env_var`
Expected: All new env var tests pass

Run: `cargo test`
Expected: All tests pass

**Commit:** `test: add env var override tests for named targets`

<!-- END_TASK_4 -->

<!-- END_SUBCOMPONENT_B -->
