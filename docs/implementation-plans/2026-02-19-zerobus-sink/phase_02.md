# ZeroBus Sink Implementation Plan - Phase 2: Configuration

**Goal:** Add `ZerobusSinkConfig` with YAML deserialization, env var overrides, and validation, following the established `DeltaSinkConfig` pattern.

**Architecture:** A new `ZerobusSinkConfig` struct in `src/config.rs` mirrors the `DeltaSinkConfig` pattern: serde `Deserialize` with defaults, `enabled: bool` defaulting to false, env var overrides in `Config::load()`, and validation when enabled. The struct is added to `Config`, `YamlConfig`, and `config.example.yaml`.

**Tech Stack:** serde (already in project), std::env

**Scope:** 6 phases from original design (phase 2 of 6)

**Codebase verified:** 2026-02-19

---

## Acceptance Criteria Coverage

This phase implements and tests:

### zerobus-sink.AC4: Config and metrics follow conventions
- **zerobus-sink.AC4.1 Success:** `ZerobusSinkConfig` loads from YAML with serde defaults and is disabled by default
- **zerobus-sink.AC4.2 Success:** Env vars (`CERTSTREAM_ZEROBUS_*`) override YAML values for all config fields
- **zerobus-sink.AC4.3 Failure:** Validation rejects enabled config with empty endpoint, unity_catalog_url, table_name, client_id, or client_secret

---

<!-- START_SUBCOMPONENT_A (tasks 1-4) -->
<!-- START_TASK_1 -->
### Task 1: Add ZerobusSinkConfig struct to src/config.rs

**Verifies:** zerobus-sink.AC4.1

**Files:**
- Modify: `src/config.rs` (insert new struct after `QueryApiConfig` at line 362, before `Config` struct at line 368)

**Implementation:**

Insert the `ZerobusSinkConfig` struct after the `QueryApiConfig` default functions (after line 362) and before the `Config` struct (line 369). Follow the exact same pattern as `DeltaSinkConfig` (lines 287-320) and `QueryApiConfig` (lines 322-362):

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct ZerobusSinkConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub unity_catalog_url: String,
    #[serde(default)]
    pub table_name: String,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    #[serde(default = "default_zerobus_max_inflight_records")]
    pub max_inflight_records: usize,
}

fn default_zerobus_max_inflight_records() -> usize {
    10000
}

impl Default for ZerobusSinkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            unity_catalog_url: String::new(),
            table_name: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            max_inflight_records: default_zerobus_max_inflight_records(),
        }
    }
}
```

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Compiles (struct exists but isn't used yet)

**Commit:** `feat: add ZerobusSinkConfig struct`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Wire ZerobusSinkConfig into Config and YamlConfig

**Verifies:** zerobus-sink.AC4.1

**Files:**
- Modify: `src/config.rs` — add field to `Config` struct (after line 387), `YamlConfig` struct (after line 421), `Config::load()` return (after line 609), and `test_config()` helper (after line 751)

**Implementation:**

1. Add to `Config` struct (after `query_api: QueryApiConfig` at line 387):
```rust
    pub zerobus_sink: ZerobusSinkConfig,
```

2. Add to `YamlConfig` struct (after `query_api` at line 421):
```rust
    #[serde(default)]
    zerobus_sink: Option<ZerobusSinkConfig>,
```

3. Add to `Config::load()` — after the `query_api` env var overrides block (around line 589), before the `Self {` return:
```rust
        let zerobus_sink = yaml_config.zerobus_sink.unwrap_or_default();
```
(Env var overrides will be added in the next task.)

4. Add to `Config::load()` return struct (after `query_api,` at line 609):
```rust
            zerobus_sink,
```

5. Add to `test_config()` helper in the test module (after `query_api: QueryApiConfig::default(),` at line 751):
```rust
            zerobus_sink: ZerobusSinkConfig::default(),
```

6. Fix pre-existing test compilation error in `src/backfill.rs:2380` — the test Config constructor there is missing the `query_api` field (pre-existing bug). Add both the missing `query_api: QueryApiConfig::default(),` and the new `zerobus_sink: ZerobusSinkConfig::default(),` fields to that constructor. You will need to add `use crate::config::{QueryApiConfig, ZerobusSinkConfig};` to the test module's imports if not already present.

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Compiles without errors

**Commit:** `feat: wire ZerobusSinkConfig into Config and YamlConfig`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add env var overrides for ZerobusSinkConfig

**Verifies:** zerobus-sink.AC4.2

**Files:**
- Modify: `src/config.rs` — add env var override block in `Config::load()` (after the line where `zerobus_sink` is initialized from YAML, before the `Self {` return)

**Implementation:**

Replace the simple `let zerobus_sink = ...` line from Task 2 with the full override block. Follow the exact pattern used for `delta_sink` (lines 560-572) and `query_api` (lines 574-589):

```rust
        let mut zerobus_sink = yaml_config.zerobus_sink.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_ENABLED") {
            zerobus_sink.enabled = v.parse().unwrap_or(zerobus_sink.enabled);
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_ENDPOINT") {
            zerobus_sink.endpoint = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_UNITY_CATALOG_URL") {
            zerobus_sink.unity_catalog_url = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_TABLE_NAME") {
            zerobus_sink.table_name = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_CLIENT_ID") {
            zerobus_sink.client_id = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_CLIENT_SECRET") {
            zerobus_sink.client_secret = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_MAX_INFLIGHT_RECORDS") {
            zerobus_sink.max_inflight_records = v.parse().unwrap_or(zerobus_sink.max_inflight_records);
        }
```

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Compiles without errors

**Commit:** `feat: add CERTSTREAM_ZEROBUS_* env var overrides`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Add validation and --validate-config output for ZerobusSinkConfig

**Verifies:** zerobus-sink.AC4.3

**Files:**
- Modify: `src/config.rs` — add validation rules in `Config::validate()` (after delta_sink validation at line 682)
- Modify: `src/main.rs` — add ZeroBus status output in `print_config_validation()` (after delta_sink output at line 672)

**Implementation:**

1. Add validation in `Config::validate()` after the delta_sink block (after line 682, before the `if errors.is_empty()` at line 684):

```rust
        if self.zerobus_sink.enabled {
            if self.zerobus_sink.endpoint.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.endpoint".to_string(),
                    message: "Endpoint cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
            if self.zerobus_sink.unity_catalog_url.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.unity_catalog_url".to_string(),
                    message: "Unity Catalog URL cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
            if self.zerobus_sink.table_name.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.table_name".to_string(),
                    message: "Table name cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
            if !self.zerobus_sink.table_name.is_empty()
                && self.zerobus_sink.table_name.matches('.').count() != 2
            {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.table_name".to_string(),
                    message: "Table name must be in Unity Catalog format: catalog.schema.table".to_string(),
                });
            }
            if self.zerobus_sink.client_id.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.client_id".to_string(),
                    message: "Client ID cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
            if self.zerobus_sink.client_secret.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.client_secret".to_string(),
                    message: "Client secret cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
        }
```

2. Add to `print_config_validation()` in `src/main.rs` (after the delta_sink block ending at line 672, before the closing `}` of the `Ok(())` arm at line 673):

```rust
            println!("ZeroBus sink enabled: {}", config.zerobus_sink.enabled);
            if config.zerobus_sink.enabled {
                println!("  Endpoint: {}", config.zerobus_sink.endpoint);
                println!("  Unity Catalog URL: {}", config.zerobus_sink.unity_catalog_url);
                println!("  Table name: {}", config.zerobus_sink.table_name);
                println!("  Max inflight records: {}", config.zerobus_sink.max_inflight_records);
            }
```

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Compiles without errors

**Commit:** `feat: add ZeroBus sink config validation and --validate-config output`
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_TASK_5 -->
### Task 5: Add ZeroBus section to config.example.yaml

**Files:**
- Modify: `config.example.yaml` (after the `query_api` section ending around line 70)

**Implementation:**

Add after the `query_api` section:

```yaml

# ZeroBus Ingest sink (Databricks managed Delta table)
# Streams CT entries to a Databricks-managed Delta table via ZeroBus SDK.
# Credentials should be provided via env vars: CERTSTREAM_ZEROBUS_CLIENT_ID, CERTSTREAM_ZEROBUS_CLIENT_SECRET
# zerobus_sink:
#   enabled: false
#   endpoint: ""                          # ZeroBus endpoint URL
#   unity_catalog_url: ""                 # Databricks Unity Catalog URL
#   table_name: ""                        # Unity Catalog table: catalog.schema.table
#   client_id: ""                         # OAuth client ID (prefer env var)
#   client_secret: ""                     # OAuth client secret (prefer env var)
#   max_inflight_records: 10000           # Max records in-flight without acknowledgment
```

**Verification:**
Run: `cat config.example.yaml` — verify the section appears correctly formatted

**Commit:** `docs: add zerobus_sink section to config.example.yaml`
<!-- END_TASK_5 -->

<!-- START_SUBCOMPONENT_B (tasks 6-7) -->
<!-- START_TASK_6 -->
### Task 6: Write tests for ZerobusSinkConfig defaults and env var overrides

**Verifies:** zerobus-sink.AC4.1, zerobus-sink.AC4.2

**Files:**
- Modify: `src/config.rs` — add tests in existing `#[cfg(test)] mod tests` block (starting at line 728)

**Testing:**

Tests must verify each AC listed above:
- zerobus-sink.AC4.1: Test that `ZerobusSinkConfig::default()` returns `enabled: false`, empty strings for endpoint/uc_url/table_name/client_id/client_secret, and `max_inflight_records: 10000`. Test that YAML deserialization with an empty `zerobus_sink:` block produces the same defaults.
- zerobus-sink.AC4.2: Test that each `CERTSTREAM_ZEROBUS_*` env var overrides the corresponding field. Use `std::env::set_var`/`remove_var` in tests (note: env var tests should be careful about parallel execution).

Follow the existing test patterns in `src/config.rs:728+`. Use `assert_eq!` for all field comparisons.

**Verification:**
Run: `cargo test --bin certstream-server-rust config::tests` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: All tests pass

**Commit:** `test: add ZerobusSinkConfig defaults and env var override tests`
<!-- END_TASK_6 -->

<!-- START_TASK_7 -->
### Task 7: Write tests for ZerobusSinkConfig validation

**Verifies:** zerobus-sink.AC4.3

**Files:**
- Modify: `src/config.rs` — add validation tests in existing `#[cfg(test)] mod tests` block

**Testing:**

Tests must verify AC4.3 — validation rejects enabled config with empty required fields:
- Test: enabled=true with empty endpoint produces validation error for `zerobus_sink.endpoint`
- Test: enabled=true with empty unity_catalog_url produces validation error
- Test: enabled=true with empty table_name produces validation error
- Test: enabled=true with table_name missing two dots (e.g., "just_table" or "schema.table") produces validation error
- Test: enabled=true with valid table_name format (e.g., "catalog.schema.table") passes table_name validation
- Test: enabled=true with empty client_id produces validation error
- Test: enabled=true with empty client_secret produces validation error
- Test: enabled=false with all fields empty passes validation (disabled sinks are not validated)
- Test: enabled=true with all required fields populated passes validation

Use `test_config()` helper (line 732), set `zerobus_sink` fields, call `config.validate()`, check for specific error field names.

**Verification:**
Run: `cargo test --bin certstream-server-rust config::tests` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: All tests pass

**Commit:** `test: add ZeroBus sink config validation tests`
<!-- END_TASK_7 -->
<!-- END_SUBCOMPONENT_B -->
