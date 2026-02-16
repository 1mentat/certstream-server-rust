# Delta Sink Implementation Plan - Phase 1: Dependencies and Configuration

**Goal:** Add deltalake crate dependency, define `DeltaSinkConfig`, wire into config loading

**Architecture:** Write-aside pattern where a Delta Lake sink subscribes to the existing broadcast channel as a parallel consumer. This phase sets up the dependency and config scaffolding.

**Tech Stack:** deltalake (delta-rs) crate with arrow re-exports, serde for config deserialization

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-16

---

## Acceptance Criteria Coverage

This phase is infrastructure — no ACs are tested here. Verification is operational (cargo build succeeds, config parses correctly).

**Verifies: None** (infrastructure phase — verified operationally)

---

<!-- START_TASK_1 -->
### Task 1: Add deltalake dependency to Cargo.toml

**Files:**
- Modify: `Cargo.toml` (line 48, after `flate2 = "1.0"`)

**Step 1: Add the deltalake dependency**

Add `deltalake` to `[dependencies]` in `Cargo.toml`. The deltalake crate re-exports arrow types, so no separate `arrow` crate is needed. Use the `deltalake` feature set that includes the `datafusion` feature (needed for `DeltaOps::write`).

Add the following line after `flate2 = "1.0"` (line 48):

```toml
deltalake = { version = "0.24", features = ["datafusion"] }
```

**Important:** The `datafusion` feature is required because `DeltaOps::write()` (used for appending RecordBatches) is gated behind it. Without it, only read operations are available.

**Note on version:** Use `0.24` as the starting point. If this version has compatibility issues with edition 2024 or rustc 1.91.1, the executor should check crates.io for the latest compatible version and adjust. The API patterns (`DeltaOps`, `CreateBuilder`, `open_table`) are stable across recent versions.

**Step 2: Verify build succeeds**

Run: `cargo build 2>&1 | tail -5`
Expected: Build succeeds (the deltalake crate and its transitive arrow dependencies compile)

**Note:** First build with deltalake will take several minutes due to arrow/parquet compilation. This is expected.

**Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add deltalake dependency for Delta sink feature"
```
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Define DeltaSinkConfig struct in config.rs

**Files:**
- Modify: `src/config.rs` (add struct after `HotReloadConfig` at line 285, add to `Config` struct at line 309, add to `YamlConfig` at line 338, add to `Config::load()` at line 472-475, add to Config constructor at line 494, add default functions, add env var overrides)

**Step 1: Add the DeltaSinkConfig struct**

Add the following after the `HotReloadConfig` struct (after line 285, before `fn default_true()`):

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct DeltaSinkConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_delta_sink_table_path")]
    pub table_path: String,
    #[serde(default = "default_delta_sink_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_delta_sink_flush_interval_secs")]
    pub flush_interval_secs: u64,
}

impl Default for DeltaSinkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            table_path: default_delta_sink_table_path(),
            batch_size: default_delta_sink_batch_size(),
            flush_interval_secs: default_delta_sink_flush_interval_secs(),
        }
    }
}

fn default_delta_sink_table_path() -> String {
    "./data/certstream".to_string()
}

fn default_delta_sink_batch_size() -> usize {
    10000
}

fn default_delta_sink_flush_interval_secs() -> u64 {
    30
}
```

**Note:** Line numbers throughout this plan are approximate — locate insertion points by struct/function names rather than exact line numbers.

This follows the exact pattern of existing config structs like `ConnectionLimitConfig` and `AuthConfig`: `#[derive(Debug, Clone, Deserialize)]`, `#[serde(default)]` / `#[serde(default = "fn_name")]` on fields, explicit `Default` impl calling default functions.

**Step 2: Add `delta_sink` field to the `Config` struct**

In the `Config` struct (line 291-310), add a new field after `hot_reload`:

```rust
    pub hot_reload: HotReloadConfig,
    pub delta_sink: DeltaSinkConfig,
    pub config_path: Option<String>,
```

**Step 3: Add `delta_sink` field to the `YamlConfig` struct**

In the `YamlConfig` struct (line 312-339), add after the `hot_reload` field:

```rust
    #[serde(default)]
    hot_reload: Option<HotReloadConfig>,
    #[serde(default)]
    delta_sink: Option<DeltaSinkConfig>,
```

**Step 4: Add env var overrides in `Config::load()`**

After the `hot_reload` env var block (after line 475), add:

```rust
        let mut delta_sink = yaml_config.delta_sink.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_ENABLED") {
            delta_sink.enabled = v.parse().unwrap_or(delta_sink.enabled);
        }
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_TABLE_PATH") {
            delta_sink.table_path = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_BATCH_SIZE") {
            delta_sink.batch_size = v.parse().unwrap_or(delta_sink.batch_size);
        }
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_FLUSH_INTERVAL_SECS") {
            delta_sink.flush_interval_secs = v.parse().unwrap_or(delta_sink.flush_interval_secs);
        }
```

This follows the exact env var override pattern used for `connection_limit` (lines 443-452) and `auth` (lines 461-470).

**Step 5: Add `delta_sink` to the Config constructor**

In the `Self { ... }` block at the end of `Config::load()` (around line 477-495), add `delta_sink` after `hot_reload`:

```rust
            hot_reload,
            delta_sink,
            config_path,
```

**Step 6: Add validation for delta_sink config in `Config::validate()`**

In the `Config::validate()` method (around line 498-560), add validation after the existing rate_limit check (around line 553):

```rust
        if self.delta_sink.enabled && self.delta_sink.batch_size == 0 {
            errors.push(ConfigValidationError {
                field: "delta_sink.batch_size".to_string(),
                message: "Batch size must be greater than 0 when delta sink is enabled".to_string(),
            });
        }
        if self.delta_sink.enabled && self.delta_sink.flush_interval_secs == 0 {
            errors.push(ConfigValidationError {
                field: "delta_sink.flush_interval_secs".to_string(),
                message: "Flush interval must be greater than 0 when delta sink is enabled".to_string(),
            });
        }
```

This prevents: division-by-zero in the 2x overflow check (batch_size=0) and busy-loop from zero-duration interval (flush_interval_secs=0).

**Step 7: Update the test helper `test_config()` function**

In the test module's `test_config()` function (line 603-623), add `delta_sink` after `hot_reload`:

```rust
            hot_reload: HotReloadConfig::default(),
            delta_sink: DeltaSinkConfig::default(),
            config_path: None,
```

**Step 8: Verify build and tests pass**

Run: `cargo build 2>&1 | tail -3`
Expected: Build succeeds

Run: `cargo test 2>&1 | tail -5`
Expected: All tests pass (including existing config tests)

**Step 9: Commit**

```bash
git add src/config.rs
git commit -m "feat: add DeltaSinkConfig with defaults and env var overrides"
```
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add delta_sink section to config.example.yaml

**Files:**
- Modify: `config.example.yaml` (add section at end of file, after line 54)

**Step 1: Add delta_sink configuration section**

Add the following to the end of `config.example.yaml`:

```yaml

# Delta Lake storage sink
# Writes CT entries to a local Delta table for historical analysis
delta_sink:
  enabled: false                    # Off by default
  table_path: "./data/certstream"   # Local filesystem path for Delta table
  batch_size: 10000                 # Records before size-triggered flush
  flush_interval_secs: 30           # Max seconds between flushes
```

**Step 2: Verify the YAML parses correctly**

Run: `cargo test test_yaml_config 2>&1 | tail -5`
Expected: All YAML-related tests pass

**Step 3: Commit**

```bash
git add config.example.yaml
git commit -m "docs: add delta_sink section to example config"
```
<!-- END_TASK_3 -->
