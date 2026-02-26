# Zstd Compression Implementation Plan

**Goal:** Add zstd compression to all Delta Lake write paths with configurable compression level

**Architecture:** New `compression_level` field in `DeltaSinkConfig`, validated at startup via `ZstdLevel::try_new()`, threaded through `flush_buffer()` and `run_merge()` as `WriterProperties` with `Compression::ZSTD`

**Tech Stack:** Rust, deltalake 0.25, parquet 54.x (new direct dependency for `ZstdLevel`/`Compression`/`WriterProperties`)

**Scope:** 3 phases from original design (phases 1-3)

**Codebase verified:** 2026-02-20

---

## Acceptance Criteria Coverage

This phase implements and tests:

### zstd-compression.AC1: Compression level is configurable
- **zstd-compression.AC1.1 Success:** Server starts with default compression_level=9 when field is omitted from config
- **zstd-compression.AC1.2 Success:** Server starts with compression_level=1 when explicitly set in config.yaml
- **zstd-compression.AC1.3 Failure:** Server exits with clear error when compression_level=0 (below valid range)
- **zstd-compression.AC1.4 Failure:** Server exits with clear error when compression_level=23 (above valid range)

---

<!-- START_TASK_1 -->
### Task 1: Fix pre-existing compilation error in backfill.rs test helper

There is a pre-existing compilation error at `src/backfill.rs` around line 2380 where a `Config` struct literal is missing the `query_api` field. Additionally, the `make_test_config()` helper in the same test module (around line 2397-2402) constructs `DeltaSinkConfig` via struct literal (not `DeltaSinkConfig::default()`), so it will also need the new `compression_level` field added in Task 2. Fix both issues now to prevent cascading compilation errors.

**Files:**
- Modify: `src/backfill.rs` (~line 2380 for Config, ~line 2397-2402 for DeltaSinkConfig)

**Implementation:**

**1.** Find the `Config { ... }` struct literal in the test module (around line 2380) that is missing the `query_api` field. Add the missing field:

```rust
query_api: QueryApiConfig::default(),
```

Ensure the test module imports `QueryApiConfig` (it may already be imported via `use super::*` or `use crate::config::*`). If not, add the import.

**2.** Find the `make_test_config()` helper in the same test module. It constructs `DeltaSinkConfig` via struct literal (not `Default`). After Task 2 adds the `compression_level` field to `DeltaSinkConfig`, this will break. Preemptively add:

```rust
compression_level: 9,
```

to the `DeltaSinkConfig { ... }` struct literal inside `make_test_config()` (around line 2397-2402).

**Verification:**

Run: `cargo test --no-run` in the worktree directory
Expected: Compiles without errors (specifically, the `missing field 'query_api'` error is gone)

**Commit:** `fix: add missing fields in backfill test config helper`
<!-- END_TASK_1 -->

<!-- START_SUBCOMPONENT_A (tasks 2-4) -->

<!-- START_TASK_2 -->
### Task 2: Add parquet dependency and compression_level config field

**Files:**
- Modify: `Cargo.toml` (dependencies section)
- Modify: `src/config.rs` (`DeltaSinkConfig` struct at ~line 288, default fn at ~line 310, `Default` impl at ~line 299, env var loading at ~line 560)
- Modify: `config.example.yaml` (delta_sink section at ~line 55)

**Implementation:**

**Cargo.toml:** Add `parquet` as a direct dependency. The exact version (54.2.1) is already a transitive dependency of deltalake 0.25, so this adds no new compilation. Place it near the `deltalake` entry:

```toml
parquet = "54"
```

**src/config.rs — default function:** Add near the other `default_delta_sink_*` functions (around line 310-320):

```rust
fn default_delta_sink_compression_level() -> i32 {
    9
}
```

**src/config.rs — DeltaSinkConfig struct:** Add the new field after the existing fields (around line 288-298):

```rust
#[serde(default = "default_delta_sink_compression_level")]
pub compression_level: i32,
```

**src/config.rs — Default impl:** Add to the `Default::default()` impl for `DeltaSinkConfig` (around line 299-308):

```rust
compression_level: default_delta_sink_compression_level(),
```

**src/config.rs — env var loading:** Add to the env var override block for delta_sink (around line 560-572), following the existing pattern:

```rust
if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL") {
    delta_sink.compression_level = v.parse().unwrap_or(delta_sink.compression_level);
}
```

**config.example.yaml:** Add `compression_level` to the `delta_sink` section with a comment, after the existing fields:

```yaml
  compression_level: 9              # Zstd compression level (1-22, default 9)
```

**Verification:**

Run: `cargo check`
Expected: Compiles without errors

**Commit:** `feat: add compression_level field to DeltaSinkConfig`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add startup validation for compression_level

**Verifies:** zstd-compression.AC1.3, zstd-compression.AC1.4

**Files:**
- Modify: `src/config.rs` (add import, add validation in `Config::validate()` at ~line 614)
- Modify: `src/main.rs` (`print_config_validation` at ~line 647)

**Implementation:**

**src/config.rs — import:** Add at the top of the file with other imports:

```rust
use parquet::basic::ZstdLevel;
```

**src/config.rs — Config::validate():** Add compression_level validation. The existing delta_sink checks (batch_size, flush_interval_secs) are inside an `if self.delta_sink.enabled { ... }` block ending around line 682. Place the compression_level validation **OUTSIDE and AFTER** that `if` block — as a standalone check that runs regardless of `delta_sink.enabled`. This is necessary because backfill and merge modes use `compression_level` even when the live delta_sink is disabled:

```rust
if ZstdLevel::try_new(self.delta_sink.compression_level).is_err() {
    errors.push(ConfigValidationError {
        field: "delta_sink.compression_level".to_string(),
        message: format!(
            "Compression level {} is invalid. Must be between 1 and 22",
            self.delta_sink.compression_level
        ),
    });
}
```

**src/main.rs — print_config_validation():** Add compression_level to the delta_sink output section. Place it inside the existing `if config.delta_sink.enabled { ... }` block, alongside the existing `table_path`, `batch_size`, and `flush_interval` prints (for consistency with existing pattern):

```rust
println!("  Compression level: {} (zstd)", config.delta_sink.compression_level);
```

**Verification:**

Run: `cargo check`
Expected: Compiles without errors

**Commit:** `feat: add startup validation for compression_level via ZstdLevel`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Tests for compression_level config validation

**Verifies:** zstd-compression.AC1.1, zstd-compression.AC1.2, zstd-compression.AC1.3, zstd-compression.AC1.4

**Files:**
- Modify: `src/config.rs` (add tests in existing `#[cfg(test)] mod tests` block)

**Testing:**

Tests must be added to the existing `mod tests` block in `src/config.rs`. The test module already has a `test_config()` helper that creates a valid `Config` with all defaults. Use this helper for all tests.

Tests must verify each AC listed:
- **zstd-compression.AC1.1:** Default config has `compression_level == 9` and passes `validate()`
- **zstd-compression.AC1.2:** Config with `compression_level = 1` passes `validate()`
- **zstd-compression.AC1.3:** Config with `compression_level = 0` fails `validate()` with error on `"delta_sink.compression_level"` field
- **zstd-compression.AC1.4:** Config with `compression_level = 23` fails `validate()` with error on `"delta_sink.compression_level"` field

Follow the existing test pattern in `src/config.rs`. Example from codebase:
```rust
#[test]
fn test_validate_zero_port() {
    let config = Config { port: 0, ..test_config() };
    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.field == "port"));
}
```

Also add a serde deserialization test for the compression_level field, following the existing `test_ct_log_config_deserialize_*` pattern:
- Deserialize YAML with `compression_level: 5` and verify it's set correctly
- Deserialize YAML without `compression_level` and verify it defaults to 9

Note: The `test_config()` helper in `src/config.rs` uses `DeltaSinkConfig::default()`, so it will automatically pick up the new field — no changes needed there. The `make_test_config()` helper in `src/backfill.rs` uses a struct literal and was already updated in Task 1.

**Verification:**

Run: `cargo test` in the worktree directory
Expected: All tests pass, including the new compression_level tests

**Commit:** `test: add compression_level config validation tests`
<!-- END_TASK_4 -->

<!-- END_SUBCOMPONENT_A -->
