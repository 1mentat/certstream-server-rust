# Named Targets Implementation Plan — Phase 2: Validation

**Goal:** Validate all defined targets at config load time, catching invalid URIs, out-of-range compression levels, and missing S3 credentials.

**Architecture:** Extend `Config::validate()` in `src/config.rs` to iterate all entries in `config.targets`, validating each target's URI via `parse_table_uri()`, compression levels via `ZstdLevel::try_new()`, and S3 credential completeness when S3 URIs are used. Follows the existing error-accumulation pattern.

**Tech Stack:** Rust, parquet::basic::ZstdLevel, existing parse_table_uri()

**Scope:** 6 phases from original design (this is phase 2 of 6)

**Codebase verified:** 2026-03-05

---

## Acceptance Criteria Coverage

This phase implements and tests:

### named-targets.AC3: Validation
- **named-targets.AC3.1 Success:** Valid target with `file://` table_path passes validation
- **named-targets.AC3.2 Success:** Valid target with `s3://` table_path and complete S3 storage passes validation
- **named-targets.AC3.3 Failure:** Target with bare path (no URI scheme) fails validation
- **named-targets.AC3.4 Failure:** Target with `s3://` table_path but missing S3 credentials fails validation
- **named-targets.AC3.5 Failure:** Target with compression_level outside 1-22 range fails validation
- **named-targets.AC3.6 Success:** Empty targets map passes validation (no targets defined is valid)

---

## Codebase Verification Findings

- ✓ `Config::validate()` at `src/config.rs:875-1070` — accumulates `Vec<ConfigValidationError>`, returns `Ok(())` or `Err(errors)`
- ✓ `ConfigValidationError` at `src/config.rs:618-622` — `{ field: String, message: String }`
- ✓ Compression level validation pattern at lines 1046-1063 — `ZstdLevel::try_new(level).is_err()`
- ✓ S3 credential validation pattern at lines 965-1004 — checks `starts_with("s3://")`, then validates `storage.s3` exists with non-empty endpoint, region, access_key_id, secret_access_key
- ✓ URI validation pattern at lines 945-963 — `parse_table_uri(&path)` returns `Result<TableLocation, String>`
- ✓ `ZstdLevel` imported at line 7 — `use parquet::basic::ZstdLevel;`
- ✓ No existing identifier validator — target name validation needs to handle non-empty checks (HashMap keys are guaranteed non-empty by YAML deserialization)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->

<!-- START_TASK_1 -->
### Task 1: Add target validation to Config::validate()

**Verifies:** named-targets.AC3.1, named-targets.AC3.2, named-targets.AC3.3, named-targets.AC3.4, named-targets.AC3.5, named-targets.AC3.6

**Files:**
- Modify: `src/config.rs` (add target validation block inside `Config::validate()`, before the final `if errors.is_empty()` return at the end)

**Implementation:**

Add a validation block for named targets inside `Config::validate()`. Place it after the existing compression level validation (around line 1063) and before the final error-return block. The block iterates all targets and validates:

1. **URI format** — call `parse_table_uri(&target.table_path)` for each target
2. **Compression levels** — if `compression_level` is `Some`, validate via `ZstdLevel::try_new()`; same for `heavy_column_compression_level`
3. **S3 credentials** — if table_path starts with `s3://`, check that the target has a `StorageConfig` with complete S3 credentials. The target can use its own `target.storage` or fall back to `self.storage` (matching `resolve_target()` behavior). The S3 credential check validates the effective storage config.

```rust
// Validate named targets
for (name, target) in &self.targets {
    let target_field = format!("targets.{}", name);

    // Validate URI format
    match parse_table_uri(&target.table_path) {
        Ok(location) => {
            // If S3 URI, validate S3 credentials are available
            if matches!(location, TableLocation::S3 { .. }) {
                let effective_storage = target.storage.as_ref().unwrap_or(&self.storage);
                match &effective_storage.s3 {
                    None => {
                        errors.push(ConfigValidationError {
                            field: format!("{}.storage.s3", target_field),
                            message: format!(
                                "Target '{}' uses s3:// URI but has no S3 storage configuration (neither target-level nor global)",
                                name
                            ),
                        });
                    }
                    Some(s3) => {
                        if s3.endpoint.is_empty() {
                            errors.push(ConfigValidationError {
                                field: format!("{}.storage.s3.endpoint", target_field),
                                message: format!("S3 endpoint cannot be empty for target '{}'", name),
                            });
                        }
                        if s3.region.is_empty() {
                            errors.push(ConfigValidationError {
                                field: format!("{}.storage.s3.region", target_field),
                                message: format!("S3 region cannot be empty for target '{}'", name),
                            });
                        }
                        if s3.access_key_id.is_empty() {
                            errors.push(ConfigValidationError {
                                field: format!("{}.storage.s3.access_key_id", target_field),
                                message: format!("S3 access key ID cannot be empty for target '{}'", name),
                            });
                        }
                        if s3.secret_access_key.is_empty() {
                            errors.push(ConfigValidationError {
                                field: format!("{}.storage.s3.secret_access_key", target_field),
                                message: format!("S3 secret access key cannot be empty for target '{}'", name),
                            });
                        }
                    }
                }
            }
        }
        Err(e) => {
            errors.push(ConfigValidationError {
                field: format!("{}.table_path", target_field),
                message: e,
            });
        }
    }

    // Validate compression levels if specified
    if let Some(level) = target.compression_level {
        if ZstdLevel::try_new(level).is_err() {
            errors.push(ConfigValidationError {
                field: format!("{}.compression_level", target_field),
                message: format!(
                    "Compression level {} is invalid for target '{}'. Must be between 1 and 22",
                    level, name
                ),
            });
        }
    }
    if let Some(level) = target.heavy_column_compression_level {
        if ZstdLevel::try_new(level).is_err() {
            errors.push(ConfigValidationError {
                field: format!("{}.heavy_column_compression_level", target_field),
                message: format!(
                    "Heavy column compression level {} is invalid for target '{}'. Must be between 1 and 22",
                    level, name
                ),
            });
        }
    }
}
```

**Verification:**

Run: `cargo build`
Expected: Builds without errors

Run: `cargo test`
Expected: All existing tests pass (empty targets map means no validation runs for existing tests)

**Commit:** `feat: add named target validation to Config::validate()`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add validation tests for named targets

**Verifies:** named-targets.AC3.1, named-targets.AC3.2, named-targets.AC3.3, named-targets.AC3.4, named-targets.AC3.5, named-targets.AC3.6

**Files:**
- Modify: `src/config.rs` (add tests to existing `#[cfg(test)] mod tests` block)

**Testing:**

Tests follow the existing validation test pattern: construct a Config with `test_config()`, set specific target values, call `config.validate()`, check result.

Tests must verify each AC listed above:
- named-targets.AC3.1: Create Config with one target having `table_path: "file:///tmp/test"`, no storage config needed. Call `validate()`, verify it returns `Ok(())` (no validation errors related to this target).
- named-targets.AC3.2: Create Config with one target having `table_path: "s3://bucket/path"` and a complete `StorageConfig` with S3 credentials (per-target storage with non-empty endpoint, region, access_key_id, secret_access_key). Call `validate()`, verify it returns `Ok(())`.
- named-targets.AC3.3: Create Config with one target having `table_path: "/tmp/bare-path"` (no URI scheme). Call `validate()`, verify it returns `Err` with an error whose field is `targets.<name>.table_path`.
- named-targets.AC3.4: Create Config with one target having `table_path: "s3://bucket/path"` but no S3 storage config (neither per-target nor global). Call `validate()`, verify it returns `Err` with an error whose field contains `storage.s3`. Also test: S3 URI with per-target storage that has empty credentials (endpoint, region, etc.) — verify each missing credential produces an error.
- named-targets.AC3.5: Create Config with one target having `compression_level: Some(0)` (below range) and another with `compression_level: Some(23)` (above range). Call `validate()`, verify errors for both. Also test `heavy_column_compression_level` out of range.
- named-targets.AC3.6: Create Config with `targets: HashMap::new()` (empty). Call `validate()`, verify it returns `Ok(())` (no target-related errors).

Follow existing naming convention: `test_validate_target_*`.

**Verification:**

Run: `cargo test config::tests::test_validate_target`
Expected: All new validation tests pass

Run: `cargo test`
Expected: All tests pass

**Commit:** `test: add validation tests for named targets`

<!-- END_TASK_2 -->

<!-- END_SUBCOMPONENT_A -->
