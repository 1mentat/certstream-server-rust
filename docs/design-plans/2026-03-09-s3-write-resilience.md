# S3 Write Resilience: Provider-Aware Storage Defaults

## Summary

S3 write failures (transport errors, delta log read failures) in extract-metadata when writing to Tigris are caused by a config inheritance bug: named targets with their own `storage.s3` section completely replace the global storage config, losing `conditional_put: "etag"` which Tigris requires for correct Delta log commits. The fix adds provider detection (from endpoint URL or explicit annotation) to `resolve_storage_options`, automatically applying provider-specific defaults like `conditional_put: "etag"` for Tigris, R2, and MinIO — benefiting all code paths without per-callsite changes.

## Definition of Done
- **Provider detection**: `resolve_storage_options` detects the S3 provider from the endpoint URL (or explicit `provider` field) and applies provider-specific defaults for missing settings
- **conditional_put auto-default**: Tigris, R2, and MinIO endpoints automatically get `conditional_put: "etag"` when not explicitly configured
- **No override of explicit config**: Provider defaults only fill gaps — explicitly set values are never overridden
- **Result**: extract-metadata (and other operations) writing to Tigris complete successfully without transport errors caused by missing `conditional_put`

## Acceptance Criteria

### s3-write-resilience.AC1: Provider Detection
- **AC1.1**: When `S3StorageConfig.provider` is explicitly set (e.g., `"tigris"`), detection uses that value without consulting the endpoint URL
- **AC1.2**: When `provider` is not set, detection matches endpoint URL against known patterns: `tigris` in URL → Tigris, `amazonaws.com` → AWS, `r2.cloudflarestorage.com` → R2
- **AC1.3**: When endpoint URL matches no known pattern, provider is `Generic` (no defaults applied)
- **AC1.4**: Provider detection is case-insensitive for URL matching

### s3-write-resilience.AC2: Provider-Specific Defaults
- **AC2.1**: Tigris provider auto-sets `conditional_put: "etag"` when not explicitly configured
- **AC2.2**: R2 provider auto-sets `conditional_put: "etag"` when not explicitly configured
- **AC2.3**: MinIO provider auto-sets `conditional_put: "etag"` when not explicitly configured
- **AC2.4**: AWS and Generic providers do not auto-set `conditional_put`
- **AC2.5**: Explicitly configured `conditional_put` is never overridden by provider defaults

### s3-write-resilience.AC3: Config Integration
- **AC3.1**: `S3StorageConfig` has a new `provider: Option<String>` field with `#[serde(default)]`
- **AC3.2**: Env var `CERTSTREAM_STORAGE_S3_PROVIDER` sets the global storage provider
- **AC3.3**: Named target storage configs support `provider` field in their `storage.s3` section
- **AC3.4**: When provider defaults are applied, an info-level log message is emitted (e.g., "Provider detected as tigris from endpoint, setting conditional_put=etag")

### s3-write-resilience.AC4: No Regressions
- **AC4.1**: Existing configs without `provider` field continue to work unchanged
- **AC4.2**: Global storage config with explicit `conditional_put` is not affected
- **AC4.3**: Named targets without `storage` section still fall back to global storage config
- **AC4.4**: All existing tests pass

## Architecture

### Root Cause

Named target resolution in `Config::resolve_target()` (`config.rs:1256`) uses:
```rust
let storage = target.storage.as_ref().unwrap_or(&self.storage);
```

This is a full replacement, not a merge. When a named target has its own `storage.s3` section (e.g., for a Tigris endpoint), `conditional_put` defaults to `None` via `#[serde(default)]` on `Option<String>`. The global storage config's `conditional_put: "etag"` is lost.

Without `conditional_put: "etag"`, delta-rs uses a fallback commit protocol that doesn't work correctly on Tigris, causing cascading failures: transport errors during commit attempts, then delta log read failures.

The live delta_sink works because it uses the global `config.storage` directly (which has `conditional_put: "etag"`).

### Solution

Add provider detection to `resolve_storage_options` so that provider-specific defaults are applied after building the base storage options HashMap. This is a single-point fix that benefits all code paths.

### Data Flow

```
S3StorageConfig
  ├── provider: Option<String>     (explicit annotation)
  ├── endpoint: String             (used for auto-detection)
  └── conditional_put: Option<String> (may be None)
         │
         ▼
resolve_storage_options(location, storage)
  1. Build base HashMap from explicit config fields
  2. Detect provider: explicit provider > endpoint URL matching
  3. Apply provider defaults for missing keys
  4. Log any auto-applied defaults
  5. Return enriched HashMap
```

### Provider Registry

| Provider | URL Pattern | `conditional_put` default |
|----------|-------------|---------------------------|
| Tigris | `tigris` in endpoint | `"etag"` |
| AWS | `amazonaws.com` in endpoint | — |
| R2 | `r2.cloudflarestorage.com` in endpoint | `"etag"` |
| MinIO | explicit `provider: "minio"` only | `"etag"` |
| Generic | no match | — |

MinIO has no reliable URL pattern, so it requires explicit annotation via the `provider` field.

## Existing Patterns

- **Env var pattern**: `CERTSTREAM_<SECTION>_<FIELD>` — new field follows as `CERTSTREAM_STORAGE_S3_PROVIDER`
- **Option fields with serde default**: Same pattern as `conditional_put: Option<String>` and `allow_http: Option<bool>`
- **resolve_storage_options**: Already the single point where storage options are built — provider defaults slot in naturally at the end

## Implementation Phases

### Phase 1: Provider Detection and Defaults
**Files**: `src/config.rs`

1. Add `S3Provider` enum: `Tigris`, `Aws`, `R2`, `MinIO`, `Generic`
2. Add `detect_s3_provider(endpoint: &str, explicit_provider: &Option<String>) -> S3Provider` function
3. Add `provider: Option<String>` field to `S3StorageConfig`
4. Add env var handling for `CERTSTREAM_STORAGE_S3_PROVIDER`
5. Modify `resolve_storage_options` to detect provider and apply defaults after building base options
6. Add info logging when defaults are auto-applied

### Phase 2: Config Example and Tests
**Files**: `src/config.rs` (tests), `config.example.yaml`

1. Add unit tests for `detect_s3_provider` with each URL pattern
2. Add unit tests for `resolve_storage_options` verifying provider defaults are applied
3. Add unit test verifying explicit `conditional_put` is not overridden
4. Update `config.example.yaml` with `provider` field documentation
5. Run full test suite to verify no regressions

## Additional Considerations

- **Future extensibility**: The provider registry can be extended with additional defaults (e.g., `http1_only`, timeout tuning) without changing the detection infrastructure
- **Named target storage**: The `TargetConfig.storage` field also supports `S3StorageConfig`, so `provider` is available at both global and per-target levels
- **No breaking changes**: `provider` is `Option<String>` with `#[serde(default)]`, so existing configs are unaffected

## Glossary
- **conditional_put**: Storage option telling object_store/delta-rs to use ETag-based conditional PUTs (`If-Match`/`If-None-Match` headers) for atomic Delta log commits. Required by Tigris, R2, and MinIO for safe concurrent writes.
- **Named target**: A reusable Delta table configuration in `config.targets` (e.g., `main`, `tigris`) that can override global settings including storage config.
- **Provider detection**: Auto-identification of the S3-compatible storage provider from the endpoint URL, used to apply provider-specific defaults.
- **resolve_storage_options**: Function in `config.rs` that builds a `HashMap<String, String>` of storage configuration options from a `TableLocation` and `StorageConfig`. Single point where all code paths get their S3 credentials and settings.
