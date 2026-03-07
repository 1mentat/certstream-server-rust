# Named Targets Design

## Summary

The named targets feature replaces the current approach where CLI operations (migrate, extract-metadata, backfill, merge, reparse-audit) reference Delta table locations through ad-hoc flags like `--output <path>`, `--staging-path <path>`, and `--source <path>`. Instead, operators define named entries in a `targets:` section of `config.yaml`, each specifying a table URI, optional per-target storage credentials (for S3), and optional write settings. CLI commands then reference these entries by name via `--target <name>` and `--source <name>`, keeping raw filesystem or cloud paths out of shell invocations entirely.

The implementation is structured as four cooperating layers. The config layer adds a `HashMap<String, TargetConfig>` to the existing `Config` struct and validates every entry at startup. The resolution layer provides a `resolve_target()` function that looks up a named entry and fills any unspecified write settings from `delta_sink` defaults, producing a fully concrete `ResolvedTarget` struct with pre-computed storage options. The CLI layer removes the raw-path flags and adds `--target` / `--source`. The dispatch layer in `main.rs` calls `resolve_target()` before invoking any operation function, so each operation receives a `ResolvedTarget` directly rather than a `Config` plus scattered string arguments.

## Definition of Done
A `targets` config section supporting named Delta table targets, each with table_path (URI), per-target storage credentials, and write settings (compression_level, heavy_column_compression_level, offline_batch_size). CLI `--target <name>` and `--source <name>` flags replace the current ad-hoc `--output`, `--staging-path`, `--source <path>` flags. All transformation modes (migrate, extract-metadata, backfill w/ staging, merge) are refactored to resolve their source/target from named target configs. Missing target fields fall back to `delta_sink` values. Existing functionality is preserved — nothing breaks for users who don't use targets.

## Acceptance Criteria

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

### named-targets.AC3: Validation
- **named-targets.AC3.1 Success:** Valid target with `file://` table_path passes validation
- **named-targets.AC3.2 Success:** Valid target with `s3://` table_path and complete S3 storage passes validation
- **named-targets.AC3.3 Failure:** Target with bare path (no URI scheme) fails validation
- **named-targets.AC3.4 Failure:** Target with `s3://` table_path but missing S3 credentials fails validation
- **named-targets.AC3.5 Failure:** Target with compression_level outside 1-22 range fails validation
- **named-targets.AC3.6 Success:** Empty targets map passes validation (no targets defined is valid)

### named-targets.AC4: CLI flags
- **named-targets.AC4.1 Success:** `--target foo` parses correctly into `CliArgs.target = Some("foo")`
- **named-targets.AC4.2 Success:** `--source bar` parses correctly into `CliArgs.source = Some("bar")`
- **named-targets.AC4.3 Success:** `--output`, `--staging-path` flags removed (not recognized)
- **named-targets.AC4.4 Success:** Help text shows `--target` and `--source` flags with descriptions

### named-targets.AC5: Dispatch resolution
- **named-targets.AC5.1 Failure:** `--migrate` without `--source` and `--target` exits with error
- **named-targets.AC5.2 Failure:** `--migrate --source X` without `--target` exits with error
- **named-targets.AC5.3 Failure:** `--extract-metadata` without `--source` and `--target` exits with error
- **named-targets.AC5.4 Failure:** `--merge` without `--source` and `--target` exits with error
- **named-targets.AC5.5 Failure:** `--backfill` without `--target` exits with error
- **named-targets.AC5.6 Failure:** `--reparse-audit` without `--source` exits with error
- **named-targets.AC5.7 Failure:** `--target nonexistent` where name not in config exits with error
- **named-targets.AC5.8 Failure:** `--target` combined with `--sink zerobus` exits with error (irrelevant for zerobus)

### named-targets.AC6: Operation function signatures
- **named-targets.AC6.1 Success:** `run_migrate` accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` and writes to target table_path
- **named-targets.AC6.2 Success:** `run_extract_metadata` accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` and writes to target table_path
- **named-targets.AC6.3 Success:** `run_merge` accepts `(source: ResolvedTarget, target: ResolvedTarget, ...)` and merges source into target
- **named-targets.AC6.4 Success:** `run_backfill` accepts `(config, target: ResolvedTarget, ...)` and writes to target table_path
- **named-targets.AC6.5 Success:** `run_reparse_audit` accepts `(source: ResolvedTarget, ...)` and reads from source table_path
- **named-targets.AC6.6 Success:** No operation function references `config.delta_sink` for table paths or write settings

### named-targets.AC7: Integration and cleanup
- **named-targets.AC7.1 Success:** Full config -> CLI -> resolve -> migrate flow works end-to-end
- **named-targets.AC7.2 Success:** No dead code from old `--output`/`--staging-path`/`--source <path>` patterns remains
- **named-targets.AC7.3 Success:** CLAUDE.md updated to reflect new CLI and config contracts

## Glossary

- **Delta Lake / Delta table**: Open-source storage layer adding ACID transactions on top of Parquet files. Every operation in this codebase that reads or writes archived CT records does so through a Delta table.
- **`TargetConfig`**: The new per-entry struct in the `targets:` config section. Holds `table_path`, optional `StorageConfig`, and optional write settings. Fields not specified fall back to `delta_sink` values via `resolve_target()`.
- **`ResolvedTarget`**: The concrete, fully-populated struct produced by `resolve_target()`. All optional fields have been filled in, and storage credentials have been converted to the `HashMap<String, String>` format that `DeltaTableBuilder` accepts.
- **`StorageConfig` / `S3StorageConfig`**: Existing config structs that hold S3 connection details (endpoint, region, credentials). Named targets add an optional per-target `StorageConfig` alongside the existing global `config.storage`.
- **`parse_table_uri()`**: Existing function in `src/config.rs` that converts a URI string into a `TableLocation` enum (`Local` or `S3`), rejecting bare paths and unsupported schemes.
- **`resolve_storage_options()`**: Existing function in `src/config.rs` that converts a `TableLocation` + `StorageConfig` into the `HashMap<String, String>` expected by `DeltaTableBuilder`.
- **`open_or_create_table()`**: Universal Delta table entry point in `src/delta_sink.rs`. Accepts a path string, Arrow schema, and storage options map. Named targets change how its arguments are assembled, not the function itself.
- **WriterProperties**: Parquet-writer configuration (from the `parquet` crate) controlling per-column encoding and compression. `delta_sink::delta_writer_properties()` builds the instance used across all write paths.
- **ZSTD / `ZstdLevel`**: Zstandard compression applied to Parquet columns. `ZstdLevel::try_new(n)` validates compression level range (1-22); reused for named-target validation.
- **`delta_sink`**: The existing live-streaming sink that writes CT records to a Delta table as they arrive. Its config (`DeltaSinkConfig`) provides fallback values for named targets that don't specify their own.
- **Staging table**: A separate Delta table used as a temporary write destination during backfill, later merged into the main table. Currently specified by `--staging-path`; after this change, specified by `--target`.
- **ZeroBus**: A Databricks streaming ingestion SDK. Alternative write backend for backfill (`--sink zerobus`). Unaffected by named targets.
- **Dispatch layer**: The branching logic in `main.rs` that selects an execution mode and calls the corresponding operation function. After this change, each mode's dispatch block resolves named targets before calling the operation.

## Architecture

Named targets introduce a `targets` section in `config.yaml` where each entry is a named Delta table destination (or source) with its own table_path, storage credentials, and write settings. CLI operations reference targets by name instead of passing raw paths.

**Config layer:** A `TargetConfig` struct holds `table_path: String`, `storage: Option<StorageConfig>`, and `Option<T>` wrappers for write settings (`compression_level`, `heavy_column_compression_level`, `offline_batch_size`). `Config` gains a `targets: HashMap<String, TargetConfig>` field (default: empty map). Env var pattern: `CERTSTREAM_TARGETS_<NAME>_<FIELD>`.

**Resolution layer:** A `resolve_target()` function in `src/config.rs` takes a target name, looks it up in `config.targets`, fills `None` fields from `config.delta_sink`, resolves storage options (target-level `StorageConfig` with fallback to global `config.storage`), and produces a `ResolvedTarget` struct:

```rust
pub struct ResolvedTarget {
    pub table_path: String,
    pub storage_options: HashMap<String, String>,
    pub compression_level: i32,
    pub heavy_column_compression_level: i32,
    pub offline_batch_size: usize,
}
```

**CLI layer:** Two new flags: `--target <name>` (write destination) and `--source <name>` (read source). Both resolve from `config.targets` exclusively — no reserved names, no implicit delta_sink references. Removed flags: `--output`, `--staging-path`, `--source <path>`.

**Dispatch layer:** `main.rs` resolves targets before calling operation functions. Operations receive `ResolvedTarget` values directly instead of `Config` + scattered CLI args.

**Data flow:**

```
config.yaml targets section
  -> Config::load() deserializes HashMap<String, TargetConfig>
  -> Config::validate() validates all targets (URIs, compression ranges, S3 creds)
  -> CLI --target/--source parsed in CliArgs
  -> main.rs dispatch: resolve_target(name, &config) -> ResolvedTarget
  -> operation function receives ResolvedTarget(s)
  -> operation calls open_or_create_table(resolved.table_path, schema, resolved.storage_options)
```

**What stays unchanged:** `delta_sink` and `zerobus_sink` configs, the live streaming sink, `query_api` config, global `storage` section (now also serves as fallback for targets without per-target storage).

## Existing Patterns

Investigation found mature, consistent patterns in the codebase that this design follows:

**Config loading:** `Config::load()` in `src/config.rs` loads YAML first, then overlays env vars with `CERTSTREAM_<SECTION>_<FIELD>` convention. Named targets follow this: `CERTSTREAM_TARGETS_<NAME>_<FIELD>`. Config structs use `#[serde(default)]` with default functions.

**URI parsing and storage resolution:** `parse_table_uri()` and `resolve_storage_options()` in `src/config.rs` are the established pair for converting a table_path string into a `TableLocation` + `HashMap<String, String>` storage options. Every module that opens a Delta table calls these two functions. Named target resolution reuses them directly.

**Table operations:** `open_or_create_table(table_path, schema, storage_options)` in `src/delta_sink.rs` is the universal entry point. All write paths (live sink, backfill, migrate, extract-metadata) already use it. Named targets don't change this function — they change how its arguments are assembled.

**Validation:** `Config::validate()` collects all errors in `Vec<ConfigValidationError>` and returns them together. Named target validation follows this pattern — iterate all targets, validate URI format, compression ranges, and S3 credential completeness.

**Divergence:** Currently, `resolve_storage_options()` takes a global `StorageConfig`. Named targets introduce per-target `StorageConfig` — the function signature doesn't change, but the caller passes target-level config instead of (or falling back to) global config. This is the only structural divergence.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: TargetConfig and ResolvedTarget Structs
**Goal:** Define the config structures and resolution logic without changing any CLI or operations.

**Components:**
- `TargetConfig` struct in `src/config.rs` — `table_path: String`, `storage: Option<StorageConfig>`, `Option<i32>` compression fields, `Option<usize>` offline_batch_size
- `ResolvedTarget` struct in `src/config.rs` — all fields concrete (no Options), includes pre-computed `storage_options: HashMap<String, String>`
- `targets: HashMap<String, TargetConfig>` field on `Config` struct in `src/config.rs`
- `resolve_target()` method on `Config` in `src/config.rs` — lookup + inheritance + storage resolution
- YAML deserialization for `targets` section in `Config::load()` / `YamlConfig`
- Env var loading for `CERTSTREAM_TARGETS_<NAME>_<FIELD>` pattern in `Config::load()`

**Dependencies:** None (first phase)

**Done when:** `TargetConfig` deserializes from YAML, `resolve_target()` produces correct `ResolvedTarget` with inheritance from `delta_sink` and storage fallback, all tests pass for named-targets.AC1.* and named-targets.AC2.*
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Validation
**Goal:** Validate all defined targets at config load time.

**Components:**
- Target validation in `Config::validate()` in `src/config.rs` — iterate targets, validate URI format via `parse_table_uri()`, compression ranges via `ZstdLevel::try_new()`, S3 credential completeness when S3 URIs used
- Validation of target names (non-empty, valid characters)

**Dependencies:** Phase 1 (structs exist)

**Done when:** Invalid targets produce validation errors at startup, valid targets pass validation, all tests pass for named-targets.AC3.*
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: CLI Changes
**Goal:** Replace ad-hoc CLI flags with `--target` and `--source`.

**Components:**
- `CliArgs` struct in `src/cli.rs` — remove `staging_path`, `migrate_output`, `migrate_source`, `output_path` fields; add `target: Option<String>`, `source: Option<String>`
- CLI parser in `src/cli.rs` — remove `--output`, `--staging-path`, `--source <path>` parsing; add `--target <name>`, `--source <name>` parsing
- Help text in `src/cli.rs` — update `print_help()` to reflect new flags and remove old ones
- `config.example.yaml` — add `targets:` section with commented examples

**Dependencies:** Phase 1 (structs exist to validate against)

**Done when:** New flags parse correctly, old flags removed, help text updated, all tests pass for named-targets.AC4.*
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Main Dispatch Refactor
**Goal:** Resolve named targets in `main.rs` dispatch and pass `ResolvedTarget` to operations.

**Components:**
- Target resolution in `main.rs` — each mode's dispatch block resolves `--target`/`--source` to `ResolvedTarget` via `config.resolve_target(name)`
- Validation in `main.rs` — check required flags per mode (e.g., migrate requires both --source and --target), check target names exist in config
- Error messages in `main.rs` — clear errors for missing/unknown target names

**Dependencies:** Phase 1 (resolution), Phase 2 (validation), Phase 3 (CLI flags)

**Done when:** Each mode resolves targets before calling operation functions, unknown target names produce errors, missing required flags produce errors, all tests pass for named-targets.AC5.*
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Refactor Operation Functions
**Goal:** Change operation function signatures to accept `ResolvedTarget` instead of `Config` + raw paths.

**Components:**
- `run_migrate()` in `src/backfill.rs` — signature changes to `(source: ResolvedTarget, target: ResolvedTarget, from_date, to_date, shutdown)`, remove internal `config.delta_sink.*` references
- `run_extract_metadata()` in `src/table_ops.rs` — signature changes to `(source: ResolvedTarget, target: ResolvedTarget, from_date, to_date, shutdown)`
- `run_merge()` in `src/backfill.rs` — signature changes to `(source: ResolvedTarget, target: ResolvedTarget, shutdown)`
- `run_backfill()` in `src/backfill.rs` — signature changes to `(config, target: ResolvedTarget, backfill_from, backfill_logs, backfill_sink, shutdown)`, `config` retained for CT log settings and zerobus config
- `run_reparse_audit()` in `src/table_ops.rs` — signature changes to `(source: ResolvedTarget, from_date, to_date, shutdown)`

**Dependencies:** Phase 4 (dispatch passes ResolvedTarget)

**Done when:** All operation functions use `ResolvedTarget` fields instead of `config.delta_sink.*`, no operation function takes raw path strings, internal `parse_table_uri`/`resolve_storage_options` calls removed from operation functions (moved to resolution layer), all existing tests updated and passing for named-targets.AC6.*
<!-- END_PHASE_5 -->

<!-- START_PHASE_6 -->
### Phase 6: Integration Testing and Cleanup
**Goal:** Verify end-to-end flows work with named targets and clean up dead code.

**Components:**
- Integration tests exercising full config → CLI → resolve → operation flow
- Remove dead CLI fields and parsing code that was replaced
- Remove any remaining references to old `--output`, `--staging-path` patterns
- Update CLAUDE.md contracts to reflect new CLI and config structure

**Dependencies:** Phase 5 (all operations refactored)

**Done when:** End-to-end flows verified, no dead code from old CLI patterns remains, CLAUDE.md updated, all tests pass for named-targets.AC7.*
<!-- END_PHASE_6 -->

## Additional Considerations

**Backfill with --sink zerobus:** Backfill's `--sink zerobus` flag still works alongside `--target`. The target is only used for Delta writes. When `--sink zerobus`, the zerobus config comes from `config.zerobus_sink` (unchanged). The `--target` flag would be irrelevant for zerobus sink backfill — validation should reject `--target` when `--sink zerobus`.

**Future sync operation:** The named targets config is designed to support a future sync operation (copy data between targets). That operation would use `--source X --target Y` with the same resolution flow. No design changes needed — just a new operation function that accepts two `ResolvedTarget` values.
