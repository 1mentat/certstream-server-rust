# Zstd Compression for Delta Lake Writes

## Summary

This change switches all Delta Lake write operations in certstream-server-rust from the default Snappy compression codec to Zstd (Zstandard). Zstd offers significantly better compression ratios than Snappy at the cost of slightly more CPU, which reduces storage footprint for the append-heavy certificate transparency record tables without changing any data schemas or read paths.

The implementation touches three write paths that all funnel through shared helpers: the live `delta_sink` (which streams incoming CT log entries to disk), the `backfill` writer (which fills historical gaps by batch-fetching from CT logs), and the `merge` operation (which promotes staging table data into the main table). A new `compression_level` integer field is added to `DeltaSinkConfig` with a default of 9, validated at startup using the codec library's own range check so the server refuses to start with an invalid level rather than failing silently at write time. Because Delta Lake's Parquet reader handles mixed compression codecs transparently, existing Snappy-compressed files already on disk continue to be readable alongside the new Zstd files with no migration required.

## Definition of Done
Switch all Delta Lake writes (live delta_sink, backfill, and merge) from snappy to zstd compression by passing WriterProperties with the zstd codec through flush_buffer() and any other write paths.

- New Parquet files are written with zstd compression at the configured level (default: 9)
- A `compression_level` field is added to `DeltaSinkConfig` (integer, default 9, configurable via `CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL` env var and config.yaml)
- Existing snappy data continues to read correctly (mixed-codec coexistence)
- Backfill and merge operations also use the configured compression
- Codec is hardcoded to zstd (not configurable); only the compression level is configurable
- No built-in OPTIMIZE command; recompression of existing data is handled externally

## Acceptance Criteria

### zstd-compression.AC1: Compression level is configurable
- **zstd-compression.AC1.1 Success:** Server starts with default compression_level=9 when field is omitted from config
- **zstd-compression.AC1.2 Success:** Server starts with compression_level=1 when explicitly set in config.yaml
- **zstd-compression.AC1.3 Failure:** Server exits with clear error when compression_level=0 (below valid range)
- **zstd-compression.AC1.4 Failure:** Server exits with clear error when compression_level=23 (above valid range)

### zstd-compression.AC2: All Delta writes use zstd compression
- **zstd-compression.AC2.1 Success:** Live delta_sink writes Parquet files with ZSTD codec at configured level
- **zstd-compression.AC2.2 Success:** Backfill writer writes Parquet files with ZSTD codec at configured level
- **zstd-compression.AC2.3 Success:** Merge operation writes Parquet files with ZSTD codec at configured level
- **zstd-compression.AC2.4 Success:** Existing snappy-compressed files remain readable after new zstd files are written to the same table

## Glossary

- **Delta Lake**: An open-source storage layer built on top of Parquet files that adds ACID transactions, schema enforcement, and time-travel via a transaction log (`_delta_log/`). Used here to persist Certificate Transparency records durably.
- **Parquet**: A columnar binary file format optimized for analytical workloads. Delta Lake stores its data as a collection of Parquet files.
- **Snappy**: A fast, low-CPU compression codec used as the default by Parquet/Delta Lake. Prioritizes compression and decompression speed over ratio.
- **Zstd (Zstandard)**: A general-purpose compression algorithm developed by Meta that achieves higher compression ratios than Snappy, especially at higher levels, with acceptable CPU overhead. Levels run from 1 (fastest, least compression) to 22 (slowest, most compression).
- **ZstdLevel**: A Parquet crate type that wraps a validated zstd compression level integer. `ZstdLevel::try_new()` rejects values outside the valid range, enabling fail-fast validation at startup.
- **WriterProperties**: A Parquet builder struct that controls per-file write behavior — including codec, page size, and encoding. Passed into Delta write and merge builders via `.with_writer_properties()`.
- **delta_sink**: The live sink task in this codebase (`src/delta_sink.rs`) that consumes the broadcast channel of incoming CT certificate messages and writes them to the Delta table in batches.
- **flush_buffer()**: A public helper in `src/delta_sink.rs` that converts a buffer of `DeltaCertRecord` structs into an Arrow `RecordBatch` and writes it to the Delta table. Shared by both the live sink and the backfill writer.
- **Backfill**: An offline mode (`--backfill`) that detects and fills gaps in the Delta table by re-fetching certificate entries from CT logs, then writing them through the same writer infrastructure as the live sink.
- **Merge**: An offline mode (`--merge --staging-path`) that promotes records from a staging Delta table into the main table using a `MERGE INTO` statement with deduplication.
- **DeltaOps**: The delta-rs Rust API surface for table mutations. Provides `.write()` and `.merge()` builder methods that accept `WriterProperties`.
- **Mixed-codec coexistence**: The property of a Delta table that allows individual Parquet files within the same table to use different compression codecs. Readers select the appropriate decompressor per file.
- **OPTIMIZE**: A Delta Lake maintenance command that rewrites existing Parquet files — useful for recompressing old Snappy data to Zstd after deploying this change. Explicitly out of scope for this design.
- **Certificate Transparency (CT)**: A public logging ecosystem (RFC 6962 and Static CT API) where certificate authorities record newly issued TLS certificates. This server watches those logs and streams the entries.

## Architecture

Add zstd compression to all Delta Lake write paths by passing `WriterProperties` with `Compression::ZSTD(ZstdLevel)` through the existing write and merge operations.

**Configuration**: A new `compression_level: i32` field in `DeltaSinkConfig` (default 9) controls the zstd compression level. Validated at startup via `ZstdLevel::try_new()` to fail fast on invalid values (valid range: 1-22). Configurable via `config.yaml` (`delta_sink.compression_level`) and env var (`CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL`).

**Write path**: `flush_buffer()` in `src/delta_sink.rs` gains a `compression_level: i32` parameter. Inside, it constructs `WriterProperties` with `Compression::ZSTD(ZstdLevel::try_new(compression_level))` and chains `.with_writer_properties(props)` onto the `DeltaOps::write()` builder. Since startup validation already confirmed the level is valid, runtime construction uses `.expect()`.

**Merge path**: `run_merge()` in `src/backfill.rs` constructs `WriterProperties` with the same zstd configuration and chains `.with_writer_properties(props)` onto the `DeltaOps::merge()` builder. The compression level comes from `config.delta_sink.compression_level`.

**Data flow**: Config loads `compression_level` -> startup validates via `ZstdLevel::try_new()` -> value threaded to `run_delta_sink()`, backfill writer task, and `run_merge()` -> each write/merge operation constructs `WriterProperties` from the level.

**Mixed-codec coexistence**: Existing snappy-compressed Parquet files remain readable. Delta Lake / Arrow Parquet readers handle mixed compression codecs transparently within a single table. New files are written with zstd; old files are untouched until an external OPTIMIZE rewrites them.

## Existing Patterns

Investigation found that `DeltaSinkConfig` in `src/config.rs` follows a consistent pattern: serde `Deserialize` with `#[serde(default)]` on each field, default functions for each value, and env var overrides following `CERTSTREAM_DELTA_SINK_*` naming. The new `compression_level` field follows this exact pattern.

`flush_buffer()` in `src/delta_sink.rs` is a public helper reused by both the live sink (`run_delta_sink()`) and the backfill writer task. Adding a parameter to `flush_buffer()` propagates the change to both paths via their existing call sites.

No existing compression configuration exists anywhere in the codebase. This design introduces the first write-property customization.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Configuration and Validation

**Goal:** Add `compression_level` to config with startup validation

**Components:**
- `DeltaSinkConfig` in `src/config.rs` — new `compression_level: i32` field with default 9, serde default function, env var override
- Startup validation in `src/config.rs` or `src/main.rs` — call `ZstdLevel::try_new(config.delta_sink.compression_level)` at startup, exit with error if invalid
- `config.example.yaml` — add `compression_level: 9` to `delta_sink` section

**Dependencies:** None

**Done when:** Server starts with default compression_level=9, rejects invalid values (0, -1, 23) at startup with clear error message, accepts valid values (1-22). Tests verify config validation for valid and invalid levels.

Covers: `zstd-compression.AC1.1`, `zstd-compression.AC1.2`, `zstd-compression.AC1.3`, `zstd-compression.AC1.4`
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Write Path Changes

**Goal:** Apply zstd compression to all Delta Lake write and merge operations

**Components:**
- `flush_buffer()` in `src/delta_sink.rs` — add `compression_level: i32` parameter, construct `WriterProperties`, chain `.with_writer_properties()`
- `run_delta_sink()` in `src/delta_sink.rs` — pass `config.delta_sink.compression_level` to `flush_buffer()`
- Backfill writer task in `src/backfill.rs` — pass `compression_level` to all 4 `flush_buffer()` call sites
- `run_merge()` in `src/backfill.rs` — construct `WriterProperties` with zstd, chain `.with_writer_properties()` on merge builder
- New imports from `parquet` crate (via deltalake re-exports): `Compression`, `ZstdLevel`, `WriterProperties`

**Dependencies:** Phase 1 (config field exists)

**Done when:** New Parquet files written by delta_sink, backfill, and merge use zstd compression. Test writes a batch via `flush_buffer()` and verifies resulting Parquet file metadata shows ZSTD codec.

Covers: `zstd-compression.AC2.1`, `zstd-compression.AC2.2`, `zstd-compression.AC2.3`, `zstd-compression.AC2.4`
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Documentation

**Goal:** Update project documentation to reflect compression configuration

**Components:**
- `CLAUDE.md` — update Delta Sink Contracts section with `compression_level` field, env var, and zstd-hardcoded note
- `config.example.yaml` — ensure compression_level is documented with comment

**Dependencies:** Phase 2 (implementation complete)

**Done when:** CLAUDE.md accurately describes the compression configuration. config.example.yaml includes the new field with explanatory comment.
<!-- END_PHASE_3 -->

## Additional Considerations

**Parquet dependency**: `WriterProperties`, `Compression`, and `ZstdLevel` come from the `parquet` crate, which is a transitive dependency of `deltalake`. Import via `deltalake::datafusion::parquet::*` re-exports to avoid adding a direct `parquet` dependency to `Cargo.toml`. If re-exports are not available in deltalake 0.25, add `parquet` as a direct dependency matching the version pulled by deltalake.

**External OPTIMIZE for existing data**: After deploying, existing 123GB of snappy-compressed data can be recompressed by running OPTIMIZE externally (via delta-rs Python bindings or a one-off Rust script). This is deliberately out of scope — the table works fine with mixed codecs, and recompression can happen at leisure.
