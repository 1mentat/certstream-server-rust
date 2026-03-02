# Delta Encoding Optimization Design

## Summary

This design optimizes how certificate transparency records are stored in the Delta Lake table by targeting two categories of inefficiency. First, Parquet encoding settings are tuned per-column: low-cardinality columns (such as `source_name`, `update_type`, and `issuer_aggregated`) get dictionary encoding to exploit repetition across millions of records, while high-cardinality columns that would defeat dictionary encoding (such as `fingerprint` and `sha256`) get plain encoding. A second, higher compression level is applied specifically to the `as_der` column — the largest column by byte volume — without penalizing faster-path columns. A new `delta_writer_properties()` function centralizes these settings so all three write paths (live sink, backfill, and merge) stay consistent. Second, the `as_der` column type changes from a base64-encoded UTF-8 string to raw binary bytes, eliminating the ~33% size inflation that base64 encoding introduces on the largest column in the schema.

Because the type change is not backward-compatible with existing Delta tables, a `--migrate` CLI mode is added that reads an existing table partition-by-partition, decodes the old base64 strings back to raw bytes, and writes a new table at a separate output path using the updated schema and encoding settings. The source table is left untouched throughout. Once the migration is complete, the operator performs a directory swap during a coordinated downtime window. The ZeroBus sink, which sends records over a separate protobuf wire format, is updated to re-apply base64 encoding when populating its protobuf string field, keeping that integration unaffected by the internal schema change.

## Definition of Done
1. **Per-column encoding optimization** — `WriterProperties` configured with dictionary encoding for low-cardinality columns (`update_type`, `source_name`, `source_url`, `signature_algorithm`, `issuer_aggregated`), appropriate compression levels per column, and tuned row group/page sizes. Applied to all three write paths (live sink, backfill, merge).

2. **Schema change: `as_der` from Utf8 to Binary** — Store raw DER bytes instead of base64-encoded strings, eliminating the ~33% base64 overhead on the largest column (~40-50% of row size).

3. **Schema change: `chain` column format optimization** — Evaluate and implement a more efficient storage format for chain certificates (currently List<Utf8> of JSON strings). Specific format determined during brainstorming.

4. **Table migration tool** — A CLI mode (`--migrate --output <path>`) that reads an existing Delta table and writes a new table with the updated schema at the output path. Source table is unmodified. Must be developable/testable independently and then runnable against the production table at `~/work/certstream`.

5. **No regression** — Query API continues to work (it excludes `as_der`/`chain`). All existing tests pass. All write paths produce valid Delta tables.

**Out of scope:**
- Sorting records before writing (architectural changes to broadcast channel flow)
- Custom zstd dictionaries (not supported by Parquet/arrow-rs)
- Changes to the ZeroBus sink (separate protobuf path, unaffected)

## Acceptance Criteria

### delta-encoding-opt.AC1: Per-column encoding optimization
- **delta-encoding-opt.AC1.1 Success:** WriterProperties enables dictionary encoding for update_type, source_name, source_url, signature_algorithm, issuer_aggregated, and chain list items
- **delta-encoding-opt.AC1.2 Success:** WriterProperties disables dictionary encoding for high-cardinality columns (cert_link, serial_number, fingerprint, sha256, sha1, subject_aggregated, as_der)
- **delta-encoding-opt.AC1.3 Success:** as_der column uses heavy_column_compression_level; all other columns use compression_level
- **delta-encoding-opt.AC1.4 Success:** All three write paths (live sink, backfill, merge) use the centralized delta_writer_properties() function

### delta-encoding-opt.AC2: as_der schema change
- **delta-encoding-opt.AC2.1 Success:** as_der stored as Binary (raw DER bytes) in Delta table, not base64 Utf8
- **delta-encoding-opt.AC2.2 Success:** Round-trip: write DER bytes → read back → bytes match original
- **delta-encoding-opt.AC2.3 Success:** ZeroBus sink correctly base64-encodes raw bytes for protobuf string field

### delta-encoding-opt.AC3: Table migration
- **delta-encoding-opt.AC3.1 Success:** --migrate --output <path> reads source table and writes new table with Binary as_der at output path
- **delta-encoding-opt.AC3.2 Success:** Migrated as_der values match STANDARD.decode() of original base64 strings
- **delta-encoding-opt.AC3.3 Success:** All non-as_der columns pass through unchanged
- **delta-encoding-opt.AC3.4 Failure:** Graceful shutdown (CancellationToken) leaves completed partitions in output table and does not corrupt data

### delta-encoding-opt.AC4: Configuration
- **delta-encoding-opt.AC4.1 Success:** heavy_column_compression_level defaults to 15, configurable via CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL
- **delta-encoding-opt.AC4.2 Failure:** Invalid heavy_column_compression_level (outside 1-22) rejected at config validation

### delta-encoding-opt.AC5: No regression
- **delta-encoding-opt.AC5.1 Success:** Query API returns correct results against tables written with new schema
- **delta-encoding-opt.AC5.2 Success:** All existing tests pass after changes
- **delta-encoding-opt.AC5.3 Success:** ZeroBus sink continues to function (protobuf as_der field populated correctly)

## Glossary

- **Arrow**: An in-memory columnar data format (from the Apache Arrow project) used by DataFusion and the `deltalake` crate to represent tabular data as typed arrays before writing to Parquet files.
- **base64**: A text encoding that represents arbitrary binary data using 64 printable ASCII characters, inflating the original byte size by approximately 33%.
- **broadcast channel**: A Tokio multi-producer, multi-consumer channel where every active receiver sees every message. Used here so the live sink, WebSocket, SSE, and other consumers each independently read the same stream of CT records.
- **CancellationToken**: A Tokio utility that signals cooperative shutdown to async tasks. Tasks check the token periodically and stop cleanly when it is cancelled.
- **Certificate Transparency (CT)**: A public ecosystem of append-only logs operated by various parties (Google, Let's Encrypt, Cloudflare, etc.) that record every TLS certificate issued by trusted CAs. This project monitors those logs in real time.
- **chain / chain certificate**: The sequence of intermediate CA certificates that links a leaf certificate to a trusted root. Stored as a list of JSON strings in the Delta schema.
- **DataFusion**: An in-process SQL query engine built on Apache Arrow, used here to read Delta tables for gap detection, merge operations, and the migration tool.
- **Delta Lake**: An open-source storage layer that adds ACID transactions, schema enforcement, and time travel to Parquet files stored on a filesystem or object store. The `_delta_log/` directory contains the transaction log.
- **DeltaCertRecord**: The Rust struct in `src/delta_sink.rs` that represents one row in the Delta table. It is the in-memory form of a CT record before conversion to an Arrow `RecordBatch`.
- **DER (Distinguished Encoding Rules)**: The canonical binary encoding of an X.509 certificate, defined in ASN.1 standards. A DER-encoded certificate is a compact byte sequence that parsers and TLS stacks consume directly.
- **dictionary encoding**: A Parquet column encoding that stores a lookup table of unique values and replaces each row value with a small integer index into that table. Effective when column cardinality is low relative to row count; counterproductive for high-cardinality columns where the dictionary overflows and falls back to plain encoding.
- **`flush_buffer()`**: A public function in `src/delta_sink.rs` that converts a buffered list of `DeltaCertRecord`s into an Arrow `RecordBatch` and writes it to the Delta table. Shared by the live sink, backfill fetchers, and the merge path.
- **issuer_aggregated**: A denormalized string column combining key issuer fields into a single searchable value. Repeated across millions of records because a relatively small number of CAs issue the bulk of certificates.
- **leaf certificate**: The end-entity TLS certificate issued to a domain owner, as opposed to intermediate CA or root CA certificates. The primary subject of each CT log entry.
- **Parquet**: A columnar binary file format optimized for analytical workloads, used as the physical storage format by Delta Lake. Supports pluggable encoding (dictionary, RLE, PLAIN) and compression (zstd, snappy, gzip) per column.
- **partition**: In Delta Lake, a directory-level split of the table data by the value of one or more columns. Here, records are partitioned by `seen_date` (YYYY-MM-DD), so each day's data lives in its own subdirectory.
- **protobuf (Protocol Buffers)**: A binary serialization format from Google, used as the wire format for the ZeroBus sink. The schema is defined in `proto/cert_record.proto` and compiled to Rust by `prost-build` at build time.
- **RecordBatch**: The fundamental unit of data in Apache Arrow: a fixed number of rows across a set of typed Arrow arrays with a shared schema.
- **row group**: The top-level unit of data within a Parquet file. Encoding and compression statistics are tracked per row group; tuning row group size trades read amplification against memory usage during writes.
- **`update_type`**: A two-value column (`X509LogEntry` or `PrecertLogEntry`) indicating whether a CT entry is a final certificate or a precertificate. Extremely low cardinality; dictionary encoding reduces it to a single-bit index in practice.
- **`WriterProperties`**: A struct from the `parquet` crate (used via `arrow` / `deltalake`) that configures how Parquet files are written — including per-column encoding hints, compression codec, row group size, and page size.
- **ZeroBus**: A Databricks-internal streaming ingestion service. The ZeroBus sink sends CT records to a Unity Catalog Delta table via a gRPC-based SDK, using protobuf as the wire format. It is a separate write path from the local Delta sink and is unaffected by the schema changes.
- **zstd (Zstandard)**: A fast lossless compression algorithm used as the Parquet codec in this project. Supports compression levels 1-22; higher levels compress more but are slower to write. The design uses level 9 for most columns and level 15 for `as_der`.

## Architecture

Two categories of change: **schema changes** that alter column types in the Delta table, and **WriterProperties tuning** that optimizes how Parquet files encode and compress data without changing the logical schema.

### Schema Changes

**`as_der` column: `DataType::Utf8` → `DataType::Binary`**

The leaf certificate DER is currently base64-encoded in `src/ct/parser.rs:124` via `STANDARD.encode(der_bytes)` and stored as a UTF-8 string. This inflates the data by ~33%. The raw DER bytes (`der_bytes: &[u8]`) are available before encoding. The change removes the base64 step and stores raw bytes directly.

Affected types flow through: `parse_certificate()` → `LeafCert.as_der` (`src/models/certificate.rs`) → `DeltaCertRecord.as_der` (`src/delta_sink.rs`) → `records_to_batch()` → Arrow `BinaryArray` → Parquet BYTE_ARRAY.

**`chain` column: stays `List<Utf8>` (JSON strings)**

Brainstorming determined that chain items — JSON-serialized `ChainCert` metadata without DER bytes (`as_der: null`) — benefit more from dictionary encoding than from format changes. There are only ~500-1000 unique intermediate CAs in the CT ecosystem, so chain item strings repeat heavily across millions of records. Parquet dictionary encoding captures this repetition effectively. Changing to a binary serialization format (CBOR/MessagePack) would add complexity for marginal gain (~3-5% on a column that's already well-served by dictionary + zstd).

### WriterProperties Configuration

A new `delta_writer_properties(compression_level: i32, heavy_column_compression_level: i32)` function in `src/delta_sink.rs` becomes the single source of truth for all Parquet write settings. All three write paths call this function instead of constructing `WriterProperties` inline.

**Per-column dictionary encoding:**
- Enabled: `update_type` (2 values), `source_name` (~100-200), `source_url` (~100-200), `signature_algorithm` (~15-20), `issuer_aggregated` (~500-1000), `chain.item` (list items, ~500-1000 unique intermediate CAs)
- Disabled: `cert_link`, `serial_number`, `fingerprint`, `sha256`, `sha1`, `subject_aggregated`, `as_der` (all high cardinality — dictionary would overflow and fall back to PLAIN)

**Per-column compression levels:**
- `as_der`: uses `heavy_column_compression_level` (default 15). This is the largest column, never queried, and benefits most from higher compression at the cost of write speed.
- All other columns: use `compression_level` (default 9).

### Table Migration

A new `--migrate` CLI mode reads the existing table partition-by-partition via DataFusion, transforms `as_der` from base64 Utf8 to raw Binary, and writes to a new output table with the updated schema and optimized WriterProperties. The output path is specified via `--migrate --output <path>`. The user handles the directory swap during coordinated downtime (stop server → move old dir aside → move output dir to original path → start server).

### Boundary: ZeroBus Sink

The ZeroBus sink (`src/zerobus_sink.rs`) reads from the broadcast channel, not from Delta. Its protobuf schema expects `as_der` as a string. `CertRecord::from_delta_cert()` must base64-encode the raw bytes when populating the protobuf field. This is the only consumer boundary affected by the schema change.

## Existing Patterns

### WriterProperties construction
The codebase already constructs `WriterProperties` with zstd compression in three places: `delta_sink.rs:504-509` (live sink), `backfill.rs` (reuses `flush_buffer()`), and `backfill.rs:1001-1007` (merge). All use the same pattern: `WriterProperties::builder().set_compression(Compression::ZSTD(...)).build()`. The new `delta_writer_properties()` function centralizes this.

### CLI modes
The binary already has three execution modes (`--backfill`, `--merge`, default server). The new `--migrate` mode follows the same pattern: a function in `backfill.rs` that takes config and shutdown token, returns an exit code.

### Configuration pattern
`DeltaSinkConfig` uses serde defaults and `CERTSTREAM_DELTA_SINK_*` env var overrides. The new `heavy_column_compression_level` field follows the same pattern with `CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL`.

### Partition-by-partition processing
The merge mode already reads and processes data partition-by-partition via DataFusion SQL. The migration tool follows this established pattern.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: WriterProperties Optimization
**Goal:** Centralize and optimize Parquet encoding settings across all write paths.

**Components:**
- `delta_writer_properties()` function in `src/delta_sink.rs` — constructs `WriterProperties` with per-column dictionary encoding, per-column compression levels, and row group size settings
- `DeltaSinkConfig` in `src/config.rs` — new `heavy_column_compression_level` field with validation
- `config.example.yaml` — new field documented
- `src/delta_sink.rs:flush_buffer()` — calls `delta_writer_properties()` instead of inline builder
- `src/backfill.rs:run_merge()` — calls `delta_writer_properties()` instead of inline builder

**Dependencies:** None (first phase)

**Covers:** delta-encoding-opt.AC1.1, delta-encoding-opt.AC1.2, delta-encoding-opt.AC1.3, delta-encoding-opt.AC1.4, delta-encoding-opt.AC5.1, delta-encoding-opt.AC5.2

**Done when:** All write paths use centralized WriterProperties. Config validation rejects invalid heavy compression levels. Tests verify per-column settings are correctly applied.
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Schema Change — `as_der` to Binary
**Goal:** Store raw DER bytes instead of base64-encoded strings.

**Components:**
- `src/ct/parser.rs` — `parse_certificate()` returns `Option<Vec<u8>>` instead of `Option<String>` for `as_der`; removes `STANDARD.encode()` call
- `src/models/certificate.rs` — `LeafCert.as_der` type changes from `Option<String>` to `Option<Vec<u8>>`
- `src/delta_sink.rs` — `DeltaCertRecord.as_der` type changes from `String` to `Vec<u8>`; `delta_schema()` changes `as_der` field to `DataType::Binary`; `records_to_batch()` uses `BinaryBuilder` for `as_der` column; `from_message()` handles `Vec<u8>` directly
- `src/zerobus_sink.rs` — `CertRecord::from_delta_cert()` base64-encodes raw bytes for protobuf string field

**Dependencies:** Phase 1 (WriterProperties must be in place so new Binary column gets correct encoding settings)

**Covers:** delta-encoding-opt.AC2.1, delta-encoding-opt.AC2.2, delta-encoding-opt.AC2.3, delta-encoding-opt.AC5.1, delta-encoding-opt.AC5.2, delta-encoding-opt.AC5.3

**Done when:** Live sink writes `as_der` as Binary. Round-trip test confirms raw DER bytes are stored and retrievable. ZeroBus sink correctly base64-encodes bytes for protobuf. All existing tests pass (updated for new types).
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Table Migration Tool
**Goal:** CLI mode to migrate existing Delta tables from old schema to new schema.

**Components:**
- `src/cli.rs` — new `--migrate` and `--output <path>` flags
- `src/main.rs` — dispatch to migration function when `--migrate` is set
- Migration function in `src/backfill.rs` — reads source table at `config.delta_sink.table_path` partition-by-partition via DataFusion, decodes base64 `as_der` to raw bytes, writes to output table at `--output` path with new schema and optimized WriterProperties

**Dependencies:** Phase 2 (new schema must be finalized)

**Covers:** delta-encoding-opt.AC3.1, delta-encoding-opt.AC3.2, delta-encoding-opt.AC3.3, delta-encoding-opt.AC3.4

**Done when:** Migration tool converts a test table from old schema to new schema at the output path. Migrated data is readable with correct Binary `as_der` values. Source table is unmodified. Graceful shutdown preserves completed partitions in output table.
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Integration Verification
**Goal:** Verify all write paths produce valid tables with the new schema and encoding settings, end-to-end.

**Components:**
- Integration tests for live sink → write → read round-trip with new schema
- Integration tests for backfill writer with new schema
- Integration tests for merge with new schema
- Verify migration tool against a multi-partition test table

**Dependencies:** Phase 3 (all components must be in place)

**Covers:** delta-encoding-opt.AC4.1, delta-encoding-opt.AC4.2, delta-encoding-opt.AC5.1, delta-encoding-opt.AC5.2, delta-encoding-opt.AC5.3

**Done when:** All write paths produce tables with correct schema, correct encoding settings, and readable data. Query API works against tables written with new schema.
<!-- END_PHASE_4 -->

## Additional Considerations

**Backward compatibility:** After migration, the server cannot read old-schema tables (Binary vs Utf8 mismatch). The migration writes to a separate output directory, leaving the source table intact. Rollback means keeping the old directory and restarting with old code.

**Migration performance:** Large tables (months of CT data) may take significant time. Partition-by-partition processing keeps memory bounded. Progress logging per partition lets operators estimate completion time. CancellationToken support allows safe interruption — completed partitions are already written to the output table.

**Directory swap procedure:** The migration tool intentionally does not modify the source table or perform directory renames. The operator handles the swap during coordinated downtime: stop server, move old directory aside, move output directory to the original path, start server. This keeps the tool simple and avoids any risk of Delta transaction log path issues from directory renames.
