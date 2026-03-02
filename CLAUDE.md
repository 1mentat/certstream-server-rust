# certstream-server-rust

Last verified: 2026-03-02
Last context update: 2026-03-02

## Tech Stack
- Language: Rust (edition 2024)
- Async runtime: Tokio
- Web framework: Axum 0.8 (WebSocket + SSE)
- Metrics: metrics + prometheus exporter
- Storage: Delta Lake (via deltalake 0.25) for optional CT record archival
- Streaming: ZeroBus SDK (via databricks-zerobus-ingest-sdk) for optional Databricks ingestion
- Serialization: Protobuf (via prost + prost-build) for ZeroBus wire format
- Testing: cargo test (unit + integration)

## Commands
- `cargo build` - Build the project
- `cargo test` - Run all tests
- `cargo run` - Start server (reads config.yaml or env vars)
- `cargo run -- --validate-config` - Validate configuration without starting
- `cargo run -- --backfill` - Run delta backfill mode (catch-up gaps)
- `cargo run -- --backfill --from 0` - Run historical backfill from index 0
- `cargo run -- --backfill --logs "google"` - Backfill only logs matching filter
- `cargo run -- --backfill --staging-path /tmp/staging` - Backfill into staging table
- `cargo run -- --backfill --sink zerobus --from 0` - Backfill via ZeroBus sink
- `cargo run -- --merge --staging-path /tmp/staging` - Merge staging into main table
- `cargo run -- --migrate --output <PATH>` - Migrate Delta table to new schema
- `cargo run -- --migrate --output <PATH> --source <SRC> --from <DATE> --to <DATE>` - Migrate with source and date filters
- `cargo run -- --reparse-audit` - Audit stored certs against current parsing code
- `cargo run -- --reparse-audit --from-date 2026-02-01 --to-date 2026-02-28` - Audit with date filter
- `cargo run -- --extract-metadata --output /tmp/metadata` - Extract metadata-only Delta table
- `cargo run -- --extract-metadata --output /tmp/metadata --from-date 2026-02-01` - Extract with date filter

## Project Structure
- `src/main.rs` - Entry point, server startup, task orchestration
- `src/config.rs` - All configuration structs, YAML + env var loading, validation
- `src/ct/` - Certificate Transparency log fetching and watching
- `src/ct/fetch.rs` - Shared fetch functions for RFC 6962 and Static CT logs
- `src/backfill.rs` - Backfill mode: gap detection, fetcher tasks, writer task (delta or zerobus), staging merge
- `src/models/` - Data models (CertificateMessage, PreSerializedMessage)
- `src/websocket/` - WebSocket stream handlers
- `src/sse.rs` - SSE stream handler
- `src/delta_sink.rs` - Delta Lake storage sink (optional, disabled by default)
- `src/dedup.rs` - Cross-log certificate deduplication filter
- `src/query.rs` - Query API: search certificates in Delta Lake (optional, disabled by default)
- `src/api.rs` - REST API endpoints
- `src/middleware.rs` - Auth and rate limiting middleware
- `src/rate_limit.rs` - Rate limiter implementation
- `src/health.rs` - Health check endpoints
- `src/hot_reload.rs` - Config hot-reload via file watcher
- `src/state.rs` - Shared server state management
- `src/zerobus_sink.rs` - ZeroBus streaming sink (optional, disabled by default)
- `src/table_ops.rs` - Table operations: reparse audit and metadata extraction
- `src/cli.rs` - CLI argument parsing
- `proto/cert_record.proto` - Protobuf schema for ZeroBus wire format (20 fields mirroring DeltaCertRecord)
- `build.rs` - prost-build: compiles proto to Rust + file descriptor set

## Architecture
All CT log entries flow through a single `broadcast::channel<Arc<PreSerializedMessage>>`.
Consumers (WebSocket, SSE, delta_sink, zerobus_sink) each subscribe independently via `tx.subscribe()`.
The delta_sink and zerobus_sink are spawned as optional tokio tasks and do not affect other consumers.

In Delta Lake storage, the `as_der` column is stored as raw binary bytes (not base64-encoded). The WriterProperties builder applies dictionary encoding to low-cardinality columns (update_type, source_name, source_url, signature_algorithm, issuer_aggregated) and skips it for high-cardinality columns (fingerprint, sha256, sha1, serial_number, as_der, subject_aggregated, cert_link), with per-column ZSTD compression using `compression_level` for most columns and `heavy_column_compression_level` for the `as_der` column.

The binary has six execution modes selected in main.rs:
1. **Server mode** (default): starts the WebSocket/SSE server and live CT log watchers
2. **Backfill mode** (`--backfill`): runs gap detection against the Delta table, spawns per-log fetcher tasks and a single writer task, then exits with code 0 (success) or 1 (errors). With `--staging-path`, writes to a separate staging table instead of the main table.
3. **Merge mode** (`--merge --staging-path <PATH>`): merges a staging Delta table into the main table using Delta MERGE INTO with deduplication, then deletes the staging directory on success
4. **Migrate mode** (`--migrate --output <PATH>`): reads an existing Delta table, converts `as_der` from base64 Utf8 to raw Binary, and writes to a new output table with optimized WriterProperties
5. **Reparse audit mode** (`--reparse-audit`): reads stored certificates from the Delta table, reparses each from `as_der`, compares parsed fields against stored values, and prints a mismatch report. Exits 0 on success (even with mismatches), 1 on infrastructure failure or shutdown.
6. **Metadata extraction mode** (`--extract-metadata --output <PATH>`): reads the source Delta table and writes a 19-column metadata-only Delta table (all columns except `as_der`) to the output path. Exits 0 on success, 1 on failure or shutdown.

## Key Conventions
- Config structs use serde Deserialize with defaults; env vars override YAML
- Env var pattern: `CERTSTREAM_<SECTION>_<FIELD>` (e.g., `CERTSTREAM_DELTA_SINK_ENABLED`)
- All optional features use an `enabled: bool` field (default false)
- Graceful shutdown via CancellationToken propagated to all tasks

## Delta Sink Contracts
- **Disabled by default** (`delta_sink.enabled = false`)
- **Config**: `DeltaSinkConfig { enabled, table_path, batch_size, flush_interval_secs, compression_level, heavy_column_compression_level, offline_batch_size }`
- **Compression**: zstd (hardcoded codec, not configurable); `compression_level` (i32, default 9, range 1-22) configurable via `CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL` env var; `heavy_column_compression_level` (i32, default 15, range 1-22) configurable via `CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL` env var; both validated at startup via `ZstdLevel::try_new()`; applied to all write paths (live sink, backfill, merge) with per-column encoding via WriterProperties
- **Offline batch size**: `offline_batch_size` (usize, default 100000) configurable via `CERTSTREAM_DELTA_SINK_OFFLINE_BATCH_SIZE` env var; controls how many rows are accumulated before each Delta commit in offline operations (extract-metadata, migrate); larger values = fewer commits = faster bulk writes
- **Entry point**: `delta_sink::run_delta_sink(config, rx, shutdown)` spawned in main
- **Schema**: 20-column Arrow schema, partitioned by `seen_date` (YYYY-MM-DD); `as_der` is `DataType::Binary` (raw DER bytes, not base64)
- **Flush triggers**: batch_size threshold OR flush_interval_secs timer OR graceful shutdown
- **Buffer overflow**: if buffer > 2x batch_size, drops oldest half
- **Error recovery**: failed writes retain buffer for retry; table handle reopened
- **Non-fatal startup**: if table creation fails, task exits without crashing server
- **Metrics**: `certstream_delta_*` (records_written, flushes, write_errors, buffer_size, flush_duration_seconds, messages_lagged)
- **Public helpers**: `delta_schema()`, `open_or_create_table()`, `delta_writer_properties(compression_level, heavy_column_compression_level)`, `flush_buffer(table, buffer, schema, batch_size, compression_level, heavy_column_compression_level)`, `records_to_batch()`, `DeltaCertRecord::from_message()` are public for reuse by backfill

## ZeroBus Sink Contracts
- **Disabled by default** (`zerobus_sink.enabled = false`)
- **Config**: `ZerobusSinkConfig { enabled, endpoint, unity_catalog_url, table_name, client_id, client_secret, max_inflight_records }`
- **Defaults**: max_inflight_records `10000`, all strings empty
- **Env vars**: `CERTSTREAM_ZEROBUS_ENABLED`, `CERTSTREAM_ZEROBUS_ENDPOINT`, `CERTSTREAM_ZEROBUS_UNITY_CATALOG_URL`, `CERTSTREAM_ZEROBUS_TABLE_NAME`, `CERTSTREAM_ZEROBUS_CLIENT_ID`, `CERTSTREAM_ZEROBUS_CLIENT_SECRET`, `CERTSTREAM_ZEROBUS_MAX_INFLIGHT_RECORDS`
- **Endpoint format**: `CERTSTREAM_ZEROBUS_ENDPOINT` is a gRPC endpoint: `https://<workspace-id>.zerobus.<region>.cloud.databricks.com` (workspace-id is numeric, from `?o=` param; region from DNS e.g. `us-east-2`)
- **UC URL format**: `CERTSTREAM_ZEROBUS_UNITY_CATALOG_URL` is the workspace base URL: `https://<workspace-instance>.cloud.databricks.com` (no `/api/...` path — SDK appends `/oidc/v1/token` internally)
- **Table requirements**: target table must be a managed Delta table in a catalog with an explicit managed storage location (not default workspace storage); ZeroBus error 4024 if using default storage
- **Validation (when enabled)**: endpoint, unity_catalog_url, table_name, client_id, client_secret must be non-empty; table_name must be Unity Catalog format (`catalog.schema.table`, exactly 2 dots)
- **Entry point**: `zerobus_sink::run_zerobus_sink(config, rx, shutdown)` spawned in main
- **Wire format**: Protobuf `CertRecord` (20 fields mirroring `DeltaCertRecord`), compiled from `proto/cert_record.proto` via `build.rs`
- **Conversion**: `proto::CertRecord::from_delta_cert(&DeltaCertRecord)` maps all 20 fields
- **Descriptor**: `cert_record_descriptor_proto()` returns `DescriptorProto` from compiled file descriptor set for SDK `TableProperties`
- **Data flow**: receives `PreSerializedMessage` from broadcast -> deserializes JSON to `DeltaCertRecord` -> converts to `CertRecord` (base64-encoding `as_der` bytes back to string for protobuf wire format) -> encodes to protobuf bytes -> ingests via ZeroBus SDK
- **Error handling**: retryable errors recreate the stream and retry the record once; non-retryable errors skip the record; failed stream recreation exits the sink
- **Non-fatal startup**: if SDK or stream creation fails, task exits without crashing server
- **Graceful shutdown**: flushes and closes the stream on CancellationToken
- **Lagged handling**: logs warning and increments counter (matches delta_sink pattern)
- **Metrics**: `certstream_zerobus_records_ingested` (counter), `certstream_zerobus_ingest_errors` (counter), `certstream_zerobus_stream_recoveries` (counter), `certstream_zerobus_records_skipped` (counter), `certstream_zerobus_messages_lagged` (counter)
- **Integration test**: gated behind `feature = "integration"`, requires env vars `ZEROBUS_TEST_ENDPOINT`, `ZEROBUS_TEST_UC_URL`, `ZEROBUS_TEST_TABLE_NAME`, `ZEROBUS_TEST_CLIENT_ID`, `ZEROBUS_TEST_CLIENT_SECRET`

## Query API Contracts
- **Disabled by default** (`query_api.enabled = false`)
- **Config**: `QueryApiConfig { enabled, table_path, max_results_per_page, default_results_per_page, query_timeout_secs }`
- **Defaults**: table_path `./data/certstream`, max 500, default 50, timeout 30s
- **Env vars**: `CERTSTREAM_QUERY_API_ENABLED`, `CERTSTREAM_QUERY_API_TABLE_PATH`, `CERTSTREAM_QUERY_API_MAX_RESULTS_PER_PAGE`, `CERTSTREAM_QUERY_API_DEFAULT_RESULTS_PER_PAGE`, `CERTSTREAM_QUERY_API_QUERY_TIMEOUT_SECS`
- **Endpoint**: `GET /api/query/certs` merged into main Axum router when enabled
- **Query params**: `domain`, `issuer`, `from` (YYYY-MM-DD), `to` (YYYY-MM-DD), `limit`, `cursor`
- **At least one filter required**: returns 400 if no domain/issuer/from/to provided
- **Domain search modes**: exact (contains `.`), contains (no `.` or `*`), suffix (`*.` prefix) -- auto-classified from input
- **SQL injection prevention**: `escape_like_pattern()` for LIKE contexts, `escape_sql_string()` for non-LIKE contexts
- **Cursor-based pagination**: Base64-encoded JSON `{v: delta_version, k: last_cert_index}`; pinned to Delta table version for consistency
- **Pagination detection**: fetches `limit+1` rows; if extra row exists, `has_more=true` and `next_cursor` returned
- **Cursor errors**: invalid cursor returns 400; expired/vacuumed version returns 410 Gone
- **Table errors**: missing table returns 503; other errors return 500
- **Query timeout**: DataFusion execution wrapped in `tokio::time::timeout`; exceeded returns 504
- **Response fields**: `version`, `results[]` (cert_index, fingerprint, sha256, serial_number, subject, issuer, not_before, not_after, all_domains, source_name, seen, is_ca), `next_cursor`, `has_more`
- **Heavy fields excluded**: as_der, chain, cert_link, source_url, update_type, signature_algorithm not returned
- **Startup validation**: warns if table_path does not exist (non-fatal; queries return 503 until data written)
- **Metrics**: `certstream_query_requests` (counter, labeled by status), `certstream_query_duration_seconds` (histogram), `certstream_query_results_count` (histogram)
- **Reads from**: same Delta table written by delta_sink and backfill

## CLI Shared Flag Contracts
- **`--from` dual purpose**: stored as raw `String` in `CliArgs.backfill_from`; interpreted as u64 integer in backfill mode (parsed at dispatch site in `main.rs`), as YYYY-MM-DD date in migrate mode (validated via `validate_date_format()`)
- **`validate_date_format(date, flag_name)`**: public helper in `cli.rs`; validates YYYY-MM-DD format with year 2000-2099, month 01-12, day 01-31; does NOT validate days-per-month (e.g., Feb 31 passes); returns `Result<(), String>`

## Backfill Contracts
- **CLI flags**: `--backfill` activates backfill mode; `--from <INDEX>` sets historical start (parsed as u64 integer at dispatch); `--logs <FILTER>` filters logs by substring; `--staging-path <PATH>` writes to staging table instead of main table; `--sink <NAME>` selects writer backend (`delta` default, `zerobus`)
- **Entry point**: `backfill::run_backfill(config, staging_path, backfill_from, backfill_logs, backfill_sink, shutdown)` called from main, returns exit code (i32)
- **State file dependency**: backfill loads `StateManager` from `config.ct_log.state_file` (default: `certstream_state.json`) and uses each log's `current_index` as the per-log ceiling. Logs not in the state file are skipped with a warning. If no logs have state file entries, backfill exits with code 0.
- **Two modes**: catch-up (no `--from`, fills internal gaps within existing Delta data) and historical (`--from N`, backfills from index N to ceiling)
- **Gap detection**: `detect_gaps(table_path, staging_path, logs, backfill_from)` queries Delta table(s) via DataFusion SQL, finds internal gaps only (LEAD window function). When staging_path is provided and the staging table exists, both tables are queried via a UNION ALL view using `COUNT(DISTINCT cert_index)` to avoid double-counting duplicates. The second element of the `logs` tuple is the ceiling (from state file `current_index`).
- **Gap detection fallback**: if main table does not exist but staging table does, gap detection uses staging alone; if neither exists, falls back to original mode-based logic (empty work items for catch-up, full range for historical)
- **Staging write path**: when `--staging-path` is provided, the writer task writes to the staging table path instead of `config.delta_sink.table_path`; gap detection still reads both tables to avoid re-fetching already-staged records
- **Catch-up rules**: only backfills logs already present in Delta table; logs not in table are skipped; only internal gaps are filled (no frontier gaps)
- **Historical rules**: backfills all logs from `--from` index to ceiling; logs not in Delta get full range from `--from` to ceiling
- **Sink selection**: `--sink zerobus` requires `zerobus_sink.enabled = true` in config and `--from` (historical mode only; gap detection not supported for remote tables); `--sink delta` or omitted defaults to Delta writer; unknown sink names exit with error
- **ZeroBus backfill writer**: `run_zerobus_writer(config, rx, shutdown)` streams records via ZeroBus SDK; same retry/recovery logic as live sink (retryable errors recreate stream, non-retryable skip record); flushes and closes stream on completion
- **ZeroBus work items**: when `--sink zerobus`, gap detection is skipped; work items built directly from `--from` to ceiling for each log
- **Architecture**: mpsc channel from N fetcher tasks to 1 writer task; fetchers send `DeltaCertRecord`; writer (delta or zerobus) flushes on batch_size/timer/channel close/shutdown
- **Fetcher retry**: rate-limit errors get exponential backoff (up to 10 retries, max 60s); HTTP/parse errors get up to 3 retries
- **Delta writer reuses**: `delta_sink::flush_buffer()` and `delta_sink::open_or_create_table()` from the live sink
- **Exit code**: 0 if all fetchers and writer succeed, 1 if any errors occurred
- **Graceful shutdown**: CancellationToken checked per batch in fetchers; writer flushes remaining buffer on cancellation

## Merge Contracts
- **CLI flags**: `--merge --staging-path <PATH>` activates merge mode; `--merge` without `--staging-path` exits with error
- **Entry point**: `backfill::run_merge(config, staging_path, shutdown)` called from main, returns exit code (i32)
- **Merge predicate**: `target.source_url = source.source_url AND target.cert_index = source.cert_index` (deduplication key)
- **Merge behavior**: uses `DeltaOps::merge()` with `when_not_matched_insert` only (no updates, no deletes); matched source rows are silently skipped
- **Batch-by-batch**: staging records are read via DataFusion SQL, then each RecordBatch is merged individually into the main table
- **Missing staging table**: if staging table does not exist (NotATable or InvalidTableLocation), exits with code 0 (not an error)
- **Empty staging table**: if all batches have zero rows, exits with code 0
- **Staging cleanup**: on successful merge, the staging directory is deleted via `remove_dir_all`; cleanup failure is non-fatal (logged as warning)
- **Error handling**: any failure during merge leaves staging intact for retry and exits with code 1
- **Graceful shutdown**: CancellationToken checked between batch merges; if cancelled, exits with code 1 (staging left intact)

## Migrate Contracts
- **CLI flags**: `--migrate --output <PATH>` activates migrate mode; `--source <PATH>` overrides source table (default: `config.delta_sink.table_path`); `--from <DATE>` start date filter (YYYY-MM-DD, inclusive); `--to <DATE>` end date filter (YYYY-MM-DD, inclusive); `--migrate` without `--output` exits with error; `--from` and `--to` validated via `cli::validate_date_format()` at dispatch
- **Entry point**: `backfill::run_migrate(config, output_path, source_path, from_date, to_date, shutdown)` called from main, returns exit code (i32)
- **Purpose**: converts an existing Delta table from old schema (as_der as base64 Utf8) to new schema (as_der as raw Binary) with optimized WriterProperties
- **Source table**: reads from `source_path` parameter; defaults to `config.delta_sink.table_path` when `--source` is not provided; if source table cannot be opened, exits with code 1
- **Output table**: created at `output_path` via `open_or_create_table()`; uses the current `delta_schema()` (with Binary as_der)
- **Partition-by-partition**: queries distinct `seen_date` partitions from source, processes each partition sequentially; within each partition, data is streamed via `execute_stream()` (one RecordBatch at a time) to avoid OOM on large partitions; batches are accumulated up to `offline_batch_size` rows before each Delta commit
- **Partition filtering**: when `--from` is provided, partitions with `seen_date < from` are skipped; when `--to` is provided, partitions with `seen_date > to` are skipped; filters are inclusive and applied via lexicographic string comparison
- **as_der conversion**: base64-decodes each Utf8 string to raw bytes; decode failures produce null values and are logged as warnings (non-fatal)
- **Column alignment**: source columns are matched by name (not index) and cast to target types if needed, handling DataFusion column reordering
- **WriterProperties**: applies `delta_writer_properties(compression_level, heavy_column_compression_level)` to all writes
- **Empty source**: if source table has no partitions or no partitions match filters, exits with code 0
- **Graceful shutdown**: CancellationToken checked between partitions; if cancelled, exits with code 1
- **Error handling**: any failure (table open, query, write) exits with code 1

## Reparse Audit Contracts
- **CLI flags**: `--reparse-audit` activates reparse audit mode; `--from-date <YYYY-MM-DD>` filters partitions from date; `--to-date <YYYY-MM-DD>` filters partitions to date
- **Entry point**: `table_ops::run_reparse_audit(config, from_date, to_date, shutdown)` called from main, returns `(i32, AuditReport)`
- **Source table**: reads from `config.delta_sink.table_path` (same Delta table as delta_sink and backfill)
- **Columns read**: cert_index, source_url, seen_date, as_der, subject_aggregated, issuer_aggregated, all_domains, signature_algorithm, is_ca, not_before, not_after, serial_number
- **Comparison fields**: subject_aggregated, issuer_aggregated, all_domains (sorted), signature_algorithm, is_ca, not_before, not_after, serial_number (8 fields compared)
- **as_der handling**: supports both Binary and Utf8 (base64-encoded) column types; null or empty values counted as unparseable
- **Unparseable records**: invalid base64, null as_der, or `parse_certificate()` returning None are counted as unparseable (not mismatches)
- **Sample diffs**: collects up to 10 `SampleDiff` records with per-field stored vs reparsed values
- **AuditReport struct**: `{ total_records, mismatch_record_count, unparseable_count, partition_count, field_mismatch_counts: HashMap<String, u64>, sample_diffs: Vec<SampleDiff> }`
- **Exit code**: 0 on successful completion (even with mismatches); 1 on missing table, query failure, invalid date format, or shutdown
- **No records**: if date filter matches zero rows, prints informational message and exits 0
- **Graceful shutdown**: CancellationToken checked per batch; on cancellation, prints partial report and exits 1
- **Public helpers**: `date_filter_clause(from_date, to_date)` returns SQL WHERE fragment or Err for invalid format; `metadata_schema()` returns 19-column Arrow Schema (delta_schema minus as_der)

## Metadata Extraction Contracts
- **CLI flags**: `--extract-metadata` activates extraction mode; `--output <PATH>` (required) sets output Delta table path; `--from-date <YYYY-MM-DD>` and `--to-date <YYYY-MM-DD>` filter partitions
- **Validation**: `--extract-metadata` without `--output` prints error and exits 1 (checked in main.rs)
- **Entry point**: `table_ops::run_extract_metadata(config, output_path, from_date, to_date, shutdown)` called from main, returns i32
- **Source table**: reads from `config.delta_sink.table_path`
- **Output schema**: 19 columns (all delta_schema columns except `as_der`): cert_index, update_type, seen, seen_date, source_name, source_url, cert_link, serial_number, fingerprint, sha256, sha1, not_before, not_after, is_ca, signature_algorithm, subject_aggregated, issuer_aggregated, all_domains, chain
- **Output table creation**: uses `delta_sink::open_or_create_table()` with `metadata_schema()`; creates output directory via `create_dir_all`
- **Partitioning**: output table partitioned by `seen_date` (inherits from `open_or_create_table` behavior)
- **Compression**: zstd with `config.delta_sink.compression_level`
- **Write mode**: `SaveMode::Append`; accumulates DataFusion batches up to `offline_batch_size` rows before each Delta commit to minimize transaction overhead
- **Exit code**: 0 on success or empty date range; 1 on missing source table, query failure, write failure, invalid date, or shutdown
- **No records**: if date filter matches zero rows, exits 0
- **Graceful shutdown**: CancellationToken checked per batch; on cancellation, partial output left intact, exits 1

## CT Fetch Contracts
- **Module**: `ct::fetch` (public module)
- **Error type**: `FetchError { HttpError, InvalidResponse, RateLimited(u16), NotAvailable(u16) }`
- **RFC 6962**: `get_tree_size(client, base_url, timeout)` and `fetch_entries(client, base_url, start, end, source, timeout)`
- **Static CT**: `get_checkpoint_tree_size(client, base_url, timeout)` and `fetch_tile_entries(client, base_url, tile_index, partial_width, offset_in_tile, source, timeout, issuer_cache)`
- **Shared by**: watcher, static_ct poller, and backfill fetchers
- **Parse failures**: skipped with debug log and metrics counter increment, not treated as errors

## Boundaries
- Safe to edit: `src/`, `config.example.yaml`
- Never manually edit: `Cargo.lock`
- Immutable once deployed: Delta table `_delta_log/` transaction logs
