# certstream-server-rust

Last verified: 2026-03-09
Last context update: 2026-03-09

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
- `cargo run -- --backfill --target staging` - Backfill into staging table (named target)
- `cargo run -- --backfill --sink zerobus --from 0` - Backfill via ZeroBus sink
- `cargo run -- --merge --source staging --target main` - Merge staging into main table (named targets)
- `cargo run -- --reparse-audit --source main` - Audit stored certs against current parsing code (named target)
- `cargo run -- --reparse-audit --source main --from-date 2026-02-01 --to-date 2026-02-28` - Audit with date filter (named target)
- `cargo run -- --extract-metadata --source main --target metadata` - Extract metadata-only Delta table (named targets)
- `cargo run -- --extract-metadata --source main --target metadata --from-date 2026-02-01` - Extract with date filter (named targets)

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

The binary has five execution modes selected in main.rs:
1. **Server mode** (default): when delta_sink is enabled, checkpoints the Delta log (fatal on failure), then starts the WebSocket/SSE server and live CT log watchers
2. **Backfill mode** (`--backfill` with optional `--target <NAME>`): runs gap detection against the target Delta table, spawns per-log fetcher tasks and a single writer task, then exits with code 0 (success) or 1 (errors). With `--target <NAME>`, writes to the named target table instead of the default delta_sink table.
3. **Merge mode** (`--merge --source <NAME> --target <NAME>`): merges source Delta table into target table using Delta MERGE INTO with deduplication, then deletes the source directory on success
4. **Reparse audit mode** (`--reparse-audit --source <NAME>`): reads stored certificates from source target Delta table, reparses each from `as_der`, compares parsed fields against stored values, and prints a mismatch report. Exits 0 on success (even with mismatches), 1 on infrastructure failure or shutdown.
5. **Metadata extraction mode** (`--extract-metadata --source <NAME> --target <NAME>`): reads source Delta table and writes a 19-column metadata-only Delta table (all columns except `as_der`) to target table. Exits 0 on success, 1 on failure or shutdown.

## Key Conventions
- Config structs use serde Deserialize with defaults; env vars override YAML
- Env var pattern: `CERTSTREAM_<SECTION>_<FIELD>` (e.g., `CERTSTREAM_DELTA_SINK_ENABLED`)
- All optional features use an `enabled: bool` field (default false)
- Graceful shutdown via CancellationToken propagated to all tasks
- **Table paths require URI scheme**: all `table_path` values must use `file://` for local filesystem or `s3://` for S3-compatible storage; bare paths are rejected by `parse_table_uri()` at config validation and CLI dispatch
- **Defense-in-depth URI handling**: production code uses `DeltaTableBuilder::from_valid_uri()` which returns a Result, rather than `from_uri()` which panics on invalid URIs

## Storage Config Contracts
- **Config**: `StorageConfig { s3: Option<S3StorageConfig> }` — top-level config section for storage backend credentials
- **S3StorageConfig**: `{ endpoint, region, access_key_id, secret_access_key, conditional_put: Option<String>, allow_http: Option<bool>, provider: Option<String> }`
- **Env vars**: `CERTSTREAM_STORAGE_S3_ENDPOINT`, `CERTSTREAM_STORAGE_S3_REGION`, `CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID`, `CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY`, `CERTSTREAM_STORAGE_S3_CONDITIONAL_PUT`, `CERTSTREAM_STORAGE_S3_ALLOW_HTTP`, `CERTSTREAM_STORAGE_S3_PROVIDER`
- **Env var trigger**: if no `storage.s3` section in YAML, `CERTSTREAM_STORAGE_S3_ENDPOINT` being non-empty triggers creation of S3 config from env vars; if endpoint is empty/unset, no S3 config is created even if other S3 env vars are set
- **TableLocation enum**: `Local { path }` or `S3 { uri }`; `as_uri()` returns the string for `DeltaTableBuilder::from_uri()` (stripped path for Local, full `s3://` URI for S3)
- **`parse_table_uri(uri)`**: parses `file://` to `Local`, `s3://` to `S3`; rejects empty URIs, unsupported schemes, and bare paths (suggests `file://` prefix)
- **S3Provider enum**: `Tigris`, `Aws`, `R2`, `MinIO`, `Generic` — represents detected S3-compatible storage provider
- **`detect_s3_provider(endpoint, explicit_provider)`**: returns `S3Provider`; priority: explicit `provider` field > endpoint URL pattern matching; explicit values: `"tigris"`, `"aws"`, `"r2"`, `"minio"` (case-insensitive), unknown strings map to `Generic`; URL patterns: `"tigris"` in URL -> Tigris, `"amazonaws.com"` -> Aws, `"r2.cloudflarestorage.com"` -> R2, otherwise Generic; MinIO is not URL-detectable (requires explicit provider)
- **`resolve_storage_options(location, storage)`**: returns `HashMap<String, String>` — empty for Local, AWS-compatible options (`AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `conditional_put`, `AWS_ALLOW_HTTP`) for S3; additionally applies provider-specific defaults: for Tigris, R2, and MinIO providers, auto-sets `conditional_put=etag` if not explicitly configured (logged via `tracing::info`); Aws and Generic providers do not get auto-defaults
- **Provider-specific defaults**: `conditional_put=etag` is auto-applied for Tigris/R2/MinIO because these providers support ETag-based conditional writes required for safe Delta Lake concurrent access; explicit `conditional_put` in config is never overridden
- **Validation (when S3 URIs used)**: `storage.s3` must be present; `endpoint`, `region`, `access_key_id`, `secret_access_key` must be non-empty; validated during `Config::validate()`
- **Validation scope**: `delta_sink.table_path` validated when delta_sink enabled; `query_api.table_path` validated when query_api enabled; target table paths validated at CLI dispatch in main.rs when `--source` or `--target` flags are used

## Named Targets Contracts
- **TargetConfig struct**: `{ table_path, storage: Option<StorageConfig>, compression_level: Option<i32>, heavy_column_compression_level: Option<i32>, offline_batch_size: Option<usize> }`
- **ResolvedTarget struct**: `{ table_path: String, storage_options: HashMap<String, String>, compression_level: i32, heavy_column_compression_level: i32, offline_batch_size: usize }` — result of resolving a target with inheritance from delta_sink defaults; `table_path` is a URI string (e.g., `file:///path` or `s3://bucket/key`) produced by `TableLocation::as_uri().to_string()`
- **targets config section**: optional `targets:` map in main config; keys are target names (e.g., `"main"`, `"staging"`, `"archive"`), values are `TargetConfig` structs with table_path (required) and optional compression/batch size settings
- **Env var pattern**: `CERTSTREAM_TARGETS_<NAME>_<FIELD>` (e.g., `CERTSTREAM_TARGETS_STAGING_TABLE_PATH`, `CERTSTREAM_TARGETS_ARCHIVE_COMPRESSION_LEVEL`); env vars are merged into targets from YAML at startup
- **resolve_target() function**: looks up target by name in `config.targets` map; applies inheritance: if target omits `compression_level`, falls back to `delta_sink.compression_level`; same for `heavy_column_compression_level` and `offline_batch_size`; parses `table_path` via `parse_table_uri()` and resolves storage options via `resolve_storage_options()` to build `ResolvedTarget`
- **No implicit defaults**: all operations require explicit `--target` and/or `--source` flags; omitting a required flag prints an error and exits with code 1 (no fallback to `delta_sink.table_path`)
- **--target and --source flags**: `--target <NAME>` specifies the named target for write operations; `--source <NAME>` specifies the named target for read operations; both are resolved via `config.resolve_target(name)` at dispatch time in main.rs
- **Validation**: target names must exist in `config.targets` or error is printed and operation exits; table_path in target must be valid URI with scheme (file:// or s3://); compression levels must be in valid zstd range (1-22)
- **Storage inheritance**: per-target storage IS supported via optional `storage` field in `TargetConfig`; if a target specifies S3 table URI with per-target storage credentials, those are used; if per-target storage omitted, falls back to global `config.storage` (implemented via `target.storage.as_ref().unwrap_or(&self.storage)` in resolve_target at src/config.rs:1256)

## Delta Sink Contracts
- **Disabled by default** (`delta_sink.enabled = false`)
- **Config**: `DeltaSinkConfig { enabled, table_path, batch_size, flush_interval_secs, compression_level, heavy_column_compression_level, offline_batch_size }`
- **Compression**: zstd (hardcoded codec, not configurable); `compression_level` (i32, default 9, range 1-22) configurable via `CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL` env var; `heavy_column_compression_level` (i32, default 15, range 1-22) configurable via `CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL` env var; both validated at startup via `ZstdLevel::try_new()`; applied to all write paths (live sink, backfill, merge) with per-column encoding via WriterProperties
- **Offline batch size**: `offline_batch_size` (usize, default 100000) configurable via `CERTSTREAM_DELTA_SINK_OFFLINE_BATCH_SIZE` env var; controls how many rows are accumulated before each Delta commit in offline operations (extract-metadata); larger values = fewer commits = faster bulk writes
- **Entry point**: `delta_sink::run_delta_sink(config, rx, shutdown)` spawned in main
- **Schema**: 20-column Arrow schema, partitioned by `seen_date` (YYYY-MM-DD); `as_der` is `DataType::Binary` (raw DER bytes, not base64)
- **Flush triggers**: batch_size threshold OR flush_interval_secs timer OR graceful shutdown
- **Buffer overflow**: if buffer > 2x batch_size, drops oldest half
- **Error recovery**: failed writes retain buffer for retry; table handle reopened
- **Non-fatal startup**: if table creation fails, task exits without crashing server
- **Startup checkpoint**: when delta_sink is enabled, `checkpoint_table(&config.delta_sink, &config.storage)` is called synchronously in main before spawning the live sink task; creates a checkpoint parquet file in `_delta_log/` and cleans up expired log files; checkpoint failure is fatal (exits with code 1); log cleanup failure is non-fatal (logged as warning, returns Ok)
- **Metrics**: `certstream_delta_*` (records_written, flushes, write_errors, buffer_size, flush_duration_seconds, messages_lagged, checkpoint_duration_seconds, checkpoint_logs_cleaned)
- **Public helpers**: `delta_schema()`, `open_or_create_table()`, `delta_writer_properties(compression_level, heavy_column_compression_level)`, `flush_buffer(table, buffer, schema, batch_size, compression_level, heavy_column_compression_level)`, `records_to_batch()`, `DeltaCertRecord::from_message()`, `checkpoint_table(config, storage)` are public for reuse by backfill

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
- **Storage options**: `QueryApiState` holds `storage_options: HashMap<String, String>` resolved from `parse_table_uri()` and `resolve_storage_options()` at startup; passed to `DeltaTableBuilder` for all table opens (latest and versioned)
- **Startup validation**: for local paths, warns if table_path does not exist (non-fatal; queries return 503 until data written); for S3 paths, skips local filesystem check
- **Metrics**: `certstream_query_requests` (counter, labeled by status), `certstream_query_duration_seconds` (histogram), `certstream_query_results_count` (histogram)
- **Reads from**: same Delta table written by delta_sink and backfill

## Backfill Contracts
- **CLI flags**: `--backfill` activates backfill mode; `--from <INDEX>` sets historical start (parsed as u64 integer at dispatch); `--logs <FILTER>` filters logs by substring; `--target <NAME>` selects named target table (required for delta sink, forbidden for zerobus sink); `--sink <NAME>` selects writer backend (`delta` default, `zerobus`)
- **Entry point**: `backfill::run_backfill(config, target: Option<ResolvedTarget>, backfill_from, backfill_logs, backfill_sink, shutdown)` called from main, returns exit code (i32)
- **ResolvedTarget parameter**: `target` is a pre-resolved target containing `table_path`, `storage_options`, `compression_level`, `heavy_column_compression_level`, and `offline_batch_size`. For zerobus sink, target is None (zerobus doesn't use a Delta table).
- **State file dependency**: backfill loads `StateManager` from `config.ct_log.state_file` (default: `certstream_state.json`) and uses each log's `current_index` as the per-log ceiling. Logs not in the state file are skipped with a warning. If no logs have state file entries, backfill exits with code 0.
- **Two modes**: catch-up (no `--from`, fills internal gaps within existing Delta data) and historical (`--from N`, backfills from index N to ceiling)
- **Gap detection**: `detect_gaps(table_path, logs, backfill_from, storage_options)` queries the target Delta table via DataFusion SQL, finds internal gaps only (LEAD window function). The second element of the `logs` tuple is the ceiling (from state file `current_index`).
- **Catch-up rules**: only backfills logs already present in target table; logs not in table are skipped; only internal gaps are filled (no frontier gaps)
- **Historical rules**: backfills all logs from `--from` index to ceiling; logs not in target table get full range from `--from` to ceiling
- **Sink selection**: `--sink zerobus` requires `zerobus_sink.enabled = true` in config and `--from` (historical mode only; gap detection not supported for remote tables); `--sink delta` or omitted defaults to Delta writer; unknown sink names exit with error
- **ZeroBus backfill writer**: `run_zerobus_writer(config, rx, shutdown)` streams records via ZeroBus SDK; same retry/recovery logic as live sink (retryable errors recreate stream, non-retryable skip record); flushes and closes stream on completion
- **ZeroBus work items**: when `--sink zerobus`, gap detection is skipped; work items built directly from `--from` to ceiling for each log
- **Architecture**: mpsc channel from N fetcher tasks to 1 writer task; fetchers send `DeltaCertRecord`; writer (delta or zerobus) flushes on batch_size/timer/channel close/shutdown
- **Fetcher retry**: rate-limit errors get exponential backoff (up to 10 retries, max 60s); HTTP/parse errors get up to 3 retries
- **Delta writer reuses**: `delta_sink::flush_buffer()` and `delta_sink::open_or_create_table()` from the live sink
- **Exit code**: 0 if all fetchers and writer succeed, 1 if any errors occurred
- **Graceful shutdown**: CancellationToken checked per batch in fetchers; writer flushes remaining buffer on cancellation

## Merge Contracts
- **CLI flags**: `--merge --source <NAME> --target <NAME>` activates merge mode; `--merge` without both flags exits with error
- **Entry point**: `backfill::run_merge(source: ResolvedTarget, target: ResolvedTarget, shutdown)` called from main, returns exit code (i32)
- **ResolvedTarget parameters**: `source` and `target` are pre-resolved targets containing `table_path`, `storage_options`, `compression_level`, `heavy_column_compression_level`, and `offline_batch_size`
- **Merge predicate**: `target.source_url = source.source_url AND target.cert_index = source.cert_index` (deduplication key)
- **Merge behavior**: uses `DeltaOps::merge()` with `when_not_matched_insert` only (no updates, no deletes); matched source rows are silently skipped
- **Batch-by-batch**: source records are read via DataFusion SQL, then each RecordBatch is merged individually into the target table
- **Missing source table**: if source table does not exist (NotATable or InvalidTableLocation), exits with code 0 (not an error)
- **Empty source table**: if all batches have zero rows, exits with code 0
- **Source cleanup**: on successful merge, the source directory is deleted via `remove_dir_all`; cleanup failure is non-fatal (logged as warning)
- **Error handling**: any failure during merge leaves source intact for retry and exits with code 1
- **Graceful shutdown**: CancellationToken checked between batch merges; if cancelled, exits with code 1 (source left intact)

## Reparse Audit Contracts
- **CLI flags**: `--reparse-audit --source <NAME>` activates reparse audit mode; `--from-date <YYYY-MM-DD>` filters partitions from date; `--to-date <YYYY-MM-DD>` filters partitions to date; `--source` is required (exits with error if omitted)
- **Entry point**: `table_ops::run_reparse_audit(source: ResolvedTarget, from_date, to_date, shutdown)` called from main, returns `(i32, AuditReport)`
- **ResolvedTarget parameter**: `source` is a pre-resolved target containing `table_path`, `storage_options`, and other configuration
- **Source table**: reads from `source.table_path`
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
- **CLI flags**: `--extract-metadata --source <NAME> --target <NAME>` activates extraction mode; `--from-date <YYYY-MM-DD>` and `--to-date <YYYY-MM-DD>` filter partitions
- **Validation**: `--extract-metadata` without both `--source` and `--target` prints error and exits 1 (checked in main.rs)
- **Entry point**: `table_ops::run_extract_metadata(source: ResolvedTarget, target: ResolvedTarget, from_date, to_date, shutdown)` called from main, returns i32
- **ResolvedTarget parameters**: `source` and `target` are pre-resolved targets containing `table_path`, `storage_options`, `compression_level`, `heavy_column_compression_level`, and `offline_batch_size`
- **Source table**: reads from `source.table_path`
- **Output schema**: 19 columns (all delta_schema columns except `as_der`): cert_index, update_type, seen, seen_date, source_name, source_url, cert_link, serial_number, fingerprint, sha256, sha1, not_before, not_after, is_ca, signature_algorithm, subject_aggregated, issuer_aggregated, all_domains, chain
- **Output table creation**: uses `delta_sink::open_or_create_table()` with `metadata_schema()`; creates output directory via `create_dir_all`
- **Partitioning**: output table partitioned by `seen_date` (inherits from `open_or_create_table` behavior)
- **Compression**: zstd with `target.compression_level`
- **Write mode**: `SaveMode::Append`; accumulates DataFusion batches up to `target.offline_batch_size` rows before each Delta commit to minimize transaction overhead
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

## Delta Table Replication
- **No native CLONE**: delta-rs 0.25 does not support Delta CLONE or DEEP CLONE operations
- **Recommended approach**: use `aws s3 sync` to copy the entire Delta table (parquet data files + `_delta_log/` transaction log directory) to an S3-compatible destination
- **Example**: `aws s3 sync /data/certstream s3://bucket/certstream --endpoint-url https://s3.example.com` — copies all parquet files and the `_delta_log/` directory, preserving the Delta table structure
- **Incremental sync**: subsequent `aws s3 sync` calls only transfer new/changed files, making ongoing replication efficient
- **Tigris note**: for Tigris S3-compatible storage, use `--endpoint-url https://t3.storage.dev`; large initial syncs may encounter connection limits under heavy concurrent uploads

## Boundaries
- Safe to edit: `src/`, `config.example.yaml`
- Never manually edit: `Cargo.lock`
- Immutable once deployed: Delta table `_delta_log/` transaction logs
