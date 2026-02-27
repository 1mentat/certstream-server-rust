# Delta Encoding Optimization — Phase 1: WriterProperties Optimization

**Goal:** Centralize and optimize Parquet encoding settings across all write paths with per-column dictionary encoding and compression levels.

**Architecture:** A new `delta_writer_properties()` function in `src/delta_sink.rs` becomes the single source of truth for all Parquet write settings. All three write paths (live sink, backfill writer, merge) call this function instead of constructing `WriterProperties` inline. A new `heavy_column_compression_level` config field controls the compression level for the `as_der` column independently.

**Tech Stack:** Rust, parquet crate (WriterProperties, ColumnPath, Compression, ZstdLevel), deltalake, serde

**Scope:** 4 phases from original design (phase 1 of 4)

**Codebase verified:** 2026-02-27

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-encoding-opt.AC1: Per-column encoding optimization
- **delta-encoding-opt.AC1.1 Success:** WriterProperties enables dictionary encoding for update_type, source_name, source_url, signature_algorithm, issuer_aggregated, and chain list items
- **delta-encoding-opt.AC1.2 Success:** WriterProperties disables dictionary encoding for high-cardinality columns (cert_link, serial_number, fingerprint, sha256, sha1, subject_aggregated, as_der)
- **delta-encoding-opt.AC1.3 Success:** as_der column uses heavy_column_compression_level; all other columns use compression_level
- **delta-encoding-opt.AC1.4 Success:** All three write paths (live sink, backfill, merge) use the centralized delta_writer_properties() function

### delta-encoding-opt.AC4: Configuration
- **delta-encoding-opt.AC4.1 Success:** heavy_column_compression_level defaults to 15, configurable via CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL
- **delta-encoding-opt.AC4.2 Failure:** Invalid heavy_column_compression_level (outside 1-22) rejected at config validation

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
## Subcomponent A: Configuration

<!-- START_TASK_1 -->
### Task 1: Add heavy_column_compression_level to DeltaSinkConfig

**Verifies:** delta-encoding-opt.AC4.1, delta-encoding-opt.AC4.2

**Files:**
- Modify: `src/config.rs:289-312` (DeltaSinkConfig struct + Default impl)
- Modify: `src/config.rs:326-328` (add default function after existing `default_delta_sink_compression_level`)
- Modify: `src/config.rs:620-622` (add env var override after existing compression_level override)
- Modify: `src/config.rs:798-806` (add validation after existing compression_level validation)
- Modify: `config.example.yaml:62` (add new field after compression_level line)

**Implementation:**

**1. Add field to DeltaSinkConfig struct** (`src/config.rs:289-300`):

Add `heavy_column_compression_level` after `compression_level` (line 299):

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
    #[serde(default = "default_delta_sink_compression_level")]
    pub compression_level: i32,
    #[serde(default = "default_delta_sink_heavy_column_compression_level")]
    pub heavy_column_compression_level: i32,
}
```

**2. Update Default impl** (`src/config.rs:302-312`):

Add `heavy_column_compression_level: default_delta_sink_heavy_column_compression_level()` to the Default impl.

**3. Add default function** (after `default_delta_sink_compression_level` at line 328):

```rust
fn default_delta_sink_heavy_column_compression_level() -> i32 {
    15
}
```

**4. Add env var override** (after line 622):

```rust
if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL") {
    delta_sink.heavy_column_compression_level = v.parse().unwrap_or(delta_sink.heavy_column_compression_level);
}
```

**5. Add validation** (after line 806, before the `if errors.is_empty()` check):

```rust
if ZstdLevel::try_new(self.delta_sink.heavy_column_compression_level).is_err() {
    errors.push(ConfigValidationError {
        field: "delta_sink.heavy_column_compression_level".to_string(),
        message: format!(
            "Heavy column compression level {} is invalid. Must be between 1 and 22",
            self.delta_sink.heavy_column_compression_level
        ),
    });
}
```

**6. Update config.example.yaml** (after line 62):

```yaml
  heavy_column_compression_level: 15  # Zstd level for as_der column (1-22, default 15)
```

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat: add heavy_column_compression_level to DeltaSinkConfig`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Config validation tests for heavy_column_compression_level

**Verifies:** delta-encoding-opt.AC4.1, delta-encoding-opt.AC4.2

**Files:**
- Modify: `src/config.rs` (test module, after existing compression_level tests near line 1537)

**Testing:**
Tests must verify each AC listed above, following the existing patterns in `src/config.rs` test module (lines 852-1538):

- delta-encoding-opt.AC4.1: Test that `DeltaSinkConfig::default()` has `heavy_column_compression_level == 15`. Test that deserializing YAML without the field defaults to 15. Test that deserializing YAML with the field set to a custom value preserves it.
- delta-encoding-opt.AC4.2: Test that `Config::validate()` fails when `heavy_column_compression_level` is 0 (below range). Test that it fails when 23 (above range). Test that it passes for valid values (1 and 22 at the boundaries). Follow the exact pattern of existing tests `test_validate_compression_level_zero` and `test_validate_compression_level_too_high` at lines 1472-1501.

Also update `make_test_config` in `src/backfill.rs:2567-2573` to include `heavy_column_compression_level: 15` in the `DeltaSinkConfig` literal (it currently has 4 fields; add the 5th).

**Verification:**
Run: `cargo test -- test_delta_sink_config test_validate`
Expected: All existing and new config tests pass

**Commit:** `test: add config validation tests for heavy_column_compression_level`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-5) -->
## Subcomponent B: Centralized WriterProperties

<!-- START_TASK_3 -->
### Task 3: Create delta_writer_properties() function

**Verifies:** delta-encoding-opt.AC1.1, delta-encoding-opt.AC1.2, delta-encoding-opt.AC1.3

**Files:**
- Modify: `src/delta_sink.rs:11-12` (add import for `ColumnPath`)
- Modify: `src/delta_sink.rs` (add new public function before `flush_buffer` at line 474)

**Implementation:**

**1. Add import** (`src/delta_sink.rs`, after line 12):

```rust
use parquet::schema::types::ColumnPath;
```

**2. Add function** (insert before `flush_buffer` at line 474):

```rust
/// Constructs WriterProperties with per-column encoding and compression settings.
///
/// This is the single source of truth for all Parquet write settings. All write paths
/// (live sink, backfill writer, merge) must use this function.
///
/// # Per-column dictionary encoding
/// - Enabled for low-cardinality columns: update_type, source_name, source_url,
///   signature_algorithm, issuer_aggregated, chain list items
/// - Disabled for high-cardinality columns: cert_link, serial_number, fingerprint,
///   sha256, sha1, subject_aggregated, as_der
///
/// # Per-column compression
/// - as_der: uses heavy_column_compression_level (default 15)
/// - All other columns: use compression_level (default 9)
pub fn delta_writer_properties(compression_level: i32, heavy_column_compression_level: i32) -> WriterProperties {
    let zstd = Compression::ZSTD(
        ZstdLevel::try_new(compression_level).expect("compression level validated at startup"),
    );
    let heavy_zstd = Compression::ZSTD(
        ZstdLevel::try_new(heavy_column_compression_level)
            .expect("heavy column compression level validated at startup"),
    );

    WriterProperties::builder()
        // Global compression default
        .set_compression(zstd)
        // Dictionary encoding: enabled for low-cardinality columns
        .set_column_dictionary_enabled(ColumnPath::from("update_type"), true)
        .set_column_dictionary_enabled(ColumnPath::from("source_name"), true)
        .set_column_dictionary_enabled(ColumnPath::from("source_url"), true)
        .set_column_dictionary_enabled(ColumnPath::from("signature_algorithm"), true)
        .set_column_dictionary_enabled(ColumnPath::from("issuer_aggregated"), true)
        .set_column_dictionary_enabled(ColumnPath::from("chain.list.item"), true)
        // Dictionary encoding: disabled for high-cardinality columns
        .set_column_dictionary_enabled(ColumnPath::from("cert_link"), false)
        .set_column_dictionary_enabled(ColumnPath::from("serial_number"), false)
        .set_column_dictionary_enabled(ColumnPath::from("fingerprint"), false)
        .set_column_dictionary_enabled(ColumnPath::from("sha256"), false)
        .set_column_dictionary_enabled(ColumnPath::from("sha1"), false)
        .set_column_dictionary_enabled(ColumnPath::from("subject_aggregated"), false)
        .set_column_dictionary_enabled(ColumnPath::from("as_der"), false)
        // Heavy compression for as_der column
        .set_column_compression(ColumnPath::from("as_der"), heavy_zstd)
        .build()
}
```

**Important note on list column paths:** The Parquet 3-level list structure for Arrow's `List<Utf8>` with inner field named "item" produces the column path `chain.list.item`. `ColumnPath::from("chain.list.item")` splits on `.` to create the 3-component path `["chain", "list", "item"]`. If the Parquet schema uses a different inner field name (e.g., "element"), adjust accordingly. The test in Task 5 will verify the correct path by inspecting actual Parquet metadata.

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat: add centralized delta_writer_properties() function`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Update flush_buffer and all write paths to use delta_writer_properties

**Verifies:** delta-encoding-opt.AC1.4

**Files:**
- Modify: `src/delta_sink.rs:474-480` (flush_buffer signature)
- Modify: `src/delta_sink.rs:504-509` (replace inline WriterProperties)
- Modify: `src/delta_sink.rs:697` (live sink batch flush caller)
- Modify: `src/delta_sink.rs:742` (live sink timer flush caller)
- Modify: `src/delta_sink.rs:766` (live sink shutdown flush caller)
- Modify: `src/backfill.rs:440-444` (run_writer signature)
- Modify: `src/backfill.rs:490,505,515,530` (backfill writer flush_buffer callers)
- Modify: `src/backfill.rs:849-856` (backfill spawn site — extract + pass heavy_column_compression_level)
- Modify: `src/backfill.rs:1001-1007` (merge path — replace inline WriterProperties)

**Implementation:**

**1. Update flush_buffer signature** (`src/delta_sink.rs:474-480`):

Add `heavy_column_compression_level: i32` parameter after `compression_level`:

```rust
pub async fn flush_buffer(
    table: DeltaTable,
    buffer: &mut Vec<DeltaCertRecord>,
    schema: &Arc<Schema>,
    batch_size: usize,
    compression_level: i32,
    heavy_column_compression_level: i32,
) -> (Option<DeltaTable>, Result<usize, Box<dyn std::error::Error + Send + Sync>>)
```

**2. Replace inline WriterProperties in flush_buffer** (`src/delta_sink.rs:504-509`):

Replace:
```rust
let writer_props = WriterProperties::builder()
    .set_compression(Compression::ZSTD(
        ZstdLevel::try_new(compression_level).expect("compression level validated at startup"),
    ))
    .build();
```

With:
```rust
let writer_props = delta_writer_properties(compression_level, heavy_column_compression_level);
```

**3. Update live sink callers** (`src/delta_sink.rs`):

All three `flush_buffer` calls in `run_delta_sink` currently pass `config.compression_level` as the last argument. Add `config.heavy_column_compression_level` as the new last argument:

Line 697 (batch flush):
```rust
let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, config.batch_size, config.compression_level, config.heavy_column_compression_level).await;
```

Line 742 (timer flush):
```rust
let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, config.batch_size, config.compression_level, config.heavy_column_compression_level).await;
```

Line 766 (shutdown flush):
```rust
let (table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, config.batch_size, config.compression_level, config.heavy_column_compression_level).await;
```

**4. Update backfill run_writer signature** (`src/backfill.rs:440-444`):

Add `heavy_column_compression_level: i32` parameter after `compression_level`:

```rust
async fn run_writer(
    table_path: String,
    batch_size: usize,
    flush_interval_secs: u64,
    compression_level: i32,
    heavy_column_compression_level: i32,
    mut rx: mpsc::Receiver<DeltaCertRecord>,
    shutdown: CancellationToken,
) -> WriterResult
```

**5. Update backfill writer flush_buffer callers** (`src/backfill.rs`):

All four `flush_buffer` calls in `run_writer` currently pass `compression_level` as the last argument. Add `heavy_column_compression_level` to each:

- Line 490: batch size threshold trigger
- Line 505: channel closed (all fetchers done)
- Line 515: timer-based flush
- Line 530: shutdown flush

```rust
let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size, compression_level, heavy_column_compression_level).await;
```

**6. Update backfill spawn site** (`src/backfill.rs:849-856`):

Extract `heavy_column_compression_level` from config and pass to `run_writer`:

```rust
let table_path = staging_path
    .unwrap_or_else(|| config.delta_sink.table_path.clone());
let batch_size = config.delta_sink.batch_size;
let flush_interval_secs = config.delta_sink.flush_interval_secs;
let compression_level = config.delta_sink.compression_level;
let heavy_column_compression_level = config.delta_sink.heavy_column_compression_level;
let shutdown_clone = shutdown.clone();
tokio::spawn(async move {
    run_writer(table_path, batch_size, flush_interval_secs, compression_level, heavy_column_compression_level, rx, shutdown_clone).await
})
```

**7. Update merge path** (`src/backfill.rs:1001-1007`):

Replace inline WriterProperties with centralized function. Add import of `delta_writer_properties` to the existing import line at `src/backfill.rs:4`:

Update import:
```rust
use crate::delta_sink::{delta_schema, delta_writer_properties, DeltaCertRecord, flush_buffer, open_or_create_table};
```

Replace lines 1001-1007:
```rust
let writer_props = delta_writer_properties(
    config.delta_sink.compression_level,
    config.delta_sink.heavy_column_compression_level,
);
```

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `refactor: centralize all write paths to use delta_writer_properties()`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Tests for per-column WriterProperties and write path centralization

**Verifies:** delta-encoding-opt.AC1.1, delta-encoding-opt.AC1.2, delta-encoding-opt.AC1.3, delta-encoding-opt.AC1.4

**Files:**
- Modify: `src/delta_sink.rs` (test module, after existing `test_flush_buffer_writes_zstd_compression` at line 2056)
- Modify: `src/delta_sink.rs:1996` (update existing flush_buffer test call to include new parameter)

**Testing:**

First, update the existing `test_flush_buffer_writes_zstd_compression` test (line 1996) to pass the new `heavy_column_compression_level` parameter to `flush_buffer`:
```rust
let (table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, 10, 9, 15).await;
```

Also update any other test calls to `flush_buffer` in the test module to include the new parameter (search for all `flush_buffer(` calls in the test module).

Then add new tests. Tests must verify each AC listed above:

- delta-encoding-opt.AC1.1: Test that `delta_writer_properties(9, 15)` returns WriterProperties where dictionary encoding is enabled for `update_type`, `source_name`, `source_url`, `signature_algorithm`, `issuer_aggregated`. Use `WriterProperties::dictionary_enabled(&ColumnPath)` to check each. Also verify `chain.list.item` — note the correct column path may need adjustment if Parquet uses a different list element name; write to a table with chain data and inspect the Parquet file metadata to confirm.

- delta-encoding-opt.AC1.2: Test that the returned WriterProperties has dictionary encoding disabled for `cert_link`, `serial_number`, `fingerprint`, `sha256`, `sha1`, `subject_aggregated`, `as_der`. Use `WriterProperties::dictionary_enabled(&ColumnPath)` to check each.

- delta-encoding-opt.AC1.3: Write records to a Delta table using `flush_buffer` with `compression_level=9` and `heavy_column_compression_level=15`. Read the resulting Parquet file metadata. Verify that the `as_der` column has ZSTD compression (the level is embedded in the codec). Verify that other columns also have ZSTD compression. Follow the pattern of existing `test_flush_buffer_writes_zstd_compression` (lines 1972-2056) which reads Parquet metadata via `SerializedFileReader`.

- delta-encoding-opt.AC1.4: This is structurally verified — if the code compiles with `flush_buffer` and `run_merge` both calling `delta_writer_properties`, the centralization is complete. The test can call `delta_writer_properties` directly and verify it returns non-default WriterProperties.

**Verification:**
Run: `cargo test`
Expected: All tests pass (existing + new)

**Commit:** `test: verify per-column encoding and compression in WriterProperties`
<!-- END_TASK_5 -->
<!-- END_SUBCOMPONENT_B -->
