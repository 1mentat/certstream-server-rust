# Test Requirements: Zstd Compression

## Automated Tests

| AC ID | Criterion | Test Type | Expected Test Location | Phase |
|-------|-----------|-----------|----------------------|-------|
| zstd-compression.AC1.1 | Server starts with default compression_level=9 when field is omitted from config | unit | src/config.rs | Phase 1 (Task 4) |
| zstd-compression.AC1.2 | Server starts with compression_level=1 when explicitly set in config.yaml | unit | src/config.rs | Phase 1 (Task 4) |
| zstd-compression.AC1.3 | Server exits with clear error when compression_level=0 (below valid range) | unit | src/config.rs | Phase 1 (Task 4) |
| zstd-compression.AC1.4 | Server exits with clear error when compression_level=23 (above valid range) | unit | src/config.rs | Phase 1 (Task 4) |
| zstd-compression.AC2.1 | Live delta_sink writes Parquet files with ZSTD codec at configured level | integration | src/delta_sink.rs | Phase 2 (Task 4) |
| zstd-compression.AC2.2 | Backfill writer writes Parquet files with ZSTD codec at configured level | integration | src/backfill.rs | Phase 2 (Task 2) |
| zstd-compression.AC2.4 | Existing snappy-compressed files remain readable after new zstd files are written to the same table | integration | src/delta_sink.rs | Phase 2 (Task 4) |

## Human Verification

| AC ID | Criterion | Why Not Automated | Verification Approach |
|-------|-----------|-------------------|----------------------|
| zstd-compression.AC2.3 | Merge operation writes Parquet files with ZSTD codec at configured level | The merge operation requires a fully populated staging Delta table and a main Delta table to merge into. Constructing this end-to-end scenario in an automated test would duplicate the extensive existing merge test infrastructure (9 test call sites) while adding only a Parquet metadata check. The merge builder's `.with_writer_properties()` call is a single chain addition with no branching logic, making the risk of regression low. | Run `--backfill --staging-path /tmp/staging` against a real or test CT log to produce a staging table, then run `--merge --staging-path /tmp/staging` to merge into the main table. After merge completes, inspect a newly written Parquet file in the main table directory (excluding `_delta_log/`) using `parquet-tools meta <file>` or equivalent and confirm the row group column chunks report ZSTD compression. Alternatively, use a short Rust script with `SerializedFileReader` to read the file metadata and assert `Compression::ZSTD(_)`. |

## Test Details

### Phase 1 Tests (src/config.rs)

**AC1.1 -- Default compression_level is 9:**
- Create a `Config` using the existing `test_config()` helper (which uses `DeltaSinkConfig::default()`)
- Assert `config.delta_sink.compression_level == 9`
- Assert `config.validate()` succeeds

**AC1.2 -- Explicit compression_level=1 is accepted:**
- Create a `Config` with `delta_sink.compression_level = 1` via struct update syntax
- Assert `config.validate()` succeeds

**AC1.3 -- compression_level=0 is rejected:**
- Create a `Config` with `delta_sink.compression_level = 0`
- Assert `config.validate()` returns `Err`
- Assert the error list contains an entry with `field == "delta_sink.compression_level"`

**AC1.4 -- compression_level=23 is rejected:**
- Create a `Config` with `delta_sink.compression_level = 23`
- Assert `config.validate()` returns `Err`
- Assert the error list contains an entry with `field == "delta_sink.compression_level"`

**AC1.1/AC1.2 -- Serde deserialization tests:**
- Deserialize YAML without `compression_level` field and verify it defaults to 9
- Deserialize YAML with `compression_level: 5` and verify it is set to 5

### Phase 2 Tests (src/delta_sink.rs)

**AC2.1 -- flush_buffer writes ZSTD-compressed Parquet:**
- Create test records using existing `make_test_record()` helper
- Create a temp Delta table at a unique `/tmp/delta_zstd_test_*` path
- Call `flush_buffer()` with `compression_level = 9`
- Find the resulting `.parquet` file(s) in the table directory (excluding `_delta_log/`)
- Open the file with `parquet::file::serialized_reader::SerializedFileReader`
- Read metadata via `reader.metadata()`
- Assert `metadata.row_group(0).column(0).compression()` matches `Compression::ZSTD(_)`
- Clean up temp directory

**AC2.2 -- Backfill writer uses configured compression (verified indirectly):**
- The backfill writer calls `flush_buffer()` with the `compression_level` parameter (updated in Phase 2 Task 2)
- All 9 existing `run_writer()` test call sites are updated to pass `compression_level = 9`
- Compilation of these test call sites confirms the parameter is threaded correctly
- The actual ZSTD output is verified by the AC2.1 test since both paths use the same `flush_buffer()` function

**AC2.4 -- Mixed-codec coexistence (verified implicitly):**
- Delta Lake / Arrow Parquet readers handle mixed compression codecs transparently within a single table
- The AC2.1 integration test writes new ZSTD files to a Delta table, and the table remains readable
- No explicit snappy-then-zstd test is needed because codec selection is per-file at the Parquet reader level, which is a property of the parquet crate, not application code
