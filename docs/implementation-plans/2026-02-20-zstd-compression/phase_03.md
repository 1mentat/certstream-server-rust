# Zstd Compression Implementation Plan — Phase 3

**Goal:** Update project documentation to reflect compression configuration

**Architecture:** Update CLAUDE.md Delta Sink Contracts section to include compression_level field, env var, and zstd-hardcoded note. config.example.yaml was already updated in Phase 1.

**Tech Stack:** Markdown documentation

**Scope:** 3 phases from original design (phases 1-3)

**Codebase verified:** 2026-02-20

---

## Acceptance Criteria Coverage

**Verifies: None** — this is a documentation-only phase. config.example.yaml was updated in Phase 1 Task 2.

---

<!-- START_TASK_1 -->
### Task 1: Update CLAUDE.md Delta Sink Contracts section

**Files:**
- Modify: `CLAUDE.md` (Delta Sink Contracts section, ~lines 61-71)

**Implementation:**

Update the Delta Sink Contracts section in `CLAUDE.md`. Two changes needed:

**1. Update the Config line** to include `compression_level`. Change:

```
- **Config**: `DeltaSinkConfig { enabled, table_path, batch_size, flush_interval_secs }`
```

To:

```
- **Config**: `DeltaSinkConfig { enabled, table_path, batch_size, flush_interval_secs, compression_level }`
```

**2. Add a Compression bullet** after the Config line. Add this new line:

```
- **Compression**: zstd (hardcoded codec, not configurable); `compression_level` (i32, default 9, range 1-22) configurable via `CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL` env var; validated at startup via `ZstdLevel::try_new()`; applied to all write paths (live sink, backfill, merge)
```

The final Delta Sink Contracts section should read:

```
## Delta Sink Contracts
- **Disabled by default** (`delta_sink.enabled = false`)
- **Config**: `DeltaSinkConfig { enabled, table_path, batch_size, flush_interval_secs, compression_level }`
- **Compression**: zstd (hardcoded codec, not configurable); `compression_level` (i32, default 9, range 1-22) configurable via `CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL` env var; validated at startup via `ZstdLevel::try_new()`; applied to all write paths (live sink, backfill, merge)
- **Entry point**: `delta_sink::run_delta_sink(config, rx, shutdown)` spawned in main
- **Schema**: 20-column Arrow schema, partitioned by `seen_date` (YYYY-MM-DD)
- **Flush triggers**: batch_size threshold OR flush_interval_secs timer OR graceful shutdown
- **Buffer overflow**: if buffer > 2x batch_size, drops oldest half
- **Error recovery**: failed writes retain buffer for retry; table handle reopened
- **Non-fatal startup**: if table creation fails, task exits without crashing server
- **Metrics**: `certstream_delta_*` (records_written, flushes, write_errors, buffer_size, flush_duration_seconds, messages_lagged)
- **Public helpers**: `delta_schema()`, `open_or_create_table()`, `flush_buffer()`, `records_to_batch()`, `DeltaCertRecord::from_message()` are public for reuse by backfill
```

**Verification:**

Read the updated CLAUDE.md and verify the Delta Sink Contracts section accurately reflects:
- `compression_level` is in the Config struct listing
- Zstd is the hardcoded codec
- Compression level range is 1-22 with default 9
- Env var is documented
- Applied to all write paths

**Commit:** `docs: update CLAUDE.md with zstd compression configuration`
<!-- END_TASK_1 -->
