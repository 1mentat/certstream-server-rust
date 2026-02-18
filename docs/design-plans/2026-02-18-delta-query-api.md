# Delta Query API Design

## Summary

The Delta Query API adds a read-only REST endpoint (`GET /api/query/certs`) to the existing certstream-server-rust Axum server. It allows external clients — such as domain monitoring tools or threat intelligence pipelines — to search historical certificate records stored in the Delta Lake table that the live server already writes to. Clients can filter by domain name pattern, certificate issuer, and date range, and receive structured JSON results containing certificate metadata suitable for domain monitoring (subject names, issuer, validity window, observed timestamp, and fingerprints). The API is disabled by default and follows the same configuration, authentication, rate-limiting, and metrics conventions already present in the server.

The core design trade-off is simplicity of state management: rather than maintaining a long-lived, shared query session, each HTTP request opens its own DeltaTable instance pinned to a specific table version. This version is encoded in an opaque, Base64-encoded pagination cursor so that a multi-page result set always reads from the same consistent snapshot of the data, even as the live sink continues writing new records concurrently. DataFusion, the SQL execution engine embedded in the Delta Lake Rust library, handles the actual query execution, and the same patterns already used by the backfill mode (session context creation, Arrow RecordBatch downcasting) are reused here without modification.

## Definition of Done
A read-only REST API that queries the existing Delta Lake table, enabling domain monitoring workflows — search certificates by domain pattern, filter by issuer and date range, with cursor-based pagination pinned to Delta table snapshots.

**Success criteria:**
- Clients can search historical certificates by domain (substring/wildcard), issuer, and date range
- Results are paginated with stable cursors that pin to a Delta table version
- Response format includes certificate details suitable for domain monitoring (domains, issuer, timestamps, fingerprints)
- API integrates into the existing Axum server with existing auth and rate-limit middleware
- Configurable via the existing YAML + env var pattern

**Out of scope:**
- Aggregate/analytics endpoints (counts, trends)
- MCP exposure (future follow-up)
- S3/cloud storage backend
- Write operations

## Acceptance Criteria

### delta-query-api.AC1: Certificate search by domain, issuer, and date range
- **delta-query-api.AC1.1 Success:** Date range query (`from`/`to`) returns only certificates with `seen_date` within range
- **delta-query-api.AC1.2 Success:** Domain contains search (`paypal`) matches certificates where any domain in `all_domains` contains the substring (case-insensitive)
- **delta-query-api.AC1.3 Success:** Domain suffix search (`*.example.com`) matches certificates where any domain ends with `.example.com`
- **delta-query-api.AC1.4 Success:** Domain exact search (`example.com`) matches certificates where `all_domains` contains the exact value
- **delta-query-api.AC1.5 Success:** Issuer search matches certificates where `issuer_aggregated` contains the substring (case-insensitive)
- **delta-query-api.AC1.6 Success:** Filters combine correctly (domain + issuer + date range returns intersection)
- **delta-query-api.AC1.7 Failure:** Request with no filters returns 400

### delta-query-api.AC2: Stable cursor pagination pinned to Delta version
- **delta-query-api.AC2.1 Success:** First request returns results with `next_cursor` and `has_more: true` when more results exist
- **delta-query-api.AC2.2 Success:** Subsequent request with cursor returns next page from same Delta version
- **delta-query-api.AC2.3 Success:** Final page returns `has_more: false` with no `next_cursor`
- **delta-query-api.AC2.4 Failure:** Invalid cursor returns 400
- **delta-query-api.AC2.5 Failure:** Expired cursor (vacuumed version) returns 410
- **delta-query-api.AC2.6 Edge:** `limit` parameter respected, capped at `max_results_per_page`

### delta-query-api.AC3: Response format for domain monitoring
- **delta-query-api.AC3.1 Success:** Response includes cert_index, fingerprint, sha256, serial_number, subject, issuer, not_before, not_after, all_domains, source_name, seen, is_ca
- **delta-query-api.AC3.2 Success:** Response excludes heavy fields (as_der, chain)
- **delta-query-api.AC3.3 Success:** Response includes `version` field showing Delta table version

### delta-query-api.AC4: Integration with existing server
- **delta-query-api.AC4.1 Success:** Query endpoint protected by existing auth middleware when auth is enabled
- **delta-query-api.AC4.2 Success:** Query endpoint subject to existing rate limiting when rate limiting is enabled
- **delta-query-api.AC4.3 Success:** Query API disabled by default (`enabled: false`), server starts normally without it

### delta-query-api.AC5: Configuration and operational
- **delta-query-api.AC5.1 Success:** YAML config parsed with defaults for all fields
- **delta-query-api.AC5.2 Success:** Env var overrides work (`CERTSTREAM_QUERY_API_*`)
- **delta-query-api.AC5.3 Failure:** Inaccessible `table_path` logged as warning at startup (non-fatal)
- **delta-query-api.AC5.4 Success:** Query timeout returns 504 when DataFusion execution exceeds `query_timeout_secs`
- **delta-query-api.AC5.5 Success:** Prometheus metrics emitted (request count by status, duration, result count)
- **delta-query-api.AC5.6 Failure:** Table not found at query time returns 503

## Glossary

- **Arrow RecordBatch**: The in-memory columnar data structure produced by DataFusion query execution. Query results arrive as a list of RecordBatches, each holding a fixed number of rows in typed column arrays that must be downcast to their concrete type before values can be extracted.
- **cert_index**: The sequential position of a certificate entry within its source CT log. Used as the pagination key in cursors because it is monotonically increasing and stable within a given Delta table version.
- **cursor**: An opaque, Base64-encoded token returned in paginated API responses. Encodes the Delta table version (`v`) and the last `cert_index` seen (`k`), enabling the next request to resume from exactly the same point in the same data snapshot.
- **DataFusion**: An in-process SQL query engine (part of the Apache Arrow ecosystem) embedded in the Delta Lake Rust library. Used here to execute SQL against parquet files in the Delta table without a separate database server.
- **Delta Lake**: An open-source storage format layered on top of Parquet files that adds ACID transactions, versioned snapshots, and a transaction log (`_delta_log/`). The server uses it for durable CT record archival.
- **delta_sink**: The existing optional Tokio task (`src/delta_sink.rs`) that consumes the broadcast channel and writes batches of CT records to the Delta Lake table. The query API reads from the same table the sink writes.
- **ILIKE**: A case-insensitive SQL LIKE operator supported by DataFusion. Used for substring and suffix domain matching and issuer filtering.
- **issuer_aggregated**: A single string column in the Delta schema that combines issuer fields into one searchable value. The query API's issuer filter runs a case-insensitive substring match against this column.
- **MVCC**: Multi-Version Concurrency Control. Delta Lake's mechanism for allowing concurrent readers and writers without locking. A reader pinned to version N sees a consistent snapshot even as a writer creates version N+1 simultaneously.
- **Partition pruning**: An optimization where the query engine skips reading Parquet files that cannot match a filter. The Delta table is partitioned by `seen_date`, so a date range filter avoids reading data from irrelevant date directories entirely.
- **seen_date**: The date partition column in the Delta schema, formatted as `YYYY-MM-DD`. Drives partition pruning for date-range queries.
- **SessionContext**: DataFusion's entry point for registering tables and executing SQL queries. A new instance is created per request to avoid shared mutable state between concurrent HTTP requests.
- **UNNEST**: A SQL operation that expands an array column into individual rows. Used to match `all_domains` (a list of hostnames) against a LIKE pattern — each domain in the array becomes a separate row so the ILIKE condition can be applied.
- **Vacuum**: The Delta Lake operation that permanently deletes old Parquet files and transaction log entries beyond the retention threshold (default 7 days). Once a table version is vacuumed, any cursor encoding that version becomes invalid, triggering the 410 Gone response.

## Architecture

Read-only REST endpoint (`GET /api/query/certs`) that queries the existing Delta Lake table via DataFusion SQL. Each request opens its own DeltaTable instance at the version encoded in the pagination cursor (or latest version for the first request), ensuring stable pagination without shared mutable state.

**Request flow:**
1. Parse query parameters (domain, issuer, from, to, limit, cursor)
2. Open DeltaTable — at latest version (no cursor) or pinned version (from cursor)
3. Create per-request `SessionContext`, register table as `ct_records`
4. Build SQL from filters, execute query, collect Arrow RecordBatches
5. Map results to JSON response with next cursor

**API surface:**

`GET /api/query/certs` with query parameters:

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `domain` | string | no | Domain pattern: `paypal` = contains, `*.example.com` = suffix, `example.com` = exact |
| `issuer` | string | no | Issuer substring match (ILIKE on `issuer_aggregated`) |
| `from` | date | no | Start date inclusive (YYYY-MM-DD), drives partition pruning |
| `to` | date | no | End date inclusive (YYYY-MM-DD) |
| `limit` | int | no | Results per page (default 50, max 500) |
| `cursor` | string | no | Opaque pagination cursor from previous response |

At least one of `domain`, `issuer`, `from`, or `to` is required.

**Response contract:**

```json
{
  "version": 7808,
  "results": [
    {
      "cert_index": 12345,
      "fingerprint": "AB:CD:...",
      "sha256": "...",
      "serial_number": "...",
      "subject": "CN=example.com",
      "issuer": "CN=Let's Encrypt Authority X3, O=Let's Encrypt",
      "not_before": 1708300800,
      "not_after": 1716076800,
      "all_domains": ["example.com", "www.example.com"],
      "source_name": "Google Argon 2025",
      "seen": "2026-02-17T14:30:00Z",
      "is_ca": false
    }
  ],
  "next_cursor": "eyJ2Ijo3ODA4LCJrIjoxMjM0NX0=",
  "has_more": true
}
```

Fields excluded from response (heavy, not needed for monitoring): `as_der`, `chain`, `cert_link`, `source_url`.

**Cursor encoding:** Base64-encoded JSON: `{"v": 7808, "k": 12345}` where `v` is the Delta table version and `k` is the last `cert_index` returned. Pagination uses `WHERE cert_index > k ORDER BY cert_index LIMIT N+1` — if N+1 rows returned, `has_more: true` and the last row is excluded from results.

**Domain search modes:**

| Input | Detection | Query |
|-------|-----------|-------|
| `paypal` | No `.` or `*` | `UNNEST(all_domains) AS d WHERE d ILIKE '%paypal%'` |
| `*.example.com` | Starts with `*.` | `UNNEST(all_domains) AS d WHERE d ILIKE '%.example.com'` |
| `example.com` | Contains `.`, no `*` | `array_has(all_domains, 'example.com')` |

Contains/suffix modes use UNNEST + ILIKE with DISTINCT to deduplicate rows. Exact mode uses `array_has()` to avoid UNNEST overhead. All pattern matching is case-insensitive.

**Error responses:**

| Condition | Status | Body |
|-----------|--------|------|
| No filter params | 400 | `"At least one filter required"` |
| Invalid cursor | 400 | `"Invalid cursor"` |
| Expired cursor (vacuumed version) | 410 | `"Cursor expired, please restart query"` |
| Table not found | 503 | `"Query service unavailable"` |
| Query timeout | 504 | `"Query timed out"` |
| DataFusion error | 500 | `"Internal query error"` |

**Configuration contract:**

```yaml
query_api:
  enabled: false
  table_path: "./data/certstream"
  max_results_per_page: 500
  default_results_per_page: 50
  query_timeout_secs: 30
```

Env var overrides: `CERTSTREAM_QUERY_API_ENABLED`, `CERTSTREAM_QUERY_API_TABLE_PATH`, etc. `table_path` defaults to `delta_sink.table_path` value when not explicitly set.

## Existing Patterns

Investigation found several patterns this design follows:

**Config extension** (from `DeltaSinkConfig` in `src/config.rs:287-320`): New `QueryApiConfig` struct with `#[serde(default)]` fields, separate default functions, `Default` trait impl, and `CERTSTREAM_QUERY_API_*` env var overrides.

**Conditional router assembly** (from `src/main.rs:458-543`): Query API router merged into app behind `config.query_api.enabled` flag, following the same `if config.X.enabled { app = app.merge(router) }` pattern.

**Shared state via Arc** (from `ApiState` in `src/api.rs:266-270`): New `QueryApiState` struct with `Arc`-wrapped components, extracted in handlers via `State(state): State<Arc<QueryApiState>>`.

**Delta table opening** (from `delta_sink::open_or_create_table` in `src/delta_sink.rs:234-255`): Query API reuses `deltalake::open_table()` for reads. Does not need `open_or_create_table` since the query API only reads existing tables.

**DataFusion query pattern** (from `src/backfill.rs:48-99`): `SessionContext::new()` → register table → SQL execution → `collect()` → column downcasting from Arrow batches. Same result extraction pattern with typed column access.

**Metrics naming** (from existing `certstream_delta_*` counters): New metrics use `certstream_query_*` prefix.

**No divergence from existing patterns.** All components follow established conventions in the codebase.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Config + Module Scaffolding

**Goal:** Add configuration and route stub so the query API can be enabled without functionality.

**Components:**
- `QueryApiConfig` struct in `src/config.rs` — enabled flag, table_path, page size limits, timeout
- `src/query.rs` module — handler stub returning 501 Not Implemented
- Router wiring in `src/main.rs` — conditional merge behind `enabled` flag
- `QueryApiState` struct in `src/query.rs` — holds config for handlers

**Dependencies:** None (first phase)

**Done when:** Server builds and starts with `query_api.enabled = true` in config, `GET /api/query/certs` returns 501, config YAML + env var overrides parse correctly
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Table Opening + Date Range Query

**Goal:** Open Delta table per request and execute basic date range filtering.

**Components:**
- Per-request DeltaTable opening in `src/query.rs` — open at latest version, handle table-not-found
- `QueryCertResult` response struct in `src/query.rs` — serializable result type
- SQL query builder in `src/query.rs` — date range filter on `seen_date` partition column
- Arrow RecordBatch to response mapping in `src/query.rs` — column downcasting

**Dependencies:** Phase 1

**Covers:** delta-query-api.AC1.1, delta-query-api.AC1.5, delta-query-api.AC4.1, delta-query-api.AC4.2, delta-query-api.AC5.3

**Done when:** `GET /api/query/certs?from=2026-02-17&to=2026-02-18` returns certificate records from the Delta table, partition pruning skips irrelevant date partitions, table-not-found returns 503
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Domain + Issuer Search

**Goal:** Implement domain pattern matching and issuer filtering.

**Components:**
- Domain pattern detection in `src/query.rs` — classify input as exact/contains/suffix
- UNNEST + ILIKE query path in `src/query.rs` — for contains and suffix domain search
- `array_has` query path in `src/query.rs` — for exact domain match
- Issuer ILIKE filter in `src/query.rs` — substring match on `issuer_aggregated`
- Input sanitization in `src/query.rs` — escape `%`, `_`, `'` in LIKE patterns
- Combined filter SQL construction in `src/query.rs` — all filters compose in single query

**Dependencies:** Phase 2

**Covers:** delta-query-api.AC1.2, delta-query-api.AC1.3, delta-query-api.AC1.4, delta-query-api.AC5.1, delta-query-api.AC5.2

**Done when:** All three domain search modes work (exact, contains, suffix), issuer filter works, filters combine correctly, special characters in patterns don't cause SQL injection or syntax errors
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Cursor Pagination

**Goal:** Implement stable cursor-based pagination pinned to Delta table versions.

**Components:**
- Cursor struct and base64 encode/decode in `src/query.rs` — `{"v": version, "k": last_cert_index}`
- Version-pinned table opening in `src/query.rs` — `load_version()` from cursor
- LIMIT N+1 / has_more logic in `src/query.rs` — fetch one extra to detect more pages
- `cert_index > k` cursor filter in query builder — appended to WHERE clause
- Error handling for expired/invalid cursors in `src/query.rs` — 410 and 400 responses

**Dependencies:** Phase 3

**Covers:** delta-query-api.AC2.1, delta-query-api.AC2.2, delta-query-api.AC2.3, delta-query-api.AC2.4, delta-query-api.AC2.5, delta-query-api.AC5.4

**Done when:** Multi-page pagination returns stable results across pages, cursor pins to table version, expired cursors return 410, invalid cursors return 400, page size respects limit param and max cap
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Metrics, Timeout, Polish

**Goal:** Add observability, query timeout protection, and config validation.

**Components:**
- Prometheus metrics in `src/query.rs` — request counter (by status), duration histogram, results histogram
- Query timeout via `tokio::time::timeout` in `src/query.rs` — wraps DataFusion execution
- Config validation in `src/config.rs` — table_path accessibility check at startup
- Filter requirement enforcement in `src/query.rs` — reject requests with no filters (400)

**Dependencies:** Phase 4

**Covers:** delta-query-api.AC3.1, delta-query-api.AC3.2, delta-query-api.AC3.3, delta-query-api.AC5.5, delta-query-api.AC5.6

**Done when:** Metrics appear in Prometheus endpoint, long queries timeout with 504, config validation catches inaccessible table_path at startup, empty filter requests rejected
<!-- END_PHASE_5 -->

## Additional Considerations

**Input sanitization:** User-provided domain and issuer strings are embedded in SQL LIKE patterns. Characters `%`, `_`, and `'` must be escaped before embedding. DataFusion does not support bind parameters in SQL mode, so string escaping is the defense layer. The implementation should use a dedicated escaping function, tested against injection attempts.

**Concurrent reads with delta_sink:** Delta Lake MVCC guarantees read isolation. The query API reads from a specific version while delta_sink writes new versions concurrently. No coordination needed between the two.

**Vacuum and cursor lifetime:** Default Delta Lake vacuum retention is 7 days. Cursors encode the table version and remain valid for the retention period. The 410 Gone response for expired cursors provides clear feedback to clients.
