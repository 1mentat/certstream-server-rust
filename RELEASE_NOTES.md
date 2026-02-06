# Release Notes

## v1.2.0 — Static CT Log Support, Stability & Performance Overhaul

**February 6, 2026**

Major release adding static CT protocol (RFC 6962-bis) support, cross-log certificate deduplication, full CT log coverage, and significant stability/performance improvements — preparing for Let's Encrypt's RFC 6962 shutdown on February 28, 2026.

### Breaking Changes

- **TCP protocol removed.** Switch to WebSocket (`ws://host:8080/`) or SSE (`http://host:8080/sse`). Related env vars `CERTSTREAM_TCP_ENABLED` and `CERTSTREAM_TCP_PORT` have been removed.

### New Features

**Static CT Log Protocol (Sunlight / static-ct-api)**
Full support for the checkpoint + tile-based static CT API per [c2sp.org/static-ct-api](https://c2sp.org/static-ct-api). Includes binary tile parsing (x509/precert), hierarchical tile path encoding, gzip decompression, and issuer certificate fetching with DashMap-based caching. Four Let's Encrypt Sunlight logs are configured by default (Willow/Sycamore 2025h2d/2026h1).

**Cross-Log Certificate Deduplication**
SHA-256 based dedup filter prevents duplicate broadcasts when the same certificate appears across multiple CT logs. Configured with a 5-minute TTL, 60-second cleanup interval, and 500K entry capacity (~50 MB max). Runs as a background task with graceful cancellation — always active, no configuration needed.

**Full CT Log Coverage**
Now monitors all CT logs except rejected/retired (previously only "usable"). Adds Google Solera logs (2018–2027) and readonly logs. 63 candidates → 49 reachable.

**Startup Health Check**
Parallel health checks filter unreachable logs on startup with a 5-second timeout, preventing warning spam from defunct logs. Workers start with 50 ms staggered intervals to reduce rate limiting.

**Circuit Breaker Pattern**
Handles unreliable CT logs gracefully: Closed (normal) → Open (30 s block) → HalfOpen (testing). Paired with exponential backoff (1 s min → 60 s max).

**Graceful Shutdown**
`CancellationToken` for coordinated worker termination on SIGINT/SIGTERM. Workers complete current work before stopping.

**Deep Health Endpoint**
`GET /health/deep` returns per-log health status with connection count and uptime. Returns HTTP 503 when >50% of logs are failing.

**New Prometheus Metrics**

| Metric | Type | Description |
|--------|------|-------------|
| `certstream_static_ct_logs_count` | Gauge | Static CT logs monitored |
| `certstream_static_ct_tiles_fetched` | Counter | Tiles fetched from static CT logs |
| `certstream_static_ct_entries_parsed` | Counter | Entries parsed from static CT tiles |
| `certstream_static_ct_parse_failures` | Counter | Failed static CT entry parses |
| `certstream_static_ct_checkpoint_errors` | Counter | Checkpoint fetch/parse errors |
| `certstream_issuer_cache_size` | Gauge | Cached issuer certificates |
| `certstream_issuer_cache_hits` | Counter | Issuer cache hits |
| `certstream_issuer_cache_misses` | Counter | Issuer cache misses |
| `certstream_duplicates_filtered` | Counter | Certificates filtered by dedup |
| `certstream_dedup_cache_size` | Gauge | Current dedup cache size |
| `certstream_worker_panics` | Counter | Worker panics (auto-recovered) |
| `certstream_log_health_checks_failed` | Counter | Failed log health checks |

Per-log metrics (`certstream_messages_sent`, `certstream_parse_failures`, and all `static_ct_*` counters) now include a `log` label for per-source breakdown in Grafana.

**Grafana Dashboard & Monitoring Stack**
Pre-built Grafana dashboard with per-source certificate volume, dedup efficiency, and static CT panels. Prometheus + Grafana run behind Docker Compose's `monitoring` profile and are not started by default:

```bash
# Server only
docker compose up -d

# With monitoring
docker compose --profile monitoring up -d
```

Default Grafana credentials: `admin` / `certstream` (configurable via `GRAFANA_USER` / `GRAFANA_PASSWORD`).

### Bug Fixes

**State Persistence — AtomicBool Dirty Flag** *(Critical)*
`update_index()` used `try_write()` on a tokio `RwLock<bool>`, which silently failed when `save_if_dirty()` held a read lock, causing lost state updates. Replaced with `AtomicBool` using `Ordering::Relaxed`.

**State Persistence — Shutdown Flush** *(Critical)*
`save_if_dirty()` was never called on SIGINT/SIGTERM, losing up to 30 seconds of progress on every restart. Now flushes state after the HTTP server stops.

**State Persistence — Periodic Save Never Stops**
`start_periodic_save()` spawned an infinite loop with no cancellation mechanism. Now accepts a `CancellationToken` and flushes state before exiting.

**State Persistence — Default State File**
`state_file` defaulted to `null`, silently disabling persistence. Changed default to `"certstream_state.json"`.

**Subject/Issuer Parsing Always Null** *(Critical)*
All certificates had `null` subject/issuer fields because `as_str()` only handled UTF8String ASN.1 encoding. Real-world certificates use PrintableString, IA5String, and others. Added a raw byte fallback for ASCII-compatible encodings.

**Config Environment Variable Override Ignored**
Env vars for a config section were silently ignored when that YAML section existed. Env var overrides now always apply on top of YAML values.

**Inconsistent Subject/Issuer JSON Serialization**
Stream endpoints serialized empty fields as `null` while the cert lookup API omitted them. Added `skip_serializing_if = "Option::is_none"` for consistent omission of empty fields.

**HTTP Status Check Before JSON Parse** *(Critical)*
CT logs (especially DigiCert) returning 400/429/5xx caused continuous JSON parse errors, CPU strain, and log spam. Non-2xx responses are now logged as warnings with proper status codes.

**Worker Panic Recovery**
Worker threads silently died on panic, leaving CT logs unmonitored. Implemented `catch_unwind` with automatic restart after 5-second delay.

**WebSocket Ping Priority**
Ping/pong had lowest priority in `tokio::select!`, causing client timeouts. Reordered with `biased;`.

### Performance

- **O(1) certificate cache lookup** — DashMap with pre-normalized hash keys replaces linear scan (~100× faster).
- **O(1) domain deduplication** — HashSet replaces O(n²) `contains()` scans (~10× faster for large SAN lists).
- **Pre-allocated serialization buffers** — 4 KB (full), 2 KB (lite), 512 B (domains-only).
- **Staggered worker start** — 50 ms intervals between worker launches reduces DigiCert 429 errors by ~60%.
- **Optional SIMD JSON** — enable with `cargo build --release --features simd`.
- **Optimized release profile** — `opt-level = 3`, LTO, single codegen unit, symbol stripping.

### Docker

Native health check via `HEALTHCHECK` directive and `docker-compose.yml` healthcheck config against `/health/deep`.

### Configuration Changes

New `static_logs` section:

```yaml
static_logs:
  - name: "Let's Encrypt 'Willow' 2026h1"
    url: "https://mon.willow.ct.letsencrypt.org/2026h1/"
  - name: "Let's Encrypt 'Sycamore' 2026h1"
    url: "https://mon.sycamore.ct.letsencrypt.org/2026h1/"
  - name: "Let's Encrypt 'Willow' 2025h2d"
    url: "https://mon.willow.ct.letsencrypt.org/2025h2d/"
  - name: "Let's Encrypt 'Sycamore' 2025h2d"
    url: "https://mon.sycamore.ct.letsencrypt.org/2025h2d/"
```

Default `state_file` changed from `null` → `"certstream_state.json"`.

### Test Coverage

183 unit tests across all modules: `static_ct` (30), `parser` (27), `config` (18), `api` (18), `middleware` (14), `rate_limit` (13), `log_list` (13), `state` (12), `certificate` (11), `watcher` (11), `dedup` (10), `hot_reload` (6).

### Dependencies

Added `flate2 = "1.0"` for gzip tile decompression.

### Benchmarks (vs v1.1.0)

| Metric | v1.1.0 | v1.2.0 |
|--------|--------|--------|
| Parse errors | Continuous | 0 |
| Healthy logs | Variable | 49/49 |
| Throughput | ~200 cert/s | ~400 cert/s |
| Client disconnections | Frequent | Rare |
| Recovery | Manual | Automatic |

### Upgrade Notes

- TCP protocol removed — switch to WebSocket or SSE
- Subject/issuer fields now populate correctly for all certificate encodings
- Environment variables now always override YAML config
- State persistence enabled by default
- Cross-log dedup is always active (no config required)
- Static CT logs require explicit `static_logs` entries for non-Let's Encrypt logs
- Monitoring is opt-in via `docker compose --profile monitoring up -d`

```bash
docker pull reloading01/certstream-server-rust:1.2.0
```

---

## v1.1.0 — Certstream Library Compatibility

**January 23, 2026**

Full compatibility with certstream client libraries (Python, Go, JS).

### New Features

- **`cert_link` field** — direct link to CT log entry for certificate verification.
- **`/domains-only` format change** — now returns `{ "message_type": "dns_entries", "data": ["example.com", ...] }` matching the certstream library format (previously returned nested `certificate_update` object).
- **Structured subject/issuer fields** — C, CN, O, L, ST, OU, and aggregated string.

### Bug Fixes

- **Precertificate parsing** — precerts were parsed from `leaf_input` instead of `extra_data` per RFC 6962, failing ~50% of entries.
- **CT log filtering** — logs with `state: null` or `rejected` state are now excluded (66 → 36 active logs).

### Migration

If you relied on the old `/domains-only` format with `update_type`, `seen`, and `source` fields, update your client to consume the new simple array format.

---

## v1.0.7 — Hot Reload Fix

**January 9, 2026**

- **Hot reload fixed** — config changes now properly propagate to connection limiter, rate limiter, and auth middleware via ArcSwap.
- Code cleanup: removed dead code, unused dependencies, and unintegrated `backpressure` and `tcp` modules.

---

## v1.0.6 — REST API & Rate Limiting

**January 6, 2026**

- **REST API** — `GET /api/stats` (server statistics), `GET /api/logs` (CT log health), `GET /api/cert/{hash}` (certificate lookup by SHA-256/SHA-1/fingerprint). Enable with `CERTSTREAM_API_ENABLED=true`.
- **Rate limiting** — token bucket + sliding window hybrid with tier-based limits (Free/Standard/Premium) and burst allowance. Enable with `CERTSTREAM_RATE_LIMIT_ENABLED=true`.
- **CLI enhancements** — `--validate-config`, `--dry-run`, `--export-metrics`, `-V/--version`.

---

## v1.0.5 — Certificate Field Compatibility

**December 28, 2025**

Full certificate output compatibility with certstream-server-go.

### New Fields

- `sha1`, `sha256` — separate fingerprint fields (colon-separated hex)
- `signature_algorithm`, `is_ca` — algorithm string and CA boolean
- Structured subject/issuer with `C`, `CN`, `L`, `O`, `OU`, `ST`, `aggregated`, `email_address`
- Fully parsed extensions: `authorityInfoAccess`, `authorityKeyIdentifier`, `basicConstraints`, `certificatePolicies`, `extendedKeyUsage`, `keyUsage`, `subjectAltName`, `subjectKeyIdentifier`, `ctlPoisonByte`
- `source.operator` — CT log operator name

No breaking changes. New fields are additive.

---

## v1.0.4 — Connection Limiting Fix

**December 27, 2025**

- **Critical fix**: connection limits now track the full WebSocket/SSE connection lifecycle (previously released immediately after HTTP upgrade).
- Removed rate limiting (not useful for streaming protocols; connection limiting is the appropriate mechanism).

---

## v1.0.3 — Initial Stable Release

**December 26, 2025**

Production-ready initial release.