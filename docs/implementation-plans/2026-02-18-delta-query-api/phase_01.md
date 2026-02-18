# Delta Query API Implementation Plan - Phase 1: Config + Module Scaffolding

**Goal:** Add configuration and route stub so the query API can be enabled without functionality.

**Architecture:** New `QueryApiConfig` struct in `src/config.rs` following the `DeltaSinkConfig` pattern, a `src/query.rs` module with a stub handler returning 501, and conditional router wiring in `src/main.rs` behind an `enabled` flag.

**Tech Stack:** Rust, Axum 0.8, serde YAML, env var overrides

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-18

---

## Acceptance Criteria Coverage

This phase is infrastructure scaffolding. It establishes the config and routing foundation for subsequent phases.

### delta-query-api.AC4: Integration with existing server
- **delta-query-api.AC4.1 Success:** Query endpoint protected by existing auth middleware when auth is enabled
- **delta-query-api.AC4.2 Success:** Query endpoint subject to existing rate limiting when rate limiting is enabled
- **delta-query-api.AC4.3 Success:** Query API disabled by default (`enabled: false`), server starts normally without it

### delta-query-api.AC5: Configuration and operational
- **delta-query-api.AC5.1 Success:** YAML config parsed with defaults for all fields
- **delta-query-api.AC5.2 Success:** Env var overrides work (`CERTSTREAM_QUERY_API_*`)

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Add QueryApiConfig to src/config.rs

**Verifies:** delta-query-api.AC4.3, delta-query-api.AC5.1, delta-query-api.AC5.2

**Files:**
- Modify: `src/config.rs:287-320` (add new struct after `DeltaSinkConfig`)
- Modify: `src/config.rs:326-346` (add field to `Config` struct)
- Modify: `src/config.rs:348-377` (add field to `YamlConfig` struct)
- Modify: `src/config.rs:515-548` (add env var overrides and Self construction)
- Modify: `src/config.rs:669-690` (add to `test_config()` helper)

**Implementation:**

Add `QueryApiConfig` struct after `DeltaSinkConfig` (after line 320), following the exact same pattern:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct QueryApiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_query_api_table_path")]
    pub table_path: String,
    #[serde(default = "default_query_api_max_results_per_page")]
    pub max_results_per_page: usize,
    #[serde(default = "default_query_api_default_results_per_page")]
    pub default_results_per_page: usize,
    #[serde(default = "default_query_api_timeout_secs")]
    pub query_timeout_secs: u64,
}

impl Default for QueryApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            table_path: default_query_api_table_path(),
            max_results_per_page: default_query_api_max_results_per_page(),
            default_results_per_page: default_query_api_default_results_per_page(),
            query_timeout_secs: default_query_api_timeout_secs(),
        }
    }
}

fn default_query_api_table_path() -> String {
    // Same default as delta_sink.table_path — both point to the same Delta table.
    // Override via CERTSTREAM_QUERY_API_TABLE_PATH env var if they differ.
    "./data/certstream".to_string()
}

fn default_query_api_max_results_per_page() -> usize {
    500
}

fn default_query_api_default_results_per_page() -> usize {
    50
}

fn default_query_api_timeout_secs() -> u64 {
    30
}
```

Add `query_api: QueryApiConfig` field to the `Config` struct (after `delta_sink` field at line 344).

Add `query_api: Option<QueryApiConfig>` to `YamlConfig` (after `delta_sink` field at line 376).

Add env var overrides in `Config::load()` after the `delta_sink` env var block (after line 527):

```rust
let mut query_api = yaml_config.query_api.unwrap_or_default();
if let Ok(v) = env::var("CERTSTREAM_QUERY_API_ENABLED") {
    query_api.enabled = v.parse().unwrap_or(query_api.enabled);
}
if let Ok(v) = env::var("CERTSTREAM_QUERY_API_TABLE_PATH") {
    query_api.table_path = v;
}
if let Ok(v) = env::var("CERTSTREAM_QUERY_API_MAX_RESULTS_PER_PAGE") {
    query_api.max_results_per_page = v.parse().unwrap_or(query_api.max_results_per_page);
}
if let Ok(v) = env::var("CERTSTREAM_QUERY_API_DEFAULT_RESULTS_PER_PAGE") {
    query_api.default_results_per_page = v.parse().unwrap_or(query_api.default_results_per_page);
}
if let Ok(v) = env::var("CERTSTREAM_QUERY_API_QUERY_TIMEOUT_SECS") {
    query_api.query_timeout_secs = v.parse().unwrap_or(query_api.query_timeout_secs);
}
```

Add `query_api` to the `Self { ... }` construction at line 529 (after `delta_sink,`).

Update `test_config()` helper in the test module (after `delta_sink: DeltaSinkConfig::default(),` at line 687) to include `query_api: QueryApiConfig::default(),`.

**Testing:**
Tests must verify each AC listed above:
- delta-query-api.AC4.3: `QueryApiConfig::default()` has `enabled: false`
- delta-query-api.AC5.1: All default values match design spec (table_path = "./data/certstream", max_results_per_page = 500, default_results_per_page = 50, query_timeout_secs = 30)
- delta-query-api.AC5.2: YAML deserialization round-trip produces correct values

Add these tests in the existing `src/config.rs` test module:

```rust
#[test]
fn test_query_api_config_defaults() {
    let config = QueryApiConfig::default();
    assert!(!config.enabled);
    assert_eq!(config.table_path, "./data/certstream");
    assert_eq!(config.max_results_per_page, 500);
    assert_eq!(config.default_results_per_page, 50);
    assert_eq!(config.query_timeout_secs, 30);
}

#[test]
fn test_query_api_config_deserialize() {
    let yaml = r#"
enabled: true
table_path: "/custom/path"
max_results_per_page: 100
default_results_per_page: 25
query_timeout_secs: 60
"#;
    let config: QueryApiConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.enabled);
    assert_eq!(config.table_path, "/custom/path");
    assert_eq!(config.max_results_per_page, 100);
    assert_eq!(config.default_results_per_page, 25);
    assert_eq!(config.query_timeout_secs, 60);
}
```

**Verification:**
Run: `cargo test config::tests`
Expected: All config tests pass including the two new ones

**Commit:** `feat(query-api): add QueryApiConfig with YAML + env var support`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create src/query.rs with stub handler and QueryApiState

**Files:**
- Create: `src/query.rs`
- Modify: `src/main.rs:1-15` (add `mod query;` declaration)

**Implementation:**

Create `src/query.rs` with a stub handler that returns 501 Not Implemented, and a `QueryApiState` struct:

```rust
use crate::config::QueryApiConfig;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use std::sync::Arc;

pub struct QueryApiState {
    pub config: QueryApiConfig,
}

pub fn query_api_router(state: Arc<QueryApiState>) -> Router {
    Router::new()
        .route("/api/query/certs", get(handle_query_certs))
        .with_state(state)
}

async fn handle_query_certs(
    State(_state): State<Arc<QueryApiState>>,
) -> impl IntoResponse {
    (StatusCode::NOT_IMPLEMENTED, "Query API not yet implemented")
}
```

Add `mod query;` to `src/main.rs` module declarations (after `mod models;` at line 11, to maintain alphabetical ordering near existing modules).

**Verification:**
Run: `cargo build`
Expected: Builds without errors

**Commit:** `feat(query-api): add query module with stub handler`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Wire query API router into main.rs

**Verifies:** delta-query-api.AC4.1, delta-query-api.AC4.2

**Files:**
- Modify: `src/main.rs:458-490` (add conditional query API router merge)

**Implementation:**

After the existing API router block (after line 490, `info!("REST API enabled");`), but **before** the auth middleware layer (line 510) and rate-limit middleware layer (line 520), add:

```rust
if config.query_api.enabled {
    let query_api_state = Arc::new(query::QueryApiState {
        config: config.query_api.clone(),
    });
    let query_router = query::query_api_router(query_api_state);
    app = app.merge(query_router);
    info!("Query API enabled");
}
```

**AC4.1 and AC4.2 are satisfied by placement:** The query router is merged into `app` before the auth middleware layer (`src/main.rs:510-518`) and rate-limit middleware layer (`src/main.rs:520-541`) are applied. These middleware layers wrap the entire `app`, so the query endpoint inherits both protections automatically when they are enabled in config. No query-specific middleware wiring is needed.

Add `use query::QueryApiState;` is not needed since we access via `query::` prefix, matching the existing pattern where `api::handle_stats` etc. are used with module prefix.

**Testing:**
- delta-query-api.AC4.1: Verified structurally — the query router is merged before auth middleware is applied as a layer. When `config.auth.enabled = true`, all routes including `/api/query/certs` require valid auth tokens. This follows the same pattern as existing API routes.
- delta-query-api.AC4.2: Verified structurally — same as above for rate limiting. When `config.rate_limit.enabled = true`, the query endpoint is rate-limited.

**Verification:**
Run: `cargo build`
Expected: Builds without errors

Run: `cargo test`
Expected: All 249+ tests pass (new config tests add to the count)

**Commit:** `feat(query-api): wire query router into main with enabled flag`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->
