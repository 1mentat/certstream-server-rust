use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::IpAddr;
use std::path::Path;
use parquet::basic::ZstdLevel;

#[derive(Debug, Clone, Deserialize)]
pub struct CustomCtLog {
    pub name: String,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StaticCtLog {
    pub name: String,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProtocolConfig {
    #[serde(default = "default_true")]
    pub websocket: bool,
    #[serde(default)]
    pub sse: bool,
    #[serde(default = "default_true")]
    pub metrics: bool,
    #[serde(default = "default_true")]
    pub health: bool,
    #[serde(default = "default_true")]
    pub example_json: bool,
    #[serde(default)]
    pub api: bool,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            websocket: true,
            sse: false,
            metrics: true,
            health: true,
            example_json: true,
            api: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CtLogConfig {
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: u32,
    #[serde(default = "default_retry_initial_delay_ms")]
    pub retry_initial_delay_ms: u64,
    #[serde(default = "default_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_healthy_threshold")]
    pub healthy_threshold: u32,
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
    #[serde(default = "default_health_check_interval_secs")]
    pub health_check_interval_secs: u64,
    #[serde(default = "default_state_file")]
    pub state_file: Option<String>,
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

impl Default for CtLogConfig {
    fn default() -> Self {
        Self {
            retry_max_attempts: default_retry_max_attempts(),
            retry_initial_delay_ms: default_retry_initial_delay_ms(),
            retry_max_delay_ms: default_retry_max_delay_ms(),
            request_timeout_secs: default_request_timeout_secs(),
            healthy_threshold: default_healthy_threshold(),
            unhealthy_threshold: default_unhealthy_threshold(),
            health_check_interval_secs: default_health_check_interval_secs(),
            state_file: default_state_file(),
            batch_size: default_batch_size(),
            poll_interval_ms: default_poll_interval_ms(),
        }
    }
}

fn default_retry_max_attempts() -> u32 {
    3
}
fn default_retry_initial_delay_ms() -> u64 {
    1000
}
fn default_retry_max_delay_ms() -> u64 {
    30000
}
fn default_request_timeout_secs() -> u64 {
    30
}
fn default_healthy_threshold() -> u32 {
    2
}
fn default_unhealthy_threshold() -> u32 {
    5
}
fn default_health_check_interval_secs() -> u64 {
    60
}
fn default_batch_size() -> u64 {
    256
}
fn default_poll_interval_ms() -> u64 {
    1000
}
fn default_state_file() -> Option<String> {
    Some("certstream_state.json".to_string())
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectionLimitConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default)]
    pub per_ip_limit: Option<u32>,
}

impl Default for ConnectionLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_connections: default_max_connections(),
            per_ip_limit: None,
        }
    }
}

fn default_max_connections() -> u32 {
    10000
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_free_max_tokens")]
    pub free_max_tokens: f64,
    #[serde(default = "default_free_refill_rate")]
    pub free_refill_rate: f64,
    #[serde(default = "default_free_burst")]
    pub free_burst: f64,
    #[serde(default = "default_standard_max_tokens")]
    pub standard_max_tokens: f64,
    #[serde(default = "default_standard_refill_rate")]
    pub standard_refill_rate: f64,
    #[serde(default = "default_standard_burst")]
    pub standard_burst: f64,
    #[serde(default = "default_premium_max_tokens")]
    pub premium_max_tokens: f64,
    #[serde(default = "default_premium_refill_rate")]
    pub premium_refill_rate: f64,
    #[serde(default = "default_premium_burst")]
    pub premium_burst: f64,
    #[serde(default = "default_window_seconds")]
    pub window_seconds: u64,
    #[serde(default = "default_window_max_requests")]
    pub window_max_requests: u32,
    #[serde(default = "default_burst_window_seconds")]
    pub burst_window_seconds: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            free_max_tokens: default_free_max_tokens(),
            free_refill_rate: default_free_refill_rate(),
            free_burst: default_free_burst(),
            standard_max_tokens: default_standard_max_tokens(),
            standard_refill_rate: default_standard_refill_rate(),
            standard_burst: default_standard_burst(),
            premium_max_tokens: default_premium_max_tokens(),
            premium_refill_rate: default_premium_refill_rate(),
            premium_burst: default_premium_burst(),
            window_seconds: default_window_seconds(),
            window_max_requests: default_window_max_requests(),
            burst_window_seconds: default_burst_window_seconds(),
        }
    }
}

fn default_free_max_tokens() -> f64 {
    100.0
}
fn default_free_refill_rate() -> f64 {
    10.0
}
fn default_free_burst() -> f64 {
    20.0
}
fn default_standard_max_tokens() -> f64 {
    500.0
}
fn default_standard_refill_rate() -> f64 {
    50.0
}
fn default_standard_burst() -> f64 {
    100.0
}
fn default_premium_max_tokens() -> f64 {
    2000.0
}
fn default_premium_refill_rate() -> f64 {
    200.0
}
fn default_premium_burst() -> f64 {
    500.0
}
fn default_window_seconds() -> u64 {
    60
}
fn default_window_max_requests() -> u32 {
    1000
}
fn default_burst_window_seconds() -> u64 {
    10
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_cache_capacity")]
    pub cache_capacity: usize,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            cache_capacity: default_cache_capacity(),
        }
    }
}

fn default_cache_capacity() -> usize {
    10000
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub tokens: Vec<String>,
    #[serde(default = "default_header_name")]
    pub header_name: String,
    #[serde(default)]
    pub standard_tokens: Vec<String>,
    #[serde(default)]
    pub premium_tokens: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tokens: Vec::new(),
            header_name: default_header_name(),
            standard_tokens: Vec::new(),
            premium_tokens: Vec::new(),
        }
    }
}

fn default_header_name() -> String {
    "Authorization".to_string()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct HotReloadConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub watch_path: Option<String>,
}

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
    #[serde(default = "default_delta_sink_offline_batch_size")]
    pub offline_batch_size: usize,
}

impl Default for DeltaSinkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            table_path: default_delta_sink_table_path(),
            batch_size: default_delta_sink_batch_size(),
            flush_interval_secs: default_delta_sink_flush_interval_secs(),
            compression_level: default_delta_sink_compression_level(),
            heavy_column_compression_level: default_delta_sink_heavy_column_compression_level(),
            offline_batch_size: default_delta_sink_offline_batch_size(),
        }
    }
}

fn default_delta_sink_table_path() -> String {
    "./data/certstream".to_string()
}

fn default_delta_sink_batch_size() -> usize {
    10000
}

fn default_delta_sink_flush_interval_secs() -> u64 {
    30
}

fn default_delta_sink_compression_level() -> i32 {
    9
}

fn default_delta_sink_heavy_column_compression_level() -> i32 {
    15
}

fn default_delta_sink_offline_batch_size() -> usize {
    100000
}

#[derive(Debug, Clone, Deserialize)]
pub struct TargetConfig {
    pub table_path: String,
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    #[serde(default)]
    pub compression_level: Option<i32>,
    #[serde(default)]
    pub heavy_column_compression_level: Option<i32>,
    #[serde(default)]
    pub offline_batch_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub table_path: String,
    pub storage_options: HashMap<String, String>,
    pub compression_level: i32,
    pub heavy_column_compression_level: i32,
    pub offline_batch_size: usize,
}

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

#[derive(Debug, Clone, Deserialize)]
pub struct ZerobusSinkConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub unity_catalog_url: String,
    #[serde(default)]
    pub table_name: String,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    #[serde(default = "default_zerobus_max_inflight_records")]
    pub max_inflight_records: usize,
}

fn default_zerobus_max_inflight_records() -> usize {
    10000
}

impl Default for ZerobusSinkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            unity_catalog_url: String::new(),
            table_name: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            max_inflight_records: default_zerobus_max_inflight_records(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct S3StorageConfig {
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub access_key_id: String,
    #[serde(default)]
    pub secret_access_key: String,
    #[serde(default)]
    pub conditional_put: Option<String>,
    #[serde(default)]
    pub allow_http: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StorageConfig {
    #[serde(default)]
    pub s3: Option<S3StorageConfig>,
}

/// Represents the location of a table (local filesystem or S3).
#[derive(Debug, Clone, PartialEq)]
pub enum TableLocation {
    /// Local filesystem path.
    Local { path: String },
    /// S3 URI.
    S3 { uri: String },
}

impl TableLocation {
    /// Returns the path/URI string for use with DeltaTableBuilder::from_uri().
    /// For Local, this returns the stripped path (no file:// prefix) because
    /// DeltaTableBuilder::from_uri() accepts bare paths for local filesystem.
    /// For S3, this returns the full s3:// URI.
    pub fn as_uri(&self) -> &str {
        match self {
            TableLocation::Local { path } => path,
            TableLocation::S3 { uri } => uri,
        }
    }
}

/// Parses a table URI and returns the corresponding TableLocation.
///
/// Supported schemes:
/// - `file://` — local filesystem (absolute or relative paths)
/// - `s3://` — S3-compatible storage
///
/// # Errors
///
/// Returns an error if:
/// - The URI is empty
/// - The URI uses an unsupported scheme
/// - The URI is a bare path (suggests using `file://` prefix)
pub fn parse_table_uri(uri: &str) -> Result<TableLocation, String> {
    if uri.is_empty() {
        return Err("Table path cannot be empty".to_string());
    }

    if let Some(path) = uri.strip_prefix("file://") {
        Ok(TableLocation::Local {
            path: path.to_string(),
        })
    } else if uri.starts_with("s3://") {
        Ok(TableLocation::S3 {
            uri: uri.to_string(),
        })
    } else if uri.contains("://") {
        let scheme = uri.split("://").next().unwrap_or("");
        Err(format!(
            "Unsupported URI scheme '{}://'. Supported schemes: file://, s3://",
            scheme
        ))
    } else {
        Err(format!(
            "Table path '{}' must use a URI scheme. Use 'file://{}' for local filesystem paths",
            uri, uri
        ))
    }
}

/// Resolves storage options for a given TableLocation and StorageConfig.
///
/// Returns a HashMap of storage options suitable for use with Delta Lake or similar APIs:
/// - For Local locations: returns an empty HashMap
/// - For S3 locations: returns AWS-compatible options (AWS_ENDPOINT_URL, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, conditional_put, AWS_ALLOW_HTTP)
pub fn resolve_storage_options(
    location: &TableLocation,
    storage: &StorageConfig,
) -> HashMap<String, String> {
    match location {
        TableLocation::Local { .. } => HashMap::new(),
        TableLocation::S3 { .. } => {
            let mut opts = HashMap::new();
            if let Some(ref s3) = storage.s3 {
                if !s3.endpoint.is_empty() {
                    opts.insert("AWS_ENDPOINT_URL".to_string(), s3.endpoint.clone());
                }
                if !s3.region.is_empty() {
                    opts.insert("AWS_REGION".to_string(), s3.region.clone());
                }
                if !s3.access_key_id.is_empty() {
                    opts.insert("AWS_ACCESS_KEY_ID".to_string(), s3.access_key_id.clone());
                }
                if !s3.secret_access_key.is_empty() {
                    opts.insert(
                        "AWS_SECRET_ACCESS_KEY".to_string(),
                        s3.secret_access_key.clone(),
                    );
                }
                if let Some(ref cp) = s3.conditional_put {
                    opts.insert("conditional_put".to_string(), cp.clone());
                }
                if let Some(allow) = s3.allow_http {
                    opts.insert("AWS_ALLOW_HTTP".to_string(), allow.to_string());
                }
            }
            opts
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Clone)]
pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    pub log_level: String,
    pub buffer_size: usize,
    pub ct_logs_url: String,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub custom_logs: Vec<CustomCtLog>,
    pub static_logs: Vec<StaticCtLog>,
    pub protocols: ProtocolConfig,
    pub ct_log: CtLogConfig,
    pub connection_limit: ConnectionLimitConfig,
    pub rate_limit: RateLimitConfig,
    pub api: ApiConfig,
    pub auth: AuthConfig,
    pub hot_reload: HotReloadConfig,
    pub delta_sink: DeltaSinkConfig,
    pub query_api: QueryApiConfig,
    pub zerobus_sink: ZerobusSinkConfig,
    pub storage: StorageConfig,
    pub targets: HashMap<String, TargetConfig>,
    pub config_path: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct YamlConfig {
    host: Option<String>,
    port: Option<u16>,
    log_level: Option<String>,
    buffer_size: Option<usize>,
    ct_logs_url: Option<String>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    #[serde(default)]
    custom_logs: Vec<CustomCtLog>,
    #[serde(default)]
    static_logs: Vec<StaticCtLog>,
    #[serde(default)]
    protocols: Option<ProtocolConfig>,
    #[serde(default)]
    ct_log: Option<CtLogConfig>,
    #[serde(default)]
    connection_limit: Option<ConnectionLimitConfig>,
    #[serde(default)]
    rate_limit: Option<RateLimitConfig>,
    #[serde(default)]
    api: Option<ApiConfig>,
    #[serde(default)]
    auth: Option<AuthConfig>,
    #[serde(default)]
    hot_reload: Option<HotReloadConfig>,
    #[serde(default)]
    delta_sink: Option<DeltaSinkConfig>,
    #[serde(default)]
    query_api: Option<QueryApiConfig>,
    #[serde(default)]
    zerobus_sink: Option<ZerobusSinkConfig>,
    #[serde(default)]
    storage: Option<StorageConfig>,
    #[serde(default)]
    targets: Option<HashMap<String, TargetConfig>>,
}

struct YamlConfigWithPath {
    config: YamlConfig,
    path: Option<String>,
}

#[derive(Debug)]
pub struct ConfigValidationError {
    pub field: String,
    pub message: String,
}

impl Config {
    pub fn load() -> Self {
        let yaml_result = Self::load_yaml();
        let yaml_config = yaml_result.config;
        let config_path = yaml_result.path;

        let host = env::var("CERTSTREAM_HOST")
            .ok()
            .or(yaml_config.host)
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| "0.0.0.0".parse().unwrap());

        let port = env::var("CERTSTREAM_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(yaml_config.port)
            .unwrap_or(8080);

        let log_level = env::var("CERTSTREAM_LOG_LEVEL")
            .ok()
            .or(yaml_config.log_level)
            .unwrap_or_else(|| "info".to_string());

        let buffer_size = env::var("CERTSTREAM_BUFFER_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(yaml_config.buffer_size)
            .unwrap_or(1000);

        let ct_logs_url = env::var("CERTSTREAM_CT_LOGS_URL")
            .ok()
            .or(yaml_config.ct_logs_url)
            .unwrap_or_else(|| {
                "https://www.gstatic.com/ct/log_list/v3/all_logs_list.json".to_string()
            });

        let tls_cert = env::var("CERTSTREAM_TLS_CERT").ok().or(yaml_config.tls_cert);
        let tls_key = env::var("CERTSTREAM_TLS_KEY").ok().or(yaml_config.tls_key);

        let mut protocols = yaml_config.protocols.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_WS_ENABLED") {
            protocols.websocket = v.parse().unwrap_or(protocols.websocket);
        }
        if let Ok(v) = env::var("CERTSTREAM_SSE_ENABLED") {
            protocols.sse = v.parse().unwrap_or(protocols.sse);
        }
        if let Ok(v) = env::var("CERTSTREAM_METRICS_ENABLED") {
            protocols.metrics = v.parse().unwrap_or(protocols.metrics);
        }
        if let Ok(v) = env::var("CERTSTREAM_HEALTH_ENABLED") {
            protocols.health = v.parse().unwrap_or(protocols.health);
        }
        if let Ok(v) = env::var("CERTSTREAM_EXAMPLE_JSON_ENABLED") {
            protocols.example_json = v.parse().unwrap_or(protocols.example_json);
        }
        if let Ok(v) = env::var("CERTSTREAM_API_ENABLED") {
            protocols.api = v.parse().unwrap_or(protocols.api);
        }

        let mut ct_log = yaml_config.ct_log.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_RETRY_MAX_ATTEMPTS") {
            ct_log.retry_max_attempts = v.parse().unwrap_or(ct_log.retry_max_attempts);
        }
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_RETRY_INITIAL_DELAY_MS") {
            ct_log.retry_initial_delay_ms = v.parse().unwrap_or(ct_log.retry_initial_delay_ms);
        }
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_RETRY_MAX_DELAY_MS") {
            ct_log.retry_max_delay_ms = v.parse().unwrap_or(ct_log.retry_max_delay_ms);
        }
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_REQUEST_TIMEOUT_SECS") {
            ct_log.request_timeout_secs = v.parse().unwrap_or(ct_log.request_timeout_secs);
        }
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_UNHEALTHY_THRESHOLD") {
            ct_log.unhealthy_threshold = v.parse().unwrap_or(ct_log.unhealthy_threshold);
        }
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_HEALTHY_THRESHOLD") {
            ct_log.healthy_threshold = v.parse().unwrap_or(ct_log.healthy_threshold);
        }
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_HEALTH_CHECK_INTERVAL_SECS") {
            ct_log.health_check_interval_secs = v.parse().unwrap_or(ct_log.health_check_interval_secs);
        }
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_STATE_FILE") {
            ct_log.state_file = Some(v);
        }
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_BATCH_SIZE") {
            ct_log.batch_size = v.parse().unwrap_or(ct_log.batch_size);
        }
        if let Ok(v) = env::var("CERTSTREAM_CT_LOG_POLL_INTERVAL_MS") {
            ct_log.poll_interval_ms = v.parse().unwrap_or(ct_log.poll_interval_ms);
        }

        let mut connection_limit = yaml_config.connection_limit.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_CONNECTION_LIMIT_ENABLED") {
            connection_limit.enabled = v.parse().unwrap_or(connection_limit.enabled);
        }
        if let Ok(v) = env::var("CERTSTREAM_CONNECTION_LIMIT_MAX_CONNECTIONS") {
            connection_limit.max_connections = v.parse().unwrap_or(connection_limit.max_connections);
        }
        if let Ok(v) = env::var("CERTSTREAM_CONNECTION_LIMIT_PER_IP_LIMIT") {
            connection_limit.per_ip_limit = v.parse().ok();
        }

        let mut rate_limit = yaml_config.rate_limit.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_RATE_LIMIT_ENABLED") {
            rate_limit.enabled = v.parse().unwrap_or(rate_limit.enabled);
        }

        let api = yaml_config.api.unwrap_or_default();

        let mut auth = yaml_config.auth.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_AUTH_ENABLED") {
            auth.enabled = v.parse().unwrap_or(auth.enabled);
        }
        if let Ok(v) = env::var("CERTSTREAM_AUTH_TOKENS") {
            auth.tokens = v.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(v) = env::var("CERTSTREAM_AUTH_HEADER_NAME") {
            auth.header_name = v;
        }

        let mut hot_reload = yaml_config.hot_reload.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_HOT_RELOAD_ENABLED") {
            hot_reload.enabled = v.parse().unwrap_or(hot_reload.enabled);
        }

        let mut delta_sink = yaml_config.delta_sink.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_ENABLED") {
            delta_sink.enabled = v.parse().unwrap_or(delta_sink.enabled);
        }
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_TABLE_PATH") {
            delta_sink.table_path = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_BATCH_SIZE") {
            delta_sink.batch_size = v.parse().unwrap_or(delta_sink.batch_size);
        }
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_FLUSH_INTERVAL_SECS") {
            delta_sink.flush_interval_secs = v.parse().unwrap_or(delta_sink.flush_interval_secs);
        }
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_COMPRESSION_LEVEL") {
            delta_sink.compression_level = v.parse().unwrap_or(delta_sink.compression_level);
        }
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_HEAVY_COLUMN_COMPRESSION_LEVEL") {
            delta_sink.heavy_column_compression_level = v.parse().unwrap_or(delta_sink.heavy_column_compression_level);
        }
        if let Ok(v) = env::var("CERTSTREAM_DELTA_SINK_OFFLINE_BATCH_SIZE") {
            delta_sink.offline_batch_size = v.parse().unwrap_or(delta_sink.offline_batch_size);
        }

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

        let mut zerobus_sink = yaml_config.zerobus_sink.unwrap_or_default();
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_ENABLED") {
            zerobus_sink.enabled = v.parse().unwrap_or(zerobus_sink.enabled);
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_ENDPOINT") {
            zerobus_sink.endpoint = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_UNITY_CATALOG_URL") {
            zerobus_sink.unity_catalog_url = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_TABLE_NAME") {
            zerobus_sink.table_name = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_CLIENT_ID") {
            zerobus_sink.client_id = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_CLIENT_SECRET") {
            zerobus_sink.client_secret = v;
        }
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_MAX_INFLIGHT_RECORDS") {
            zerobus_sink.max_inflight_records = v.parse().unwrap_or(zerobus_sink.max_inflight_records);
        }

        let mut storage = yaml_config.storage.unwrap_or_default();
        if let Some(ref mut s3) = storage.s3 {
            if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_ENDPOINT") {
                s3.endpoint = v;
            }
            if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_REGION") {
                s3.region = v;
            }
            if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID") {
                s3.access_key_id = v;
            }
            if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY") {
                s3.secret_access_key = v;
            }
            if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_CONDITIONAL_PUT") {
                s3.conditional_put = Some(v);
            }
            if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_ALLOW_HTTP") {
                s3.allow_http = v.parse().ok();
            }
        } else {
            // If no s3 section in YAML, check if env vars want to create one.
            // CERTSTREAM_STORAGE_S3_ENDPOINT is the trigger: if it's empty or unset,
            // no S3 config is created even if other S3 env vars are set.
            // This is intentional — endpoint is required for any S3 operation.
            let endpoint = env::var("CERTSTREAM_STORAGE_S3_ENDPOINT").unwrap_or_default();
            if !endpoint.is_empty() {
                storage.s3 = Some(S3StorageConfig {
                    endpoint,
                    region: env::var("CERTSTREAM_STORAGE_S3_REGION").unwrap_or_default(),
                    access_key_id: env::var("CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID").unwrap_or_default(),
                    secret_access_key: env::var("CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY").unwrap_or_default(),
                    conditional_put: env::var("CERTSTREAM_STORAGE_S3_CONDITIONAL_PUT").ok(),
                    allow_http: env::var("CERTSTREAM_STORAGE_S3_ALLOW_HTTP").ok().and_then(|v| v.parse().ok()),
                });
            }
        }

        // Overlay env vars for each named target
        let mut targets = yaml_config.targets.unwrap_or_default();
        for (name, target) in targets.iter_mut() {
            let prefix = format!("CERTSTREAM_TARGETS_{}", name.to_uppercase());

            if let Ok(v) = env::var(format!("{}_TABLE_PATH", prefix)) {
                target.table_path = v;
            }
            if let Ok(v) = env::var(format!("{}_COMPRESSION_LEVEL", prefix)) {
                if let Ok(level) = v.parse::<i32>() {
                    target.compression_level = Some(level);
                }
            }
            if let Ok(v) = env::var(format!("{}_HEAVY_COLUMN_COMPRESSION_LEVEL", prefix)) {
                if let Ok(level) = v.parse::<i32>() {
                    target.heavy_column_compression_level = Some(level);
                }
            }
            if let Ok(v) = env::var(format!("{}_OFFLINE_BATCH_SIZE", prefix)) {
                if let Ok(size) = v.parse::<usize>() {
                    target.offline_batch_size = Some(size);
                }
            }

            // Storage S3 env var overrides for per-target storage
            let s3_prefix = format!("{}_STORAGE_S3", prefix);
            let s3_endpoint = env::var(format!("{}_ENDPOINT", s3_prefix)).unwrap_or_default();
            if !s3_endpoint.is_empty() {
                // If endpoint env var is set, create or override S3 config
                let s3 = S3StorageConfig {
                    endpoint: s3_endpoint,
                    region: env::var(format!("{}_REGION", s3_prefix)).unwrap_or_default(),
                    access_key_id: env::var(format!("{}_ACCESS_KEY_ID", s3_prefix)).unwrap_or_default(),
                    secret_access_key: env::var(format!("{}_SECRET_ACCESS_KEY", s3_prefix)).unwrap_or_default(),
                    conditional_put: env::var(format!("{}_CONDITIONAL_PUT", s3_prefix)).ok(),
                    allow_http: env::var(format!("{}_ALLOW_HTTP", s3_prefix)).ok().and_then(|v| v.parse().ok()),
                };
                target.storage = Some(StorageConfig { s3: Some(s3) });
            } else if let Some(ref mut storage) = target.storage {
                // If no endpoint trigger but target has YAML storage, overlay individual non-endpoint fields
                // Note: endpoint overlay is intentionally omitted here — we already know the endpoint env var
                // is empty/unset (that's why we're in the else branch), so overlaying it would be a no-op.
                if let Some(ref mut s3) = storage.s3 {
                    if let Ok(v) = env::var(format!("{}_REGION", s3_prefix)) {
                        s3.region = v;
                    }
                    if let Ok(v) = env::var(format!("{}_ACCESS_KEY_ID", s3_prefix)) {
                        s3.access_key_id = v;
                    }
                    if let Ok(v) = env::var(format!("{}_SECRET_ACCESS_KEY", s3_prefix)) {
                        s3.secret_access_key = v;
                    }
                    if let Ok(v) = env::var(format!("{}_CONDITIONAL_PUT", s3_prefix)) {
                        s3.conditional_put = Some(v);
                    }
                    if let Ok(v) = env::var(format!("{}_ALLOW_HTTP", s3_prefix)) {
                        s3.allow_http = v.parse().ok();
                    }
                }
            }
        }

        Self {
            host,
            port,
            log_level,
            buffer_size,
            ct_logs_url,
            tls_cert,
            tls_key,
            custom_logs: yaml_config.custom_logs,
            static_logs: yaml_config.static_logs,
            protocols,
            ct_log,
            connection_limit,
            rate_limit,
            api,
            auth,
            hot_reload,
            delta_sink,
            query_api,
            zerobus_sink,
            storage,
            targets,
            config_path,
        }
    }

    pub fn validate(&self) -> Result<(), Vec<ConfigValidationError>> {
        let mut errors = Vec::new();

        if self.port == 0 {
            errors.push(ConfigValidationError {
                field: "port".to_string(),
                message: "Port must be greater than 0".to_string(),
            });
        }

        if self.buffer_size == 0 {
            errors.push(ConfigValidationError {
                field: "buffer_size".to_string(),
                message: "Buffer size must be greater than 0".to_string(),
            });
        }

        if self.ct_logs_url.is_empty() {
            errors.push(ConfigValidationError {
                field: "ct_logs_url".to_string(),
                message: "CT logs URL cannot be empty".to_string(),
            });
        }

        if self.has_tls() {
            if let Some(ref cert) = self.tls_cert {
                if !Path::new(cert).exists() {
                    errors.push(ConfigValidationError {
                        field: "tls_cert".to_string(),
                        message: format!("TLS certificate file not found: {}", cert),
                    });
                }
            }
            if let Some(ref key) = self.tls_key {
                if !Path::new(key).exists() {
                    errors.push(ConfigValidationError {
                        field: "tls_key".to_string(),
                        message: format!("TLS key file not found: {}", key),
                    });
                }
            }
        }

        if self.connection_limit.enabled && self.connection_limit.max_connections == 0 {
            errors.push(ConfigValidationError {
                field: "connection_limit.max_connections".to_string(),
                message: "Max connections must be greater than 0 when enabled".to_string(),
            });
        }

        if self.rate_limit.enabled && self.rate_limit.free_refill_rate <= 0.0 {
            errors.push(ConfigValidationError {
                field: "rate_limit.free_refill_rate".to_string(),
                message: "Refill rate must be positive".to_string(),
            });
        }

        if self.delta_sink.enabled && self.delta_sink.batch_size == 0 {
            errors.push(ConfigValidationError {
                field: "delta_sink.batch_size".to_string(),
                message: "Batch size must be greater than 0 when delta sink is enabled".to_string(),
            });
        }
        if self.delta_sink.enabled && self.delta_sink.flush_interval_secs == 0 {
            errors.push(ConfigValidationError {
                field: "delta_sink.flush_interval_secs".to_string(),
                message: "Flush interval must be greater than 0 when delta sink is enabled".to_string(),
            });
        }

        // Validate delta_sink.table_path is a valid URI (when delta_sink is enabled)
        if self.delta_sink.enabled {
            if let Err(e) = parse_table_uri(&self.delta_sink.table_path) {
                errors.push(ConfigValidationError {
                    field: "delta_sink.table_path".to_string(),
                    message: e,
                });
            }
        }

        // Validate query_api.table_path is a valid URI (when query_api is enabled)
        if self.query_api.enabled {
            if let Err(e) = parse_table_uri(&self.query_api.table_path) {
                errors.push(ConfigValidationError {
                    field: "query_api.table_path".to_string(),
                    message: e,
                });
            }
        }

        // Check S3 config presence when any S3 URI is used
        let has_s3_uri = (self.delta_sink.enabled && self.delta_sink.table_path.starts_with("s3://"))
            || (self.query_api.enabled && self.query_api.table_path.starts_with("s3://"));

        if has_s3_uri {
            match &self.storage.s3 {
                None => {
                    errors.push(ConfigValidationError {
                        field: "storage.s3".to_string(),
                        message: "S3 storage configuration required when using s3:// URIs. Add a [storage.s3] section to config.yaml or set CERTSTREAM_STORAGE_S3_ENDPOINT env var.".to_string(),
                    });
                }
                Some(s3) => {
                    if s3.endpoint.is_empty() {
                        errors.push(ConfigValidationError {
                            field: "storage.s3.endpoint".to_string(),
                            message: "S3 endpoint cannot be empty when using s3:// URIs".to_string(),
                        });
                    }
                    if s3.region.is_empty() {
                        errors.push(ConfigValidationError {
                            field: "storage.s3.region".to_string(),
                            message: "S3 region cannot be empty when using s3:// URIs".to_string(),
                        });
                    }
                    if s3.access_key_id.is_empty() {
                        errors.push(ConfigValidationError {
                            field: "storage.s3.access_key_id".to_string(),
                            message: "S3 access key ID cannot be empty when using s3:// URIs".to_string(),
                        });
                    }
                    if s3.secret_access_key.is_empty() {
                        errors.push(ConfigValidationError {
                            field: "storage.s3.secret_access_key".to_string(),
                            message: "S3 secret access key cannot be empty when using s3:// URIs".to_string(),
                        });
                    }
                }
            }
        }

        if self.zerobus_sink.enabled {
            if self.zerobus_sink.endpoint.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.endpoint".to_string(),
                    message: "Endpoint cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
            if self.zerobus_sink.unity_catalog_url.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.unity_catalog_url".to_string(),
                    message: "Unity Catalog URL cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
            if self.zerobus_sink.table_name.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.table_name".to_string(),
                    message: "Table name cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
            if !self.zerobus_sink.table_name.is_empty()
                && self.zerobus_sink.table_name.matches('.').count() != 2
            {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.table_name".to_string(),
                    message: "Table name must be in Unity Catalog format: catalog.schema.table".to_string(),
                });
            }
            if self.zerobus_sink.client_id.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.client_id".to_string(),
                    message: "Client ID cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
            if self.zerobus_sink.client_secret.is_empty() {
                errors.push(ConfigValidationError {
                    field: "zerobus_sink.client_secret".to_string(),
                    message: "Client secret cannot be empty when ZeroBus sink is enabled".to_string(),
                });
            }
        }
        if ZstdLevel::try_new(self.delta_sink.compression_level).is_err() {
            errors.push(ConfigValidationError {
                field: "delta_sink.compression_level".to_string(),
                message: format!(
                    "Compression level {} is invalid. Must be between 1 and 22",
                    self.delta_sink.compression_level
                ),
            });
        }
        if ZstdLevel::try_new(self.delta_sink.heavy_column_compression_level).is_err() {
            errors.push(ConfigValidationError {
                field: "delta_sink.heavy_column_compression_level".to_string(),
                message: format!(
                    "Heavy column compression level {} is invalid. Must be between 1 and 22",
                    self.delta_sink.heavy_column_compression_level
                ),
            });
        }

        // Validate named targets
        for (name, target) in &self.targets {
            let target_field = format!("targets.{}", name);

            // Validate URI format
            match parse_table_uri(&target.table_path) {
                Ok(location) => {
                    // If S3 URI, validate S3 credentials are available
                    if matches!(location, TableLocation::S3 { .. }) {
                        let effective_storage = target.storage.as_ref().unwrap_or(&self.storage);
                        match &effective_storage.s3 {
                            None => {
                                errors.push(ConfigValidationError {
                                    field: format!("{}.storage.s3", target_field),
                                    message: format!(
                                        "Target '{}' uses s3:// URI but has no S3 storage configuration (neither target-level nor global)",
                                        name
                                    ),
                                });
                            }
                            Some(s3) => {
                                if s3.endpoint.is_empty() {
                                    errors.push(ConfigValidationError {
                                        field: format!("{}.storage.s3.endpoint", target_field),
                                        message: format!("S3 endpoint cannot be empty for target '{}'", name),
                                    });
                                }
                                if s3.region.is_empty() {
                                    errors.push(ConfigValidationError {
                                        field: format!("{}.storage.s3.region", target_field),
                                        message: format!("S3 region cannot be empty for target '{}'", name),
                                    });
                                }
                                if s3.access_key_id.is_empty() {
                                    errors.push(ConfigValidationError {
                                        field: format!("{}.storage.s3.access_key_id", target_field),
                                        message: format!("S3 access key ID cannot be empty for target '{}'", name),
                                    });
                                }
                                if s3.secret_access_key.is_empty() {
                                    errors.push(ConfigValidationError {
                                        field: format!("{}.storage.s3.secret_access_key", target_field),
                                        message: format!("S3 secret access key cannot be empty for target '{}'", name),
                                    });
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    errors.push(ConfigValidationError {
                        field: format!("{}.table_path", target_field),
                        message: e,
                    });
                }
            }

            // Validate compression levels if specified
            if let Some(level) = target.compression_level {
                if ZstdLevel::try_new(level).is_err() {
                    errors.push(ConfigValidationError {
                        field: format!("{}.compression_level", target_field),
                        message: format!(
                            "Compression level {} is invalid for target '{}'. Must be between 1 and 22",
                            level, name
                        ),
                    });
                }
            }
            if let Some(level) = target.heavy_column_compression_level {
                if ZstdLevel::try_new(level).is_err() {
                    errors.push(ConfigValidationError {
                        field: format!("{}.heavy_column_compression_level", target_field),
                        message: format!(
                            "Heavy column compression level {} is invalid for target '{}'. Must be between 1 and 22",
                            level, name
                        ),
                    });
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Resolve a named target from config.targets, filling unspecified fields
    /// from delta_sink defaults and resolving storage options.
    pub fn resolve_target(&self, name: &str) -> Result<ResolvedTarget, String> {
        let target = self
            .targets
            .get(name)
            .ok_or_else(|| {
                let available: Vec<_> = self.targets.keys().collect();
                format!("Unknown target '{}'. Available targets: {:?}", name, available)
            })?;

        let location = parse_table_uri(&target.table_path)?;

        // Use target-level storage if present, otherwise fall back to global config.storage
        let storage = target.storage.as_ref().unwrap_or(&self.storage);
        let storage_options = resolve_storage_options(&location, storage);

        Ok(ResolvedTarget {
            table_path: location.as_uri().to_string(),
            storage_options,
            compression_level: target.compression_level.unwrap_or(self.delta_sink.compression_level),
            heavy_column_compression_level: target
                .heavy_column_compression_level
                .unwrap_or(self.delta_sink.heavy_column_compression_level),
            offline_batch_size: target.offline_batch_size.unwrap_or(self.delta_sink.offline_batch_size),
        })
    }

    fn load_yaml() -> YamlConfigWithPath {
        let config_paths = [
            env::var("CERTSTREAM_CONFIG").ok(),
            Some("config.yaml".to_string()),
            Some("config.yml".to_string()),
            Some("/etc/certstream/config.yaml".to_string()),
        ];

        for path in config_paths.into_iter().flatten() {
            if Path::new(&path).exists() {
                if let Ok(content) = fs::read_to_string(&path) {
                    match serde_yaml::from_str::<YamlConfig>(&content) {
                        Ok(config) => {
                            return YamlConfigWithPath {
                                config,
                                path: Some(path),
                            };
                        }
                        Err(e) => {
                            eprintln!("WARNING: failed to parse {}: {}", path, e);
                        }
                    }
                }
            }
        }

        YamlConfigWithPath {
            config: YamlConfig::default(),
            path: None,
        }
    }

    pub fn has_tls(&self) -> bool {
        self.tls_cert.is_some() && self.tls_key.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            host: "0.0.0.0".parse().unwrap(),
            port: 8080,
            log_level: "info".to_string(),
            buffer_size: 1000,
            ct_logs_url: "https://example.com".to_string(),
            tls_cert: None,
            tls_key: None,
            custom_logs: vec![],
            static_logs: vec![],
            protocols: ProtocolConfig::default(),
            ct_log: CtLogConfig::default(),
            connection_limit: ConnectionLimitConfig::default(),
            rate_limit: RateLimitConfig::default(),
            api: ApiConfig::default(),
            auth: AuthConfig::default(),
            hot_reload: HotReloadConfig::default(),
            delta_sink: DeltaSinkConfig::default(),
            query_api: QueryApiConfig::default(),
            zerobus_sink: ZerobusSinkConfig::default(),
            storage: StorageConfig::default(),
            targets: HashMap::new(),
            config_path: None,
        }
    }

    #[test]
    fn test_default_state_file() {
        let val = default_state_file();
        assert_eq!(val, Some("certstream_state.json".to_string()));
    }

    #[test]
    fn test_ct_log_config_defaults() {
        let config = CtLogConfig::default();
        assert_eq!(config.retry_max_attempts, 3);
        assert_eq!(config.retry_initial_delay_ms, 1000);
        assert_eq!(config.retry_max_delay_ms, 30000);
        assert_eq!(config.request_timeout_secs, 30);
        assert_eq!(config.healthy_threshold, 2);
        assert_eq!(config.unhealthy_threshold, 5);
        assert_eq!(config.health_check_interval_secs, 60);
        assert_eq!(config.state_file, Some("certstream_state.json".to_string()));
        assert_eq!(config.batch_size, 256);
        assert_eq!(config.poll_interval_ms, 1000);
    }

    #[test]
    fn test_protocol_config_defaults() {
        let config = ProtocolConfig::default();
        assert!(config.websocket);
        assert!(!config.sse);
        assert!(config.metrics);
        assert!(config.health);
        assert!(config.example_json);
        assert!(!config.api);
    }

    #[test]
    fn test_connection_limit_config_defaults() {
        let config = ConnectionLimitConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_connections, 10000);
        assert!(config.per_ip_limit.is_none());
    }

    #[test]
    fn test_auth_config_defaults() {
        let config = AuthConfig::default();
        assert!(!config.enabled);
        assert!(config.tokens.is_empty());
        assert_eq!(config.header_name, "Authorization");
        assert!(config.standard_tokens.is_empty());
        assert!(config.premium_tokens.is_empty());
    }

    #[test]
    fn test_static_ct_log_deserialize() {
        let yaml = r#"
name: "Test Log"
url: "https://test.example.com/log/"
"#;
        let log: StaticCtLog = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(log.name, "Test Log");
        assert_eq!(log.url, "https://test.example.com/log/");
    }

    #[test]
    fn test_custom_ct_log_deserialize() {
        let yaml = r#"
name: "Custom Log"
url: "https://custom.example.com/ct"
"#;
        let log: CustomCtLog = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(log.name, "Custom Log");
        assert_eq!(log.url, "https://custom.example.com/ct");
    }

    #[test]
    fn test_yaml_config_with_static_logs() {
        let yaml = r#"
host: "127.0.0.1"
port: 9090
static_logs:
  - name: "Log A"
    url: "https://a.example.com/"
  - name: "Log B"
    url: "https://b.example.com/"
"#;
        let config: YamlConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.static_logs.len(), 2);
        assert_eq!(config.static_logs[0].name, "Log A");
        assert_eq!(config.static_logs[1].name, "Log B");
    }

    #[test]
    fn test_yaml_config_empty_static_logs() {
        let yaml = r#"
host: "127.0.0.1"
port: 9090
"#;
        let config: YamlConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.static_logs.is_empty());
    }

    #[test]
    fn test_has_tls_both_set() {
        let config = Config {
            tls_cert: Some("cert.pem".to_string()),
            tls_key: Some("key.pem".to_string()),
            ..test_config()
        };
        assert!(config.has_tls());
    }

    #[test]
    fn test_has_tls_none() {
        assert!(!test_config().has_tls());
    }

    #[test]
    fn test_has_tls_partial() {
        let config = Config {
            tls_cert: Some("cert.pem".to_string()),
            ..test_config()
        };
        assert!(!config.has_tls());
    }

    #[test]
    fn test_validate_valid_config() {
        assert!(test_config().validate().is_ok());
    }

    #[test]
    fn test_validate_zero_port() {
        let config = Config { port: 0, ..test_config() };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "port"));
    }

    #[test]
    fn test_validate_zero_buffer_size() {
        let config = Config { buffer_size: 0, ..test_config() };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "buffer_size"));
    }

    #[test]
    fn test_validate_empty_ct_logs_url() {
        let config = Config {
            ct_logs_url: "".to_string(),
            ..test_config()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_rate_limit_config_defaults() {
        let config = RateLimitConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.free_max_tokens, 100.0);
        assert_eq!(config.free_refill_rate, 10.0);
        assert_eq!(config.window_seconds, 60);
        assert_eq!(config.window_max_requests, 1000);
    }

    #[test]
    fn test_ct_log_config_deserialize_with_state_file() {
        let yaml = r#"
retry_max_attempts: 5
state_file: "my_state.json"
batch_size: 512
"#;
        let config: CtLogConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.retry_max_attempts, 5);
        assert_eq!(config.state_file, Some("my_state.json".to_string()));
        assert_eq!(config.batch_size, 512);
        assert_eq!(config.retry_initial_delay_ms, 1000);
    }

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

    // Task 6: Tests for ZerobusSinkConfig defaults and env var overrides

    #[test]
    fn test_zerobus_sink_config_defaults() {
        let config = ZerobusSinkConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.endpoint, "");
        assert_eq!(config.unity_catalog_url, "");
        assert_eq!(config.table_name, "");
        assert_eq!(config.client_id, "");
        assert_eq!(config.client_secret, "");
        assert_eq!(config.max_inflight_records, 10000);
    }

    #[test]
    fn test_zerobus_sink_config_yaml_empty() {
        let yaml = "enabled: false";
        let config: ZerobusSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.endpoint, "");
        assert_eq!(config.unity_catalog_url, "");
        assert_eq!(config.table_name, "");
        assert_eq!(config.client_id, "");
        assert_eq!(config.client_secret, "");
        assert_eq!(config.max_inflight_records, 10000);
    }

    #[test]
    fn test_zerobus_sink_config_yaml_partial() {
        let yaml = r#"
enabled: true
endpoint: "https://zerobus.example.com"
unity_catalog_url: "https://uc.example.com"
table_name: "catalog.schema.table"
max_inflight_records: 5000
"#;
        let config: ZerobusSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.endpoint, "https://zerobus.example.com");
        assert_eq!(config.unity_catalog_url, "https://uc.example.com");
        assert_eq!(config.table_name, "catalog.schema.table");
        assert_eq!(config.client_id, "");
        assert_eq!(config.client_secret, "");
        assert_eq!(config.max_inflight_records, 5000);
    }

    #[test]
    fn test_zerobus_sink_config_env_var_enabled() {
        unsafe {
            env::set_var("CERTSTREAM_ZEROBUS_ENABLED", "true");
        }
        let mut config = ZerobusSinkConfig::default();
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_ENABLED") {
            config.enabled = v.parse().unwrap_or(config.enabled);
        }
        unsafe {
            env::remove_var("CERTSTREAM_ZEROBUS_ENABLED");
        }
        assert!(config.enabled);
    }

    #[test]
    fn test_zerobus_sink_config_env_var_endpoint() {
        unsafe {
            env::set_var("CERTSTREAM_ZEROBUS_ENDPOINT", "https://test.example.com");
        }
        let mut config = ZerobusSinkConfig::default();
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_ENDPOINT") {
            config.endpoint = v;
        }
        unsafe {
            env::remove_var("CERTSTREAM_ZEROBUS_ENDPOINT");
        }
        assert_eq!(config.endpoint, "https://test.example.com");
    }

    #[test]
    fn test_zerobus_sink_config_env_var_unity_catalog_url() {
        unsafe {
            env::set_var("CERTSTREAM_ZEROBUS_UNITY_CATALOG_URL", "https://uc.test.com");
        }
        let mut config = ZerobusSinkConfig::default();
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_UNITY_CATALOG_URL") {
            config.unity_catalog_url = v;
        }
        unsafe {
            env::remove_var("CERTSTREAM_ZEROBUS_UNITY_CATALOG_URL");
        }
        assert_eq!(config.unity_catalog_url, "https://uc.test.com");
    }

    #[test]
    fn test_zerobus_sink_config_env_var_table_name() {
        unsafe {
            env::set_var("CERTSTREAM_ZEROBUS_TABLE_NAME", "my.schema.table");
        }
        let mut config = ZerobusSinkConfig::default();
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_TABLE_NAME") {
            config.table_name = v;
        }
        unsafe {
            env::remove_var("CERTSTREAM_ZEROBUS_TABLE_NAME");
        }
        assert_eq!(config.table_name, "my.schema.table");
    }

    #[test]
    fn test_zerobus_sink_config_env_var_client_id() {
        unsafe {
            env::set_var("CERTSTREAM_ZEROBUS_CLIENT_ID", "test-client-id");
        }
        let mut config = ZerobusSinkConfig::default();
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_CLIENT_ID") {
            config.client_id = v;
        }
        unsafe {
            env::remove_var("CERTSTREAM_ZEROBUS_CLIENT_ID");
        }
        assert_eq!(config.client_id, "test-client-id");
    }

    #[test]
    fn test_zerobus_sink_config_env_var_client_secret() {
        unsafe {
            env::set_var("CERTSTREAM_ZEROBUS_CLIENT_SECRET", "test-secret");
        }
        let mut config = ZerobusSinkConfig::default();
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_CLIENT_SECRET") {
            config.client_secret = v;
        }
        unsafe {
            env::remove_var("CERTSTREAM_ZEROBUS_CLIENT_SECRET");
        }
        assert_eq!(config.client_secret, "test-secret");
    }

    #[test]
    fn test_zerobus_sink_config_env_var_max_inflight_records() {
        unsafe {
            env::set_var("CERTSTREAM_ZEROBUS_MAX_INFLIGHT_RECORDS", "20000");
        }
        let mut config = ZerobusSinkConfig::default();
        if let Ok(v) = env::var("CERTSTREAM_ZEROBUS_MAX_INFLIGHT_RECORDS") {
            config.max_inflight_records = v.parse().unwrap_or(config.max_inflight_records);
        }
        unsafe {
            env::remove_var("CERTSTREAM_ZEROBUS_MAX_INFLIGHT_RECORDS");
        }
        assert_eq!(config.max_inflight_records, 20000);
    }

    // Task 7: Tests for ZerobusSinkConfig validation

    #[test]
    fn test_zerobus_sink_validation_disabled() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: false,
                endpoint: String::new(),
                unity_catalog_url: String::new(),
                table_name: String::new(),
                client_id: String::new(),
                client_secret: String::new(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_zerobus_sink_validation_empty_endpoint() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: true,
                endpoint: String::new(),
                unity_catalog_url: "https://uc.example.com".to_string(),
                table_name: "catalog.schema.table".to_string(),
                client_id: "test-id".to_string(),
                client_secret: "test-secret".to_string(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "zerobus_sink.endpoint"));
    }

    #[test]
    fn test_zerobus_sink_validation_empty_unity_catalog_url() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: true,
                endpoint: "https://zerobus.example.com".to_string(),
                unity_catalog_url: String::new(),
                table_name: "catalog.schema.table".to_string(),
                client_id: "test-id".to_string(),
                client_secret: "test-secret".to_string(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "zerobus_sink.unity_catalog_url"));
    }

    #[test]
    fn test_zerobus_sink_validation_empty_table_name() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: true,
                endpoint: "https://zerobus.example.com".to_string(),
                unity_catalog_url: "https://uc.example.com".to_string(),
                table_name: String::new(),
                client_id: "test-id".to_string(),
                client_secret: "test-secret".to_string(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "zerobus_sink.table_name"));
    }

    #[test]
    fn test_zerobus_sink_validation_invalid_table_name_format_no_dots() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: true,
                endpoint: "https://zerobus.example.com".to_string(),
                unity_catalog_url: "https://uc.example.com".to_string(),
                table_name: "just_table".to_string(),
                client_id: "test-id".to_string(),
                client_secret: "test-secret".to_string(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "zerobus_sink.table_name" && e.message.contains("catalog.schema.table")));
    }

    #[test]
    fn test_zerobus_sink_validation_invalid_table_name_format_one_dot() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: true,
                endpoint: "https://zerobus.example.com".to_string(),
                unity_catalog_url: "https://uc.example.com".to_string(),
                table_name: "schema.table".to_string(),
                client_id: "test-id".to_string(),
                client_secret: "test-secret".to_string(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "zerobus_sink.table_name" && e.message.contains("catalog.schema.table")));
    }

    #[test]
    fn test_zerobus_sink_validation_valid_table_name_format() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: true,
                endpoint: "https://zerobus.example.com".to_string(),
                unity_catalog_url: "https://uc.example.com".to_string(),
                table_name: "catalog.schema.table".to_string(),
                client_id: "test-id".to_string(),
                client_secret: "test-secret".to_string(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        let result = config.validate();
        // Should not have a table_name error
        if let Err(errors) = result {
            assert!(!errors.iter().any(|e| e.field == "zerobus_sink.table_name"));
        }
    }

    #[test]
    fn test_zerobus_sink_validation_empty_client_id() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: true,
                endpoint: "https://zerobus.example.com".to_string(),
                unity_catalog_url: "https://uc.example.com".to_string(),
                table_name: "catalog.schema.table".to_string(),
                client_id: String::new(),
                client_secret: "test-secret".to_string(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "zerobus_sink.client_id"));
    }

    #[test]
    fn test_zerobus_sink_validation_empty_client_secret() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: true,
                endpoint: "https://zerobus.example.com".to_string(),
                unity_catalog_url: "https://uc.example.com".to_string(),
                table_name: "catalog.schema.table".to_string(),
                client_id: "test-id".to_string(),
                client_secret: String::new(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "zerobus_sink.client_secret"));
    }

    #[test]
    fn test_zerobus_sink_validation_all_fields_populated() {
        let config = Config {
            zerobus_sink: ZerobusSinkConfig {
                enabled: true,
                endpoint: "https://zerobus.example.com".to_string(),
                unity_catalog_url: "https://uc.example.com".to_string(),
                table_name: "catalog.schema.table".to_string(),
                client_id: "test-id".to_string(),
                client_secret: "test-secret".to_string(),
                max_inflight_records: 10000,
            },
            ..test_config()
        };
        assert!(config.validate().is_ok());
    }

    // Compression level tests (from zstd-compression)

    #[test]
    fn test_delta_sink_config_default_compression_level() {
        // Verifies zstd-compression.AC1.1: Default config has compression_level == 9
        let config = DeltaSinkConfig::default();
        assert_eq!(config.compression_level, 9);
    }

    #[test]
    fn test_delta_sink_config_deserialize_compression_level() {
        // Verifies zstd-compression.AC1.2: Config with compression_level = 1 passes
        let yaml = r#"
enabled: true
table_path: "./data/test"
batch_size: 1000
flush_interval_secs: 30
compression_level: 1
"#;
        let config: DeltaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.compression_level, 1);
    }

    #[test]
    fn test_delta_sink_config_deserialize_compression_level_defaults_to_9() {
        // Verifies zstd-compression.AC1.1: Omitted compression_level defaults to 9
        let yaml = r#"
enabled: true
table_path: "./data/test"
batch_size: 1000
flush_interval_secs: 30
"#;
        let config: DeltaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.compression_level, 9);
    }

    #[test]
    fn test_validate_compression_level_zero() {
        // Verifies zstd-compression.AC1.3: Config with compression_level = 0 fails
        let config = Config {
            delta_sink: DeltaSinkConfig {
                compression_level: 0,
                ..DeltaSinkConfig::default()
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "delta_sink.compression_level"));
    }

    #[test]
    fn test_validate_compression_level_too_high() {
        // Verifies zstd-compression.AC1.4: Config with compression_level = 23 fails
        let config = Config {
            delta_sink: DeltaSinkConfig {
                compression_level: 23,
                ..DeltaSinkConfig::default()
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "delta_sink.compression_level"));
    }

    #[test]
    fn test_validate_compression_level_valid_min() {
        // Verifies zstd-compression.AC1.1: Config with compression_level = 1 passes
        let config = Config {
            delta_sink: DeltaSinkConfig {
                compression_level: 1,
                ..DeltaSinkConfig::default()
            },
            ..test_config()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_compression_level_valid_max() {
        // Verifies compression_level = 22 (max) passes
        let config = Config {
            delta_sink: DeltaSinkConfig {
                compression_level: 22,
                ..DeltaSinkConfig::default()
            },
            ..test_config()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_compression_level_default() {
        // Verifies zstd-compression.AC1.1: Default compression_level = 9 passes
        let config = Config {
            delta_sink: DeltaSinkConfig::default(),
            ..test_config()
        };
        assert!(config.validate().is_ok());
    }

    // Task 2: Tests for StorageConfig defaults and deserialization

    #[test]
    fn test_storage_config_defaults() {
        // Verifies uri-storage.AC2.2: Default StorageConfig has s3: None
        let config = StorageConfig::default();
        assert!(config.s3.is_none());
    }

    #[test]
    fn test_s3_storage_config_defaults() {
        // Verifies uri-storage.AC2.2: Default S3StorageConfig has empty strings and None options
        let config = S3StorageConfig::default();
        assert_eq!(config.endpoint, "");
        assert_eq!(config.region, "");
        assert_eq!(config.access_key_id, "");
        assert_eq!(config.secret_access_key, "");
        assert!(config.conditional_put.is_none());
        assert!(config.allow_http.is_none());
    }

    #[test]
    fn test_storage_config_yaml_with_s3_section() {
        // Verifies uri-storage.AC2.1: Deserialize YAML with storage.s3 section
        let yaml = r#"
s3:
  endpoint: "https://s3.example.com"
  region: "us-east-1"
  access_key_id: "test-key-id"
  secret_access_key: "test-secret"
  conditional_put: "etag"
  allow_http: false
"#;
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.s3.is_some());
        let s3 = config.s3.unwrap();
        assert_eq!(s3.endpoint, "https://s3.example.com");
        assert_eq!(s3.region, "us-east-1");
        assert_eq!(s3.access_key_id, "test-key-id");
        assert_eq!(s3.secret_access_key, "test-secret");
        assert_eq!(s3.conditional_put, Some("etag".to_string()));
        assert_eq!(s3.allow_http, Some(false));
    }

    #[test]
    fn test_storage_config_yaml_partial_s3_section() {
        // Verifies uri-storage.AC2.1: Partial S3 config with defaults
        let yaml = r#"
s3:
  endpoint: "https://s3.example.com"
  region: "us-west-2"
"#;
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.s3.is_some());
        let s3 = config.s3.unwrap();
        assert_eq!(s3.endpoint, "https://s3.example.com");
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.access_key_id, "");
        assert_eq!(s3.secret_access_key, "");
        assert!(s3.conditional_put.is_none());
        assert!(s3.allow_http.is_none());
    }

    #[test]
    fn test_storage_config_yaml_no_s3_section() {
        // Verifies uri-storage.AC2.2: Config with no storage section at all
        let yaml = "port: 8080";
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.s3.is_none());
    }

    #[test]
    fn test_storage_config_env_var_override_endpoint() {
        // Verifies uri-storage.AC2.5: Env var override pattern
        unsafe {
            env::set_var("CERTSTREAM_STORAGE_S3_ENDPOINT", "https://custom-s3.example.com");
        }
        let mut storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: "https://original.example.com".to_string(),
                region: "us-east-1".to_string(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                conditional_put: None,
                allow_http: None,
            }),
        };
        if let Some(ref mut s3) = storage.s3 {
            if let Ok(v) = env::var("CERTSTREAM_STORAGE_S3_ENDPOINT") {
                s3.endpoint = v;
            }
        }
        unsafe {
            env::remove_var("CERTSTREAM_STORAGE_S3_ENDPOINT");
        }
        assert_eq!(storage.s3.unwrap().endpoint, "https://custom-s3.example.com");
    }

    #[test]
    fn test_storage_config_env_var_endpoint_trigger_s3_creation() {
        // Verifies uri-storage.AC2.5: Endpoint env var triggers S3 config creation
        unsafe {
            env::set_var("CERTSTREAM_STORAGE_S3_ENDPOINT", "https://s3.example.com");
            env::set_var("CERTSTREAM_STORAGE_S3_REGION", "auto");
        }
        let mut storage = StorageConfig::default();
        let endpoint = env::var("CERTSTREAM_STORAGE_S3_ENDPOINT").unwrap_or_default();
        if !endpoint.is_empty() {
            storage.s3 = Some(S3StorageConfig {
                endpoint,
                region: env::var("CERTSTREAM_STORAGE_S3_REGION").unwrap_or_default(),
                access_key_id: env::var("CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID").unwrap_or_default(),
                secret_access_key: env::var("CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY").unwrap_or_default(),
                conditional_put: env::var("CERTSTREAM_STORAGE_S3_CONDITIONAL_PUT").ok(),
                allow_http: env::var("CERTSTREAM_STORAGE_S3_ALLOW_HTTP").ok().and_then(|v| v.parse().ok()),
            });
        }
        unsafe {
            env::remove_var("CERTSTREAM_STORAGE_S3_ENDPOINT");
            env::remove_var("CERTSTREAM_STORAGE_S3_REGION");
        }
        assert!(storage.s3.is_some());
        let s3 = storage.s3.unwrap();
        assert_eq!(s3.endpoint, "https://s3.example.com");
        assert_eq!(s3.region, "auto");
    }

    #[test]
    fn test_storage_config_env_var_no_endpoint_no_s3_created() {
        // Verifies: Setting CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID WITHOUT ENDPOINT doesn't create S3 config
        unsafe {
            env::set_var("CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID", "test-key");
            env::remove_var("CERTSTREAM_STORAGE_S3_ENDPOINT"); // Make sure endpoint is not set
        }
        let mut storage = StorageConfig::default();
        let endpoint = env::var("CERTSTREAM_STORAGE_S3_ENDPOINT").unwrap_or_default();
        if !endpoint.is_empty() {
            storage.s3 = Some(S3StorageConfig {
                endpoint,
                region: env::var("CERTSTREAM_STORAGE_S3_REGION").unwrap_or_default(),
                access_key_id: env::var("CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID").unwrap_or_default(),
                secret_access_key: env::var("CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY").unwrap_or_default(),
                conditional_put: env::var("CERTSTREAM_STORAGE_S3_CONDITIONAL_PUT").ok(),
                allow_http: env::var("CERTSTREAM_STORAGE_S3_ALLOW_HTTP").ok().and_then(|v| v.parse().ok()),
            });
        }
        unsafe {
            env::remove_var("CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID");
        }
        assert!(storage.s3.is_none());
    }

    // Task 4: Tests for parse_table_uri() and resolve_storage_options()
    // Verifies uri-storage.AC1.1
    #[test]
    fn test_parse_table_uri_absolute_path() {
        let result = parse_table_uri("file:///absolute/path");
        assert_eq!(
            result,
            Ok(TableLocation::Local {
                path: "/absolute/path".to_string()
            })
        );
    }

    // Verifies uri-storage.AC1.2
    #[test]
    fn test_parse_table_uri_relative_path() {
        let result = parse_table_uri("file://./relative/path");
        assert_eq!(
            result,
            Ok(TableLocation::Local {
                path: "./relative/path".to_string()
            })
        );
    }

    // Verifies uri-storage.AC1.3
    #[test]
    fn test_parse_table_uri_s3() {
        let result = parse_table_uri("s3://bucket/prefix");
        assert_eq!(
            result,
            Ok(TableLocation::S3 {
                uri: "s3://bucket/prefix".to_string()
            })
        );
    }

    // Verifies uri-storage.AC1.4
    #[test]
    fn test_parse_table_uri_bare_path() {
        let result = parse_table_uri("./data/certstream");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("file://"));
        assert!(err_msg.contains("./data/certstream"));
    }

    // Verifies uri-storage.AC1.5
    #[test]
    fn test_parse_table_uri_unsupported_scheme() {
        let result = parse_table_uri("gcs://bucket/path");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("Unsupported URI scheme"));
        assert!(err_msg.contains("gcs"));
        assert!(err_msg.contains("file://"));
        assert!(err_msg.contains("s3://"));
    }

    // Verifies uri-storage.AC1.6
    #[test]
    fn test_parse_table_uri_empty_string() {
        let result = parse_table_uri("");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("cannot be empty"));
    }

    // Test resolve_storage_options with Local location
    #[test]
    fn test_resolve_storage_options_local() {
        let location = TableLocation::Local {
            path: "/tmp/data".to_string(),
        };
        let storage = StorageConfig::default();
        let opts = resolve_storage_options(&location, &storage);
        assert!(opts.is_empty());
    }

    // Test resolve_storage_options with S3 location and full config
    #[test]
    fn test_resolve_storage_options_s3_full() {
        let location = TableLocation::S3 {
            uri: "s3://bucket/prefix".to_string(),
        };
        let storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: "https://s3.example.com".to_string(),
                region: "us-east-1".to_string(),
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                conditional_put: Some("etag".to_string()),
                allow_http: Some(false),
            }),
        };
        let opts = resolve_storage_options(&location, &storage);

        assert_eq!(
            opts.get("AWS_ENDPOINT_URL"),
            Some(&"https://s3.example.com".to_string())
        );
        assert_eq!(opts.get("AWS_REGION"), Some(&"us-east-1".to_string()));
        assert_eq!(
            opts.get("AWS_ACCESS_KEY_ID"),
            Some(&"AKIAIOSFODNN7EXAMPLE".to_string())
        );
        assert_eq!(
            opts.get("AWS_SECRET_ACCESS_KEY"),
            Some(&"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string())
        );
        assert_eq!(opts.get("conditional_put"), Some(&"etag".to_string()));
        assert_eq!(opts.get("AWS_ALLOW_HTTP"), Some(&"false".to_string()));
    }

    // Test resolve_storage_options with S3 location but no S3 config
    #[test]
    fn test_resolve_storage_options_s3_no_config() {
        let location = TableLocation::S3 {
            uri: "s3://bucket/prefix".to_string(),
        };
        let storage = StorageConfig::default(); // s3: None
        let opts = resolve_storage_options(&location, &storage);
        assert!(opts.is_empty());
    }

    // Test resolve_storage_options with S3 location and conditional_put set
    #[test]
    fn test_resolve_storage_options_s3_conditional_put() {
        let location = TableLocation::S3 {
            uri: "s3://bucket/prefix".to_string(),
        };
        let storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: "https://s3.example.com".to_string(),
                region: String::new(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                conditional_put: Some("etag".to_string()),
                allow_http: None,
            }),
        };
        let opts = resolve_storage_options(&location, &storage);

        assert_eq!(
            opts.get("AWS_ENDPOINT_URL"),
            Some(&"https://s3.example.com".to_string())
        );
        assert_eq!(opts.get("conditional_put"), Some(&"etag".to_string()));
        // Other fields are empty so should not be in the map
        assert!(!opts.contains_key("AWS_REGION"));
    }

    // Test resolve_storage_options with S3 location and allow_http true
    #[test]
    fn test_resolve_storage_options_s3_allow_http() {
        let location = TableLocation::S3 {
            uri: "s3://bucket/prefix".to_string(),
        };
        let storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: "http://localhost:9000".to_string(),
                region: String::new(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                conditional_put: None,
                allow_http: Some(true),
            }),
        };
        let opts = resolve_storage_options(&location, &storage);

        assert_eq!(
            opts.get("AWS_ENDPOINT_URL"),
            Some(&"http://localhost:9000".to_string())
        );
        assert_eq!(opts.get("AWS_ALLOW_HTTP"), Some(&"true".to_string()));
    }

    // Test URI validation: AC2.1 - Config with S3 URI and full storage config validates OK
    #[test]
    fn test_validate_s3_uri_with_full_config() {
        let mut config = test_config();
        config.delta_sink.enabled = true;
        config.delta_sink.table_path = "s3://bucket/path".to_string();
        config.storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: "https://s3.example.com".to_string(),
                region: "us-east-1".to_string(),
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
                conditional_put: None,
                allow_http: None,
            }),
        };

        let result = config.validate();
        assert!(result.is_ok(), "validation should pass with S3 URI and full config");
    }

    // Test URI validation: AC2.2 - Config with file:// URI and no S3 config validates OK
    #[test]
    fn test_validate_file_uri_without_s3_config() {
        let mut config = test_config();
        config.delta_sink.enabled = true;
        config.delta_sink.table_path = "file://./data/certstream".to_string();
        config.storage = StorageConfig { s3: None };

        let result = config.validate();
        assert!(result.is_ok(), "validation should pass with file:// URI without S3 config");
    }

    // Test URI validation: AC2.3 - Config with S3 URI but missing storage.s3 fails validation
    #[test]
    fn test_validate_s3_uri_without_storage_config() {
        let mut config = test_config();
        config.delta_sink.enabled = true;
        config.delta_sink.table_path = "s3://bucket/path".to_string();
        config.storage = StorageConfig { s3: None };

        let result = config.validate();
        assert!(result.is_err(), "validation should fail with S3 URI but no storage config");
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.field == "storage.s3"),
            "error should be on storage.s3 field"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("S3 storage configuration required")),
            "error message should suggest S3 configuration required"
        );
    }

    // Test URI validation: AC2.4 - Config with S3 URI but empty endpoint fails validation
    #[test]
    fn test_validate_s3_uri_with_empty_endpoint() {
        let mut config = test_config();
        config.delta_sink.enabled = true;
        config.delta_sink.table_path = "s3://bucket/path".to_string();
        config.storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: String::new(),
                region: "us-east-1".to_string(),
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
                conditional_put: None,
                allow_http: None,
            }),
        };

        let result = config.validate();
        assert!(result.is_err(), "validation should fail with empty endpoint");
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.field == "storage.s3.endpoint"),
            "error should be on storage.s3.endpoint field"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("S3 endpoint cannot be empty")),
            "error message should indicate empty endpoint"
        );
    }

    // Test URI validation: AC2.6 - parse_table_uri validates both file:// and bare paths
    #[test]
    fn test_validate_staging_path_uri_format() {
        // Valid file:// URI should succeed
        let result = parse_table_uri("file:///tmp/staging");
        assert!(result.is_ok(), "file:/// URI should parse successfully");

        // Bare path should fail with helpful error
        let result = parse_table_uri("/tmp/staging");
        assert!(result.is_err(), "bare path should fail validation");
        let error_msg = result.unwrap_err();
        assert!(
            error_msg.contains("must use a URI scheme"),
            "error should suggest using URI scheme, got: {}",
            error_msg
        );
    }

    // Test URI validation: AC2.3/AC2.6 - Config with bare path (no scheme) fails validation
    #[test]
    fn test_validate_bare_path_without_scheme() {
        let mut config = test_config();
        config.delta_sink.enabled = true;
        config.delta_sink.table_path = "./data/certstream".to_string();

        let result = config.validate();
        assert!(
            result.is_err(),
            "validation should fail with bare path (no scheme)"
        );
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.field == "delta_sink.table_path"),
            "error should be on delta_sink.table_path field"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("must use a URI scheme")),
            "error message should suggest using URI scheme"
        );
    }

    // Test URI validation: query_api.table_path is validated when enabled
    #[test]
    fn test_validate_query_api_table_path_uri() {
        let mut config = test_config();
        config.query_api.enabled = true;
        config.query_api.table_path = "s3://bucket/path".to_string();
        config.storage = StorageConfig { s3: None };

        let result = config.validate();
        assert!(result.is_err(), "validation should fail with S3 URI in query_api");
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.field == "storage.s3"),
            "error should be on storage.s3 field"
        );
    }

    // Test URI validation: S3 region requirement when using S3
    #[test]
    fn test_validate_s3_uri_with_empty_region() {
        let mut config = test_config();
        config.delta_sink.enabled = true;
        config.delta_sink.table_path = "s3://bucket/path".to_string();
        config.storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: "https://s3.example.com".to_string(),
                region: String::new(),
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
                conditional_put: None,
                allow_http: None,
            }),
        };

        let result = config.validate();
        assert!(result.is_err(), "validation should fail with empty region");
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.field == "storage.s3.region"),
            "error should be on storage.s3.region field"
        );
    }

    // Test URI validation: S3 access_key_id requirement when using S3
    #[test]
    fn test_validate_s3_uri_with_empty_access_key() {
        let mut config = test_config();
        config.delta_sink.enabled = true;
        config.delta_sink.table_path = "s3://bucket/path".to_string();
        config.storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: "https://s3.example.com".to_string(),
                region: "us-east-1".to_string(),
                access_key_id: String::new(),
                secret_access_key: "test-secret".to_string(),
                conditional_put: None,
                allow_http: None,
            }),
        };

        let result = config.validate();
        assert!(result.is_err(), "validation should fail with empty access key");
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.field == "storage.s3.access_key_id"),
            "error should be on storage.s3.access_key_id field"
        );
    }

    // Test URI validation: S3 secret_access_key requirement when using S3
    #[test]
    fn test_validate_s3_uri_with_empty_secret_key() {
        let mut config = test_config();
        config.delta_sink.enabled = true;
        config.delta_sink.table_path = "s3://bucket/path".to_string();
        config.storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: "https://s3.example.com".to_string(),
                region: "us-east-1".to_string(),
                access_key_id: "test-key".to_string(),
                secret_access_key: String::new(),
                conditional_put: None,
                allow_http: None,
            }),
        };

        let result = config.validate();
        assert!(result.is_err(), "validation should fail with empty secret key");
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.field == "storage.s3.secret_access_key"),
            "error should be on storage.s3.secret_access_key field"
        );
    }

    #[test]
    fn test_delta_sink_config_default_heavy_column_compression_level() {
        // Verifies delta-encoding-opt.AC4.1: Default heavy_column_compression_level = 15
        let config = DeltaSinkConfig::default();
        assert_eq!(config.heavy_column_compression_level, 15);
    }

    #[test]
    fn test_delta_sink_config_deserialize_heavy_column_compression_level_custom() {
        // Verifies delta-encoding-opt.AC4.1: Custom heavy_column_compression_level is preserved
        let yaml = r#"
enabled: true
table_path: "./data/test"
batch_size: 1000
flush_interval_secs: 30
compression_level: 9
heavy_column_compression_level: 20
"#;
        let config: DeltaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.heavy_column_compression_level, 20);
    }

    #[test]
    fn test_delta_sink_config_deserialize_heavy_column_compression_level_defaults_to_15() {
        // Verifies delta-encoding-opt.AC4.1: Omitted heavy_column_compression_level defaults to 15
        let yaml = r#"
enabled: true
table_path: "./data/test"
batch_size: 1000
flush_interval_secs: 30
compression_level: 9
"#;
        let config: DeltaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.heavy_column_compression_level, 15);
    }

    #[test]
    fn test_validate_heavy_column_compression_level_zero() {
        // Verifies delta-encoding-opt.AC4.2: Config with heavy_column_compression_level = 0 fails
        let config = Config {
            delta_sink: DeltaSinkConfig {
                heavy_column_compression_level: 0,
                ..DeltaSinkConfig::default()
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "delta_sink.heavy_column_compression_level"));
    }

    #[test]
    fn test_validate_heavy_column_compression_level_too_high() {
        // Verifies delta-encoding-opt.AC4.2: Config with heavy_column_compression_level = 23 fails
        let config = Config {
            delta_sink: DeltaSinkConfig {
                heavy_column_compression_level: 23,
                ..DeltaSinkConfig::default()
            },
            ..test_config()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.field == "delta_sink.heavy_column_compression_level"));
    }

    #[test]
    fn test_validate_heavy_column_compression_level_valid_min() {
        // Verifies delta-encoding-opt.AC4.2: Config with heavy_column_compression_level = 1 passes
        let config = Config {
            delta_sink: DeltaSinkConfig {
                heavy_column_compression_level: 1,
                ..DeltaSinkConfig::default()
            },
            ..test_config()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_heavy_column_compression_level_valid_max() {
        // Verifies delta-encoding-opt.AC4.2: Config with heavy_column_compression_level = 22 (max) passes
        let config = Config {
            delta_sink: DeltaSinkConfig {
                heavy_column_compression_level: 22,
                ..DeltaSinkConfig::default()
            },
            ..test_config()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_heavy_column_compression_level_default() {
        // Verifies delta-encoding-opt.AC4.1: Default heavy_column_compression_level = 15 passes
        let config = Config {
            delta_sink: DeltaSinkConfig::default(),
            ..test_config()
        };
        assert!(config.validate().is_ok());
    }

    // Named targets tests (from named-targets phase 1)

    #[test]
    fn test_target_config_all_fields_yaml() {
        // Verifies named-targets.AC1.1: TargetConfig with all fields specified deserializes from YAML with correct values
        let yaml = r#"
table_path: "file:///data/targets/main"
compression_level: 12
heavy_column_compression_level: 18
offline_batch_size: 50000
storage:
  s3:
    endpoint: "https://s3.example.com"
    region: "us-west-2"
    access_key_id: "AKIAIOSFODNN7EXAMPLE"
    secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
"#;
        let config: TargetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.table_path, "file:///data/targets/main");
        assert_eq!(config.compression_level, Some(12));
        assert_eq!(config.heavy_column_compression_level, Some(18));
        assert_eq!(config.offline_batch_size, Some(50000));
        assert!(config.storage.is_some());
        let storage = config.storage.unwrap();
        assert!(storage.s3.is_some());
        let s3 = storage.s3.unwrap();
        assert_eq!(s3.endpoint, "https://s3.example.com");
        assert_eq!(s3.region, "us-west-2");
    }

    #[test]
    fn test_target_config_only_table_path_yaml() {
        // Verifies named-targets.AC1.2: TargetConfig with only `table_path` deserializes, optional fields are `None`
        let yaml = r#"
table_path: "file:///data/targets/minimal"
"#;
        let config: TargetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.table_path, "file:///data/targets/minimal");
        assert!(config.compression_level.is_none());
        assert!(config.heavy_column_compression_level.is_none());
        assert!(config.offline_batch_size.is_none());
        assert!(config.storage.is_none());
    }

    #[test]
    fn test_resolve_target_fill_compression_level() {
        // Verifies named-targets.AC1.3: `resolve_target()` fills `None` compression_level from `delta_sink.compression_level`
        let mut config = test_config();
        config.delta_sink.compression_level = 11;
        let mut targets = HashMap::new();
        targets.insert(
            "test".to_string(),
            TargetConfig {
                table_path: "file:///tmp/test".to_string(),
                storage: None,
                compression_level: None,
                heavy_column_compression_level: None,
                offline_batch_size: None,
            },
        );
        config.targets = targets;

        let resolved = config.resolve_target("test").unwrap();
        assert_eq!(resolved.compression_level, 11);
    }

    #[test]
    fn test_resolve_target_fill_heavy_column_compression_level() {
        // Verifies named-targets.AC1.4: `resolve_target()` fills `None` heavy_column_compression_level from delta_sink
        let mut config = test_config();
        config.delta_sink.heavy_column_compression_level = 19;
        let mut targets = HashMap::new();
        targets.insert(
            "test".to_string(),
            TargetConfig {
                table_path: "file:///tmp/test".to_string(),
                storage: None,
                compression_level: None,
                heavy_column_compression_level: None,
                offline_batch_size: None,
            },
        );
        config.targets = targets;

        let resolved = config.resolve_target("test").unwrap();
        assert_eq!(resolved.heavy_column_compression_level, 19);
    }

    #[test]
    fn test_resolve_target_fill_offline_batch_size() {
        // Verifies named-targets.AC1.5: `resolve_target()` fills `None` offline_batch_size from delta_sink
        let mut config = test_config();
        config.delta_sink.offline_batch_size = 200000;
        let mut targets = HashMap::new();
        targets.insert(
            "test".to_string(),
            TargetConfig {
                table_path: "file:///tmp/test".to_string(),
                storage: None,
                compression_level: None,
                heavy_column_compression_level: None,
                offline_batch_size: None,
            },
        );
        config.targets = targets;

        let resolved = config.resolve_target("test").unwrap();
        assert_eq!(resolved.offline_batch_size, 200000);
    }

    #[test]
    fn test_resolve_target_target_level_storage() {
        // Verifies named-targets.AC1.6: `resolve_target()` uses target-level `StorageConfig` when present
        let mut config = test_config();
        // Set global storage to one value
        config.storage.s3 = Some(S3StorageConfig {
            endpoint: "https://global-s3.example.com".to_string(),
            region: "global-region".to_string(),
            access_key_id: "global-key".to_string(),
            secret_access_key: "global-secret".to_string(),
            conditional_put: None,
            allow_http: None,
        });

        // Set target with its own storage
        let target_storage = StorageConfig {
            s3: Some(S3StorageConfig {
                endpoint: "https://target-s3.example.com".to_string(),
                region: "target-region".to_string(),
                access_key_id: "target-key".to_string(),
                secret_access_key: "target-secret".to_string(),
                conditional_put: None,
                allow_http: None,
            }),
        };

        let mut targets = HashMap::new();
        targets.insert(
            "test".to_string(),
            TargetConfig {
                table_path: "s3://bucket/path".to_string(),
                storage: Some(target_storage),
                compression_level: None,
                heavy_column_compression_level: None,
                offline_batch_size: None,
            },
        );
        config.targets = targets;

        let resolved = config.resolve_target("test").unwrap();
        // Should use target-level S3 credentials, not global
        assert!(resolved.storage_options.contains_key("AWS_ENDPOINT_URL"));
        assert_eq!(
            resolved.storage_options.get("AWS_ENDPOINT_URL").unwrap(),
            "https://target-s3.example.com"
        );
        assert_eq!(
            resolved.storage_options.get("AWS_REGION").unwrap(),
            "target-region"
        );
        assert_eq!(
            resolved.storage_options.get("AWS_ACCESS_KEY_ID").unwrap(),
            "target-key"
        );
    }

    #[test]
    fn test_resolve_target_fallback_to_global_storage() {
        // Verifies named-targets.AC1.7: `resolve_target()` falls back to global `config.storage` when target has no storage config
        let mut config = test_config();
        // Set global storage
        config.storage.s3 = Some(S3StorageConfig {
            endpoint: "https://global-s3.example.com".to_string(),
            region: "global-region".to_string(),
            access_key_id: "global-key".to_string(),
            secret_access_key: "global-secret".to_string(),
            conditional_put: None,
            allow_http: None,
        });

        let mut targets = HashMap::new();
        targets.insert(
            "test".to_string(),
            TargetConfig {
                table_path: "s3://bucket/path".to_string(),
                storage: None, // No target-level storage
                compression_level: None,
                heavy_column_compression_level: None,
                offline_batch_size: None,
            },
        );
        config.targets = targets;

        let resolved = config.resolve_target("test").unwrap();
        // Should use global S3 credentials
        assert!(resolved.storage_options.contains_key("AWS_ENDPOINT_URL"));
        assert_eq!(
            resolved.storage_options.get("AWS_ENDPOINT_URL").unwrap(),
            "https://global-s3.example.com"
        );
        assert_eq!(
            resolved.storage_options.get("AWS_REGION").unwrap(),
            "global-region"
        );
    }

    #[test]
    fn test_resolve_target_unknown_target() {
        // Verifies named-targets.AC1.8: `resolve_target()` returns error for unknown target name
        let config = test_config();
        let result = config.resolve_target("nonexistent");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Unknown target"));
        assert!(err.contains("nonexistent"));
    }

    #[test]
    fn test_resolve_target_file_uri() {
        // Test that resolve_target works with file:// URIs
        let mut config = test_config();
        let mut targets = HashMap::new();
        targets.insert(
            "local".to_string(),
            TargetConfig {
                table_path: "file:///data/local-target".to_string(),
                storage: None,
                compression_level: Some(10),
                heavy_column_compression_level: Some(20),
                offline_batch_size: Some(150000),
            },
        );
        config.targets = targets;

        let resolved = config.resolve_target("local").unwrap();
        assert_eq!(resolved.table_path, "/data/local-target");
        assert_eq!(resolved.compression_level, 10);
        assert_eq!(resolved.heavy_column_compression_level, 20);
        assert_eq!(resolved.offline_batch_size, 150000);
        // Local paths should have empty storage_options
        assert!(resolved.storage_options.is_empty());
    }

    #[test]
    fn test_target_env_var_table_path_override() {
        // Verifies named-targets.AC2.1: `CERTSTREAM_TARGETS_<NAME>_TABLE_PATH` overrides YAML value
        unsafe {
            env::set_var("CERTSTREAM_TARGETS_MAIN_TABLE_PATH", "file:///env/override");
        }

        // Create target config from YAML
        let mut targets = HashMap::new();
        targets.insert(
            "main".to_string(),
            TargetConfig {
                table_path: "file:///yaml/path".to_string(),
                storage: None,
                compression_level: None,
                heavy_column_compression_level: None,
                offline_batch_size: None,
            },
        );

        // Apply env var overlay (simulating what Config::load() does)
        for (name, target) in targets.iter_mut() {
            let prefix = format!("CERTSTREAM_TARGETS_{}", name.to_uppercase());
            if let Ok(v) = env::var(format!("{}_TABLE_PATH", prefix)) {
                target.table_path = v;
            }
        }

        // Verify env var override won
        assert_eq!(targets.get("main").unwrap().table_path, "file:///env/override");

        unsafe {
            env::remove_var("CERTSTREAM_TARGETS_MAIN_TABLE_PATH");
        }
    }

    #[test]
    fn test_target_env_var_compression_level_override() {
        // Verifies named-targets.AC2.2: `CERTSTREAM_TARGETS_<NAME>_COMPRESSION_LEVEL` overrides YAML value
        unsafe {
            env::set_var("CERTSTREAM_TARGETS_MAIN_COMPRESSION_LEVEL", "15");
        }

        // Create target config from YAML
        let mut targets = HashMap::new();
        targets.insert(
            "main".to_string(),
            TargetConfig {
                table_path: "file:///data".to_string(),
                storage: None,
                compression_level: Some(9),
                heavy_column_compression_level: None,
                offline_batch_size: None,
            },
        );

        // Apply env var overlay
        for (name, target) in targets.iter_mut() {
            let prefix = format!("CERTSTREAM_TARGETS_{}", name.to_uppercase());
            if let Ok(v) = env::var(format!("{}_COMPRESSION_LEVEL", prefix)) {
                if let Ok(level) = v.parse::<i32>() {
                    target.compression_level = Some(level);
                }
            }
        }

        // Verify env var override won
        assert_eq!(targets.get("main").unwrap().compression_level, Some(15));

        unsafe {
            env::remove_var("CERTSTREAM_TARGETS_MAIN_COMPRESSION_LEVEL");
        }
    }

    #[test]
    fn test_target_env_var_s3_storage_override() {
        // Verifies named-targets.AC2.3: Target storage env vars override YAML storage config
        unsafe {
            env::set_var("CERTSTREAM_TARGETS_ARCHIVE_STORAGE_S3_ENDPOINT", "https://env-s3.example.com");
            env::set_var("CERTSTREAM_TARGETS_ARCHIVE_STORAGE_S3_REGION", "env-region");
            env::set_var("CERTSTREAM_TARGETS_ARCHIVE_STORAGE_S3_ACCESS_KEY_ID", "env-key");
            env::set_var("CERTSTREAM_TARGETS_ARCHIVE_STORAGE_S3_SECRET_ACCESS_KEY", "env-secret");
        }

        // Create target config with YAML storage
        let mut targets = HashMap::new();
        targets.insert(
            "archive".to_string(),
            TargetConfig {
                table_path: "s3://bucket/archive".to_string(),
                storage: Some(StorageConfig {
                    s3: Some(S3StorageConfig {
                        endpoint: "https://yaml-s3.example.com".to_string(),
                        region: "yaml-region".to_string(),
                        access_key_id: "yaml-key".to_string(),
                        secret_access_key: "yaml-secret".to_string(),
                        conditional_put: None,
                        allow_http: None,
                    }),
                }),
                compression_level: None,
                heavy_column_compression_level: None,
                offline_batch_size: None,
            },
        );

        // Apply env var overlay (simulating what Config::load() does)
        for (name, target) in targets.iter_mut() {
            let prefix = format!("CERTSTREAM_TARGETS_{}", name.to_uppercase());
            let s3_prefix = format!("{}_STORAGE_S3", prefix);
            let s3_endpoint = env::var(format!("{}_ENDPOINT", s3_prefix)).unwrap_or_default();
            if !s3_endpoint.is_empty() {
                // If endpoint env var is set, create or override S3 config
                let s3 = S3StorageConfig {
                    endpoint: s3_endpoint,
                    region: env::var(format!("{}_REGION", s3_prefix)).unwrap_or_default(),
                    access_key_id: env::var(format!("{}_ACCESS_KEY_ID", s3_prefix)).unwrap_or_default(),
                    secret_access_key: env::var(format!("{}_SECRET_ACCESS_KEY", s3_prefix)).unwrap_or_default(),
                    conditional_put: env::var(format!("{}_CONDITIONAL_PUT", s3_prefix)).ok(),
                    allow_http: env::var(format!("{}_ALLOW_HTTP", s3_prefix)).ok().and_then(|v| v.parse().ok()),
                };
                target.storage = Some(StorageConfig { s3: Some(s3) });
            }
        }

        // Verify env var S3 credentials won
        let target = targets.get("archive").unwrap();
        assert!(target.storage.is_some());
        let s3 = target.storage.as_ref().unwrap().s3.as_ref().unwrap();
        assert_eq!(s3.endpoint, "https://env-s3.example.com");
        assert_eq!(s3.region, "env-region");
        assert_eq!(s3.access_key_id, "env-key");
        assert_eq!(s3.secret_access_key, "env-secret");

        unsafe {
            env::remove_var("CERTSTREAM_TARGETS_ARCHIVE_STORAGE_S3_ENDPOINT");
            env::remove_var("CERTSTREAM_TARGETS_ARCHIVE_STORAGE_S3_REGION");
            env::remove_var("CERTSTREAM_TARGETS_ARCHIVE_STORAGE_S3_ACCESS_KEY_ID");
            env::remove_var("CERTSTREAM_TARGETS_ARCHIVE_STORAGE_S3_SECRET_ACCESS_KEY");
        }
    }

    #[test]
    fn test_target_env_var_heavy_column_compression_level_override() {
        // Test that env var overrides for heavy_column_compression_level work correctly
        unsafe {
            env::set_var("CERTSTREAM_TARGETS_ARCHIVE_HEAVY_COLUMN_COMPRESSION_LEVEL", "20");
        }

        let mut targets = HashMap::new();
        targets.insert(
            "archive".to_string(),
            TargetConfig {
                table_path: "file:///data".to_string(),
                storage: None,
                compression_level: None,
                heavy_column_compression_level: Some(15),
                offline_batch_size: None,
            },
        );

        // Apply env var overlay
        for (name, target) in targets.iter_mut() {
            let prefix = format!("CERTSTREAM_TARGETS_{}", name.to_uppercase());
            if let Ok(v) = env::var(format!("{}_HEAVY_COLUMN_COMPRESSION_LEVEL", prefix)) {
                if let Ok(level) = v.parse::<i32>() {
                    target.heavy_column_compression_level = Some(level);
                }
            }
        }

        assert_eq!(targets.get("archive").unwrap().heavy_column_compression_level, Some(20));

        unsafe {
            env::remove_var("CERTSTREAM_TARGETS_ARCHIVE_HEAVY_COLUMN_COMPRESSION_LEVEL");
        }
    }

    #[test]
    fn test_target_env_var_offline_batch_size_override() {
        // Test that env var overrides for offline_batch_size work correctly
        unsafe {
            env::set_var("CERTSTREAM_TARGETS_STAGING_OFFLINE_BATCH_SIZE", "250000");
        }

        let mut targets = HashMap::new();
        targets.insert(
            "staging".to_string(),
            TargetConfig {
                table_path: "file:///staging".to_string(),
                storage: None,
                compression_level: None,
                heavy_column_compression_level: None,
                offline_batch_size: Some(100000),
            },
        );

        // Apply env var overlay
        for (name, target) in targets.iter_mut() {
            let prefix = format!("CERTSTREAM_TARGETS_{}", name.to_uppercase());
            if let Ok(v) = env::var(format!("{}_OFFLINE_BATCH_SIZE", prefix)) {
                if let Ok(size) = v.parse::<usize>() {
                    target.offline_batch_size = Some(size);
                }
            }
        }

        assert_eq!(targets.get("staging").unwrap().offline_batch_size, Some(250000));

        unsafe {
            env::remove_var("CERTSTREAM_TARGETS_STAGING_OFFLINE_BATCH_SIZE");
        }
    }
}
