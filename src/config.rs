use serde::Deserialize;
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
}

impl Default for DeltaSinkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            table_path: default_delta_sink_table_path(),
            batch_size: default_delta_sink_batch_size(),
            flush_interval_secs: default_delta_sink_flush_interval_secs(),
            compression_level: default_delta_sink_compression_level(),
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

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
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
}
