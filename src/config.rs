use serde::Deserialize;
use std::env;
use std::fs;
use std::net::IpAddr;
use std::path::Path;

use crate::backpressure::DropPolicy;

#[derive(Debug, Clone, Deserialize)]
pub struct CustomCtLog {
    pub name: String,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProtocolConfig {
    #[serde(default = "default_true")]
    pub websocket: bool,
    #[serde(default)]
    pub sse: bool,
    #[serde(default)]
    pub tcp: bool,
    #[serde(default)]
    pub tcp_port: Option<u16>,
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
            tcp: false,
            tcp_port: None,
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
    #[serde(default)]
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
            state_file: None,
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
pub struct BackpressureConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
    #[serde(default = "default_slow_consumer_threshold")]
    pub slow_consumer_threshold: usize,
    #[serde(default = "default_drop_policy")]
    pub drop_policy: String,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            buffer_size: default_buffer_size(),
            slow_consumer_threshold: default_slow_consumer_threshold(),
            drop_policy: default_drop_policy(),
        }
    }
}

impl BackpressureConfig {
    pub fn to_backpressure_config(&self) -> crate::backpressure::BackpressureConfig {
        crate::backpressure::BackpressureConfig {
            buffer_size: self.buffer_size,
            slow_consumer_threshold: self.slow_consumer_threshold,
            drop_policy: match self.drop_policy.as_str() {
                "drop_newest" => DropPolicy::DropNewest,
                "disconnect" => DropPolicy::Disconnect,
                _ => DropPolicy::DropOldest,
            },
        }
    }
}

fn default_buffer_size() -> usize {
    1024
}
fn default_slow_consumer_threshold() -> usize {
    512
}
fn default_drop_policy() -> String {
    "drop_oldest".to_string()
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

#[derive(Debug, Clone, Deserialize)]
pub struct HotReloadConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub watch_path: Option<String>,
}

impl Default for HotReloadConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            watch_path: None,
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
    pub protocols: ProtocolConfig,
    pub ct_log: CtLogConfig,
    pub connection_limit: ConnectionLimitConfig,
    pub rate_limit: RateLimitConfig,
    pub backpressure: BackpressureConfig,
    pub api: ApiConfig,
    pub auth: AuthConfig,
    pub hot_reload: HotReloadConfig,
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
    protocols: Option<ProtocolConfig>,
    #[serde(default)]
    ct_log: Option<CtLogConfig>,
    #[serde(default)]
    connection_limit: Option<ConnectionLimitConfig>,
    #[serde(default)]
    rate_limit: Option<RateLimitConfig>,
    #[serde(default)]
    backpressure: Option<BackpressureConfig>,
    #[serde(default)]
    api: Option<ApiConfig>,
    #[serde(default)]
    auth: Option<AuthConfig>,
    #[serde(default)]
    hot_reload: Option<HotReloadConfig>,
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

        let protocols = yaml_config.protocols.unwrap_or_else(|| {
            let ws = env::var("CERTSTREAM_WS_ENABLED")
                .map(|v| v.parse().unwrap_or(true))
                .unwrap_or(true);
            let sse = env::var("CERTSTREAM_SSE_ENABLED")
                .map(|v| v.parse().unwrap_or(false))
                .unwrap_or(false);
            let tcp = env::var("CERTSTREAM_TCP_ENABLED")
                .map(|v| v.parse().unwrap_or(false))
                .unwrap_or(false);
            let tcp_port = env::var("CERTSTREAM_TCP_PORT")
                .ok()
                .and_then(|v| v.parse().ok());
            let metrics = env::var("CERTSTREAM_METRICS_ENABLED")
                .map(|v| v.parse().unwrap_or(true))
                .unwrap_or(true);
            let health = env::var("CERTSTREAM_HEALTH_ENABLED")
                .map(|v| v.parse().unwrap_or(true))
                .unwrap_or(true);
            let example_json = env::var("CERTSTREAM_EXAMPLE_JSON_ENABLED")
                .map(|v| v.parse().unwrap_or(true))
                .unwrap_or(true);
            let api = env::var("CERTSTREAM_API_ENABLED")
                .map(|v| v.parse().unwrap_or(false))
                .unwrap_or(false);

            ProtocolConfig {
                websocket: ws,
                sse,
                tcp,
                tcp_port,
                metrics,
                health,
                example_json,
                api,
            }
        });

        let ct_log = yaml_config.ct_log.unwrap_or_else(|| {
            let mut cfg = CtLogConfig::default();
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_RETRY_MAX_ATTEMPTS") {
                cfg.retry_max_attempts = v.parse().unwrap_or(cfg.retry_max_attempts);
            }
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_RETRY_INITIAL_DELAY_MS") {
                cfg.retry_initial_delay_ms = v.parse().unwrap_or(cfg.retry_initial_delay_ms);
            }
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_RETRY_MAX_DELAY_MS") {
                cfg.retry_max_delay_ms = v.parse().unwrap_or(cfg.retry_max_delay_ms);
            }
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_REQUEST_TIMEOUT_SECS") {
                cfg.request_timeout_secs = v.parse().unwrap_or(cfg.request_timeout_secs);
            }
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_UNHEALTHY_THRESHOLD") {
                cfg.unhealthy_threshold = v.parse().unwrap_or(cfg.unhealthy_threshold);
            }
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_HEALTHY_THRESHOLD") {
                cfg.healthy_threshold = v.parse().unwrap_or(cfg.healthy_threshold);
            }
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_HEALTH_CHECK_INTERVAL_SECS") {
                cfg.health_check_interval_secs = v.parse().unwrap_or(cfg.health_check_interval_secs);
            }
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_STATE_FILE") {
                cfg.state_file = Some(v);
            }
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_BATCH_SIZE") {
                cfg.batch_size = v.parse().unwrap_or(cfg.batch_size);
            }
            if let Ok(v) = env::var("CERTSTREAM_CT_LOG_POLL_INTERVAL_MS") {
                cfg.poll_interval_ms = v.parse().unwrap_or(cfg.poll_interval_ms);
            }
            cfg
        });

        let connection_limit = yaml_config.connection_limit.unwrap_or_else(|| {
            let mut cfg = ConnectionLimitConfig::default();
            if let Ok(v) = env::var("CERTSTREAM_CONNECTION_LIMIT_ENABLED") {
                cfg.enabled = v.parse().unwrap_or(false);
            }
            if let Ok(v) = env::var("CERTSTREAM_CONNECTION_LIMIT_MAX_CONNECTIONS") {
                cfg.max_connections = v.parse().unwrap_or(cfg.max_connections);
            }
            if let Ok(v) = env::var("CERTSTREAM_CONNECTION_LIMIT_PER_IP_LIMIT") {
                cfg.per_ip_limit = v.parse().ok();
            }
            cfg
        });

        let rate_limit = yaml_config.rate_limit.unwrap_or_else(|| {
            let mut cfg = RateLimitConfig::default();
            if let Ok(v) = env::var("CERTSTREAM_RATE_LIMIT_ENABLED") {
                cfg.enabled = v.parse().unwrap_or(false);
            }
            cfg
        });

        let backpressure = yaml_config.backpressure.unwrap_or_else(|| {
            let mut cfg = BackpressureConfig::default();
            if let Ok(v) = env::var("CERTSTREAM_BACKPRESSURE_ENABLED") {
                cfg.enabled = v.parse().unwrap_or(false);
            }
            cfg
        });

        let api = yaml_config.api.unwrap_or_default();

        let auth = yaml_config.auth.unwrap_or_else(|| {
            let mut cfg = AuthConfig::default();
            if let Ok(v) = env::var("CERTSTREAM_AUTH_ENABLED") {
                cfg.enabled = v.parse().unwrap_or(false);
            }
            if let Ok(v) = env::var("CERTSTREAM_AUTH_TOKENS") {
                cfg.tokens = v.split(',').map(|s| s.trim().to_string()).collect();
            }
            if let Ok(v) = env::var("CERTSTREAM_AUTH_HEADER_NAME") {
                cfg.header_name = v;
            }
            cfg
        });

        let hot_reload = yaml_config.hot_reload.unwrap_or_else(|| {
            let mut cfg = HotReloadConfig::default();
            if let Ok(v) = env::var("CERTSTREAM_HOT_RELOAD_ENABLED") {
                cfg.enabled = v.parse().unwrap_or(false);
            }
            cfg
        });

        Self {
            host,
            port,
            log_level,
            buffer_size,
            ct_logs_url,
            tls_cert,
            tls_key,
            custom_logs: yaml_config.custom_logs,
            protocols,
            ct_log,
            connection_limit,
            rate_limit,
            backpressure,
            api,
            auth,
            hot_reload,
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

        if self.rate_limit.enabled {
            if self.rate_limit.free_refill_rate <= 0.0 {
                errors.push(ConfigValidationError {
                    field: "rate_limit.free_refill_rate".to_string(),
                    message: "Refill rate must be positive".to_string(),
                });
            }
        }

        if self.backpressure.enabled {
            if self.backpressure.buffer_size == 0 {
                errors.push(ConfigValidationError {
                    field: "backpressure.buffer_size".to_string(),
                    message: "Buffer size must be greater than 0".to_string(),
                });
            }
            if self.backpressure.slow_consumer_threshold > self.backpressure.buffer_size {
                errors.push(ConfigValidationError {
                    field: "backpressure.slow_consumer_threshold".to_string(),
                    message: "Slow consumer threshold cannot exceed buffer size".to_string(),
                });
            }
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
                    if let Ok(config) = serde_yaml::from_str::<YamlConfig>(&content) {
                        return YamlConfigWithPath {
                            config,
                            path: Some(path),
                        };
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
