mod api;
mod backpressure;
mod cli;
mod config;
mod ct;
mod hot_reload;
mod middleware;
mod models;
mod rate_limit;
mod sse;
mod state;
mod tcp;
mod websocket;

use axum::{middleware as axum_middleware, routing::get, Json, Router};
use metrics_exporter_prometheus::PrometheusBuilder;
use reqwest::Client;
use smallvec::smallvec;
use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use api::{handle_cert, handle_logs, handle_stats, ApiState, CachedCert, CertificateCache, LogTracker, ServerStats};
use cli::{CliArgs, VERSION};
use config::Config;
use ct::fetch_log_list;
use hot_reload::{ConfigWatcher, HotReloadableConfig};
use middleware::{auth_middleware, rate_limit_middleware, AuthMiddleware, ConnectionLimiter};
use models::{CertificateData, CertificateMessage, ChainCert, Extensions, LeafCert, PreSerializedMessage, Source, Subject};
use rate_limit::{RateLimiter, TierTokens};
use sse::handle_sse_stream;
use state::StateManager;
use websocket::{handle_domains_only, handle_full_stream, handle_lite_stream, AppState, ConnectionCounter};

async fn health() -> &'static str {
    "OK"
}

async fn example_json() -> Json<CertificateMessage> {
    let mut subject = Subject {
        cn: Some("example.com".to_string()),
        o: Some("Example Organization".to_string()),
        c: Some("US".to_string()),
        ..Default::default()
    };
    subject.build_aggregated();

    let mut issuer = Subject {
        cn: Some("Example CA".to_string()),
        o: Some("Example Certificate Authority".to_string()),
        c: Some("US".to_string()),
        ..Default::default()
    };
    issuer.build_aggregated();

    let mut chain_issuer = Subject {
        cn: Some("Root CA".to_string()),
        o: Some("Example Root Authority".to_string()),
        ..Default::default()
    };
    chain_issuer.build_aggregated();

    let extensions = Extensions {
        key_usage: Some("Digital Signature, Key Encipherment".to_string()),
        extended_key_usage: Some("serverAuth, clientAuth".to_string()),
        basic_constraints: Some("CA:FALSE".to_string()),
        subject_alt_name: Some("DNS:example.com, DNS:www.example.com, DNS:*.example.com".to_string()),
        ..Default::default()
    };

    let chain_extensions = Extensions {
        key_usage: Some("Certificate Signing, CRL Signing".to_string()),
        basic_constraints: Some("CA:TRUE".to_string()),
        ..Default::default()
    };

    let example = CertificateMessage {
        message_type: Cow::Borrowed("certificate_update"),
        data: CertificateData {
            update_type: Cow::Borrowed("X509LogEntry"),
            leaf_cert: LeafCert {
                subject: subject.clone(),
                issuer: issuer.clone(),
                serial_number: "0123456789ABCDEF".to_string(),
                not_before: 1704067200,
                not_after: 1735689600,
                fingerprint: "AB:CD:EF:01:23:45:67:89:AB:CD:EF:01:23:45:67:89:AB:CD:EF:01".to_string(),
                sha1: "AB:CD:EF:01:23:45:67:89:AB:CD:EF:01:23:45:67:89:AB:CD:EF:01".to_string(),
                sha256: "AB:CD:EF:01:23:45:67:89:AB:CD:EF:01:23:45:67:89:AB:CD:EF:01:23:45:67:89:AB:CD:EF:01:23:45:67:89".to_string(),
                signature_algorithm: "sha256, rsa".to_string(),
                is_ca: false,
                all_domains: smallvec![
                    "example.com".to_string(),
                    "www.example.com".to_string(),
                    "*.example.com".to_string(),
                ],
                as_der: Some("BASE64_ENCODED_DER_DATA".to_string()),
                extensions,
            },
            chain: Some(vec![ChainCert {
                subject: issuer,
                issuer: chain_issuer,
                serial_number: "00112233445566".to_string(),
                not_before: 1672531200,
                not_after: 1767225600,
                fingerprint: "11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44".to_string(),
                sha1: "11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44".to_string(),
                sha256: "11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00".to_string(),
                signature_algorithm: "sha256, rsa".to_string(),
                is_ca: true,
                as_der: Some("BASE64_ENCODED_CA_DER".to_string()),
                extensions: chain_extensions,
            }]),
            cert_index: 123456789,
            seen: 1704067200.123,
            source: Arc::new(Source {
                name: Arc::from("Google 'Argon2024' log"),
                url: Arc::from("https://ct.googleapis.com/logs/argon2024"),
                operator: Arc::from("Google"),
            }),
        },
    };

    Json(example)
}

#[tokio::main]
async fn main() {
    let cli_args = CliArgs::parse();

    if cli_args.show_help {
        CliArgs::print_help();
        return;
    }

    if cli_args.show_version {
        CliArgs::print_version();
        return;
    }

    let config = Config::load();

    if cli_args.validate_config {
        println!("Validating configuration...");
        match config.validate() {
            Ok(()) => {
                println!("Configuration is valid.");
                if let Some(ref path) = config.config_path {
                    println!("Config file: {}", path);
                }
                println!("Host: {}", config.host);
                println!("Port: {}", config.port);
                println!("Log level: {}", config.log_level);
                println!("Buffer size: {}", config.buffer_size);
                println!("WebSocket: {}", config.protocols.websocket);
                println!("SSE: {}", config.protocols.sse);
                println!("TCP: {}", config.protocols.tcp);
                println!("API: {}", config.protocols.api);
                println!("Metrics: {}", config.protocols.metrics);
                println!("Connection limit enabled: {}", config.connection_limit.enabled);
                println!("Rate limit enabled: {}", config.rate_limit.enabled);
                println!("Backpressure enabled: {}", config.backpressure.enabled);
                println!("Auth enabled: {}", config.auth.enabled);
                println!("Hot reload enabled: {}", config.hot_reload.enabled);
            }
            Err(errors) => {
                eprintln!("Configuration validation failed:");
                for err in errors {
                    eprintln!("  - {}: {}", err.field, err.message);
                }
                std::process::exit(1);
            }
        }
        return;
    }

    if cli_args.export_metrics {
        let prometheus_handle = PrometheusBuilder::new()
            .install_recorder()
            .expect("failed to install prometheus recorder");
        println!("{}", prometheus_handle.render());
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.log_level)),
        )
        .init();

    info!("starting certstream-server-rust v{}", VERSION);

    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install prometheus recorder");

    let (tx, _rx) = broadcast::channel::<Arc<PreSerializedMessage>>(config.buffer_size);

    let client = Client::builder()
        .user_agent(format!("certstream-server-rust/{}", VERSION))
        .pool_max_idle_per_host(20)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true)
        .build()
        .expect("failed to build http client");

    let state_manager = StateManager::new(config.ct_log.state_file.clone());
    if config.ct_log.state_file.is_some() {
        state_manager.clone().start_periodic_save(Duration::from_secs(30));
        info!("state persistence enabled");
    }

    if config.hot_reload.enabled {
        let initial_hot_config = HotReloadableConfig {
            connection_limit: config.connection_limit.clone(),
            auth: config.auth.clone(),
        };
        let config_watcher = ConfigWatcher::new(initial_hot_config);
        let watch_path = config.hot_reload.watch_path.clone().or(config.config_path.clone());
        config_watcher.start_watching(watch_path);
        info!("hot reload enabled");
    }

    let ct_log_config = Arc::new(config.ct_log.clone());
    let log_tracker = Arc::new(LogTracker::new());
    let server_stats = Arc::new(ServerStats::new());
    let cert_cache = Arc::new(CertificateCache::new(config.api.cache_capacity));

    let tier_tokens = TierTokens {
        standard: config.auth.standard_tokens.clone(),
        premium: config.auth.premium_tokens.clone(),
    };
    let rate_limiter = RateLimiter::new(config.rate_limit.clone(), tier_tokens);

    info!(url = %config.ct_logs_url, "fetching CT log list");

    let custom_logs_count = config.custom_logs.len();
    if custom_logs_count > 0 {
        info!(count = custom_logs_count, "adding custom CT logs");
    }

    let host = config.host;
    let port = config.port;
    let has_tls = config.has_tls();
    let tls_cert = config.tls_cert.clone();
    let tls_key = config.tls_key.clone();
    let protocols = config.protocols.clone();

    if !cli_args.dry_run {
        match fetch_log_list(&client, &config.ct_logs_url, config.custom_logs.clone()).await {
            Ok(logs) => {
                info!(count = logs.len(), "found CT logs");
                metrics::gauge!("certstream_ct_logs_count").set(logs.len() as f64);

                for log in &logs {
                    log_tracker.register(
                        log.description.clone(),
                        log.normalized_url(),
                        log.operator.clone(),
                    );
                }

                let cache_for_watcher = cert_cache.clone();
                let stats_for_watcher = server_stats.clone();
                let tracker_for_watcher = log_tracker.clone();

                for log in logs {
                    let client = client.clone();
                    let tx = tx.clone();
                    let ct_config = ct_log_config.clone();
                    let state_mgr = state_manager.clone();
                    let cache = cache_for_watcher.clone();
                    let stats = stats_for_watcher.clone();
                    let tracker = tracker_for_watcher.clone();

                    tokio::spawn(async move {
                        run_watcher_with_cache(
                            client, log, tx, ct_config, state_mgr, cache, stats, tracker,
                        ).await;
                    });
                }
            }
            Err(e) => {
                error!(error = %e, "failed to fetch CT log list");
                std::process::exit(1);
            }
        }
    } else {
        info!("dry-run mode: skipping CT log connections");
    }

    let connection_limiter = ConnectionLimiter::new(config.connection_limit.clone());

    if protocols.tcp {
        let tcp_port = protocols.tcp_port.unwrap_or(port + 1);
        let tcp_addr = SocketAddr::from((host, tcp_port));
        let tcp_tx = tx.clone();
        let tcp_limiter = connection_limiter.clone();
        let tcp_stats = server_stats.clone();
        tokio::spawn(async move {
            run_tcp_server_with_stats(tcp_addr, tcp_tx, tcp_limiter, tcp_stats).await;
        });
        info!(port = tcp_port, "TCP protocol enabled");
    }

    let state = Arc::new(AppState {
        tx: tx.clone(),
        connections: ConnectionCounter::new(),
        limiter: connection_limiter.clone(),
    });
    let auth_middleware_state = Arc::new(AuthMiddleware::new(&config.auth));

    let api_state = Arc::new(ApiState {
        stats: server_stats.clone(),
        cache: cert_cache.clone(),
        log_tracker: log_tracker.clone(),
    });

    let mut app = Router::new();

    if protocols.health {
        app = app.route("/health", get(health));
    }

    if protocols.example_json {
        app = app.route("/example.json", get(example_json));
    }

    if protocols.metrics {
        app = app.route("/metrics", get(move || async move { prometheus_handle.render() }));
    }

    if protocols.api {
        let api_router = Router::new()
            .route("/api/stats", get(handle_stats))
            .route("/api/logs", get(handle_logs))
            .route("/api/cert/{hash}", get(handle_cert))
            .with_state(api_state);
        app = app.merge(api_router);
        info!("REST API enabled");
    }

    if protocols.websocket {
        let ws_router = Router::new()
            .route("/", get(handle_lite_stream))
            .route("/full-stream", get(handle_full_stream))
            .route("/domains-only", get(handle_domains_only))
            .with_state(state.clone());
        app = app.merge(ws_router);
        info!("WebSocket protocol enabled");
    }

    if protocols.sse {
        let sse_router = Router::new()
            .route("/sse", get(handle_sse_stream))
            .with_state(state.clone());
        app = app.merge(sse_router);
        info!("SSE protocol enabled");
    }

    let app = if config.auth.enabled {
        info!("token authentication enabled");
        app.layer(axum_middleware::from_fn_with_state(
            auth_middleware_state,
            auth_middleware,
        ))
    } else {
        app
    };

    let app = if config.rate_limit.enabled {
        info!("rate limiting enabled (token bucket + sliding window)");
        let limiter = rate_limiter.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                interval.tick().await;
                limiter.cleanup_stale(Duration::from_secs(600));
            }
        });
        app.layer(axum_middleware::from_fn_with_state(
            rate_limiter.clone(),
            rate_limit_middleware,
        ))
    } else {
        app
    };

    let app = app.layer(CorsLayer::permissive());

    if config.connection_limit.enabled {
        info!(
            max_connections = config.connection_limit.max_connections,
            per_ip_limit = ?config.connection_limit.per_ip_limit,
            "connection limiting enabled"
        );
    }

    if config.backpressure.enabled {
        info!(
            buffer_size = config.backpressure.buffer_size,
            threshold = config.backpressure.slow_consumer_threshold,
            policy = %config.backpressure.drop_policy,
            "backpressure enabled"
        );
    }

    let addr = SocketAddr::from((host, port));
    info!(address = %addr, "starting server");

    if has_tls {
        let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
            tls_cert.as_ref().unwrap(),
            tls_key.as_ref().unwrap(),
        )
        .await
        .expect("failed to load TLS config");

        axum_server::bind_rustls(addr, tls_config)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .expect("server error");
    } else {
        let listener = tokio::net::TcpListener::bind(addr).await.expect("failed to bind");
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .expect("server error");
    }
}

async fn run_watcher_with_cache(
    client: Client,
    log: ct::CtLog,
    tx: broadcast::Sender<Arc<PreSerializedMessage>>,
    config: Arc<config::CtLogConfig>,
    state_manager: Arc<StateManager>,
    cache: Arc<CertificateCache>,
    stats: Arc<ServerStats>,
    tracker: Arc<LogTracker>,
) {
    use crate::ct::watcher::LogHealth;
    use backon::{ExponentialBuilder, Retryable};
    use serde::Deserialize;
    use tokio::time::sleep;
    use tracing::{debug, warn};

    #[derive(Debug, Deserialize)]
    struct SthResponse {
        tree_size: u64,
    }

    #[derive(Debug, Deserialize)]
    struct EntriesResponse {
        entries: Vec<Entry>,
    }

    #[derive(Debug, Deserialize)]
    struct Entry {
        leaf_input: String,
        extra_data: String,
    }

    let base_url = log.normalized_url();
    let source = Arc::new(Source {
        name: Arc::from(log.description.as_str()),
        url: Arc::from(base_url.as_str()),
        operator: Arc::from(log.operator.as_str()),
    });

    let health = Arc::new(LogHealth::new());
    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    let error_backoff = Duration::from_secs(5);
    let timeout = Duration::from_secs(config.request_timeout_secs);

    info!(log = %log.description, url = %base_url, "starting watcher");

    let mut current_index = if let Some(saved_index) = state_manager.get_index(&base_url) {
        info!(log = %log.description, saved_index = saved_index, "resuming from saved state");
        saved_index
    } else {
        let backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(config.retry_initial_delay_ms))
            .with_max_delay(Duration::from_millis(config.retry_max_delay_ms))
            .with_max_times(config.retry_max_attempts as usize);

        let url = format!("{}/ct/v1/get-sth", base_url);
        match (|| async {
            let response: SthResponse = client.get(&url).timeout(timeout).send().await?.json().await?;
            Ok::<_, reqwest::Error>(response.tree_size)
        })
        .retry(backoff)
        .sleep(tokio::time::sleep)
        .await
        {
            Ok(size) => {
                let start = size.saturating_sub(1000);
                info!(log = %log.description, tree_size = size, starting_at = start, "starting fresh");
                start
            }
            Err(e) => {
                error!(log = %log.description, error = %e, "failed to get initial tree size");
                0
            }
        }
    };

    loop {
        if !health.is_healthy() {
            warn!(log = %log.description, errors = health.total_errors(), "log is unhealthy, waiting for recovery check");
            sleep(Duration::from_secs(config.health_check_interval_secs)).await;

            let url = format!("{}/ct/v1/get-sth", base_url);
            match client.get(&url).timeout(timeout).send().await {
                Ok(_) => {
                    health.record_success(config.healthy_threshold);
                    info!(log = %log.description, "health check passed, resuming");
                }
                Err(e) => {
                    health.record_failure(config.unhealthy_threshold);
                    warn!(log = %log.description, error = %e, "health check failed, staying disabled");
                    metrics::counter!("certstream_log_health_checks_failed").increment(1);
                    continue;
                }
            }
        }

        let sth_url = format!("{}/ct/v1/get-sth", base_url);
        let tree_size = match client.get(&sth_url).timeout(timeout).send().await {
            Ok(resp) => match resp.json::<SthResponse>().await {
                Ok(sth) => {
                    health.record_success(config.healthy_threshold);
                    sth.tree_size
                }
                Err(e) => {
                    health.record_failure(config.unhealthy_threshold);
                    warn!(log = %log.description, error = %e, "failed to parse tree size");
                    sleep(error_backoff).await;
                    continue;
                }
            },
            Err(e) => {
                health.record_failure(config.unhealthy_threshold);
                warn!(log = %log.description, error = %e, "failed to get tree size");
                sleep(error_backoff).await;
                continue;
            }
        };

        if current_index >= tree_size {
            sleep(poll_interval).await;
            continue;
        }

        let end = (current_index + config.batch_size).min(tree_size - 1);
        let entries_url = format!("{}/ct/v1/get-entries?start={}&end={}", base_url, current_index, end);

        match client.get(&entries_url).timeout(timeout).send().await {
            Ok(resp) => match resp.json::<EntriesResponse>().await {
                Ok(entries_resp) => {
                    health.record_success(config.healthy_threshold);
                    let count = entries_resp.entries.len();

                    for (i, entry) in entries_resp.entries.into_iter().enumerate() {
                        if let Some(parsed) = ct::parse_leaf_input(&entry.leaf_input, &entry.extra_data) {
                            let cert_index = current_index + i as u64;
                            let seen = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;

                            let cached = CachedCert {
                                fingerprint: parsed.leaf_cert.fingerprint.clone(),
                                sha1: parsed.leaf_cert.sha1.clone(),
                                sha256: parsed.leaf_cert.sha256.clone(),
                                serial_number: parsed.leaf_cert.serial_number.clone(),
                                subject_cn: parsed.leaf_cert.subject.cn.clone(),
                                subject_o: parsed.leaf_cert.subject.o.clone(),
                                subject_c: parsed.leaf_cert.subject.c.clone(),
                                issuer_cn: parsed.leaf_cert.issuer.cn.clone(),
                                issuer_o: parsed.leaf_cert.issuer.o.clone(),
                                issuer_c: parsed.leaf_cert.issuer.c.clone(),
                                not_before: parsed.leaf_cert.not_before,
                                not_after: parsed.leaf_cert.not_after,
                                is_ca: parsed.leaf_cert.is_ca,
                                all_domains: parsed.leaf_cert.all_domains.to_vec(),
                                signature_algorithm: parsed.leaf_cert.signature_algorithm.clone(),
                                seen,
                                source_name: log.description.clone(),
                                cert_index,
                            };
                            cache.push(cached);

                            let msg = CertificateMessage {
                                message_type: Cow::Borrowed("certificate_update"),
                                data: CertificateData {
                                    update_type: parsed.update_type,
                                    leaf_cert: parsed.leaf_cert,
                                    chain: Some(parsed.chain),
                                    cert_index,
                                    seen,
                                    source: Arc::clone(&source),
                                },
                            };

                            if let Some(serialized) = msg.pre_serialize() {
                                let msg_size = serialized.full.len() + serialized.lite.len() + serialized.domains_only.len();
                                let _ = tx.send(serialized);
                                stats.messages_sent.fetch_add(1, Ordering::Relaxed);
                                stats.certificates_processed.fetch_add(1, Ordering::Relaxed);
                                stats.bytes_sent.fetch_add(msg_size as u64, Ordering::Relaxed);
                                metrics::counter!("certstream_messages_sent").increment(1);
                            }
                        }
                    }

                    debug!(log = %log.description, count = count, "fetched entries");
                    current_index = end + 1;
                    state_manager.update_index(&base_url, current_index, tree_size);

                    tracker.update(
                        &base_url,
                        *health.status.read(),
                        current_index,
                        tree_size,
                        health.total_errors(),
                    );
                }
                Err(e) => {
                    health.record_failure(config.unhealthy_threshold);
                    warn!(log = %log.description, error = %e, "failed to parse entries");
                    sleep(error_backoff).await;
                }
            },
            Err(e) => {
                health.record_failure(config.unhealthy_threshold);
                warn!(log = %log.description, error = %e, "failed to fetch entries");
                sleep(error_backoff).await;
            }
        }
    }
}

async fn run_tcp_server_with_stats(
    addr: SocketAddr,
    tx: broadcast::Sender<Arc<PreSerializedMessage>>,
    limiter: Arc<ConnectionLimiter>,
    stats: Arc<ServerStats>,
) {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tracing::info;

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, "failed to bind TCP server");
            return;
        }
    };

    info!(address = %addr, "TCP server listening");

    loop {
        let (mut socket, peer_addr) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "failed to accept TCP connection");
                continue;
            }
        };

        let ip = peer_addr.ip();
        if !limiter.try_acquire(ip) {
            let _ = socket.shutdown().await;
            continue;
        }

        stats.tcp_connections.fetch_add(1, Ordering::Relaxed);
        let mut rx = tx.subscribe();
        let limiter_clone = limiter.clone();
        let stats_clone = stats.clone();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        let mut data = msg.lite.to_vec();
                        data.push(b'\n');
                        if socket.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            limiter_clone.release(ip);
            stats_clone.tcp_connections.fetch_sub(1, Ordering::Relaxed);
        });
    }
}
