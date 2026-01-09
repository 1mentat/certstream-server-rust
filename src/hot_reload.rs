use arc_swap::ArcSwap;
use notify::{Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::config::{AuthConfig, ConnectionLimitConfig, RateLimitConfig};

#[derive(Debug, Clone)]
pub struct HotReloadableConfig {
    pub connection_limit: ConnectionLimitConfig,
    pub rate_limit: RateLimitConfig,
    pub auth: AuthConfig,
}

pub struct HotReloadManager {
    config: ArcSwap<HotReloadableConfig>,
}

impl HotReloadManager {
    pub fn new(initial: HotReloadableConfig) -> Arc<Self> {
        Arc::new(Self {
            config: ArcSwap::new(Arc::new(initial)),
        })
    }

    pub fn get(&self) -> Arc<HotReloadableConfig> {
        self.config.load_full()
    }

    pub fn update(&self, new_config: HotReloadableConfig) {
        self.config.store(Arc::new(new_config));
        info!("hot reload: configuration updated");
        metrics::counter!("certstream_config_reloads").increment(1);
    }

    pub fn start_watching(self: Arc<Self>, config_path: Option<String>) {
        let Some(path) = config_path else {
            info!("hot reload: no config file specified, disabled");
            return;
        };

        if !Path::new(&path).exists() {
            warn!(path = %path, "hot reload: config file not found, disabled");
            return;
        }

        let path_clone = path.clone();
        let manager = self.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build runtime for config watcher");

            rt.block_on(async move {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<notify::Result<Event>>(100);

                let mut watcher = match RecommendedWatcher::new(
                    move |res| {
                        let _ = tx.blocking_send(res);
                    },
                    NotifyConfig::default(),
                ) {
                    Ok(w) => w,
                    Err(e) => {
                        error!(error = %e, "hot reload: failed to create file watcher");
                        return;
                    }
                };

                if let Err(e) = watcher.watch(Path::new(&path_clone), RecursiveMode::NonRecursive) {
                    error!(path = %path_clone, error = %e, "hot reload: failed to watch config file");
                    return;
                }

                info!(path = %path_clone, "hot reload: watching config file for changes");

                while let Some(event) = rx.recv().await {
                    match event {
                        Ok(event) => {
                            if event.kind.is_modify() || event.kind.is_create() {
                                info!("hot reload: config file changed, reloading...");
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                                if let Some(new_config) = load_hot_reloadable_config(&path_clone) {
                                    manager.update(new_config);
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "hot reload: file watch error");
                        }
                    }
                }
            });
        });
    }
}

fn load_hot_reloadable_config(path: &str) -> Option<HotReloadableConfig> {
    use serde::Deserialize;

    #[derive(Deserialize, Default)]
    struct PartialConfig {
        #[serde(default)]
        connection_limit: Option<ConnectionLimitConfig>,
        #[serde(default)]
        rate_limit: Option<RateLimitConfig>,
        #[serde(default)]
        auth: Option<AuthConfig>,
    }

    match std::fs::read_to_string(path) {
        Ok(content) => match serde_yaml::from_str::<PartialConfig>(&content) {
            Ok(cfg) => {
                let config = HotReloadableConfig {
                    connection_limit: cfg.connection_limit.unwrap_or_default(),
                    rate_limit: cfg.rate_limit.unwrap_or_default(),
                    auth: cfg.auth.unwrap_or_default(),
                };
                info!(
                    connection_limit_enabled = config.connection_limit.enabled,
                    rate_limit_enabled = config.rate_limit.enabled,
                    auth_enabled = config.auth.enabled,
                    "hot reload: loaded new configuration"
                );
                Some(config)
            }
            Err(e) => {
                error!(error = %e, "hot reload: failed to parse config file");
                None
            }
        },
        Err(e) => {
            error!(error = %e, "hot reload: failed to read config file");
            None
        }
    }
}
