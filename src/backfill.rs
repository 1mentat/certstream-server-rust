use crate::config::Config;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn run_backfill(
    _config: Config,
    backfill_from: Option<u64>,
    backfill_logs: Option<String>,
    _shutdown: CancellationToken,
) -> i32 {
    info!("backfill mode starting");

    if let Some(from) = backfill_from {
        info!(from = from, "backfill_from parameter");
    }

    if let Some(logs_filter) = backfill_logs {
        info!(logs_filter = %logs_filter, "backfill_logs filter");
    }

    info!("backfill not yet implemented");

    0
}
