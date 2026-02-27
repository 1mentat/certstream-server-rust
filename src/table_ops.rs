use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::Config;

pub async fn run_reparse_audit(
    config: Config,
    from_date: Option<String>,
    to_date: Option<String>,
    _shutdown: CancellationToken,
) -> i32 {
    info!(
        table_path = %config.delta_sink.table_path,
        from_date = ?from_date,
        to_date = ?to_date,
        "reparse audit: stub implementation"
    );
    0
}

pub async fn run_extract_metadata(
    config: Config,
    output_path: String,
    from_date: Option<String>,
    to_date: Option<String>,
    _shutdown: CancellationToken,
) -> i32 {
    info!(
        table_path = %config.delta_sink.table_path,
        output_path = %output_path,
        from_date = ?from_date,
        to_date = ?to_date,
        "extract metadata: stub implementation"
    );
    0
}
