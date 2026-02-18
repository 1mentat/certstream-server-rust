use std::env;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub validate_config: bool,
    pub dry_run: bool,
    pub export_metrics: bool,
    pub show_version: bool,
    pub show_help: bool,
    pub backfill: bool,
    pub backfill_from: Option<u64>,
    pub backfill_logs: Option<String>,
}

impl CliArgs {
    pub fn parse() -> Self {
        let args: Vec<String> = env::args().collect();

        // Parse value-bearing flags with index-based iteration
        let mut backfill_from = None;
        let mut backfill_logs = None;

        for (i, arg) in args.iter().enumerate() {
            if arg == "--from" && i + 1 < args.len() {
                if let Ok(val) = args[i + 1].parse::<u64>() {
                    backfill_from = Some(val);
                }
            } else if arg == "--logs" && i + 1 < args.len() {
                backfill_logs = Some(args[i + 1].clone());
            }
        }

        Self {
            validate_config: args.iter().any(|a| a == "--validate-config"),
            dry_run: args.iter().any(|a| a == "--dry-run"),
            export_metrics: args.iter().any(|a| a == "--export-metrics"),
            show_version: args.iter().any(|a| a == "--version" || a == "-V"),
            show_help: args.iter().any(|a| a == "--help" || a == "-h"),
            backfill: args.iter().any(|a| a == "--backfill"),
            backfill_from,
            backfill_logs,
        }
    }

    pub fn print_help() {
        println!("certstream-server-rust {}", VERSION);
        println!();
        println!("High-performance Certificate Transparency log streaming server");
        println!();
        println!("USAGE:");
        println!("    certstream-server-rust [OPTIONS]");
        println!();
        println!("OPTIONS:");
        println!("    --validate-config    Validate configuration and exit");
        println!("    --dry-run            Start server without connecting to CT logs");
        println!("    --export-metrics     Export current metrics and exit");
        println!("    -V, --version        Print version information");
        println!("    -h, --help           Print help information");
        println!();
        println!("BACKFILL OPTIONS:");
        println!("    --backfill           Activate backfill mode (requires state file)");
        println!("    --from <INDEX>       Override start index for all logs");
        println!("    --logs <FILTER>      Filter to specific logs by substring");
        println!();
        println!("    Backfill uses the state file (ct_log.state_file, default:");
        println!("    certstream_state.json) as the per-log upper bound. Logs not");
        println!("    present in the state file are skipped. Run the live server");
        println!("    first to populate the state file.");
        println!();
        println!("ENVIRONMENT VARIABLES:");
        println!("    CERTSTREAM_CONFIG              Path to config file");
        println!("    CERTSTREAM_HOST                Server host (default: 0.0.0.0)");
        println!("    CERTSTREAM_PORT                Server port (default: 8080)");
        println!("    CERTSTREAM_LOG_LEVEL           Log level (default: info)");
        println!("    CERTSTREAM_BUFFER_SIZE         Broadcast buffer size (default: 1000)");
        println!();
        println!("For more information, see: https://github.com/burakozcn01/certstream-server-rust");
    }

    pub fn print_version() {
        println!("certstream-server-rust {}", VERSION);
    }
}
