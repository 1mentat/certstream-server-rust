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
    pub staging_path: Option<String>,
    pub backfill_sink: Option<String>,
    pub merge: bool,
}

impl CliArgs {
    pub fn parse() -> Self {
        let args: Vec<String> = env::args().collect();
        Self::parse_args(&args)
    }

    /// Parse CLI arguments from a provided vector (testable).
    pub fn parse_args(args: &[String]) -> Self {
        // Parse value-bearing flags with index-based iteration
        let mut backfill_from = None;
        let mut backfill_logs = None;
        let mut staging_path = None;
        let mut backfill_sink = None;

        for (i, arg) in args.iter().enumerate() {
            if arg == "--from" && i + 1 < args.len() {
                if let Ok(val) = args[i + 1].parse::<u64>() {
                    backfill_from = Some(val);
                }
            } else if arg == "--logs" && i + 1 < args.len() {
                backfill_logs = Some(args[i + 1].clone());
            } else if arg == "--staging-path" && i + 1 < args.len() {
                staging_path = Some(args[i + 1].clone());
            } else if arg == "--sink" && i + 1 < args.len() {
                backfill_sink = Some(args[i + 1].clone());
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
            staging_path,
            backfill_sink,
            merge: args.iter().any(|a| a == "--merge"),
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
        println!("    --sink <NAME>        Writer backend: delta (default), zerobus");
        println!();
        println!("    Backfill uses the state file (ct_log.state_file, default:");
        println!("    certstream_state.json) as the per-log upper bound. Logs not");
        println!("    present in the state file are skipped. Run the live server");
        println!("    first to populate the state file.");
        println!();
        println!("STAGING/MERGE OPTIONS:");
        println!("    --staging-path <PATH>  Write backfill to staging Delta table at PATH");
        println!("    --merge                Merge staging table into main table");
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

/// Validates backfill sink command-line arguments.
///
/// # Arguments
/// * `sink_name` - The name of the sink (e.g., "delta", "zerobus"), or None
/// * `backfill_from` - The starting index for backfill, or None for catch-up mode
/// * `zerobus_enabled` - Whether zerobus_sink is enabled in config
///
/// # Returns
/// * `Ok(())` if validation passes
/// * `Err(String)` if validation fails, with a descriptive error message
pub fn validate_backfill_sink_command(
    sink_name: Option<&str>,
    backfill_from: Option<u64>,
    zerobus_enabled: bool,
) -> Result<(), String> {
    if let Some(sink) = sink_name {
        match sink {
            "delta" => {
                // delta is always valid
                Ok(())
            }
            "zerobus" => {
                // AC3.4: zerobus_sink must be enabled in config
                if !zerobus_enabled {
                    return Err(
                        "Error: --sink zerobus requires zerobus_sink.enabled = true in config"
                            .to_string(),
                    );
                }
                // AC3.3: --from is required for zerobus sink (historical mode only)
                if backfill_from.is_none() {
                    return Err(
                        "Error: --sink zerobus requires --from <INDEX> (historical mode only, catch-up gap detection not supported for remote tables)"
                            .to_string(),
                    );
                }
                Ok(())
            }
            other => {
                // AC3.5: invalid sink name
                Err(format!(
                    "Error: unknown sink '{}'. Valid sinks: delta, zerobus",
                    other
                ))
            }
        }
    } else {
        // No sink specified; defaults to delta
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ac3_2_backfill_sink_none_when_not_provided() {
        // AC3.2: Test that backfill_sink is None when --sink is not provided (backwards-compatible)
        let args = vec![
            "certstream".to_string(),
            "--backfill".to_string(),
        ];
        let parsed = CliArgs::parse_args(&args);
        assert_eq!(parsed.backfill_sink, None, "backfill_sink should be None when --sink not provided");
        assert!(parsed.backfill, "backfill should be true");
    }

    #[test]
    fn test_ac3_5_parse_sink_zerobus() {
        // AC3.5: Test that --sink zerobus is correctly parsed
        let args = vec![
            "certstream".to_string(),
            "--backfill".to_string(),
            "--sink".to_string(),
            "zerobus".to_string(),
        ];
        let parsed = CliArgs::parse_args(&args);
        assert_eq!(parsed.backfill_sink, Some("zerobus".to_string()), "backfill_sink should be zerobus");
        assert!(parsed.backfill, "backfill should be true");
    }

    #[test]
    fn test_ac3_5_parse_sink_delta() {
        // AC3.5: Test that --sink delta is correctly parsed
        let args = vec![
            "certstream".to_string(),
            "--backfill".to_string(),
            "--sink".to_string(),
            "delta".to_string(),
        ];
        let parsed = CliArgs::parse_args(&args);
        assert_eq!(parsed.backfill_sink, Some("delta".to_string()), "backfill_sink should be delta");
    }

    #[test]
    fn test_ac3_5_parse_sink_invalid() {
        // AC3.5: Test that invalid sink name is parsed (validation happens in main.rs)
        let args = vec![
            "certstream".to_string(),
            "--backfill".to_string(),
            "--sink".to_string(),
            "invalid_sink".to_string(),
        ];
        let parsed = CliArgs::parse_args(&args);
        assert_eq!(parsed.backfill_sink, Some("invalid_sink".to_string()), "backfill_sink should be parsed even if invalid");
    }

    #[test]
    fn test_sink_with_from_parameter() {
        // Test that --sink and --from are both parsed correctly together
        let args = vec![
            "certstream".to_string(),
            "--backfill".to_string(),
            "--sink".to_string(),
            "zerobus".to_string(),
            "--from".to_string(),
            "12345".to_string(),
        ];
        let parsed = CliArgs::parse_args(&args);
        assert_eq!(parsed.backfill_sink, Some("zerobus".to_string()), "backfill_sink should be zerobus");
        assert_eq!(parsed.backfill_from, Some(12345), "backfill_from should be 12345");
    }

    #[test]
    fn test_sink_without_from_parameter() {
        // Test parsing --sink zerobus without --from (validation happens in main.rs)
        let args = vec![
            "certstream".to_string(),
            "--backfill".to_string(),
            "--sink".to_string(),
            "zerobus".to_string(),
        ];
        let parsed = CliArgs::parse_args(&args);
        assert_eq!(parsed.backfill_sink, Some("zerobus".to_string()), "backfill_sink should be parsed");
        assert_eq!(parsed.backfill_from, None, "backfill_from should be None (validation in main.rs will catch this)");
    }

    #[test]
    fn test_sink_with_staging_path() {
        // Test that --sink, --from, and --staging-path are all parsed correctly
        let args = vec![
            "certstream".to_string(),
            "--backfill".to_string(),
            "--sink".to_string(),
            "zerobus".to_string(),
            "--from".to_string(),
            "100".to_string(),
            "--staging-path".to_string(),
            "/tmp/staging".to_string(),
        ];
        let parsed = CliArgs::parse_args(&args);
        assert_eq!(parsed.backfill_sink, Some("zerobus".to_string()));
        assert_eq!(parsed.backfill_from, Some(100));
        assert_eq!(parsed.staging_path, Some("/tmp/staging".to_string()));
    }

    // AC3.3: --sink zerobus without --from must be validated as an error
    #[test]
    fn test_ac3_3_zerobus_without_from_is_error() {
        let result = validate_backfill_sink_command(Some("zerobus"), None, true);
        assert!(
            result.is_err(),
            "zerobus sink without --from should produce error"
        );
        let err_msg = result.unwrap_err();
        assert!(
            err_msg.contains("requires --from"),
            "error message should mention 'requires --from', got: {}",
            err_msg
        );
    }

    // AC3.4: --sink zerobus when zerobus_sink.enabled = false must be validated as an error
    #[test]
    fn test_ac3_4_zerobus_disabled_is_error() {
        let result = validate_backfill_sink_command(Some("zerobus"), Some(0), false);
        assert!(
            result.is_err(),
            "zerobus sink when disabled in config should produce error"
        );
        let err_msg = result.unwrap_err();
        assert!(
            err_msg.contains("requires zerobus_sink.enabled = true"),
            "error message should mention 'requires zerobus_sink.enabled = true', got: {}",
            err_msg
        );
    }

    // AC3.3: --sink zerobus WITH --from should be valid (when zerobus is enabled)
    #[test]
    fn test_ac3_3_zerobus_with_from_is_valid() {
        let result = validate_backfill_sink_command(Some("zerobus"), Some(12345), true);
        assert!(
            result.is_ok(),
            "zerobus sink with --from and enabled config should be valid"
        );
    }

    // AC3.4: --sink delta when zerobus is disabled should still work
    #[test]
    fn test_ac3_4_delta_sink_always_valid() {
        let result = validate_backfill_sink_command(Some("delta"), None, false);
        assert!(
            result.is_ok(),
            "delta sink should always be valid regardless of zerobus config"
        );
    }

    // Test that no sink specified is valid (defaults to delta)
    #[test]
    fn test_no_sink_specified_is_valid() {
        let result = validate_backfill_sink_command(None, None, false);
        assert!(
            result.is_ok(),
            "no sink specified should default to delta and be valid"
        );
    }

    // AC3.5: unknown sink name should error
    #[test]
    fn test_ac3_5_unknown_sink_is_error() {
        let result = validate_backfill_sink_command(Some("unknown"), None, false);
        assert!(
            result.is_err(),
            "unknown sink name should produce error"
        );
        let err_msg = result.unwrap_err();
        assert!(
            err_msg.contains("unknown sink"),
            "error message should mention 'unknown sink', got: {}",
            err_msg
        );
    }
}
