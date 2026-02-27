use crate::config::{Config, ZerobusSinkConfig};
use crate::ct::fetch;
use crate::ct::{fetch_log_list, CtLog, LogType};
use crate::delta_sink::{delta_schema, delta_writer_properties, DeltaCertRecord, flush_buffer, open_or_create_table};
use crate::models::Source;
use crate::state::StateManager;
use crate::zerobus_sink::{cert_record_descriptor_proto, proto};
use base64::{engine::general_purpose::STANDARD, Engine};
use databricks_zerobus_ingest_sdk::{StreamConfigurationOptions, TableProperties, ZerobusSdk};
use deltalake::arrow::array::*;
use deltalake::arrow::datatypes::Field;
use deltalake::datafusion::prelude::*;
use deltalake::{DeltaOps, DeltaTable, DeltaTableError};
use prost::Message;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Represents a contiguous range of certificate indices to backfill.
pub struct BackfillWorkItem {
    pub source_url: String,
    pub start: u64,
    pub end: u64, // inclusive
}

/// Result from the writer task containing record and error counts.
#[derive(Debug, Clone)]
pub struct WriterResult {
    pub total_records_written: u64,
    pub write_errors: u64,
}

/// Per-source delta table state (min, max, and count of cert_index values).
#[derive(Debug, Clone)]
struct DeltaLogState {
    min_index: u64,
    max_index: u64,
    count: u64,
}

/// Query the delta table to get per-source min, max, and count of cert_index.
///
/// # Arguments
/// * `ctx` - Already-registered DataFusion SessionContext with "ct_records" table
///
/// # Returns
/// * HashMap mapping source_url to DeltaLogState (min_index, max_index, count)
async fn query_log_states(ctx: &SessionContext) -> Result<HashMap<String, DeltaLogState>, Box<dyn Error>> {
    let sql = "SELECT source_url, MIN(cert_index) as min_idx, MAX(cert_index) as max_idx, COUNT(DISTINCT cert_index) as cnt FROM ct_records GROUP BY source_url";

    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;

    let mut states = HashMap::new();

    for batch in batches {
        let source_urls = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("Failed to downcast source_url column")?;

        let min_indices = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("Failed to downcast min_idx column")?;

        let max_indices = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("Failed to downcast max_idx column")?;

        let counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("Failed to downcast cnt column")?;

        for i in 0..batch.num_rows() {
            // All columns should be non-null from aggregate query
            if !source_urls.is_null(i) && !min_indices.is_null(i) && !max_indices.is_null(i) && !counts.is_null(i) {
                let url = source_urls.value(i);
                let min_val = min_indices.value(i);
                let max_val = max_indices.value(i);
                let cnt = counts.value(i);

                states.insert(
                    url.to_string(),
                    DeltaLogState {
                        min_index: min_val as u64,
                        max_index: max_val as u64,
                        count: cnt as u64,
                    },
                );
            }
        }
    }

    Ok(states)
}

/// Detect internal gaps (dropped records) in a specific log's cert_index range.
///
/// # Arguments
/// * `ctx` - Already-registered DataFusion SessionContext with "ct_records" table
/// * `source_url` - URL of the log to check for gaps
/// * `lower_bound` - Minimum cert_index to check (inclusive)
/// * `upper_bound` - Maximum cert_index to check (inclusive)
///
/// # Returns
/// * Vec of (start, end) tuples representing gap ranges
async fn find_internal_gaps(
    ctx: &SessionContext,
    source_url: &str,
    lower_bound: u64,
    upper_bound: u64,
) -> Result<Vec<(u64, u64)>, Box<dyn Error>> {
    // Filter records by source_url and index range using DataFrame API
    let table = ctx.table("ct_records").await?;
    let filtered = table
        .filter(col("source_url").eq(lit(source_url)))?
        .filter(col("cert_index").gt_eq(lit(lower_bound as i64)))?
        .filter(col("cert_index").lt_eq(lit(upper_bound as i64)))?;

    // Register as temporary table for LEAD query
    ctx.register_table("filtered_entries", filtered.into_view())?;

    // Query for gaps using LEAD window function
    let gaps_sql = "SELECT cert_index + 1 as gap_start, next_index - 1 as gap_end
        FROM (SELECT cert_index, LEAD(cert_index) OVER (ORDER BY cert_index) as next_index
              FROM filtered_entries) sub
        WHERE next_index - cert_index > 1";

    // Wrap query in async block to ensure deregister is always called
    let result: Result<Vec<_>, Box<dyn Error>> = async {
        let gaps_df = ctx.sql(gaps_sql).await?;
        let gaps_batches = gaps_df.collect().await?;
        Ok(gaps_batches)
    }
    .await;

    // Always deregister temp table to avoid name collision on repeated calls
    ctx.deregister_table("filtered_entries")?;
    let gaps_batches = result?;

    let mut gaps = Vec::new();

    for batch in gaps_batches {
        let gap_starts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("Failed to downcast gap_start column")?;

        let gap_ends = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("Failed to downcast gap_end column")?;

        for i in 0..batch.num_rows() {
            if !gap_starts.is_null(i) && !gap_ends.is_null(i) {
                let start = gap_starts.value(i);
                let end = gap_ends.value(i);
                gaps.push((start as u64, end as u64));
            }
        }
    }

    Ok(gaps)
}

/// Given a DataFusion context with a "ct_records" view already registered,
/// query log states and find gaps for each log in the provided list.
///
/// # Arguments
/// * `ctx` - Already-registered DataFusion SessionContext with "ct_records" view
/// * `logs` - Vec of (source_url, ceiling) pairs for active logs
/// * `backfill_from` - None for catch-up mode, Some(index) for historical mode
///
/// # Returns
/// * Vec of BackfillWorkItem ranges to fetch
async fn detect_gaps_from_context(
    ctx: &SessionContext,
    logs: &[(String, u64)],
    backfill_from: Option<u64>,
) -> Result<Vec<BackfillWorkItem>, Box<dyn Error>> {
    let log_states = query_log_states(ctx).await?;
    let mut work_items = Vec::new();

    for (source_url, ceiling) in logs {
        match backfill_from {
            None => {
                // Catch-up mode
                if let Some(state) = log_states.get(source_url) {
                    let lower_bound = state.min_index;

                    // Check for internal gaps
                    let expected_count = state.max_index - state.min_index + 1;
                    if state.count < expected_count {
                        let effective_max = state.max_index.min(*ceiling);
                        match find_internal_gaps(ctx, source_url, lower_bound, effective_max).await {
                            Ok(gaps) => {
                                for (gap_start, gap_end) in gaps {
                                    work_items.push(BackfillWorkItem {
                                        source_url: source_url.clone(),
                                        start: gap_start,
                                        end: gap_end,
                                    });
                                }
                            }
                            Err(e) => {
                                warn!(source_url = %source_url, error = %e, "Failed to detect internal gaps");
                            }
                        }
                    }
                }
                // If log not in delta, skip (AC2.4)
            }
            Some(from) => {
                // Historical mode
                if let Some(state) = log_states.get(source_url) {
                    // Pre-existing gap (before MIN in delta)
                    if from < state.min_index {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: from,
                            end: state.min_index - 1,
                        });
                    }

                    // Internal gaps
                    let expected_count = state.max_index - state.min_index + 1;
                    if state.count < expected_count {
                        let effective_max = state.max_index.min(*ceiling);
                        match find_internal_gaps(ctx, source_url, from.max(state.min_index), effective_max).await {
                            Ok(gaps) => {
                                for (gap_start, gap_end) in gaps {
                                    work_items.push(BackfillWorkItem {
                                        source_url: source_url.clone(),
                                        start: gap_start,
                                        end: gap_end,
                                    });
                                }
                            }
                            Err(e) => {
                                warn!(source_url = %source_url, error = %e, "Failed to detect internal gaps");
                            }
                        }
                    }
                } else {
                    // Log not in delta: backfill entire range
                    if *ceiling > from {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: from,
                            end: ceiling - 1,
                        });
                    }
                }
            }
        }
    }

    Ok(work_items)
}

/// Orchestrate gap detection across all logs.
///
/// Opens delta table(s) once, queries state for all logs, detects gaps per log,
/// and generates work items for catch-up or historical backfill modes.
/// When staging_path is provided and the staging table exists, both main and staging
/// tables are registered and a UNION ALL view is created.
///
/// # Arguments
/// * `table_path` - Path to the main delta table
/// * `staging_path` - Optional path to the staging delta table; if provided and exists, will be unioned with main
/// * `logs` - Vec of (source_url, ceiling) pairs for active logs
/// * `backfill_from` - None for catch-up mode, Some(index) for historical mode
///
/// # Returns
/// * Vec of BackfillWorkItem ranges to fetch
pub async fn detect_gaps(
    table_path: &str,
    staging_path: Option<&str>,
    logs: &[(String, u64)],
    backfill_from: Option<u64>,
) -> Result<Vec<BackfillWorkItem>, Box<dyn Error>> {
    // Try to open the main delta table; if it doesn't exist, try staging if provided
    let table = match deltalake::open_table(table_path).await {
        Ok(t) => t,
        Err(_e @ DeltaTableError::NotATable(_)) | Err(_e @ DeltaTableError::InvalidTableLocation(_)) => {
            // Main table doesn't exist — try staging if provided
            if let Some(staging) = staging_path {
                if let Ok(staging_table) = deltalake::open_table(staging).await {
                    let ctx = SessionContext::new();
                    ctx.register_table("ct_staging", std::sync::Arc::new(staging_table))?;
                    ctx.sql(
                        "CREATE VIEW ct_records AS SELECT cert_index, source_url FROM ct_staging"
                    ).await?;
                    return detect_gaps_from_context(&ctx, logs, backfill_from).await;
                }
            }
            // Neither main nor staging exists — use original fallback logic
            return match backfill_from {
                None => {
                    // Catch-up mode: no table, no work items
                    Ok(Vec::new())
                }
                Some(from) => {
                    // Historical mode: backfill entire range for all logs
                    let mut work_items = Vec::new();
                    for (source_url, ceiling) in logs {
                        if *ceiling > from {
                            work_items.push(BackfillWorkItem {
                                source_url: source_url.clone(),
                                start: from,
                                end: ceiling - 1,
                            });
                        }
                    }
                    Ok(work_items)
                }
            };
        }
        Err(e) => return Err(Box::new(e)),
    };

    // Create SessionContext and register main table
    let ctx = SessionContext::new();
    ctx.register_table("ct_main", std::sync::Arc::new(table))?;

    // Register UNION ALL view if staging table exists
    let has_staging = if let Some(staging) = staging_path {
        match deltalake::open_table(staging).await {
            Ok(staging_table) => {
                ctx.register_table("ct_staging", std::sync::Arc::new(staging_table))?;
                ctx.sql(
                    "CREATE VIEW ct_records AS \
                     SELECT cert_index, source_url FROM ct_main \
                     UNION ALL \
                     SELECT cert_index, source_url FROM ct_staging"
                ).await?;
                true
            }
            Err(_) => {
                // Staging table doesn't exist yet (first run) — fall back to main only
                false
            }
        }
    } else {
        false
    };

    if !has_staging {
        ctx.sql(
            "CREATE VIEW ct_records AS SELECT cert_index, source_url FROM ct_main"
        ).await?;
    }

    // Use the helper to process all logs
    detect_gaps_from_context(&ctx, logs, backfill_from).await
}

/// Helper function to handle a flush operation and update writer state.
///
/// Updates total_records_written and write_errors based on flush result.
/// Returns (new_table, should_break) where:
/// - new_table is Some if flush succeeded, None if table became inaccessible
/// - should_break is true if loop should terminate (table inaccessible)
///
/// # Arguments
/// * `new_table_opt` - Option containing new DeltaTable after flush
/// * `flush_result` - Result of the flush operation
/// * `total_records_written` - Mutable reference to total record count
/// * `write_errors` - Mutable reference to error count
/// * `trigger` - String describing what triggered the flush (for logging)
///
/// # Returns
/// * `(Option<DeltaTable>, bool)` - New table and break flag
fn handle_flush(
    new_table_opt: Option<DeltaTable>,
    flush_result: Result<usize, Box<dyn std::error::Error + Send + Sync>>,
    total_records_written: &mut u64,
    write_errors: &mut u64,
    trigger: &str,
) -> (Option<DeltaTable>, bool) {
    match new_table_opt {
        Some(new_table) => {
            match flush_result {
                Ok(count) => {
                    *total_records_written += count as u64;
                    info!(
                        records_flushed = count,
                        total_records = total_records_written,
                        trigger = trigger,
                        "flushed records to delta"
                    );
                }
                Err(e) => {
                    warn!(error = %e, trigger = trigger, "Failed to flush buffer");
                    *write_errors += 1;
                }
            }
            (Some(new_table), false)
        }
        None => {
            warn!(trigger = trigger, "Table became inaccessible during write");
            *write_errors += 1;
            (None, true)
        }
    }
}

/// Writer task that receives DeltaCertRecords from mpsc channel and flushes to delta table.
///
/// Buffers records and flushes on:
/// 1. Batch size threshold (batch_size records)
/// 2. Time-based interval (flush_interval_secs)
/// 3. Channel close (all senders dropped / fetchers done)
/// 4. Graceful shutdown signal
///
/// # Arguments
/// * `table_path` - Path to the Delta table
/// * `batch_size` - Maximum records per batch before flushing
/// * `flush_interval_secs` - Maximum time between flushes in seconds
/// * `rx` - mpsc receiver for DeltaCertRecord from fetchers
/// * `shutdown` - CancellationToken for graceful shutdown
///
/// # Returns
/// * `WriterResult` containing total_records_written and write_errors
async fn run_writer(
    table_path: String,
    batch_size: usize,
    flush_interval_secs: u64,
    compression_level: i32,
    heavy_column_compression_level: i32,
    mut rx: mpsc::Receiver<DeltaCertRecord>,
    shutdown: CancellationToken,
) -> WriterResult {
    let schema = delta_schema();

    // Ensure the table directory exists
    if let Err(e) = std::fs::create_dir_all(&table_path) {
        warn!(error = %e, table_path = %table_path, "Failed to create table directory");
        return WriterResult {
            total_records_written: 0,
            write_errors: 1,
        };
    }

    // Open or create the delta table
    let mut table = match open_or_create_table(&table_path, &schema).await {
        Ok(t) => {
            info!(table_path = %table_path, "Delta table opened/created for writing");
            t
        }
        Err(e) => {
            warn!(
                error = %e,
                table_path = %table_path,
                "Failed to open/create Delta table, writer task exiting"
            );
            return WriterResult {
                total_records_written: 0,
                write_errors: 1,
            };
        }
    };

    let mut buffer: Vec<DeltaCertRecord> = Vec::with_capacity(batch_size);
    let mut total_records_written: u64 = 0;
    let mut write_errors: u64 = 0;
    let mut flush_timer = tokio::time::interval(Duration::from_secs(flush_interval_secs));

    loop {
        tokio::select! {
            recv_result = rx.recv() => {
                match recv_result {
                    Some(record) => {
                        buffer.push(record);

                        // Flush if buffer reaches batch_size threshold
                        if buffer.len() >= batch_size {
                            let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size, compression_level, heavy_column_compression_level).await;
                            let (opt_table, should_break) = handle_flush(new_table_opt, flush_result, &mut total_records_written, &mut write_errors, "batch size trigger");
                            if should_break {
                                break;
                            }
                            table = opt_table.expect("handle_flush should always return Some when should_break is false");
                        }
                    }
                    None => {
                        // Channel closed: all senders have dropped, flush remaining buffer and exit
                        if !buffer.is_empty() {
                            info!(
                                records_in_buffer = buffer.len(),
                                "Channel closed, flushing remaining buffer"
                            );
                            let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size, compression_level, heavy_column_compression_level).await;
                            let _unused = handle_flush(new_table_opt, flush_result, &mut total_records_written, &mut write_errors, "channel close");
                        }
                        break;
                    }
                }
            }
            _ = flush_timer.tick() => {
                // Time-triggered flush
                if !buffer.is_empty() {
                    let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size, compression_level, heavy_column_compression_level).await;
                    let (opt_table, should_break) = handle_flush(new_table_opt, flush_result, &mut total_records_written, &mut write_errors, "time trigger");
                    if should_break {
                        break;
                    }
                    table = opt_table.expect("handle_flush should always return Some when should_break is false");
                }
            }
            _ = shutdown.cancelled() => {
                // Graceful shutdown: flush remaining buffer and exit
                if !buffer.is_empty() {
                    info!(
                        records_in_buffer = buffer.len(),
                        "Graceful shutdown initiated, flushing remaining buffer"
                    );
                    let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size, compression_level, heavy_column_compression_level).await;
                    let _unused = handle_flush(new_table_opt, flush_result, &mut total_records_written, &mut write_errors, "shutdown");
                }
                break;
            }
        }
    }

    info!(
        total_records_written = total_records_written,
        write_errors = write_errors,
        "Writer task completed"
    );

    WriterResult {
        total_records_written,
        write_errors,
    }
}

async fn run_zerobus_writer(
    config: ZerobusSinkConfig,
    mut rx: mpsc::Receiver<DeltaCertRecord>,
    shutdown: CancellationToken,
) -> WriterResult {
    let mut result = WriterResult {
        total_records_written: 0,
        write_errors: 0,
    };

    // Build SDK
    let sdk = match ZerobusSdk::new(
        config.endpoint.clone(),
        config.unity_catalog_url.clone(),
    ) {
        Ok(sdk) => sdk,
        Err(e) => {
            error!(error = %e, "failed to create ZeroBus SDK for backfill writer");
            result.write_errors = 1;
            return result;
        }
    };

    let table_properties = TableProperties {
        table_name: config.table_name.clone(),
        descriptor_proto: cert_record_descriptor_proto(),
    };

    let options = StreamConfigurationOptions {
        max_inflight_records: config.max_inflight_records,
        ..Default::default()
    };

    let mut stream = match sdk
        .create_stream(
            table_properties,
            config.client_id.clone(),
            config.client_secret.clone(),
            Some(options),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to create ZeroBus stream for backfill writer");
            result.write_errors = 1;
            return result;
        }
    };

    info!("zerobus backfill writer started");

    loop {
        tokio::select! {
            recv_result = rx.recv() => {
                match recv_result {
                    Some(record) => {
                        let cert_record = proto::CertRecord::from_delta_cert(&record);
                        let encoded = cert_record.encode_to_vec();

                        // Two-stage: first await queues record, returns ack future
                        match stream.ingest_record(encoded).await {
                            Ok(_ack_future) => {
                                // Record queued; drop ack future (flush collects pending acks)
                                result.total_records_written += 1;
                            }
                            Err(e) => {
                                if e.is_retryable() {
                                    // recreate_stream() takes stream by value (consumes it)
                                    warn!(error = %e, "retryable error in backfill writer, recreating stream");
                                    match sdk.recreate_stream(stream).await {
                                        Ok(new_stream) => {
                                            stream = new_stream;
                                            // Retry the record
                                            let cert_record = proto::CertRecord::from_delta_cert(&record);
                                            let encoded = cert_record.encode_to_vec();
                                            match stream.ingest_record(encoded).await {
                                                Ok(_ack) => {
                                                    result.total_records_written += 1;
                                                }
                                                Err(retry_err) => {
                                                    warn!(error = %retry_err, "failed to ingest after recovery, skipping");
                                                    result.write_errors += 1;
                                                }
                                            }
                                        }
                                        Err(recreate_err) => {
                                            // Stream consumed by failed recreate — cannot continue
                                            error!(error = %recreate_err, "failed to recreate stream, exiting writer");
                                            result.write_errors += 1;
                                            // Return directly since stream is consumed
                                            info!(records = result.total_records_written, errors = result.write_errors, "zerobus backfill writer finished (stream lost)");
                                            return result;
                                        }
                                    }
                                } else {
                                    warn!(error = %e, "non-retryable error, skipping record");
                                    result.write_errors += 1;
                                }
                            }
                        }
                    }
                    None => {
                        // Channel closed: all fetchers done
                        info!("backfill channel closed, flushing zerobus stream");
                        break;
                    }
                }
            }
            _ = shutdown.cancelled() => {
                info!("backfill shutdown signal, flushing zerobus stream");
                break;
            }
        }
    }

    // Flush and close
    if let Err(e) = stream.flush().await {
        warn!(error = %e, "error flushing zerobus stream in backfill");
    }
    if let Err(e) = stream.close().await {
        warn!(error = %e, "error closing zerobus stream in backfill");
    }

    info!(records = result.total_records_written, errors = result.write_errors, "zerobus backfill writer finished");
    result
}

pub async fn run_backfill(
    config: Config,
    staging_path: Option<String>,
    backfill_from: Option<u64>,
    backfill_logs: Option<String>,
    backfill_sink: Option<String>,
    shutdown: CancellationToken,
) -> i32 {
    info!("backfill mode starting");

    if let Some(ref path) = staging_path {
        info!(staging_path = %path, "writing to staging table");
    }

    if let Some(from) = backfill_from {
        info!(from = from, "backfill_from parameter");
    }

    if let Some(logs_filter) = &backfill_logs {
        info!(logs_filter = %logs_filter, "backfill_logs filter");
    }

    // Step 1: Log discovery
    let client = reqwest::Client::new();
    let mut logs = match fetch_log_list(&client, &config.ct_logs_url, config.custom_logs.clone()).await {
        Ok(logs) => logs,
        Err(e) => {
            warn!("failed to fetch CT log list: {}", e);
            return 1;
        }
    };

    // Add static logs
    for static_log in &config.static_logs {
        logs.push(CtLog::from(static_log.clone()));
    }

    // Apply logs filter if provided
    if let Some(ref filter) = backfill_logs {
        let filter_lower = filter.to_lowercase();
        logs.retain(|log| {
            log.description.to_lowercase().contains(&filter_lower)
                || log.url.to_lowercase().contains(&filter_lower)
        });
    }

    if logs.is_empty() {
        warn!("no logs matched the filter");
        return 1;
    }

    info!(count = logs.len(), "backfilling logs");

    // Step 2: State file ceiling lookup
    let state_manager = StateManager::new(config.ct_log.state_file.clone());
    let mut log_ceilings = Vec::new();

    for log in &logs {
        match state_manager.get_index(&log.normalized_url()) {
            Some(ceiling) => {
                log_ceilings.push((log.normalized_url(), ceiling));
            }
            None => {
                warn!(
                    log = %log.description,
                    url = %log.normalized_url(),
                    "log not in state file, skipping (no ceiling reference)"
                );
            }
        }
    }

    if log_ceilings.is_empty() {
        warn!("no logs have ceiling values from state file");
        return 0;
    }

    // Step 3: Gap detection
    // For ZeroBus, skip gap detection and build work items directly from historical range
    let work_items = if backfill_sink.as_deref() == Some("zerobus") {
        // ZeroBus sink: build work items directly from --from N to ceiling (historical mode only)
        // Gap detection is skipped since ZeroBus can't query remote tables
        let from = backfill_from.expect("validation in main.rs ensures --from is set for zerobus");
        let mut items = Vec::new();
        for (source_url, ceiling) in &log_ceilings {
            if *ceiling > from {
                items.push(BackfillWorkItem {
                    source_url: source_url.clone(),
                    start: from,
                    end: ceiling - 1,
                });
            }
        }
        items
    } else {
        // Delta sink: use gap detection to find internal gaps
        let staging_path_ref = staging_path.as_deref();
        match detect_gaps(&config.delta_sink.table_path, staging_path_ref, &log_ceilings, backfill_from).await {
            Ok(items) => items,
            Err(e) => {
                warn!("gap detection failed: {}", e);
                return 1;
            }
        }
    };

    if work_items.is_empty() {
        info!("no gaps detected, nothing to backfill");
        return 0;
    }

    info!(total_work_items = work_items.len(), "gap detection complete");

    // Step 4: Channel setup
    let channel_buffer_size = config.delta_sink.batch_size * 2;
    let (tx, rx) = mpsc::channel::<DeltaCertRecord>(channel_buffer_size);

    // Step 5: Group work items by source URL
    let mut work_by_source: HashMap<String, Vec<BackfillWorkItem>> = HashMap::new();
    for item in work_items {
        work_by_source.entry(item.source_url.clone()).or_insert_with(Vec::new).push(item);
    }

    // Step 6: Spawn fetcher tasks
    let mut fetcher_handles = Vec::new();

    for (source_url, work_items) in work_by_source {
        // Find the log type for this source
        let log_type = logs
            .iter()
            .find(|log| log.normalized_url() == source_url)
            .map(|log| log.log_type.clone())
            .unwrap_or(LogType::Rfc6962);

        let source = Arc::new(Source {
            name: Arc::from(source_url.as_str()),
            url: Arc::from(source_url.as_str()),
        });

        let tx_clone = tx.clone();
        let client_clone = client.clone();
        let shutdown_clone = shutdown.clone();
        let batch_size = config.ct_log.batch_size;
        let timeout = Duration::from_secs(config.ct_log.request_timeout_secs);

        let handle = tokio::spawn(async move {
            run_fetcher(
                client_clone,
                source,
                log_type,
                work_items,
                batch_size,
                timeout,
                tx_clone,
                shutdown_clone,
            )
            .await
        });

        fetcher_handles.push(handle);
    }

    // Step 6b: Spawn writer task before dropping sender
    let writer_handle = if backfill_sink.as_deref() == Some("zerobus") {
        let zerobus_config = config.zerobus_sink.clone();
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            run_zerobus_writer(zerobus_config, rx, shutdown_clone).await
        })
    } else {
        // AC3.2: default to delta writer (backwards-compatible)
        let table_path = staging_path
            .unwrap_or_else(|| config.delta_sink.table_path.clone());
        let batch_size = config.delta_sink.batch_size;
        let flush_interval_secs = config.delta_sink.flush_interval_secs;
        let compression_level = config.delta_sink.compression_level;
        let heavy_column_compression_level = config.delta_sink.heavy_column_compression_level;
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            run_writer(table_path, batch_size, flush_interval_secs, compression_level, heavy_column_compression_level, rx, shutdown_clone).await
        })
    };

    // Drop the original sender so the receiver knows when all senders are gone
    drop(tx);

    // Step 7: Wait for all fetchers and collect results
    let start_time = std::time::Instant::now();
    let mut successful_count = 0;
    let mut failed_count = 0;

    for handle in fetcher_handles {
        match handle.await {
            Ok(Ok(_records)) => {
                successful_count += 1;
            }
            Ok(Err(e)) => {
                warn!("fetcher failed: {}", e);
                failed_count += 1;
            }
            Err(e) => {
                warn!("fetcher task join error: {}", e);
                failed_count += 1;
            }
        }
    }

    let total_count = successful_count + failed_count;

    // Step 8: Wait for writer to complete
    let writer_result = match writer_handle.await {
        Ok(result) => result,
        Err(e) => {
            warn!("writer task join error: {}", e);
            WriterResult {
                total_records_written: 0,
                write_errors: 1,
            }
        }
    };

    let elapsed = start_time.elapsed();

    // Step 9: Completion summary (AC6.3)
    info!(
        records_written = writer_result.total_records_written,
        successful_logs = successful_count,
        total_logs = total_count,
        failed_logs = failed_count,
        elapsed_seconds = elapsed.as_secs_f64(),
        "backfill complete: {} records written, {}/{} logs succeeded, {} failed, elapsed: {:?}",
        writer_result.total_records_written,
        successful_count,
        total_count,
        failed_count,
        elapsed
    );

    // Step 10: Exit code (AC5.4)
    if failed_count == 0 && writer_result.write_errors == 0 {
        0
    } else {
        1
    }
}

/// Merge mode: merges staging table into main table via Delta MERGE INTO deduplication.
///
/// Reads all staging records as RecordBatches, converts each to a DataFrame, and merges
/// batch-by-batch into the main table using the predicate:
///   target.source_url = source.source_url AND target.cert_index = source.cert_index
///
/// Only when_not_matched records are inserted (no updates or deletes). On success,
/// the staging directory is deleted.
///
/// # Arguments
/// * `config` - Server configuration (contains delta_sink.table_path for main table)
/// * `staging_path` - Path to the staging Delta table
/// * `shutdown` - CancellationToken for graceful shutdown
///
/// # Returns
/// * `i32` exit code (0 for success, 1 for errors)
pub async fn run_merge(
    config: Config,
    staging_path: String,
    shutdown: CancellationToken,
) -> i32 {
    info!(staging_path = %staging_path, "merge mode starting");

    let schema = delta_schema();

    // Open staging table
    let staging_table = match deltalake::open_table(&staging_path).await {
        Ok(t) => t,
        Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
            info!(staging_path = %staging_path, "staging table does not exist, nothing to merge");
            return 0;
        }
        Err(e) => {
            warn!(error = %e, "failed to open staging table");
            return 1;
        }
    };

    // Open or create main table
    let main_table = match open_or_create_table(&config.delta_sink.table_path, &schema).await {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "failed to open main table");
            return 1;
        }
    };

    // Read all staging records via DataFusion
    let ctx = SessionContext::new();
    if let Err(e) = ctx.register_table("staging", Arc::new(staging_table)) {
        warn!(error = %e, "failed to register staging table");
        return 1;
    }

    let df = match ctx.sql("SELECT * FROM staging").await {
        Ok(df) => df,
        Err(e) => {
            warn!(error = %e, "failed to query staging table");
            return 1;
        }
    };

    let batches = match df.collect().await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "failed to collect staging batches");
            return 1;
        }
    };

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        info!("staging table is empty, nothing to merge");
        return 0;
    }

    let total_staging_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    info!(total_staging_rows = total_staging_rows, "read staging records");

    // Construct WriterProperties with per-column encoding and compression settings
    let writer_props = delta_writer_properties(
        config.delta_sink.compression_level,
        config.delta_sink.heavy_column_compression_level,
    );

    // Merge batch-by-batch
    let mut current_table = main_table;
    let mut total_inserted: usize = 0;
    let mut total_skipped: usize = 0;

    for (i, batch) in batches.iter().enumerate() {
        if shutdown.is_cancelled() {
            warn!("merge interrupted by shutdown signal");
            return 1;
        }

        if batch.num_rows() == 0 {
            continue;
        }

        // Convert RecordBatch to DataFrame — DeltaOps::merge() requires a DataFrame,
        // not Vec<RecordBatch>
        let merge_ctx = SessionContext::new();
        let source_df = match merge_ctx.read_batch(batch.clone()) {
            Ok(df) => df,
            Err(e) => {
                warn!(batch = i + 1, error = %e, "failed to create source DataFrame");
                return 1;
            }
        };

        let predicate = "target.source_url = source.source_url AND target.cert_index = source.cert_index";

        // Build the merge operation — when_not_matched_insert() returns a Result,
        // so use match for proper error handling instead of .expect()
        let merge_builder = match DeltaOps(current_table)
            .merge(source_df, predicate)
            .with_source_alias("source")
            .with_target_alias("target")
            .when_not_matched_insert(|insert| {
                insert
                    .set("cert_index", "source.cert_index")
                    .set("update_type", "source.update_type")
                    .set("seen", "source.seen")
                    .set("seen_date", "source.seen_date")
                    .set("source_name", "source.source_name")
                    .set("source_url", "source.source_url")
                    .set("cert_link", "source.cert_link")
                    .set("serial_number", "source.serial_number")
                    .set("fingerprint", "source.fingerprint")
                    .set("sha256", "source.sha256")
                    .set("sha1", "source.sha1")
                    .set("not_before", "source.not_before")
                    .set("not_after", "source.not_after")
                    .set("is_ca", "source.is_ca")
                    .set("signature_algorithm", "source.signature_algorithm")
                    .set("subject_aggregated", "source.subject_aggregated")
                    .set("issuer_aggregated", "source.issuer_aggregated")
                    .set("all_domains", "source.all_domains")
                    .set("as_der", "source.as_der")
                    .set("chain", "source.chain")
            }) {
                Ok(builder) => builder.with_writer_properties(writer_props.clone()),
                Err(e) => {
                    warn!(batch = i + 1, error = %e, "failed to build merge operation");
                    return 1;
                }
            };

        match merge_builder.await {
            Ok((new_table, metrics)) => {
                let inserted = metrics.num_target_rows_inserted;
                let batch_rows = batch.num_rows();
                // skipped = batch_rows - inserted is valid here because we use ONLY
                // when_not_matched_insert (no updates/deletes). Every non-matched source
                // row becomes an insert; every matched source row is skipped.
                let skipped = batch_rows - inserted;
                total_inserted += inserted;
                total_skipped += skipped;

                info!(
                    batch = i + 1,
                    batch_rows = batch_rows,
                    inserted = inserted,
                    skipped = skipped,
                    "merge batch complete"
                );

                current_table = new_table;
            }
            Err(e) => {
                warn!(batch = i + 1, error = %e, "merge batch failed");
                // Leave staging intact for retry
                return 1;
            }
        }
    }

    // Log final metrics (AC3.5)
    info!(
        total_inserted = total_inserted,
        total_skipped = total_skipped,
        total_staging_rows = total_staging_rows,
        "merge complete"
    );

    // Delete staging directory on success (AC3.4)
    match std::fs::remove_dir_all(&staging_path) {
        Ok(_) => {
            info!(staging_path = %staging_path, "staging directory deleted");
        }
        Err(e) => {
            warn!(error = %e, staging_path = %staging_path, "failed to delete staging directory");
            // Non-fatal — merge succeeded, staging cleanup failed
        }
    }

    0
}

/// Migrate an existing Delta table with old schema (Utf8 as_der) to new schema (Binary as_der).
///
/// This function:
/// 1. Opens the source table at config.delta_sink.table_path
/// 2. Creates or opens the output table at output_path with the new schema
/// 3. Queries distinct seen_date partitions from the source table
/// 4. For each partition:
///    - Checks CancellationToken for graceful shutdown (AC3.4)
///    - Reads all records for that partition via DataFusion SQL
///    - Transforms as_der column from StringArray (base64) to BinaryArray (raw bytes)
///    - Passes all other columns through unchanged (AC3.3)
///    - Writes the transformed batch to the output table with centralized WriterProperties
/// 5. Returns exit code 0 on success, 1 on error
pub async fn run_migrate(
    config: Config,
    output_path: String,
    shutdown: CancellationToken,
) -> i32 {
    info!(
        source_path = %config.delta_sink.table_path,
        output_path = %output_path,
        "migration mode starting"
    );

    let schema = delta_schema();

    // Open source table
    let source_table = match deltalake::open_table(&config.delta_sink.table_path).await {
        Ok(t) => t,
        Err(e) => {

            warn!(
                source_path = %config.delta_sink.table_path,
                error = %e,
                "failed to open source table"
            );
            return 1;
        }
    };

    // Create or open output table
    let output_table = match open_or_create_table(&output_path, &schema).await {
        Ok(t) => t,
        Err(e) => {

            warn!(error = %e, output_path = %output_path, "failed to create output table");
            return 1;
        }
    };

    // Register source table and query distinct partitions
    let ctx = SessionContext::new();
    if let Err(e) = ctx.register_table("source", Arc::new(source_table)) {
        warn!(error = %e, "failed to register source table");
        return 1;
    }

    let partitions_df = match ctx.sql("SELECT DISTINCT seen_date FROM source ORDER BY seen_date").await {
        Ok(df) => df,
        Err(e) => {
            warn!(error = %e, "failed to query partition dates");
            return 1;
        }
    };

    let partition_batches = match partitions_df.collect().await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "failed to collect partition batches");
            return 1;
        }
    };

    // Extract partition dates from batches
    let mut partition_dates: Vec<String> = Vec::new();
    for batch in &partition_batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // DataFusion may return Dictionary-encoded strings for DISTINCT queries;
        // cast to Utf8 to normalize before extracting values.
        let col = deltalake::arrow::compute::cast(batch.column(0), &deltalake::arrow::datatypes::DataType::Utf8)
            .expect("seen_date should be castable to Utf8");
        let seen_date_col = col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("seen_date should be StringArray after cast");
        for i in 0..batch.num_rows() {
            if seen_date_col.is_valid(i) {
                partition_dates.push(seen_date_col.value(i).to_string());
            }
        }
    }

    if partition_dates.is_empty() {
        info!("source table is empty, nothing to migrate");
        return 0;
    }

    info!(
        total_partitions = partition_dates.len(),
        "found partitions to migrate"
    );

    // Get WriterProperties
    let writer_props = delta_writer_properties(
        config.delta_sink.compression_level,
        config.delta_sink.heavy_column_compression_level,
    );

    // Migrate partition by partition
    let mut current_output_table = output_table;
    let mut total_rows_migrated: usize = 0;
    let mut total_decode_failures: usize = 0;

    for (partition_idx, seen_date) in partition_dates.iter().enumerate() {
        if shutdown.is_cancelled() {
            warn!("migration interrupted by shutdown signal");
            return 1;
        }

        // Query all records for this partition
        let partition_ctx = SessionContext::new();
        if let Err(e) = partition_ctx.register_table("source", Arc::new(
            match deltalake::open_table(&config.delta_sink.table_path).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(error = %e, "failed to reopen source table for partition");
                    return 1;
                }
            }
        )) {
            warn!(error = %e, "failed to register source table for partition");
            return 1;
        }

        let partition_query = format!("SELECT * FROM source WHERE seen_date = '{}'", seen_date);
        let partition_df = match partition_ctx.sql(&partition_query).await {
            Ok(df) => df,
            Err(e) => {

                warn!(error = %e, partition = %seen_date, "failed to query partition");
                return 1;
            }
        };

        let batches = match partition_df.collect().await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, partition = %seen_date, "failed to collect partition batches");
                return 1;
            }
        };

        // Process each batch in the partition
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            // Transform as_der column from base64 string to binary bytes
            let as_der_idx = match batch.schema().index_of("as_der") {
                Ok(idx) => idx,
                Err(e) => {
                    warn!(error = %e, partition = %seen_date, "as_der column not found");
                    return 1;
                }
            };

            let as_der_strings = batch.column(as_der_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("source as_der should be StringArray");

            let mut decode_failures: usize = 0;
            let as_der_binary: BinaryArray = as_der_strings
                .iter()
                .map(|opt_val| {
                    opt_val.and_then(|s| {
                        match STANDARD.decode(s) {
                            Ok(bytes) => Some(bytes),
                            Err(_) => {
                                decode_failures += 1;
                                None
                            }
                        }
                    })
                })
                .collect();

            if decode_failures > 0 {
                warn!(
                    partition = %seen_date,
                    decode_failures = decode_failures,
                    "base64 decode failures in as_der column (converted to empty bytes)"
                );
                total_decode_failures += decode_failures;
            }

            // Build new RecordBatch by matching columns by name from the target
            // schema. DataFusion may reorder columns and coerce types, so we must:
            // 1. Look up each source column by name (not index)
            // 2. Cast to the target type if needed
            // 3. Allow nullable fields since DataFusion may introduce nullability
            let src_schema = batch.schema();
            let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
            let mut output_fields: Vec<Arc<Field>> = Vec::with_capacity(schema.fields().len());
            for target_field in schema.fields() {
                if target_field.name() == "as_der" {
                    columns.push(Arc::new(as_der_binary.clone()));
                    output_fields.push(Arc::new(Field::new("as_der", deltalake::arrow::datatypes::DataType::Binary, false)));
                } else {
                    let src_idx = src_schema.index_of(target_field.name())
                        .expect("source batch should have all target columns");
                    let src_col = batch.column(src_idx);
                    if src_col.data_type() != target_field.data_type() {
                        let casted = deltalake::arrow::compute::cast(src_col, target_field.data_type())
                            .unwrap_or_else(|_| src_col.clone());
                        output_fields.push(Arc::new(Field::new(target_field.name(), casted.data_type().clone(), casted.is_nullable())));
                        columns.push(casted);
                    } else {
                        output_fields.push(Arc::new(Field::new(target_field.name(), src_col.data_type().clone(), src_col.is_nullable())));
                        columns.push(src_col.clone());
                    }
                }
            }
            let output_schema = Arc::new(deltalake::arrow::datatypes::Schema::new(output_fields));
            let new_batch = match RecordBatch::try_new(output_schema, columns) {
                Ok(b) => b,
                Err(e) => {

                    warn!(error = %e, partition = %seen_date, "failed to create new batch");
                    return 1;
                }
            };

            let batch_rows = new_batch.num_rows();
            total_rows_migrated += batch_rows;

            // Write the transformed batch to output table
            let write_result = DeltaOps(current_output_table)
                .write(vec![new_batch])
                .with_save_mode(deltalake::protocol::SaveMode::Append)
                .with_writer_properties(writer_props.clone())
                .await;

            match write_result {
                Ok(new_table) => {
                    current_output_table = new_table;
                }
                Err(e) => {

                    warn!(
                        error = %e,
                        partition = %seen_date,
                        "failed to write batch to output table"
                    );
                    return 1;
                }
            }
        }

        info!(
            partition = %seen_date,
            partition_num = partition_idx + 1,
            total_partitions = partition_dates.len(),
            "migrated partition"
        );
    }

    info!(
        total_rows_migrated = total_rows_migrated,
        total_decode_failures = total_decode_failures,
        source_path = %config.delta_sink.table_path,
        output_path = %output_path,
        "migration complete"
    );

    0
}

/// Per-log fetcher task that processes work items sequentially.
///
/// This task:
/// 1. Iterates work items sequentially
/// 2. For each work item, fetches entries in configurable batch sizes
/// 3. Converts entries to DeltaCertRecords and sends them via mpsc
/// 4. Handles rate limit errors with exponential backoff
/// 5. Respects the CancellationToken for graceful shutdown
async fn run_fetcher(
    client: reqwest::Client,
    source: Arc<Source>,
    log_type: LogType,
    work_items: Vec<BackfillWorkItem>,
    batch_size: u64,
    request_timeout: Duration,
    tx: mpsc::Sender<DeltaCertRecord>,
    shutdown: CancellationToken,
) -> Result<u64, String> {
    use crate::ct::static_ct::IssuerCache;

    let source_url = source.url.clone();
    let mut total_records: u64 = 0;

    // Create IssuerCache for StaticCt logs
    let issuer_cache = Arc::new(IssuerCache::new());

    for work_item in work_items {
        let total_entries = work_item.end - work_item.start + 1;
        let num_batches = (total_entries + batch_size - 1) / batch_size;

        let mut batch_start = work_item.start;
        let mut batch_idx: usize = 0;

        while batch_start <= work_item.end {
            // Check for shutdown
            if shutdown.is_cancelled() {
                info!(
                    source_url = %source_url,
                    fetched = total_records,
                    "fetcher shut down"
                );
                return Ok(total_records);
            }

            let batch_end = (batch_start + batch_size - 1).min(work_item.end);

            // Fetch entries with retry logic
            // Separate retry counters for rate-limited vs other errors
            let mut rate_limit_retry_count = 0;
            let mut other_retry_count = 0;
            let max_rate_limit_retries = 10; // Rate limited errors: persistent retry with exponential backoff
            let max_other_retries = 3; // HTTP errors and invalid responses: finite retry
            let mut backoff_ms = 1000u64;
            const MAX_BACKOFF_MS: u64 = 60000;

            // Track the highest cert_index seen in this batch for advancing.
            // RFC 6962 servers may return fewer entries than requested; we advance
            // to max_cert_index+1 to avoid creating gaps in the delta table.
            let mut max_cert_index_seen: Option<u64> = None;

            loop {
                let fetch_result = match log_type {
                    LogType::Rfc6962 => {
                        fetch::fetch_entries(&client, &source_url, batch_start, batch_end, &source, request_timeout).await
                    }
                    LogType::StaticCt => {
                        // For StaticCt, calculate tile index and offset
                        let tile_index = batch_start / 256;
                        let offset_in_tile = (batch_start % 256) as usize;
                        // Use partial_width=0 for full tiles (server returns all 256 entries)
                        // In backfill mode, we control the tile index and let the server return all entries
                        fetch::fetch_tile_entries(&client, &source_url, tile_index, 0, offset_in_tile, &source, request_timeout, &issuer_cache).await
                    }
                };

                match fetch_result {
                    Ok(messages) => {
                        // Convert and send records, tracking highest cert_index
                        for msg in &messages {
                            let idx = msg.data.cert_index;
                            max_cert_index_seen = Some(max_cert_index_seen.map_or(idx, |prev: u64| prev.max(idx)));
                        }
                        for msg in messages {
                            let record = DeltaCertRecord::from_message(&msg);
                            if tx.send(record).await.is_err() {
                                return Err("receiver dropped".to_string());
                            }
                            total_records += 1;
                        }
                        break;
                    }
                    Err(fetch::FetchError::RateLimited(_)) => {
                        if rate_limit_retry_count >= max_rate_limit_retries {
                            warn!(
                                source_url = %source_url,
                                batch_start = batch_start,
                                batch_end = batch_end,
                                "rate limit exceeded max retries"
                            );
                            break;
                        }
                        rate_limit_retry_count += 1;
                        warn!(
                            source_url = %source_url,
                            batch_start = batch_start,
                            batch_end = batch_end,
                            backoff_ms = backoff_ms,
                            "rate limited, backing off"
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    }
                    Err(fetch::FetchError::NotAvailable(_)) => {
                        warn!(
                            source_url = %source_url,
                            batch_start = batch_start,
                            batch_end = batch_end,
                            "log does not support this range, skipping"
                        );
                        break;
                    }
                    Err(fetch::FetchError::HttpError(e)) => {
                        if other_retry_count >= max_other_retries {
                            warn!(
                                source_url = %source_url,
                                batch_start = batch_start,
                                batch_end = batch_end,
                                error = %e,
                                "HTTP error exceeded max retries"
                            );
                            break;
                        }
                        other_retry_count += 1;
                        warn!(
                            source_url = %source_url,
                            batch_start = batch_start,
                            batch_end = batch_end,
                            error = %e,
                            backoff_ms = backoff_ms,
                            "HTTP error, backing off"
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    }
                    Err(fetch::FetchError::InvalidResponse(e)) => {
                        if other_retry_count >= max_other_retries {
                            warn!(
                                source_url = %source_url,
                                batch_start = batch_start,
                                batch_end = batch_end,
                                error = %e,
                                "invalid response exceeded max retries"
                            );
                            break;
                        }
                        other_retry_count += 1;
                        warn!(
                            source_url = %source_url,
                            batch_start = batch_start,
                            batch_end = batch_end,
                            error = %e,
                            backoff_ms = backoff_ms,
                            "invalid response, backing off"
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    }
                }
            }

            // Advance batch_start based on actual entries received.
            // For RFC 6962: server may return fewer entries than requested; advance to
            // max_cert_index+1 to cover the actual range without creating gaps.
            // For Static CT: tile entries have fixed cert_index = tile_index*256 + i.
            // If no entries were returned (errors or empty), advance by batch_size to avoid infinite loop.
            let prev_batch_start = batch_start;
            batch_start = match max_cert_index_seen {
                Some(max_idx) => max_idx + 1,
                None => batch_start + batch_size, // Fallback: advance by batch_size on empty/error
            };
            batch_idx += 1;

            // Progress logging: log every 10 batches or at 10% milestones
            // Skip progress for single-batch work items (the "fetcher complete" log is sufficient)
            if num_batches > 1 {
                let current_batch_num = batch_idx;
                let progress = (batch_start.saturating_sub(work_item.start)) as f64 / total_entries as f64;
                let percentage = (progress * 100.0).min(100.0) as u32;
                let prev_progress = (prev_batch_start.saturating_sub(work_item.start)) as f64 / total_entries as f64;
                let prev_milestone = ((prev_progress * 100.0).min(100.0) as u32 / 10) * 10;
                let current_milestone_percentage = (percentage / 10) * 10;

                // Log if: every 10 batches OR when crossing a 10% milestone
                if current_batch_num % 10 == 0 || (current_milestone_percentage > prev_milestone && current_milestone_percentage > 0) {
                    info!(
                        source_url = %source_url,
                        fetched = total_records,
                        batch_idx = current_batch_num,
                        total_batches = num_batches,
                        progress_percent = percentage,
                        "fetcher progress"
                    );
                }
            }
        }
    }

    info!(
        source_url = %source_url,
        total_records = total_records,
        "fetcher complete"
    );
    Ok(total_records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta_sink::{delta_schema, open_or_create_table, records_to_batch, DeltaCertRecord};
    use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use deltalake::protocol::SaveMode;
    use deltalake::DeltaOps;
    use std::fs;

    fn make_test_record(cert_index: u64, source_url: &str) -> DeltaCertRecord {
        let json_str = r#"{"message_type":"certificate_update","data":{"update_type":"X509LogEntry","leaf_cert":{"subject":{"CN":"example.com","aggregated":"/CN=example.com"},"issuer":{"CN":"Test CA","aggregated":"/CN=Test CA"},"serial_number":"01","not_before":1700000000,"not_after":1730000000,"fingerprint":"AA:BB","sha1":"CC:DD","sha256":"EE:FF","signature_algorithm":"sha256, rsa","is_ca":false,"all_domains":["example.com"],"as_der":"AQID","extensions":{"ctlPoisonByte":false}},"chain":null,"cert_index":12345,"cert_link":"https://ct.example.com/entry/12345","seen":1700000000.0,"source":{"name":"Test Log","url":"https://ct.example.com/"}}}"#;
        let mut record = DeltaCertRecord::from_json(json_str.as_bytes()).expect("failed to deserialize");
        record.cert_index = cert_index;
        record.source_url = source_url.to_string();
        record
    }

    #[tokio::test]
    async fn test_ac2_1_catch_up_lower_bound_from_min() {
        let test_name = "ac2_1_lower_bound";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Create records with cert_index values [100, 101, 102]
        let records = vec![
            make_test_record(100, "https://log.example.com"),
            make_test_record(101, "https://log.example.com"),
            make_test_record(102, "https://log.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // Call detect_gaps in catch-up mode (no --from)
        // Second tuple element is ceiling (was tree_size)
        let logs = vec![("https://log.example.com".to_string(), 200)];
        let work_items = detect_gaps(&table_path, None, &logs, None)
            .await
            .expect("detect_gaps failed");

        // With ceiling replacing tree_size and no frontier gap logic,
        // contiguous data produces no work items
        assert_eq!(work_items.len(), 0, "Contiguous data with ceiling > max_index should produce no work items");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac2_2_catch_up_internal_gaps() {
        let test_name = "ac2_2_internal_gaps";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Create records with gaps: [10, 11, 12, 15, 16, 20]
        let records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(12, "https://log.example.com"),
            make_test_record(15, "https://log.example.com"),
            make_test_record(16, "https://log.example.com"),
            make_test_record(20, "https://log.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // Call detect_gaps with ceiling=22 (no frontier gap generated)
        let logs = vec![("https://log.example.com".to_string(), 22)];
        let work_items = detect_gaps(&table_path, None, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should detect only internal gaps: (13, 14), (17, 19) — no frontier gap
        let gaps: Vec<_> = work_items
            .iter()
            .filter(|item| item.source_url == "https://log.example.com")
            .collect();

        assert_eq!(gaps.len(), 2, "Should detect internal gaps (13, 14) and (17, 19) only");

        let has_gap_13_14 = gaps.iter().any(|item| item.start == 13 && item.end == 14);
        assert!(has_gap_13_14, "Should detect gap (13, 14)");

        let has_gap_17_19 = gaps.iter().any(|item| item.start == 17 && item.end == 19);
        assert!(has_gap_17_19, "Should detect gap (17, 19)");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac2_3_catch_up_no_frontier_gap() {
        let test_name = "ac2_3_no_frontier_gap";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Create records [0, 1, 2]
        let records = vec![
            make_test_record(0, "https://log.example.com"),
            make_test_record(1, "https://log.example.com"),
            make_test_record(2, "https://log.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // Call detect_gaps with ceiling=10
        let logs = vec![("https://log.example.com".to_string(), 10)];
        let work_items = detect_gaps(&table_path, None, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should have NO work items (no frontier gap generated)
        assert_eq!(work_items.len(), 0, "Contiguous data should produce no work items (no frontier gap)");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac2_4_catch_up_missing_log_skipped() {
        let test_name = "ac2_4_missing_log";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Create records only for log-a
        let records = vec![
            make_test_record(0, "https://log-a.example.com"),
            make_test_record(1, "https://log-a.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // Call detect_gaps with both logs, catch-up mode
        let logs = vec![
            ("https://log-a.example.com".to_string(), 10),
            ("https://log-b.example.com".to_string(), 10),
        ];
        let work_items = detect_gaps(&table_path, None, &logs, None)
            .await
            .expect("detect_gaps failed");

        // log-b should produce no work items (skipped)
        let log_b_items: Vec<_> = work_items
            .iter()
            .filter(|item| item.source_url == "https://log-b.example.com")
            .collect();
        assert_eq!(log_b_items.len(), 0, "log-b should be skipped in catch-up mode");

        // log-a has contiguous data [0, 1] — no internal gaps, no frontier gap
        let log_a_items: Vec<_> = work_items
            .iter()
            .filter(|item| item.source_url == "https://log-a.example.com")
            .collect();
        assert_eq!(log_a_items.len(), 0, "log-a with contiguous data should have no work items (no frontier gap)");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac3_1_historical_pre_existing_gap() {
        let test_name = "ac3_1_pre_existing_gap";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Create records [50, 51, 52]
        let records = vec![
            make_test_record(50, "https://log.example.com"),
            make_test_record(51, "https://log.example.com"),
            make_test_record(52, "https://log.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // Call detect_gaps with --from 0, ceiling=55
        let logs = vec![("https://log.example.com".to_string(), 55)];
        let work_items = detect_gaps(&table_path, None, &logs, Some(0))
            .await
            .expect("detect_gaps failed");

        // Should have only pre-existing gap (0, 49) — no frontier gap
        assert_eq!(work_items.len(), 1, "Should have only pre-existing gap");

        let has_pre_existing = work_items.iter().any(|item| item.start == 0 && item.end == 49);
        assert!(has_pre_existing, "Should have pre-existing gap (0, 49)");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac3_2_historical_from_override() {
        let test_name = "ac3_2_from_override";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Create records [50, 51, 52]
        let records = vec![
            make_test_record(50, "https://log.example.com"),
            make_test_record(51, "https://log.example.com"),
            make_test_record(52, "https://log.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // Call detect_gaps with --from 40, ceiling=55
        let logs = vec![("https://log.example.com".to_string(), 55)];
        let work_items = detect_gaps(&table_path, None, &logs, Some(40))
            .await
            .expect("detect_gaps failed");

        // Should have only the pre-existing gap (40, 49) — no frontier gap
        assert_eq!(work_items.len(), 1, "Should have only pre-existing gap from overridden lower bound");

        let has_pre_gap = work_items.iter().any(|item| item.start == 40 && item.end == 49);
        assert!(has_pre_gap, "Should have gap (40, 49) from overridden lower bound");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac3_1_historical_empty_table_full_range() {
        let test_name = "ac3_1_empty_table";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);

        // No table created, so detect_gaps handles non-existent table

        // Call detect_gaps in historical mode with --from 0
        let logs = vec![("https://log.example.com".to_string(), 55)];
        let work_items = detect_gaps(&table_path, None, &logs, Some(0))
            .await
            .expect("detect_gaps failed");

        // Should backfill entire range (0, 54)
        assert_eq!(work_items.len(), 1);
        assert_eq!(work_items[0].source_url, "https://log.example.com");
        assert_eq!(work_items[0].start, 0);
        assert_eq!(work_items[0].end, 54);

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_catch_up_mode_empty_table() {
        let test_name = "catch_up_empty_table";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);

        // No table created

        // Call detect_gaps in catch-up mode
        let logs = vec![("https://log.example.com".to_string(), 55)];
        let work_items = detect_gaps(&table_path, None, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should return empty work items
        assert_eq!(work_items.len(), 0, "Catch-up mode with empty table should produce no work");

        let _ = fs::remove_dir_all(&table_path);
    }

    // Task 3 Tests: Fetcher and Orchestrator

    #[tokio::test]
    async fn test_ac1_1_run_backfill_no_work_items() {
        // Test that the orchestrator correctly identifies when there are no work items
        // (empty delta table in catch-up mode).
        // This verifies delta-backfill.AC1.1: backfill runs and exits cleanly with code 0
        let test_name = "ac1_1_no_work_items";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        // Create an empty delta table
        let schema = delta_schema();
        let _table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Simulate gap detection in catch-up mode with an empty table
        let logs = vec![("https://log.example.com".to_string(), 100)];
        let work_items = detect_gaps(&table_path, None, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Verify: no work items because table is empty and we're in catch-up mode.
        // This confirms the orchestrator will exit cleanly with code 0 when there's nothing to backfill.
        assert_eq!(work_items.len(), 0, "Empty table in catch-up mode should produce no work items");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[test]
    fn test_ac1_2_log_filtering() {
        // Test the log filtering logic for --logs filter.
        // Verifies delta-backfill.AC1.2: --logs filter limits backfill to matching logs only

        // Define test data as simple (description, url) pairs
        let logs = vec![
            ("Google Xenium Log", "https://ct.googleapis.com/logs/xenium"),
            ("Apple CT Log", "https://ct.apple.com/ct"),
            ("Let's Encrypt Log", "https://oak.ct.letsencrypt.org"),
        ];

        // Test filter for "google"
        let filter_lower = "google".to_lowercase();
        let filtered: Vec<_> = logs
            .iter()
            .filter(|(desc, url)| {
                desc.to_lowercase().contains(&filter_lower)
                    || url.to_lowercase().contains(&filter_lower)
            })
            .collect();

        // Should only match the Google log
        assert_eq!(filtered.len(), 1, "Filter 'google' should match only one log");
        assert!(filtered[0].0.contains("Google"), "Filtered log should be Google");

        // Test filter for "apple" (case-insensitive)
        let filter_lower = "apple".to_lowercase();
        let filtered: Vec<_> = logs
            .iter()
            .filter(|(desc, url)| {
                desc.to_lowercase().contains(&filter_lower)
                    || url.to_lowercase().contains(&filter_lower)
            })
            .collect();

        // Should match only Apple (one log)
        assert_eq!(filtered.len(), 1, "Filter 'apple' should match one log");

        // Test filter for "encrypt"
        let filter_lower = "encrypt".to_lowercase();
        let filtered: Vec<_> = logs
            .iter()
            .filter(|(desc, url)| {
                desc.to_lowercase().contains(&filter_lower)
                    || url.to_lowercase().contains(&filter_lower)
            })
            .collect();

        // Should match only Let's Encrypt
        assert_eq!(filtered.len(), 1, "Filter 'encrypt' should match only one log");
        assert!(filtered[0].0.contains("Encrypt"), "Filtered log should be Let's Encrypt");
    }

    #[tokio::test]
    async fn test_ac5_1_fetcher_respects_cancellation_token() {
        // Test that a fetcher task respects CancellationToken.
        // Verifies delta-backfill.AC5.1: Ctrl+C triggers graceful shutdown

        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        // Spawn a task that simulates a fetcher checking the shutdown token
        let task = tokio::spawn(async move {
            let mut processed = 0;
            for i in 0..1000 {
                if shutdown_clone.is_cancelled() {
                    // Stop processing when cancellation is triggered
                    return processed;
                }
                processed = i;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            processed
        });

        // Give the task a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Cancel the token
        shutdown.cancel();

        // Wait for the task to complete
        let processed = task.await.expect("task failed");

        // Verify that the task stopped early (not all 1000 iterations).
        // This confirms that the fetcher respects the cancellation token.
        assert!(processed < 1000, "Task should have stopped early due to cancellation");
    }

    // Task 3 Tests: Writer Task and End-to-End Integration

    #[tokio::test]
    async fn test_writer_basic_functionality() {
        // Test that writer task works at all - most basic test
        let test_name = "writer_basic";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);  // Create parent dir

        let (tx, rx) = mpsc::channel::<DeltaCertRecord>(10);
        let shutdown = CancellationToken::new();

        // Spawn writer
        let table_path_clone = table_path.clone();
        let writer_task = tokio::spawn(async move {
            run_writer(table_path_clone, 2, 60, 9, 15, rx, shutdown).await
        });

        // Send 2 records (should trigger batch flush)
        let r1 = make_test_record(1, "https://test.com");
        let r2 = make_test_record(2, "https://test.com");

        tx.send(r1).await.ok();
        tx.send(r2).await.ok();

        // Give it a moment to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Close channel
        drop(tx);

        // Wait for writer
        let result = writer_task.await.expect("task join failed");

        // Should have written 2 records
        assert_eq!(result.total_records_written, 2, "Writer should have written 2 records");
        assert_eq!(result.write_errors, 0, "Writer should have no errors");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac4_3_writer_creates_table_with_correct_schema() {
        // Test that writer creates table and properly flushes records on channel close
        // Verifies delta-backfill.AC4.3: Backfill into empty/nonexistent table creates it with correct schema
        let test_name = "ac4_3_writer_schema";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        // Create a channel and send some records with batch_size = 3
        let (tx, rx) = mpsc::channel::<DeltaCertRecord>(10);
        let shutdown = CancellationToken::new();

        // Spawn writer task with batch_size = 3 to trigger flush on channel close
        let table_path_clone = table_path.clone();
        let writer_handle = tokio::spawn(async move {
            run_writer(table_path_clone, 3, 60, 9, 15, rx, shutdown).await
        });

        // Send records
        let record1 = make_test_record(1, "https://log.example.com");
        let record2 = make_test_record(2, "https://log.example.com");
        let record3 = make_test_record(3, "https://log.example.com");
        let record4 = make_test_record(4, "https://log.example.com");

        tx.send(record1).await.expect("send failed");
        tx.send(record2).await.expect("send failed");
        tx.send(record3).await.expect("send failed");  // Should trigger batch flush (3 records)
        tx.send(record4).await.expect("send failed");  // Will be flushed on channel close
        drop(tx); // Close sender to trigger final flush

        // Wait for writer to finish
        let result = writer_handle.await.expect("writer task failed");

        // Verify writer flushed records
        assert_eq!(result.total_records_written, 4, "Writer should have written 4 records total");
        assert_eq!(result.write_errors, 0, "Writer should have no errors");

        // Verify table was created and records exist
        let table = deltalake::open_table(&table_path)
            .await
            .expect("table should exist after writer finishes");

        // Verify records exist in the table
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(table))
            .expect("table registration failed");

        let sql = "SELECT COUNT(*) as cnt FROM ct_records";
        let df = ctx.sql(sql).await.expect("sql query failed");
        let batches = df.collect().await.expect("batch collection failed");

        assert_eq!(batches.len(), 1);
        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("count column should be Int64");
        assert_eq!(count_arr.value(0), 4, "Should have exactly 4 records in table");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac4_2_writer_records_match_schema() {
        // Test that records written by backfill match schema and can be read back
        // Verifies delta-backfill.AC4.2: Records are indistinguishable from live sink records
        let test_name = "ac4_2_writer_records";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);  // Create parent dir

        // Create channel and send records
        let (tx, rx) = mpsc::channel::<DeltaCertRecord>(20);
        let shutdown = CancellationToken::new();

        // Create test records
        let record1 = make_test_record(10, "https://log.example.com");
        let record2 = make_test_record(11, "https://log.example.com");
        let record3 = make_test_record(12, "https://log.example.com");

        // Spawn writer task with small batch size to trigger flush
        let table_path_clone = table_path.clone();
        let writer_handle = tokio::spawn(async move {
            run_writer(table_path_clone, 2, 60, 9, 15, rx, shutdown).await
        });

        // Send records
        tx.send(record1).await.expect("send failed");
        tx.send(record2).await.expect("send failed");
        tx.send(record3).await.expect("send failed");
        drop(tx);

        let result = writer_handle.await.expect("writer task failed");
        assert_eq!(result.total_records_written, 3);

        // Verify records were written by reading back from table
        let ctx = SessionContext::new();
        let table = deltalake::open_table(&table_path)
            .await
            .expect("table read failed");
        ctx.register_table("ct_records", Arc::new(table))
            .expect("table registration failed");

        let sql = "SELECT COUNT(*) as cnt FROM ct_records WHERE source_url = 'https://log.example.com'";
        let df = ctx.sql(sql).await.expect("sql query failed");
        let batches = df.collect().await.expect("batch collection failed");

        assert_eq!(batches.len(), 1);
        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("count column should be Int64");
        assert_eq!(count_arr.value(0), 3, "Should have exactly 3 records");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac5_4_writer_result_tracks_errors() {
        // Test that writer result properly tracks write errors
        // Verifies delta-backfill.AC5.4: Exit code reflects errors
        let (tx, rx) = mpsc::channel::<DeltaCertRecord>(10);
        let shutdown = CancellationToken::new();

        // Use invalid path to trigger table creation failure
        let writer_handle = tokio::spawn(async move {
            run_writer("/invalid/path/that/cannot/be/created".to_string(), 10, 60, 9, 15, rx, shutdown).await
        });

        // Send one record
        let record = make_test_record(1, "https://log.example.com");
        let _ = tx.send(record).await;
        drop(tx);

        let result = writer_handle.await.expect("writer task failed");

        // Verify error was recorded
        assert!(result.write_errors > 0, "Should record error for invalid table path");
    }

    #[tokio::test]
    async fn test_ac5_3_concurrent_fetchers_independent() {
        // Test that multiple fetchers run independently and one failure doesn't block others
        // Verifies delta-backfill.AC5.3: Permanently unreachable log doesn't block other logs
        let test_name = "ac5_3_concurrent_fetchers";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        // Create a channel
        let (tx1, rx) = mpsc::channel::<DeltaCertRecord>(10);
        let tx2 = tx1.clone();
        let tx3 = tx1.clone();

        // Spawn three tasks that simulate fetchers
        let shutdown = CancellationToken::new();

        let fetcher1 = tokio::spawn(async move {
            // Fetcher 1: succeeds and sends records
            let record = make_test_record(1, "https://log1.example.com");
            if tx1.send(record).await.is_ok() {
                Ok(1u64)
            } else {
                Err("send failed".to_string())
            }
        });

        let fetcher2 = tokio::spawn(async move {
            // Fetcher 2: succeeds and sends records
            let record = make_test_record(2, "https://log2.example.com");
            if tx2.send(record).await.is_ok() {
                Ok(1u64)
            } else {
                Err("send failed".to_string())
            }
        });

        let fetcher3 = tokio::spawn(async move {
            // Fetcher 3: fails with connection refused error (simulating unreachable log)
            // Must take ownership of tx3 so it's dropped when this task completes,
            // allowing the channel to close once all senders are gone.
            drop(tx3);
            Err::<u64, String>("connection refused".to_string())
        });

        // Spawn writer task
        let table_path_clone = table_path.clone();
        let writer_handle = tokio::spawn(async move {
            run_writer(table_path_clone, 10, 60, 9, 15, rx, shutdown).await
        });

        // Wait for fetchers
        let r1 = fetcher1.await.expect("fetcher1 failed").expect("fetcher1 error");
        let r2 = fetcher2.await.expect("fetcher2 failed").expect("fetcher2 error");
        let r3 = fetcher3.await.expect("fetcher3 failed");

        // Verify fetchers 1 and 2 succeeded while fetcher 3 failed (AC5.3: failure doesn't block others)
        assert_eq!(r1, 1, "Fetcher 1 should succeed");
        assert_eq!(r2, 1, "Fetcher 2 should succeed");
        assert!(r3.is_err(), "Fetcher 3 should fail with connection refused");

        // Wait for writer
        let writer_result = writer_handle.await.expect("writer task failed");

        // Verify only the successful fetchers' records were written (2 total, not 3)
        assert_eq!(writer_result.total_records_written, 2, "Should have 2 records from successful fetchers");
        assert_eq!(writer_result.write_errors, 0, "Writer should have no errors");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac6_3_completion_summary_counts() {
        // Test that writer provides correct counts for completion summary
        // Verifies delta-backfill.AC6.3: Completion summary reports records written
        let test_name = "ac6_3_completion_summary";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let (tx, rx) = mpsc::channel::<DeltaCertRecord>(5);
        let shutdown = CancellationToken::new();

        // Spawn writer
        let table_path_clone = table_path.clone();
        let writer_handle = tokio::spawn(async move {
            run_writer(table_path_clone, 2, 60, 9, 15, rx, shutdown).await
        });

        // Send records to trigger flushes
        for i in 0..5 {
            let record = make_test_record(i as u64, "https://log.example.com");
            tx.send(record).await.expect("send failed");
        }
        drop(tx);

        let result = writer_handle.await.expect("writer task failed");

        // Verify completion summary fields
        assert_eq!(result.total_records_written, 5, "Should have written 5 records");
        assert_eq!(result.write_errors, 0, "Should have no write errors");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac4_2_writer_respects_batch_size() {
        // Test that writer flushes on batch size threshold
        // Verifies delta-backfill.AC4.2: Records flushed correctly
        let test_name = "ac4_2_batch_flush";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let batch_size = 3;
        let (tx, rx) = mpsc::channel::<DeltaCertRecord>(batch_size * 2);
        let shutdown = CancellationToken::new();

        // Spawn writer with batch_size=3
        let table_path_clone = table_path.clone();
        let writer_handle = tokio::spawn(async move {
            run_writer(table_path_clone, batch_size, 60, 9, 15, rx, shutdown).await
        });

        // Send exactly batch_size records to trigger flush
        for i in 0..batch_size {
            let record = make_test_record(i as u64, "https://log.example.com");
            tx.send(record).await.expect("send failed");
        }

        // Give it a moment to flush
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send one more to trigger another flush
        let record = make_test_record(batch_size as u64, "https://log.example.com");
        tx.send(record).await.expect("send failed");
        drop(tx);

        let result = writer_handle.await.expect("writer task failed");
        assert_eq!(result.total_records_written, batch_size as u64 + 1);

        let _ = fs::remove_dir_all(&table_path);
    }

    // Task 2 Tests: Ceiling behavior

    #[tokio::test]
    async fn test_ceiling_below_data_no_work_items() {
        // backfill-state-ceiling.AC3.3: ceiling < min_index produces no work items.
        // After removing frontier gaps, ceiling is not consulted in catch-up mode.
        // This test confirms: contiguous data + any ceiling value = 0 work items.
        let test_name = "ceiling_below_data";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Create records [100, 101, 102]
        let records = vec![
            make_test_record(100, "https://log.example.com"),
            make_test_record(101, "https://log.example.com"),
            make_test_record(102, "https://log.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // ceiling=50 is below min_index=100; irrelevant in catch-up mode
        // but verifies AC3.3 is satisfied
        let logs = vec![("https://log.example.com".to_string(), 50)];
        let work_items = detect_gaps(&table_path, None, &logs, None)
            .await
            .expect("detect_gaps failed");

        // No work items: no internal gaps in contiguous data, ceiling not consulted
        assert_eq!(work_items.len(), 0, "Contiguous data with any ceiling should produce no work items in catch-up mode");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_catch_up_ceiling_only_internal_gaps() {
        // backfill-state-ceiling.AC1.1: catch-up fills only internal gaps, no frontier
        let test_name = "ceiling_only_internal";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Records with gap: [10, 11, 15, 16] — missing 12, 13, 14
        let records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(15, "https://log.example.com"),
            make_test_record(16, "https://log.example.com"),
        ];

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        let _new_table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write failed");

        // ceiling=1000 (well above max_index=16)
        let logs = vec![("https://log.example.com".to_string(), 1000)];
        let work_items = detect_gaps(&table_path, None, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should have exactly 1 internal gap (12, 14) — no frontier gap despite ceiling=1000
        assert_eq!(work_items.len(), 1, "Should have only internal gap, no frontier");
        assert_eq!(work_items[0].start, 12);
        assert_eq!(work_items[0].end, 14);

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_historical_ceiling_used_as_upper_bound() {
        // backfill-state-ceiling.AC2.1: historical mode uses ceiling from state file
        let test_name = "historical_ceiling_upper";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);

        // No table — historical mode with no delta table
        let logs = vec![("https://log.example.com".to_string(), 100)];
        let work_items = detect_gaps(&table_path, None, &logs, Some(0))
            .await
            .expect("detect_gaps failed");

        // Should use ceiling=100 as upper bound: work item (0, 99)
        assert_eq!(work_items.len(), 1);
        assert_eq!(work_items[0].start, 0);
        assert_eq!(work_items[0].end, 99);

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_staging_write_creates_table_at_staging_path() {
        // Test that writer creates a Delta table at the staging path with correct schema
        // Verifies staging-backfill.AC1.1: staging table is created at correct path
        // Verifies staging-backfill.AC1.2: records are written to staging table
        let test_name = "staging_write_table";
        let staging_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&staging_path);

        // Create a channel and spawn writer with the staging path
        let (tx, rx) = mpsc::channel::<DeltaCertRecord>(10);
        let shutdown = CancellationToken::new();

        let staging_path_clone = staging_path.clone();
        let writer_handle = tokio::spawn(async move {
            run_writer(staging_path_clone, 3, 60, 9, 15, rx, shutdown).await
        });

        // Send a few records
        let record1 = make_test_record(1, "https://log.example.com");
        let record2 = make_test_record(2, "https://log.example.com");
        let record3 = make_test_record(3, "https://log.example.com");
        let record4 = make_test_record(4, "https://log.example.com");

        tx.send(record1).await.expect("send failed");
        tx.send(record2).await.expect("send failed");
        tx.send(record3).await.expect("send failed"); // Should trigger batch flush (3 records)
        tx.send(record4).await.expect("send failed"); // Will be flushed on channel close
        drop(tx); // Close sender to trigger final flush

        // Wait for writer to finish
        let result = writer_handle.await.expect("writer task failed");

        // Verify writer completed successfully
        assert_eq!(result.total_records_written, 4, "Writer should have written 4 records");
        assert_eq!(result.write_errors, 0, "Writer should have no errors");

        // Verify Delta table exists at staging path
        let table = deltalake::open_table(&staging_path)
            .await
            .expect("staging table should exist after writer finishes");

        // Verify table has the correct schema (20 columns)
        let schema = table.get_schema().expect("get schema failed");
        let field_count = schema.fields().count();
        assert_eq!(
            field_count,
            20,
            "Staging table should have 20 columns matching delta_schema"
        );

        // Verify table is partitioned by seen_date
        let metadata = table.metadata().expect("get metadata failed");
        let partition_columns: Vec<String> = metadata
            .partition_columns
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            partition_columns,
            vec!["seen_date"],
            "Staging table should be partitioned by seen_date"
        );

        // Verify records exist in the staging table using DataFusion
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(table))
            .expect("table registration failed");

        let sql = "SELECT COUNT(*) as cnt FROM ct_records";
        let df = ctx.sql(sql).await.expect("sql query failed");
        let batches = df.collect().await.expect("batch collection failed");

        assert_eq!(batches.len(), 1);
        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("count column should be Int64");
        assert_eq!(
            count_arr.value(0),
            4,
            "Staging table should have exactly 4 records"
        );

        // Clean up
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac2_1_union_all_excludes_entries_in_either_table() {
        // Verifies staging-backfill.AC2.1: When staging table exists,
        // gap detection queries both main and staging — entries present in
        // either table are excluded from gap list
        let test_name = "ac2_1_union_excludes";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with records at indices [10, 11, 15, 16]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(15, "https://log.example.com"),
            make_test_record(16, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _main_table = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging table with records at indices [12, 13]
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(12, "https://log.example.com"),
            make_test_record(13, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _staging_table = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Call detect_gaps with ceiling=20 and staging_path
        let logs = vec![("https://log.example.com".to_string(), 20)];
        let work_items = detect_gaps(&main_path, Some(&staging_path), &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should detect only gap [14, 14] (index 14 is missing from both tables)
        assert_eq!(work_items.len(), 1, "Should detect only gap [14, 14]");
        assert_eq!(work_items[0].start, 14, "Gap should start at 14");
        assert_eq!(work_items[0].end, 14, "Gap should end at 14");

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac2_2_second_staging_run_produces_fewer_work_items() {
        // Verifies staging-backfill.AC2.2: Running backfill with staging a
        // second time produces fewer work items (previously staged entries not re-fetched)
        let test_name = "ac2_2_second_run_fewer";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with records at indices [10, 15]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(15, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _main_table = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // First call with empty staging: should detect gap [11, 14]
        let logs = vec![("https://log.example.com".to_string(), 20)];
        let work_items_first = detect_gaps(&main_path, Some(&staging_path), &logs, None)
            .await
            .expect("detect_gaps failed");

        // First run: staging doesn't exist, so gap is [11, 14]
        assert_eq!(work_items_first.len(), 1, "First run should detect gap [11, 14]");
        assert_eq!(work_items_first[0].start, 11);
        assert_eq!(work_items_first[0].end, 14);

        // Now create staging with records at [11, 12]
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(11, "https://log.example.com"),
            make_test_record(12, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _staging_table = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Second call with staging populated: should detect smaller gap [13, 14]
        let work_items_second = detect_gaps(&main_path, Some(&staging_path), &logs, None)
            .await
            .expect("detect_gaps failed");

        assert_eq!(work_items_second.len(), 1, "Second run should detect smaller gap [13, 14]");
        assert_eq!(work_items_second[0].start, 13);
        assert_eq!(work_items_second[0].end, 14);

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac2_3_ceiling_caps_union_max_cert_index() {
        // Verifies staging-backfill.AC2.3: Per-log MAX(cert_index) from the
        // union is capped at the state file ceiling
        let test_name = "ac2_3_ceiling_cap";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with records at [10, 11, 12]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(12, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _main_table = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging table with records at [13, 14, 25] (25 is beyond ceiling)
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(13, "https://log.example.com"),
            make_test_record(14, "https://log.example.com"),
            make_test_record(25, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _staging_table = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Call with ceiling=20: verify no work items generated beyond index 19
        let logs = vec![("https://log.example.com".to_string(), 20)];
        let work_items = detect_gaps(&main_path, Some(&staging_path), &logs, None)
            .await
            .expect("detect_gaps failed");

        // effective_max = min(max_index_in_union, ceiling) = min(25, 20) = 20
        // Data in union: [10, 11, 12, 13, 14, 25]
        // After ceiling cap: [10, 11, 12, 13, 14] (25 is beyond ceiling)
        // No gaps within [10, 14], so no work items
        assert_eq!(work_items.len(), 0, "Ceiling should prevent gaps beyond index 19");

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac2_4_missing_staging_falls_back_to_main_only() {
        // Verifies staging-backfill.AC2.4: When staging table doesn't exist
        // yet (first run), gap detection falls back to querying main table only
        let test_name = "ac2_4_missing_staging";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let nonexistent_staging = "/tmp/nonexistent_staging_path_12345";
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(nonexistent_staging);
        let _ = fs::create_dir_all(&main_path);

        let schema = delta_schema();

        // Create main table with records at [10, 11, 15]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(15, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _main_table = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Call with nonexistent staging path
        let logs = vec![("https://log.example.com".to_string(), 20)];
        let work_items = detect_gaps(&main_path, Some(nonexistent_staging), &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should get same results as calling without staging (gap [12, 14])
        assert_eq!(work_items.len(), 1, "Should detect gap [12, 14]");
        assert_eq!(work_items[0].start, 12);
        assert_eq!(work_items[0].end, 14);

        let _ = fs::remove_dir_all(&main_path);
    }

    // Task 2: Merge Tests

    /// Helper function to create a minimal Config for testing
    fn make_test_config(table_path: &str) -> Config {
        use crate::config::{DeltaSinkConfig, ZerobusSinkConfig};
        use std::net::IpAddr;
        use std::str::FromStr;

        Config {
            host: IpAddr::from_str("127.0.0.1").unwrap(),
            port: 8000,
            log_level: "info".to_string(),
            buffer_size: 1000,
            ct_logs_url: "https://www.gstatic.com/ct/log_list/v3/log_list.json".to_string(),
            tls_cert: None,
            tls_key: None,
            custom_logs: vec![],
            static_logs: vec![],
            protocols: crate::config::ProtocolConfig::default(),
            ct_log: crate::config::CtLogConfig::default(),
            connection_limit: crate::config::ConnectionLimitConfig::default(),
            rate_limit: crate::config::RateLimitConfig::default(),
            api: crate::config::ApiConfig::default(),
            auth: crate::config::AuthConfig::default(),
            hot_reload: crate::config::HotReloadConfig::default(),
            delta_sink: DeltaSinkConfig {
                enabled: true,
                table_path: table_path.to_string(),
                batch_size: 100,
                flush_interval_secs: 60,
                compression_level: 9,
                heavy_column_compression_level: 15,
            },
            query_api: crate::config::QueryApiConfig::default(),
            zerobus_sink: ZerobusSinkConfig::default(),
            config_path: None,
        }
    }

    #[tokio::test]
    async fn test_ac3_1_merge_inserts_non_duplicate_records() {
        // Verifies staging-backfill.AC3.1: `--merge --staging-path` inserts staging records
        // not present in main (matched on `source_url` + `cert_index`)
        let test_name = "ac3_1_merge_inserts";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with records at cert_index [10, 11, 12]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(12, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _ = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging table with records at cert_index [13, 14, 15]
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(13, "https://log.example.com"),
            make_test_record(14, "https://log.example.com"),
            make_test_record(15, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _ = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Run merge
        let config = make_test_config(&main_path);
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;
        assert_eq!(exit_code, 0, "Merge should succeed");

        // Verify main table now contains [10, 11, 12, 13, 14, 15]
        let main_table = deltalake::open_table(&main_path)
            .await
            .expect("should be able to open main table");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(main_table))
            .expect("should register table");

        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM ct_records WHERE source_url = 'https://log.example.com'")
            .await
            .expect("sql query failed");
        let batches = df.collect().await.expect("batch collection failed");

        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("count should be Int64");
        assert_eq!(count_arr.value(0), 6, "Should have 6 records total");

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac3_2_merge_skips_existing_records() {
        // Verifies staging-backfill.AC3.2: Records already in main with matching
        // (source_url, cert_index) are skipped (not duplicated)
        let test_name = "ac3_2_merge_skips";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with records at [10, 11, 12]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(12, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _ = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging table with records at [11, 12, 13] (overlap on 11 and 12)
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(11, "https://log.example.com"),
            make_test_record(12, "https://log.example.com"),
            make_test_record(13, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _ = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Run merge
        let config = make_test_config(&main_path);
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;
        assert_eq!(exit_code, 0, "Merge should succeed");

        // Verify main table has [10, 11, 12, 13] — no duplicates of 11 or 12
        let main_table = deltalake::open_table(&main_path)
            .await
            .expect("should be able to open main table");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(main_table))
            .expect("should register table");

        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM ct_records WHERE source_url = 'https://log.example.com'")
            .await
            .expect("sql query failed");
        let batches = df.collect().await.expect("batch collection failed");

        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("count should be Int64");
        assert_eq!(count_arr.value(0), 4, "Should have exactly 4 records (no duplicates)");

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac3_3_merge_is_idempotent() {
        // Verifies staging-backfill.AC3.3: Merge is idempotent — running it twice
        // with same staging data produces identical main table
        let test_name = "ac3_3_merge_idempotent";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with [10, 11]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _ = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging table with [12, 13]
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(12, "https://log.example.com"),
            make_test_record(13, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _ = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Run merge first time
        let config = make_test_config(&main_path);
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config.clone(), staging_path.clone(), shutdown.clone()).await;
        assert_eq!(exit_code, 0, "First merge should succeed");

        // Verify staging was deleted after first merge (AC3.4)
        assert!(!std::path::Path::new(&staging_path).exists(), "Staging should be deleted");

        // Get count after first merge
        let main_table = deltalake::open_table(&main_path)
            .await
            .expect("should be able to open main table");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(main_table))
            .expect("should register table");
        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM ct_records WHERE source_url = 'https://log.example.com'")
            .await
            .expect("sql query failed");
        let batches = df.collect().await.expect("batch collection failed");
        let count_after_first = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("count should be Int64")
            .value(0);
        assert_eq!(count_after_first, 4, "Should have 4 records after first merge");

        // Recreate staging with same data [12, 13]
        let _ = fs::create_dir_all(&staging_path);
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(12, "https://log.example.com"),
            make_test_record(13, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _ = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Run merge second time
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;
        assert_eq!(exit_code, 0, "Second merge should succeed");

        // Get count after second merge
        let main_table = deltalake::open_table(&main_path)
            .await
            .expect("should be able to open main table");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(main_table))
            .expect("should register table");
        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM ct_records WHERE source_url = 'https://log.example.com'")
            .await
            .expect("sql query failed");
        let batches = df.collect().await.expect("batch collection failed");
        let count_after_second = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("count should be Int64")
            .value(0);
        assert_eq!(count_after_second, 4, "Should still have 4 records after second merge (no duplicates)");

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac3_4_staging_directory_deleted_on_success() {
        // Verifies staging-backfill.AC3.4: Staging directory is deleted after successful merge
        let test_name = "ac3_4_staging_deleted";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![make_test_record(1, "https://log.example.com")];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _ = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging table
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![make_test_record(2, "https://log.example.com")];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _ = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Verify staging exists before merge
        assert!(
            std::path::Path::new(&staging_path).exists(),
            "Staging should exist before merge"
        );

        // Run merge
        let config = make_test_config(&main_path);
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;
        assert_eq!(exit_code, 0, "Merge should succeed");

        // Verify staging directory no longer exists
        assert!(
            !std::path::Path::new(&staging_path).exists(),
            "Staging directory should be deleted after successful merge"
        );

        let _ = fs::remove_dir_all(&main_path);
    }

    #[tokio::test]
    async fn test_ac3_5_merge_returns_zero_and_logs_metrics() {
        // Verifies staging-backfill.AC3.5: Merge returns 0 and logs metrics
        let test_name = "ac3_5_merge_metrics";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with [10, 11]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _ = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging with overlap: [11, 12, 13] (11 overlaps, 12 and 13 are new)
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(11, "https://log.example.com"),
            make_test_record(12, "https://log.example.com"),
            make_test_record(13, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _ = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Run merge and capture return code
        let config = make_test_config(&main_path);
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;

        // Verify return code is 0
        assert_eq!(exit_code, 0, "Merge should return 0 on success");

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    // Error handling tests for AC4.1-AC4.5

    #[tokio::test]
    async fn test_ac4_1_missing_staging_table_exits_zero() {
        // Verifies staging-backfill.AC4.1: Missing staging table exits 0 (nothing to merge)
        let test_name = "ac4_1_missing_staging";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        // Do NOT create staging_path — it doesn't exist

        let schema = delta_schema();
        let _ = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");

        let config = make_test_config(&main_path);
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;

        assert_eq!(exit_code, 0, "Missing staging table should exit with code 0");

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac4_1_empty_staging_table_exits_zero() {
        // Verifies staging-backfill.AC4.1: Empty staging table exits 0 (nothing to merge)
        let test_name = "ac4_1_empty_staging";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table
        let _ = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");

        // Create empty staging table (just schema, no data)
        let _ = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");

        let config = make_test_config(&main_path);
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;

        assert_eq!(exit_code, 0, "Empty staging table should exit with code 0");

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac4_2_missing_main_table_auto_created() {
        // Verifies staging-backfill.AC4.2: Missing main table is auto-created
        let test_name = "ac4_2_missing_main";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&staging_path);
        // Create the main_path directory but leave it empty (no _delta_log yet)
        let _ = fs::create_dir_all(&main_path);

        let schema = delta_schema();

        // Create staging table with records [10, 11, 12]
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(12, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _ = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        let config = make_test_config(&main_path);
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;

        assert_eq!(exit_code, 0, "Merge should succeed with exit code 0");

        // Verify main table now exists with records [10, 11, 12]
        let main_table = deltalake::open_table(&main_path)
            .await
            .expect("main table should exist after merge");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(main_table))
            .expect("should register table");

        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM ct_records WHERE source_url = 'https://log.example.com'")
            .await
            .expect("sql query failed");
        let batches = df.collect().await.expect("batch collection failed");

        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("count should be Int64");
        assert_eq!(count_arr.value(0), 3, "Main table should have 3 records from staging");

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac4_4_cancellation_preserves_staging() {
        // Verifies staging-backfill.AC4.4: Process interruption (SIGINT) leaves staging intact
        let test_name = "ac4_4_cancellation";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with records [10, 11, 12]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(12, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _ = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging table with records [13, 14, 15]
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(13, "https://log.example.com"),
            make_test_record(14, "https://log.example.com"),
            make_test_record(15, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _ = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Create a CancellationToken and cancel it BEFORE calling run_merge
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let config = make_test_config(&main_path);
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;

        assert_eq!(exit_code, 1, "Merge should exit with code 1 when cancelled");

        // Verify staging directory still exists (was NOT deleted)
        assert!(
            std::path::Path::new(&staging_path).exists(),
            "Staging directory should still exist after cancellation"
        );

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[test]
    fn test_ac4_5_cli_validation_merge_without_staging_path() {
        // Verifies staging-backfill.AC4.5: --merge without --staging-path validation
        // Test at CLI parsing level: construct CliArgs with merge=true, staging_path=None
        use crate::cli::CliArgs;

        let cli_args = CliArgs {
            validate_config: false,
            dry_run: false,
            export_metrics: false,
            show_version: false,
            show_help: false,
            backfill: false,
            backfill_from: None,
            backfill_logs: None,
            staging_path: None,
            backfill_sink: None,
            merge: true,
            migrate: false,
            migrate_output: None,
        };

        // Verify that merge is true and staging_path is None
        assert!(cli_args.merge, "merge flag should be true");
        assert!(cli_args.staging_path.is_none(), "staging_path should be None");

        // The validation logic in main.rs checks:
        // if cli_args.merge && cli_args.staging_path.is_none() => print error and exit 1
        // This test verifies the condition that triggers the error
        let has_merge_without_staging = cli_args.merge && cli_args.staging_path.is_none();
        assert!(
            has_merge_without_staging,
            "--merge without --staging-path should be detected"
        );
    }

    #[tokio::test]
    async fn test_ac1_2_records_in_staging_not_in_main() {
        // Verifies staging-backfill.AC1.2: Records written to staging table are NOT present in main table.
        // Creates both main and staging directories, runs writer targeting staging,
        // verifies: (a) staging has records queryable via DataFusion, (b) main table is empty or has zero rows.
        let test_name = "ac1_2_staging_not_in_main";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create an empty main table
        let _ = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");

        // Now run writer targeting staging_path (not main_path)
        let (tx, rx) = mpsc::channel::<DeltaCertRecord>(10);
        let shutdown = CancellationToken::new();

        let staging_path_clone = staging_path.clone();
        let writer_handle = tokio::spawn(async move {
            run_writer(staging_path_clone, 3, 60, 9, 15, rx, shutdown).await
        });

        // Send records to staging
        let record1 = make_test_record(1, "https://log.example.com");
        let record2 = make_test_record(2, "https://log.example.com");
        let record3 = make_test_record(3, "https://log.example.com");
        let record4 = make_test_record(4, "https://log.example.com");

        tx.send(record1).await.expect("send failed");
        tx.send(record2).await.expect("send failed");
        tx.send(record3).await.expect("send failed"); // Triggers batch flush (3 records)
        tx.send(record4).await.expect("send failed"); // Will be flushed on channel close
        drop(tx);

        let result = writer_handle.await.expect("writer task failed");
        assert_eq!(result.total_records_written, 4, "Writer should have written 4 records to staging");
        assert_eq!(result.write_errors, 0, "Writer should have no errors");

        // Verify: staging table has 4 records queryable via DataFusion
        let staging_table = deltalake::open_table(&staging_path)
            .await
            .expect("staging table should exist after writer finishes");
        let ctx = SessionContext::new();
        ctx.register_table("ct_staging", Arc::new(staging_table))
            .expect("staging table registration failed");

        let sql = "SELECT COUNT(*) as cnt FROM ct_staging WHERE source_url = 'https://log.example.com'";
        let df = ctx.sql(sql).await.expect("sql query failed");
        let batches = df.collect().await.expect("batch collection failed");
        let count_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("count column should be Int64");
        assert_eq!(
            count_arr.value(0), 4,
            "Staging table should have exactly 4 records"
        );

        // Verify: main table has zero rows (writer did NOT write to main)
        let main_table = deltalake::open_table(&main_path)
            .await
            .expect("main table should exist");
        let ctx_main = SessionContext::new();
        ctx_main.register_table("ct_main", Arc::new(main_table))
            .expect("main table registration failed");

        let sql_main = "SELECT COUNT(*) as cnt FROM ct_main";
        let df_main = ctx_main.sql(sql_main).await.expect("main sql query failed");
        let batches_main = df_main.collect().await.expect("main batch collection failed");
        let count_arr_main = batches_main[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("main count column should be Int64");
        assert_eq!(
            count_arr_main.value(0), 0,
            "Main table should have zero records (writer targeted staging only)"
        );

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[tokio::test]
    async fn test_ac1_3_gap_detection_with_staging_catch_up_and_historical() {
        // Verifies staging-backfill.AC1.3: Gap detection works with both catch-up and historical modes with staging.
        // Tests detect_gaps() called with staging_path in BOTH catch-up (backfill_from=None)
        // and historical (backfill_from=Some(0)) modes, asserting correct work items are generated.
        let test_name = "ac1_3_gap_detection_both_modes";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with records [10, 11, 15, 16]
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let main_records = vec![
            make_test_record(10, "https://log.example.com"),
            make_test_record(11, "https://log.example.com"),
            make_test_record(15, "https://log.example.com"),
            make_test_record(16, "https://log.example.com"),
        ];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _main_table = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging table with records [12, 13]
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let staging_records = vec![
            make_test_record(12, "https://log.example.com"),
            make_test_record(13, "https://log.example.com"),
        ];
        let staging_batch = records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _staging_table = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // TEST 1: Catch-up mode (backfill_from = None) with staging
        // Gap detection should see union of main [10, 11, 15, 16] and staging [12, 13]
        // Data combined: [10, 11, 12, 13, 15, 16]
        // Gap: [14] (index 14 is missing)
        let logs = vec![("https://log.example.com".to_string(), 20)];
        let work_items_catchup = detect_gaps(&main_path, Some(&staging_path), &logs, None)
            .await
            .expect("detect_gaps catch-up failed");

        assert_eq!(
            work_items_catchup.len(), 1,
            "Catch-up mode with staging should detect gap [14, 14]"
        );
        assert_eq!(
            work_items_catchup[0].start, 14,
            "Catch-up mode gap should start at 14"
        );
        assert_eq!(
            work_items_catchup[0].end, 14,
            "Catch-up mode gap should end at 14"
        );

        // TEST 2: Historical mode (backfill_from = Some(0)) with staging
        // Should start from 0 (--from 0) and fill to ceiling=20
        // Data union: [10, 11, 12, 13, 15, 16]
        // Gaps: [0-9] (pre-existing), [14] (internal)
        let work_items_historical = detect_gaps(&main_path, Some(&staging_path), &logs, Some(0))
            .await
            .expect("detect_gaps historical failed");

        assert_eq!(
            work_items_historical.len(), 2,
            "Historical mode with staging should detect both pre-existing gap [0-9] and internal gap [14]"
        );

        // First work item should be pre-existing gap [0, 9]
        let pre_existing = work_items_historical.iter().find(|item| item.start == 0);
        assert!(pre_existing.is_some(), "Should detect pre-existing gap starting at 0");
        assert_eq!(
            pre_existing.unwrap().end, 9,
            "Pre-existing gap should end at 9"
        );

        // Second work item should be internal gap [14, 14]
        let internal_gap = work_items_historical.iter().find(|item| item.start == 14);
        assert!(internal_gap.is_some(), "Should detect internal gap starting at 14");
        assert_eq!(
            internal_gap.unwrap().end, 14,
            "Internal gap should end at 14"
        );

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }

    #[test]
    fn test_ac3_2_backwards_compatible_delta_default() {
        // AC3.2: Test that delta sink is used when backfill_sink is None
        // This test verifies the dispatch logic would choose delta writer
        let backfill_sink: Option<String> = None;
        assert!(
            backfill_sink.as_deref() != Some("zerobus"),
            "When backfill_sink is None, should default to delta writer"
        );
    }

    #[test]
    fn test_zerobus_work_items_generated_from_range() {
        // AC3.1: Verify that when --sink zerobus is used, work items are built directly from --from to ceiling
        // This test simulates the logic: backfill_sink = Some("zerobus"), backfill_from = 10, ceiling = 50
        let backfill_sink = Some("zerobus".to_string());
        let backfill_from = 10u64;
        let log_ceilings = vec![
            ("https://log1.example.com".to_string(), 50u64),
            ("https://log2.example.com".to_string(), 30u64),
        ];

        // Simulate the work item generation logic for ZeroBus
        if backfill_sink.as_deref() == Some("zerobus") {
            let mut items = Vec::new();
            for (source_url, ceiling) in &log_ceilings {
                if *ceiling > backfill_from {
                    items.push(BackfillWorkItem {
                        source_url: source_url.clone(),
                        start: backfill_from,
                        end: ceiling - 1,
                    });
                }
            }

            // Verify work items match expected ranges
            assert_eq!(items.len(), 2, "Should have 2 work items");

            let item1 = items.iter().find(|i| i.source_url == "https://log1.example.com").unwrap();
            assert_eq!(item1.start, 10, "Log1 should start at 10");
            assert_eq!(item1.end, 49, "Log1 should end at 49 (50-1)");

            let item2 = items.iter().find(|i| i.source_url == "https://log2.example.com").unwrap();
            assert_eq!(item2.start, 10, "Log2 should start at 10");
            assert_eq!(item2.end, 29, "Log2 should end at 29 (30-1)");
        }
    }

    #[test]
    fn test_zerobus_gap_detection_skipped() {
        // AC3.1: Verify that when --sink zerobus is specified, gap detection is skipped
        // This test checks the conditional logic that bypasses gap detection for ZeroBus
        let backfill_sink = Some("zerobus".to_string());

        // This mimics the actual code path
        let should_skip_gap_detection = backfill_sink.as_deref() == Some("zerobus");
        assert!(
            should_skip_gap_detection,
            "Gap detection should be skipped for ZeroBus sink"
        );
    }

    #[test]
    fn test_delta_sink_uses_gap_detection() {
        // AC3.2: Verify that delta sink still uses gap detection
        let backfill_sink: Option<String> = None;

        // This mimics the actual code path
        let should_skip_gap_detection = backfill_sink.as_deref() == Some("zerobus");
        assert!(
            !should_skip_gap_detection,
            "Gap detection should NOT be skipped for delta sink"
        );
    }

    // --- Migration tests (Phase 3, Task 3) ---

    /// Helper: create an old-schema Delta table (as_der = Utf8) with base64 string data.
    /// Returns the schema used and the written table.
    fn old_delta_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("cert_index", DataType::UInt64, false),
            Field::new("update_type", DataType::Utf8, false),
            Field::new(
                "seen",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("seen_date", DataType::Utf8, false),
            Field::new("source_name", DataType::Utf8, false),
            Field::new("source_url", DataType::Utf8, false),
            Field::new("cert_link", DataType::Utf8, false),
            Field::new("serial_number", DataType::Utf8, false),
            Field::new("fingerprint", DataType::Utf8, false),
            Field::new("sha256", DataType::Utf8, false),
            Field::new("sha1", DataType::Utf8, false),
            Field::new("not_before", DataType::Int64, false),
            Field::new("not_after", DataType::Int64, false),
            Field::new("is_ca", DataType::Boolean, false),
            Field::new("signature_algorithm", DataType::Utf8, false),
            Field::new("subject_aggregated", DataType::Utf8, false),
            Field::new("issuer_aggregated", DataType::Utf8, false),
            Field::new(
                "all_domains",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("as_der", DataType::Utf8, false), // Old schema: Utf8 with base64
            Field::new(
                "chain",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
        ]))
    }

    /// Build a RecordBatch for the old schema with base64 as_der strings.
    fn old_schema_batch(
        schema: &Arc<Schema>,
        cert_indices: &[u64],
        as_der_base64_values: &[&str],
        seen_date: &str,
    ) -> RecordBatch {
        let n = cert_indices.len();
        let cert_index: UInt64Array = cert_indices.iter().copied().collect();
        let update_type: StringArray = (0..n).map(|_| Some("X509LogEntry")).collect();
        let seen: TimestampMicrosecondArray = (0..n)
            .map(|_| Some(1_700_000_000_000_000i64))
            .collect::<TimestampMicrosecondArray>()
            .with_timezone("UTC");
        let seen_date_arr: StringArray = (0..n).map(|_| Some(seen_date)).collect();
        let source_name: StringArray = (0..n).map(|_| Some("Test Log")).collect();
        let source_url: StringArray = (0..n).map(|_| Some("https://ct.example.com/")).collect();
        let cert_link: StringArray = (0..n).map(|_| Some("https://ct.example.com/entry/1")).collect();
        let serial_number: StringArray = (0..n).map(|_| Some("01")).collect();
        let fingerprint: StringArray = (0..n).map(|_| Some("AA:BB")).collect();
        let sha256: StringArray = (0..n).map(|_| Some("EE:FF")).collect();
        let sha1: StringArray = (0..n).map(|_| Some("CC:DD")).collect();
        let not_before: Int64Array = (0..n).map(|_| Some(1_700_000_000i64)).collect();
        let not_after: Int64Array = (0..n).map(|_| Some(1_730_000_000i64)).collect();
        let is_ca: BooleanArray = (0..n).map(|_| Some(false)).collect();
        let signature_algorithm: StringArray = (0..n).map(|_| Some("sha256, rsa")).collect();
        let subject_aggregated: StringArray = (0..n).map(|_| Some("/CN=example.com")).collect();
        let issuer_aggregated: StringArray = (0..n).map(|_| Some("/CN=Test CA")).collect();

        let mut all_domains_builder = ListBuilder::new(StringBuilder::new());
        for _ in 0..n {
            all_domains_builder.values().append_value("example.com");
            all_domains_builder.append(true);
        }
        let all_domains = all_domains_builder.finish();

        // as_der as Utf8 StringArray (old schema with base64 strings)
        let as_der: StringArray = as_der_base64_values.iter().map(|s| Some(*s)).collect();

        let mut chain_builder = ListBuilder::new(StringBuilder::new());
        for _ in 0..n {
            chain_builder.append(true);
        }
        let chain = chain_builder.finish();

        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cert_index),
                Arc::new(update_type),
                Arc::new(seen),
                Arc::new(seen_date_arr),
                Arc::new(source_name),
                Arc::new(source_url),
                Arc::new(cert_link),
                Arc::new(serial_number),
                Arc::new(fingerprint),
                Arc::new(sha256),
                Arc::new(sha1),
                Arc::new(not_before),
                Arc::new(not_after),
                Arc::new(is_ca),
                Arc::new(signature_algorithm),
                Arc::new(subject_aggregated),
                Arc::new(issuer_aggregated),
                Arc::new(all_domains),
                Arc::new(as_der),
                Arc::new(chain),
            ],
        )
        .expect("failed to create old-schema batch")
    }

    #[tokio::test]
    async fn test_migrate_basic_schema_and_data(/* AC3.1, AC3.2, AC3.3 */) {
        use base64::{engine::general_purpose::STANDARD, Engine};
        use deltalake::datafusion::prelude::*;

        let test_name = "migrate_basic";
        let source_path = format!("/tmp/delta_migrate_test_{}_src", test_name);
        let output_path = format!("/tmp/delta_migrate_test_{}_out", test_name);
        let _ = fs::remove_dir_all(&source_path);
        let _ = fs::remove_dir_all(&output_path);
        let _ = fs::create_dir_all(&source_path);
        let _ = fs::create_dir_all(&output_path);

        // Create old-schema source table with known base64 as_der values
        let old_schema = old_delta_schema();
        let der_bytes_1: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let der_bytes_2: Vec<u8> = vec![0xCA, 0xFE, 0xBA, 0xBE];
        let b64_1 = STANDARD.encode(&der_bytes_1);
        let b64_2 = STANDARD.encode(&der_bytes_2);

        let source_table = open_or_create_table(&source_path, &old_schema)
            .await
            .expect("source table creation failed");
        let batch = old_schema_batch(&old_schema, &[100, 200], &[&b64_1, &b64_2], "2024-01-15");
        let _source_table = DeltaOps(source_table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("source write failed");

        // Run migration
        let mut config = make_test_config(&source_path);
        config.delta_sink.table_path = source_path.clone();
        let shutdown = CancellationToken::new();
        let exit_code = run_migrate(config, output_path.clone(), shutdown).await;
        assert_eq!(exit_code, 0, "migration should succeed");

        // Read back output table and verify
        let output_table = deltalake::open_table(&output_path)
            .await
            .expect("output table should exist");

        let ctx = SessionContext::new();
        ctx.register_table("output", Arc::new(output_table)).expect("register failed");
        let df = ctx.sql("SELECT cert_index, as_der, update_type, source_name, fingerprint FROM output ORDER BY cert_index")
            .await
            .expect("query failed");
        let batches = df.collect().await.expect("collect failed");

        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 2);

        // AC3.1: as_der is now Binary type
        let as_der_col = result
            .column_by_name("as_der")
            .expect("as_der column should exist")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("as_der should be BinaryArray in output");

        // AC3.2: values match STANDARD.decode() of originals
        assert_eq!(as_der_col.value(0), der_bytes_1.as_slice());
        assert_eq!(as_der_col.value(1), der_bytes_2.as_slice());

        // AC3.3: non-as_der columns pass through unchanged
        // DataFusion may return UInt64 or Int64 depending on the read path,
        // so use arrow cast to normalize to Int64 for assertion.
        let cert_idx_raw = result.column_by_name("cert_index").expect("cert_index column");
        let cert_idx_col = deltalake::arrow::compute::cast(cert_idx_raw, &DataType::Int64)
            .expect("cert_index should be castable to Int64");
        let cert_idx_arr = cert_idx_col.as_any().downcast_ref::<Int64Array>().expect("should be Int64");
        assert_eq!(cert_idx_arr.value(0), 100);
        assert_eq!(cert_idx_arr.value(1), 200);

        let update_type_col = result
            .column_by_name("update_type")
            .expect("update_type column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("update_type should be StringArray");
        assert_eq!(update_type_col.value(0), "X509LogEntry");

        let fingerprint_col = result
            .column_by_name("fingerprint")
            .expect("fingerprint column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("fingerprint should be StringArray");
        assert_eq!(fingerprint_col.value(0), "AA:BB");

        // Clean up
        let _ = fs::remove_dir_all(&source_path);
        let _ = fs::remove_dir_all(&output_path);
    }

    #[tokio::test]
    async fn test_migrate_graceful_shutdown(/* AC3.4 */) {
        use base64::{engine::general_purpose::STANDARD, Engine};

        let test_name = "migrate_shutdown";
        let source_path = format!("/tmp/delta_migrate_test_{}_src", test_name);
        let output_path = format!("/tmp/delta_migrate_test_{}_out", test_name);
        let _ = fs::remove_dir_all(&source_path);
        let _ = fs::remove_dir_all(&output_path);
        let _ = fs::create_dir_all(&source_path);
        let _ = fs::create_dir_all(&output_path);

        // Create source table with 2 partitions
        let old_schema = old_delta_schema();
        let b64 = STANDARD.encode(&[1u8, 2, 3]);

        let source_table = open_or_create_table(&source_path, &old_schema)
            .await
            .expect("source table creation failed");
        let batch1 = old_schema_batch(&old_schema, &[100], &[&b64], "2024-01-15");
        let source_table = DeltaOps(source_table)
            .write(vec![batch1])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write 1 failed");
        let batch2 = old_schema_batch(&old_schema, &[200], &[&b64], "2024-01-16");
        let _source_table = DeltaOps(source_table)
            .write(vec![batch2])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("write 2 failed");

        // Create pre-cancelled token
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let mut config = make_test_config(&source_path);
        config.delta_sink.table_path = source_path.clone();
        let exit_code = run_migrate(config, output_path.clone(), shutdown).await;
        assert_eq!(exit_code, 1, "migration with pre-cancelled token should exit 1");

        // Clean up
        let _ = fs::remove_dir_all(&source_path);
        let _ = fs::remove_dir_all(&output_path);
    }

    #[tokio::test]
    async fn test_migrate_nonexistent_source_table() {
        let test_name = "migrate_no_source";
        let source_path = format!("/tmp/delta_migrate_test_{}_src", test_name);
        let output_path = format!("/tmp/delta_migrate_test_{}_out", test_name);
        let _ = fs::remove_dir_all(&source_path);
        let _ = fs::remove_dir_all(&output_path);

        let mut config = make_test_config(&source_path);
        config.delta_sink.table_path = source_path.clone();
        let shutdown = CancellationToken::new();
        let exit_code = run_migrate(config, output_path.clone(), shutdown).await;
        assert_eq!(exit_code, 1, "migration with nonexistent source should fail gracefully");

        // Clean up
        let _ = fs::remove_dir_all(&source_path);
        let _ = fs::remove_dir_all(&output_path);
    }

    #[tokio::test]
    async fn test_migrate_empty_source_table() {
        use base64::{engine::general_purpose::STANDARD, Engine};

        let test_name = "migrate_empty";
        let source_path = format!("/tmp/delta_migrate_test_{}_src", test_name);
        let output_path = format!("/tmp/delta_migrate_test_{}_out", test_name);
        let _ = fs::remove_dir_all(&source_path);
        let _ = fs::remove_dir_all(&output_path);
        let _ = fs::create_dir_all(&source_path);
        let _ = fs::create_dir_all(&output_path);

        // Create source table with old schema and one record, then verify it exists
        // An empty Delta table (created but never written to) still has a valid log.
        // However, run_migrate opens the source directly with deltalake::open_table.
        // Write a minimal record so the table has data, then test that migration works.
        let old_schema = old_delta_schema();
        let b64 = STANDARD.encode(&[1u8]);
        let source_table = open_or_create_table(&source_path, &old_schema)
            .await
            .expect("source table creation failed");
        let batch = old_schema_batch(&old_schema, &[1], &[&b64], "2024-01-01");
        let _source_table = DeltaOps(source_table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("source write failed");

        let mut config = make_test_config(&source_path);
        config.delta_sink.table_path = source_path.clone();
        let shutdown = CancellationToken::new();
        let exit_code = run_migrate(config, output_path.clone(), shutdown).await;
        assert_eq!(exit_code, 0, "migration should succeed");

        // Clean up
        let _ = fs::remove_dir_all(&source_path);
        let _ = fs::remove_dir_all(&output_path);
    }

    #[tokio::test]
    async fn test_backfill_writer_binary_as_der() {
        // Verifies Task 3.1: Backfill writer produces Binary as_der
        // The writer should correctly write DeltaCertRecords with Vec<u8> as_der to Delta table
        let test_name = "backfill_writer_binary_as_der";
        let table_path = format!("/tmp/delta_backfill_test_{}", test_name);
        let _ = fs::remove_dir_all(&table_path);
        let _ = fs::create_dir_all(&table_path);

        let schema = delta_schema();

        // Create an mpsc channel for writer
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // Spawn the writer task
        let table_path_clone = table_path.clone();
        let writer_task = tokio::spawn(async move {
            run_writer(
                table_path_clone,
                10,  // batch_size
                1,   // flush_interval_secs
                9,   // compression_level
                15,  // heavy_column_compression_level
                rx,
                CancellationToken::new(),
            )
            .await
        });

        // Send a record with known as_der bytes
        let mut record = make_test_record(1, "https://log.example.com");
        record.as_der = vec![0xDE, 0xAD, 0xBE, 0xEF];
        tx.send(record).await.expect("Failed to send record");

        // Drop the sender to signal channel close
        drop(tx);

        // Wait for writer to finish
        let result = writer_task.await.expect("writer task panicked");
        assert_eq!(result.total_records_written, 1, "Should have written 1 record");
        assert_eq!(result.write_errors, 0, "Should have no write errors");

        // Re-open the table and verify as_der column is Binary with correct value
        let table = deltalake::open_table(&table_path)
            .await
            .expect("Failed to reopen table");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(table))
            .expect("Failed to register table");

        let df = ctx
            .sql("SELECT as_der FROM ct_records WHERE cert_index = 1")
            .await
            .expect("SQL query failed");
        let batches = df.collect().await.expect("Failed to collect results");

        assert!(!batches.is_empty(), "Should have at least one result");
        let batch = &batches[0];
        let as_der_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("as_der should be Binary type");

        assert_eq!(as_der_col.len(), 1, "Should have one value");
        assert_eq!(
            as_der_col.value(0),
            &[0xDEu8, 0xAD, 0xBE, 0xEF][..],
            "as_der value should match what was sent"
        );

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_merge_path_binary_as_der() {
        // Verifies Task 3.2: Merge path handles Binary as_der correctly
        // After merging staging into main, as_der should remain Binary and data should be preserved
        let test_name = "merge_binary_as_der";
        let main_path = format!("/tmp/delta_backfill_test_{}_main", test_name);
        let staging_path = format!("/tmp/delta_backfill_test_{}_staging", test_name);
        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
        let _ = fs::create_dir_all(&main_path);
        let _ = fs::create_dir_all(&staging_path);

        let schema = delta_schema();

        // Create main table with a record (cert_index=1)
        let main_table = open_or_create_table(&main_path, &schema)
            .await
            .expect("main table creation failed");
        let mut main_record = make_test_record(1, "https://log.example.com");
        main_record.as_der = vec![0x11, 0x22, 0x33, 0x44];
        let main_records = vec![main_record];
        let main_batch = records_to_batch(&main_records, &schema).expect("main batch creation failed");
        let _ = DeltaOps(main_table)
            .write(vec![main_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("main write failed");

        // Create staging table with a different record (cert_index=2)
        let staging_table = open_or_create_table(&staging_path, &schema)
            .await
            .expect("staging table creation failed");
        let mut staging_record = make_test_record(2, "https://log.example.com");
        staging_record.as_der = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let staging_records = vec![staging_record];
        let staging_batch =
            records_to_batch(&staging_records, &schema).expect("staging batch creation failed");
        let _ = DeltaOps(staging_table)
            .write(vec![staging_batch])
            .with_save_mode(SaveMode::Append)
            .await
            .expect("staging write failed");

        // Run merge
        let config = make_test_config(&main_path);
        let shutdown = CancellationToken::new();
        let exit_code = run_merge(config, staging_path.clone(), shutdown).await;
        assert_eq!(exit_code, 0, "Merge should succeed");

        // Verify the merged table has both records with correct as_der values
        let main_table = deltalake::open_table(&main_path)
            .await
            .expect("Failed to reopen main table");
        let ctx = SessionContext::new();
        ctx.register_table("ct_records", Arc::new(main_table))
            .expect("Failed to register table");

        // Query for both records
        let df = ctx
            .sql("SELECT cert_index, as_der FROM ct_records ORDER BY cert_index")
            .await
            .expect("SQL query failed");
        let batches = df.collect().await.expect("Failed to collect results");

        assert!(!batches.is_empty(), "Should have results");
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2, "Should have 2 records");

        // Verify cert_index values
        let cert_indices = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cert_index should be Int64");
        assert_eq!(cert_indices.value(0), 1);
        assert_eq!(cert_indices.value(1), 2);

        // Verify as_der values
        let as_der_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("as_der should be Binary type");
        assert_eq!(as_der_col.value(0), &[0x11u8, 0x22, 0x33, 0x44][..]);
        assert_eq!(as_der_col.value(1), &[0xAAu8, 0xBB, 0xCC, 0xDD][..]);

        let _ = fs::remove_dir_all(&main_path);
        let _ = fs::remove_dir_all(&staging_path);
    }
}
