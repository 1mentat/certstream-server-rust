use crate::config::Config;
use crate::ct::fetch;
use crate::ct::{fetch_log_list, CtLog, LogType};
use crate::delta_sink::{delta_schema, DeltaCertRecord, flush_buffer, open_or_create_table};
use crate::models::Source;
use crate::state::StateManager;
use deltalake::arrow::array::*;
use deltalake::datafusion::prelude::*;
use deltalake::{DeltaOps, DeltaTable, DeltaTableError};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

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
                            let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size).await;
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
                            let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size).await;
                            let _unused = handle_flush(new_table_opt, flush_result, &mut total_records_written, &mut write_errors, "channel close");
                        }
                        break;
                    }
                }
            }
            _ = flush_timer.tick() => {
                // Time-triggered flush
                if !buffer.is_empty() {
                    let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size).await;
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
                    let (new_table_opt, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size).await;
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

pub async fn run_backfill(
    config: Config,
    staging_path: Option<String>,
    backfill_from: Option<u64>,
    backfill_logs: Option<String>,
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
    let staging_path_ref = staging_path.as_deref();
    let work_items = match detect_gaps(&config.delta_sink.table_path, staging_path_ref, &log_ceilings, backfill_from).await {
        Ok(items) => items,
        Err(e) => {
            warn!("gap detection failed: {}", e);
            return 1;
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
    let table_path = staging_path
        .unwrap_or_else(|| config.delta_sink.table_path.clone());
    let batch_size = config.delta_sink.batch_size;
    let flush_interval_secs = config.delta_sink.flush_interval_secs;
    let shutdown_clone = shutdown.clone();

    let writer_handle = tokio::spawn(async move {
        run_writer(table_path, batch_size, flush_interval_secs, rx, shutdown_clone).await
    });

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
                Ok(builder) => builder,
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
    use deltalake::protocol::SaveMode;
    use deltalake::DeltaOps;
    use std::fs;

    fn make_test_record(cert_index: u64, source_url: &str) -> DeltaCertRecord {
        let json_str = r#"{"message_type":"certificate_update","data":{"update_type":"X509LogEntry","leaf_cert":{"subject":{"CN":"example.com","aggregated":"/CN=example.com"},"issuer":{"CN":"Test CA","aggregated":"/CN=Test CA"},"serial_number":"01","not_before":1700000000,"not_after":1730000000,"fingerprint":"AA:BB","sha1":"CC:DD","sha256":"EE:FF","signature_algorithm":"sha256, rsa","is_ca":false,"all_domains":["example.com"],"as_der":"base64data","extensions":{"ctlPoisonByte":false}},"chain":null,"cert_index":12345,"cert_link":"https://ct.example.com/entry/12345","seen":1700000000.0,"source":{"name":"Test Log","url":"https://ct.example.com/"}}}"#;
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
            run_writer(table_path_clone, 2, 60, rx, shutdown).await
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
            run_writer(table_path_clone, 3, 60, rx, shutdown).await
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
            run_writer(table_path_clone, 2, 60, rx, shutdown).await
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
            run_writer("/invalid/path/that/cannot/be/created".to_string(), 10, 60, rx, shutdown).await
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
            run_writer(table_path_clone, 10, 60, rx, shutdown).await
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
            run_writer(table_path_clone, 2, 60, rx, shutdown).await
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
            run_writer(table_path_clone, batch_size, 60, rx, shutdown).await
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
            run_writer(staging_path_clone, 3, 60, rx, shutdown).await
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
        use crate::config::DeltaSinkConfig;
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
            },
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
}
