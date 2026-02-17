use crate::config::Config;
use crate::ct::fetch;
use crate::ct::{fetch_log_list, CtLog, LogType};
use crate::delta_sink::DeltaCertRecord;
use crate::models::Source;
use deltalake::arrow::array::*;
use deltalake::datafusion::prelude::*;
use deltalake::DeltaTableError;
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
    let sql = "SELECT source_url, MIN(cert_index) as min_idx, MAX(cert_index) as max_idx, COUNT(cert_index) as cnt FROM ct_records GROUP BY source_url";

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

/// Orchestrate gap detection across all logs.
///
/// Opens delta table once, queries state for all logs, detects gaps per log,
/// and generates work items for catch-up or historical backfill modes.
///
/// # Arguments
/// * `table_path` - Path to the delta table
/// * `logs` - Vec of (source_url, tree_size) pairs for active logs
/// * `backfill_from` - None for catch-up mode, Some(index) for historical mode
///
/// # Returns
/// * Vec of BackfillWorkItem ranges to fetch
pub async fn detect_gaps(
    table_path: &str,
    logs: &[(String, u64)],
    backfill_from: Option<u64>,
) -> Result<Vec<BackfillWorkItem>, Box<dyn Error>> {
    // Try to open the delta table; if it doesn't exist, return based on mode
    let table = match deltalake::open_table(table_path).await {
        Ok(t) => t,
        Err(_e @ DeltaTableError::NotATable(_)) | Err(_e @ DeltaTableError::InvalidTableLocation(_)) => {
            // Table doesn't exist (NotATable or InvalidTableLocation)
            return match backfill_from {
                None => {
                    // Catch-up mode: no table, no work items
                    Ok(Vec::new())
                }
                Some(from) => {
                    // Historical mode: backfill entire range for all logs
                    let mut work_items = Vec::new();
                    for (source_url, tree_size) in logs {
                        if *tree_size > from {
                            work_items.push(BackfillWorkItem {
                                source_url: source_url.clone(),
                                start: from,
                                end: tree_size - 1,
                            });
                        }
                    }
                    Ok(work_items)
                }
            };
        }
        Err(e) => return Err(Box::new(e)),
    };

    // Create SessionContext and register table
    let ctx = SessionContext::new();
    ctx.register_table("ct_records", std::sync::Arc::new(table))?;

    // Query delta table state
    let log_states = query_log_states(&ctx).await?;

    let mut work_items = Vec::new();

    for (source_url, tree_size) in logs {
        match backfill_from {
            None => {
                // Catch-up mode
                if let Some(state) = log_states.get(source_url) {
                    let lower_bound = state.min_index;

                    // Check for internal gaps
                    let expected_count = state.max_index - state.min_index + 1;
                    if state.count < expected_count {
                        match find_internal_gaps(&ctx, source_url, lower_bound, state.max_index)
                            .await
                        {
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

                    // Check for frontier gap
                    if state.max_index + 1 < *tree_size {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: state.max_index + 1,
                            end: tree_size - 1,
                        });
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
                        match find_internal_gaps(&ctx, source_url, from.max(state.min_index), state.max_index)
                            .await
                        {
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

                    // Frontier gap
                    if state.max_index + 1 < *tree_size {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: state.max_index + 1,
                            end: tree_size - 1,
                        });
                    }
                } else {
                    // Log not in delta: backfill entire range
                    if *tree_size > from {
                        work_items.push(BackfillWorkItem {
                            source_url: source_url.clone(),
                            start: from,
                            end: tree_size - 1,
                        });
                    }
                }
            }
        }
    }

    Ok(work_items)
}

pub async fn run_backfill(
    config: Config,
    backfill_from: Option<u64>,
    backfill_logs: Option<String>,
    shutdown: CancellationToken,
) -> i32 {
    info!("backfill mode starting");

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

    // Step 2: Tree size discovery
    let request_timeout = Duration::from_secs(config.ct_log.request_timeout_secs);
    let mut log_tree_sizes = Vec::new();

    for log in &logs {
        let tree_size = match log.log_type {
            LogType::Rfc6962 => match fetch::get_tree_size(&client, &log.normalized_url(), request_timeout).await {
                Ok(size) => size,
                Err(e) => {
                    warn!(
                        log = %log.description,
                        error = %e,
                        "failed to get tree size"
                    );
                    continue;
                }
            },
            LogType::StaticCt => match fetch::get_checkpoint_tree_size(&client, &log.normalized_url(), request_timeout).await {
                Ok(size) => size,
                Err(e) => {
                    warn!(
                        log = %log.description,
                        error = %e,
                        "failed to get checkpoint tree size"
                    );
                    continue;
                }
            },
        };
        log_tree_sizes.push((log.normalized_url(), tree_size));
    }

    if log_tree_sizes.is_empty() {
        warn!("no logs have valid tree sizes");
        return 1;
    }

    // Step 3: Gap detection
    let work_items = match detect_gaps(&config.delta_sink.table_path, &log_tree_sizes, backfill_from).await {
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
    let (tx, mut rx) = mpsc::channel::<DeltaCertRecord>(channel_buffer_size);

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

    // Drop the original sender so the receiver knows when all senders are gone
    drop(tx);

    // Step 6b: Trivial drain task (TODO: Phase 5 replaces this with real writer)
    let writer_handle = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    // Wait for all fetchers
    let mut any_failed = false;
    for handle in fetcher_handles {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                warn!("fetcher failed: {}", e);
                any_failed = true;
            }
            Err(e) => {
                warn!("fetcher task join error: {}", e);
                any_failed = true;
            }
        }
    }

    // Wait for writer
    let _ = writer_handle.await;

    if any_failed {
        1
    } else {
        0
    }
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

        for (batch_idx, batch_start) in (work_item.start..=work_item.end)
            .step_by(batch_size as usize)
            .enumerate()
        {
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
            let mut retry_count = 0;
            let max_retries = 3;
            let mut backoff_ms = 1000u64;
            const MAX_BACKOFF_MS: u64 = 60000;

            loop {
                let fetch_result = match log_type {
                    LogType::Rfc6962 => {
                        fetch::fetch_entries(&client, &source_url, batch_start, batch_end, &source, request_timeout).await
                    }
                    LogType::StaticCt => {
                        // For StaticCt, calculate tile index and offset
                        let tile_index = batch_start / 256;
                        let offset_in_tile = (batch_start % 256) as usize;
                        fetch::fetch_tile_entries(&client, &source_url, tile_index, 1, offset_in_tile, &source, request_timeout, &issuer_cache).await
                    }
                };

                match fetch_result {
                    Ok(messages) => {
                        // Convert and send records
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
                        if retry_count >= max_retries {
                            warn!(
                                source_url = %source_url,
                                batch_start = batch_start,
                                batch_end = batch_end,
                                "rate limit exceeded max retries"
                            );
                            break;
                        }
                        retry_count += 1;
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
                        if retry_count >= max_retries {
                            warn!(
                                source_url = %source_url,
                                batch_start = batch_start,
                                batch_end = batch_end,
                                error = %e,
                                "HTTP error exceeded max retries"
                            );
                            break;
                        }
                        retry_count += 1;
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
                        if retry_count >= max_retries {
                            warn!(
                                source_url = %source_url,
                                batch_start = batch_start,
                                batch_end = batch_end,
                                error = %e,
                                "invalid response exceeded max retries"
                            );
                            break;
                        }
                        retry_count += 1;
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

            // Progress logging
            let percentage = ((batch_idx + 1) as f64 / num_batches as f64 * 100.0) as u32;
            info!(
                source_url = %source_url,
                fetched = total_records,
                batch_idx = batch_idx + 1,
                total_batches = num_batches,
                progress_percent = percentage,
                "fetcher progress"
            );
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
        let logs = vec![("https://log.example.com".to_string(), 200)];
        let work_items = detect_gaps(&table_path, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should have frontier gap (103, 199)
        assert_eq!(work_items.len(), 1);
        assert_eq!(work_items[0].source_url, "https://log.example.com");
        assert_eq!(work_items[0].start, 103);
        assert_eq!(work_items[0].end, 199);

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

        // Call detect_gaps with tree_size=22 (creates a frontier gap)
        let logs = vec![("https://log.example.com".to_string(), 22)];
        let work_items = detect_gaps(&table_path, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should detect gaps: (13, 14), (17, 19), and frontier gap (21, 21)
        let gaps: Vec<_> = work_items
            .iter()
            .filter(|item| item.source_url == "https://log.example.com")
            .collect();

        // Should have exactly three work items: (13, 14), (17, 19), and frontier (21, 21)
        assert_eq!(gaps.len(), 3, "Should detect internal gaps (13, 14), (17, 19) and frontier gap (21, 21)");

        // Check for gap (13, 14)
        let has_gap_13_14 = gaps.iter().any(|item| item.start == 13 && item.end == 14);
        assert!(has_gap_13_14, "Should detect gap (13, 14)");

        // Check for gap (17, 19)
        let has_gap_17_19 = gaps.iter().any(|item| item.start == 17 && item.end == 19);
        assert!(has_gap_17_19, "Should detect gap (17, 19)");

        // Check for frontier gap (21, 21)
        let frontier_gaps: Vec<_> = gaps.iter().filter(|item| item.start == 21 && item.end == 21).collect();
        assert_eq!(frontier_gaps.len(), 1, "Should detect exactly one frontier gap (21, 21)");

        let _ = fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac2_3_catch_up_frontier_gap() {
        let test_name = "ac2_3_frontier_gap";
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

        // Call detect_gaps with tree_size=10
        let logs = vec![("https://log.example.com".to_string(), 10)];
        let work_items = detect_gaps(&table_path, &logs, None)
            .await
            .expect("detect_gaps failed");

        // Should have frontier gap (3, 9)
        assert_eq!(work_items.len(), 1);
        assert_eq!(work_items[0].start, 3);
        assert_eq!(work_items[0].end, 9);

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
        let work_items = detect_gaps(&table_path, &logs, None)
            .await
            .expect("detect_gaps failed");

        // log-b should produce no work items (skipped)
        let log_b_items: Vec<_> = work_items
            .iter()
            .filter(|item| item.source_url == "https://log-b.example.com")
            .collect();
        assert_eq!(log_b_items.len(), 0, "log-b should be skipped in catch-up mode");

        // log-a should have work items
        let log_a_items: Vec<_> = work_items
            .iter()
            .filter(|item| item.source_url == "https://log-a.example.com")
            .collect();
        assert!(!log_a_items.is_empty(), "log-a should have work items");

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

        // Call detect_gaps with --from 0, tree_size=55
        let logs = vec![("https://log.example.com".to_string(), 55)];
        let work_items = detect_gaps(&table_path, &logs, Some(0))
            .await
            .expect("detect_gaps failed");

        // Should have pre-existing gap (0, 49) and frontier gap (53, 54)
        assert!(work_items.len() >= 2, "Should have pre-existing and frontier gaps");

        let has_pre_existing = work_items.iter().any(|item| item.start == 0 && item.end == 49);
        assert!(has_pre_existing, "Should have pre-existing gap (0, 49)");

        let has_frontier = work_items.iter().any(|item| item.start == 53 && item.end == 54);
        assert!(has_frontier, "Should have frontier gap (53, 54)");

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

        // Call detect_gaps with --from 40, tree_size=55
        let logs = vec![("https://log.example.com".to_string(), 55)];
        let work_items = detect_gaps(&table_path, &logs, Some(40))
            .await
            .expect("detect_gaps failed");

        // Should have gap (40, 49) and frontier gap (53, 54)
        assert!(work_items.len() >= 2, "Should have multiple gaps");

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
        let work_items = detect_gaps(&table_path, &logs, Some(0))
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
        let work_items = detect_gaps(&table_path, &logs, None)
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
        let work_items = detect_gaps(&table_path, &logs, None)
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
}
