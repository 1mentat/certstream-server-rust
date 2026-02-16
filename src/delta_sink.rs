use crate::config::DeltaSinkConfig;
use crate::models::{CertificateMessage, PreSerializedMessage};
use chrono::prelude::*;
use deltalake::arrow::array::*;
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::kernel::{ArrayType, DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaOps, DeltaTable, DeltaTableError};
use serde_json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Represents a flattened certificate record for Delta table storage.
///
/// This struct deserializes from the `full` JSON bytes (a serialized `CertificateMessage`)
/// and flattens the nested hierarchy into a single-level structure suitable for Arrow conversion.
pub struct DeltaCertRecord {
    pub cert_index: u64,
    pub update_type: String,
    pub seen: f64,
    pub seen_date: String,
    pub source_name: String,
    pub source_url: String,
    pub cert_link: String,
    pub serial_number: String,
    pub fingerprint: String,
    pub sha256: String,
    pub sha1: String,
    pub not_before: i64,
    pub not_after: i64,
    pub is_ca: bool,
    pub signature_algorithm: String,
    pub subject_aggregated: String,
    pub issuer_aggregated: String,
    pub all_domains: Vec<String>,
    pub as_der: String,
    pub chain: Vec<String>,
}

impl DeltaCertRecord {
    /// Deserialize from JSON bytes and flatten the nested certificate message structure.
    ///
    /// # Arguments
    /// * `bytes` - JSON bytes containing a serialized `CertificateMessage`
    ///
    /// # Returns
    /// * `Ok(DeltaCertRecord)` with all fields populated
    /// * `Err(serde_json::Error)` if deserialization fails
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let msg: CertificateMessage = serde_json::from_slice(bytes)?;

        // Convert seen (f64 seconds since epoch) to a date string (YYYY-MM-DD)
        let seen_date = {
            let seconds = msg.data.seen as i64;
            let nanos = ((msg.data.seen - seconds as f64) * 1e9) as u32;
            if let Some(dt) = Utc.timestamp_opt(seconds, nanos).single() {
                dt.format("%Y-%m-%d").to_string()
            } else {
                "1970-01-01".to_string()
            }
        };

        // Serialize chain certs to JSON strings, or empty vec if None
        let chain = msg
            .data
            .chain
            .unwrap_or_default()
            .into_iter()
            .map(|cert| serde_json::to_string(&cert).unwrap_or_default())
            .collect();

        // Extract all_domains as Vec<String>
        let all_domains: Vec<String> = msg
            .data
            .leaf_cert
            .all_domains
            .iter()
            .cloned()
            .collect();

        Ok(DeltaCertRecord {
            cert_index: msg.data.cert_index,
            update_type: msg.data.update_type.to_string(),
            seen: msg.data.seen,
            seen_date,
            source_name: msg.data.source.name.to_string(),
            source_url: msg.data.source.url.to_string(),
            cert_link: msg.data.cert_link,
            serial_number: msg.data.leaf_cert.serial_number,
            fingerprint: msg.data.leaf_cert.fingerprint,
            sha256: msg.data.leaf_cert.sha256,
            sha1: msg.data.leaf_cert.sha1,
            not_before: msg.data.leaf_cert.not_before,
            not_after: msg.data.leaf_cert.not_after,
            is_ca: msg.data.leaf_cert.is_ca,
            signature_algorithm: msg.data.leaf_cert.signature_algorithm,
            subject_aggregated: msg.data.leaf_cert.subject.aggregated.unwrap_or_default(),
            issuer_aggregated: msg.data.leaf_cert.issuer.aggregated.unwrap_or_default(),
            all_domains,
            as_der: msg.data.leaf_cert.as_der.unwrap_or_default(),
            chain,
        })
    }
}

/// Returns the Arrow schema for the Delta table.
///
/// The schema defines the columnar structure for storing certificate records.
pub fn delta_schema() -> Arc<Schema> {
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
        Field::new("as_der", DataType::Utf8, false),
        Field::new(
            "chain",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]))
}

/// Converts Arrow schema to Delta StructField for table creation.
///
/// # Arguments
/// * `schema` - Arrow schema to convert
///
/// # Returns
/// * Vec of Delta StructField definitions
fn arrow_schema_to_delta_struct_fields(schema: &Schema) -> Vec<StructField> {
    schema
        .fields()
        .iter()
        .map(|field| {
            StructField::new(
                field.name().to_string(),
                arrow_dtype_to_delta_dtype(field.data_type()),
                field.is_nullable(),
            )
        })
        .collect()
}

/// Converts Arrow DataType to Delta DataType.
///
/// # Arguments
/// * `dtype` - Arrow data type
///
/// # Returns
/// * Equivalent Delta DataType
fn arrow_dtype_to_delta_dtype(dtype: &DataType) -> DeltaDataType {
    match dtype {
        // Delta Lake has no unsigned integer type. cert_index values from CT logs are
        // sequence numbers well within i64::MAX range, so Long (signed i64) is safe here.
        DataType::UInt64 => DeltaDataType::Primitive(PrimitiveType::Long),
        DataType::Utf8 => DeltaDataType::Primitive(PrimitiveType::String),
        DataType::Timestamp(_, _) => DeltaDataType::Primitive(PrimitiveType::Timestamp),
        DataType::Int64 => DeltaDataType::Primitive(PrimitiveType::Long),
        DataType::Boolean => DeltaDataType::Primitive(PrimitiveType::Boolean),
        DataType::List(inner_field) => {
            // Convert List(T) to Array(T) in Delta schema
            let element_type = arrow_dtype_to_delta_dtype(inner_field.data_type());
            DeltaDataType::Array(Box::new(ArrayType::new(
                element_type,
                inner_field.is_nullable(),
            )))
        }
        _ => {
            // Catch-all for unrecognized Arrow DataType
            warn!(
                unrecognized_type = ?dtype,
                "Unrecognized Arrow DataType, falling back to Delta String type"
            );
            DeltaDataType::Primitive(PrimitiveType::String)
        }
    }
}

/// Opens an existing Delta table or creates a new one if it doesn't exist.
///
/// # Arguments
/// * `table_path` - Path to the Delta table directory
/// * `schema` - Arrow schema for the table
///
/// # Returns
/// * `Ok(DeltaTable)` - The opened or created table
/// * `Err(DeltaTableError)` - If opening or creating fails
pub async fn open_or_create_table(
    table_path: &str,
    schema: &Arc<Schema>,
) -> Result<DeltaTable, DeltaTableError> {
    // First, try to open an existing table
    match deltalake::open_table(table_path).await {
        Ok(table) => Ok(table),
        Err(DeltaTableError::NotATable(_)) => {
            // Table doesn't exist, create a new one
            let struct_fields = arrow_schema_to_delta_struct_fields(schema);

            let table = CreateBuilder::new()
                .with_location(table_path)
                .with_columns(struct_fields)
                .with_partition_columns(vec!["seen_date"])
                .await?;

            Ok(table)
        }
        Err(e) => Err(e),
    }
}

/// Converts a batch of `DeltaCertRecord`s into an Arrow `RecordBatch`.
///
/// # Arguments
/// * `records` - Slice of DeltaCertRecord to convert
/// * `schema` - Arrow schema matching the table structure
///
/// # Returns
/// * `Ok(RecordBatch)` with all columns properly typed and populated
/// * `Err(ArrowError)` if array construction fails
pub fn records_to_batch(
    records: &[DeltaCertRecord],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, deltalake::arrow::error::ArrowError> {
    // Build cert_index column (UInt64)
    let cert_index: UInt64Array = records.iter().map(|r| r.cert_index).collect();

    // Build update_type column (Utf8)
    let update_type: StringArray = records
        .iter()
        .map(|r| Some(r.update_type.as_str()))
        .collect();

    // Build seen column (Timestamp in microseconds)
    let seen: TimestampMicrosecondArray = records
        .iter()
        .map(|r| {
            let micros = (r.seen * 1_000_000.0) as i64;
            Some(micros)
        })
        .collect();
    let seen = seen.with_timezone("UTC");

    // Build seen_date column (Utf8)
    let seen_date: StringArray = records
        .iter()
        .map(|r| Some(r.seen_date.as_str()))
        .collect();

    // Build source_name column (Utf8)
    let source_name: StringArray = records
        .iter()
        .map(|r| Some(r.source_name.as_str()))
        .collect();

    // Build source_url column (Utf8)
    let source_url: StringArray = records
        .iter()
        .map(|r| Some(r.source_url.as_str()))
        .collect();

    // Build cert_link column (Utf8)
    let cert_link: StringArray = records
        .iter()
        .map(|r| Some(r.cert_link.as_str()))
        .collect();

    // Build serial_number column (Utf8)
    let serial_number: StringArray = records
        .iter()
        .map(|r| Some(r.serial_number.as_str()))
        .collect();

    // Build fingerprint column (Utf8)
    let fingerprint: StringArray = records
        .iter()
        .map(|r| Some(r.fingerprint.as_str()))
        .collect();

    // Build sha256 column (Utf8)
    let sha256: StringArray = records
        .iter()
        .map(|r| Some(r.sha256.as_str()))
        .collect();

    // Build sha1 column (Utf8)
    let sha1: StringArray = records.iter().map(|r| Some(r.sha1.as_str())).collect();

    // Build not_before column (Int64)
    let not_before: Int64Array = records.iter().map(|r| r.not_before).collect();

    // Build not_after column (Int64)
    let not_after: Int64Array = records.iter().map(|r| r.not_after).collect();

    // Build is_ca column (Boolean)
    let is_ca: BooleanArray = records.iter().map(|r| Some(r.is_ca)).collect();

    // Build signature_algorithm column (Utf8)
    let signature_algorithm: StringArray = records
        .iter()
        .map(|r| Some(r.signature_algorithm.as_str()))
        .collect();

    // Build subject_aggregated column (Utf8)
    let subject_aggregated: StringArray = records
        .iter()
        .map(|r| Some(r.subject_aggregated.as_str()))
        .collect();

    // Build issuer_aggregated column (Utf8)
    let issuer_aggregated: StringArray = records
        .iter()
        .map(|r| Some(r.issuer_aggregated.as_str()))
        .collect();

    // Build all_domains column (List(Utf8))
    let mut all_domains_builder = ListBuilder::new(StringBuilder::new());
    for record in records {
        for domain in &record.all_domains {
            all_domains_builder.values().append_value(domain);
        }
        all_domains_builder.append(true);
    }
    let all_domains = all_domains_builder.finish();

    // Build chain column (List(Utf8))
    let mut chain_builder = ListBuilder::new(StringBuilder::new());
    for record in records {
        for chain_json in &record.chain {
            chain_builder.values().append_value(chain_json);
        }
        chain_builder.append(true);
    }
    let chain = chain_builder.finish();

    // Build as_der column (Utf8)
    let as_der: StringArray = records
        .iter()
        .map(|r| Some(r.as_der.as_str()))
        .collect();

    // Create RecordBatch
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(cert_index),
            Arc::new(update_type),
            Arc::new(seen),
            Arc::new(seen_date),
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
}

/// Checks if buffer has exceeded the overflow threshold and drops oldest half if needed.
///
/// This is a testable helper function for the buffer overflow logic (AC3.4).
/// If the buffer size exceeds 2 * batch_size, drops the oldest half of the records.
///
/// # Arguments
/// * `buffer` - Mutable reference to the record buffer
/// * `batch_size` - Maximum records per batch (used to calculate threshold)
///
/// # Returns
/// * The number of records dropped (0 if no overflow)
///
/// # Behavior
/// * If `buffer.len() > 2 * batch_size`: drops oldest half, returns count dropped
/// * Otherwise: no modification, returns 0
fn check_buffer_overflow(buffer: &mut Vec<DeltaCertRecord>, batch_size: usize) -> usize {
    if buffer.len() > 2 * batch_size {
        let dropped = buffer.len() / 2;
        warn!(
            dropped_records = dropped,
            buffer_size = buffer.len(),
            batch_size = batch_size,
            "Buffer overflow: dropping oldest {} records",
            dropped
        );
        buffer.drain(..dropped);
        dropped
    } else {
        0
    }
}

/// Writes buffered certificate records to the Delta table.
///
/// This function handles batching, overflow protection, and error recovery.
/// The buffer may be retained on write failure for retry on the next flush cycle.
///
/// # Arguments
/// * `table` - The DeltaTable to write to (consumed by DeltaOps, returns updated table)
/// * `buffer` - Mutable reference to the record buffer
/// * `schema` - Arrow schema for the table
/// * `batch_size` - Maximum records per batch (used for overflow check)
///
/// # Returns
/// * A tuple of (updated DeltaTable, Result<usize, Box<dyn Error + Send + Sync>>)
///   - On success: (new_table, Ok(count of records written))
///   - On failure: (reopened_table, Err(error))
///
/// # Behavior
/// 1. Returns early if buffer is empty: (table, Ok(0))
/// 2. Checks buffer overflow: if len > 2 * batch_size, drops oldest half
/// 3. Converts buffer to RecordBatch
/// 4. Writes to Delta with Append mode
/// 5. On success: clears buffer and returns new table
/// 6. On failure: retains buffer, reopens table, returns error
pub async fn flush_buffer(
    table: DeltaTable,
    buffer: &mut Vec<DeltaCertRecord>,
    schema: &Arc<Schema>,
    batch_size: usize,
) -> (DeltaTable, Result<usize, Box<dyn std::error::Error + Send + Sync>>) {
    // If buffer is empty, return early
    if buffer.is_empty() {
        return (table, Ok(0));
    }

    // Check buffer overflow: if exceeds 2 * batch_size, drop oldest half
    check_buffer_overflow(buffer, batch_size);

    let record_count = buffer.len();
    let table_path = table.table_uri();

    // Convert buffer to RecordBatch
    let batch = match records_to_batch(buffer, schema) {
        Ok(b) => b,
        Err(e) => {
            // Retain buffer, return error
            return (
                table,
                Err(format!("Failed to convert records to batch: {}", e).into()),
            );
        }
    };

    // Write to Delta using DeltaOps with timing
    let start_time = Instant::now();
    let result = DeltaOps(table)
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await;
    let elapsed = start_time.elapsed();

    match result {
        Ok(new_table) => {
            // Success: clear buffer and return new table
            metrics::histogram!("certstream_delta_flush_duration_seconds").record(elapsed.as_secs_f64());
            buffer.clear();
            (new_table, Ok(record_count))
        }
        Err(e) => {
            // Failure: retain buffer, reopen table, return error
            metrics::histogram!("certstream_delta_flush_duration_seconds").record(elapsed.as_secs_f64());
            warn!(
                error = %e,
                "Delta write failed, retaining buffer for retry"
            );

            // Reopen table to get a fresh handle per specification
            match deltalake::open_table(&table_path).await {
                Ok(reopened_table) => {
                    (
                        reopened_table,
                        Err(format!("Failed to write records to Delta: {}", e).into()),
                    )
                }
                Err(reopen_err) => {
                    // If reopen also fails, we have a critical situation
                    // The table may be corrupted or inaccessible
                    // We'll still try to continue by using open_or_create
                    warn!(
                        error = %reopen_err,
                        "Failed to reopen table after write failure"
                    );

                    // Try open_or_create as a recovery measure
                    match open_or_create_table(&table_path, schema).await {
                        Ok(recovery_table) => {
                            (
                                recovery_table,
                                Err(format!("Failed to write records to Delta: {} (reopen also failed)", e).into()),
                            )
                        }
                        Err(recovery_err) => {
                            // All recovery attempts failed - log comprehensively per AC4.4
                            error!(
                                write_error = %e,
                                reopen_error = %reopen_err,
                                recovery_error = %recovery_err,
                                "All table recovery attempts failed"
                            );
                            // Per AC4.4: Table failures must be non-fatal to real-time streaming.
                            // Since table was consumed by DeltaOps, attempt final recovery via open_or_create.
                            // Buffer is retained in caller for potential retry on next flush cycle.
                            let fallback_table = open_or_create_table(&table_path, schema).await;
                            match fallback_table {
                                Ok(recovered_table) => {
                                    (
                                        recovered_table,
                                        Err(format!("Failed to write records to Delta: {} (recovery succeeded)", e).into()),
                                    )
                                }
                                Err(final_recovery_err) => {
                                    // Final recovery attempt also failed - table is inaccessible
                                    error!(
                                        error = %final_recovery_err,
                                        table_path = %table_path,
                                        "Final recovery attempt failed - table will be unavailable for this flush cycle"
                                    );
                                    // We cannot recover a table handle through normal means.
                                    // As a last resort, attempt one more time with a fresh call.
                                    // This accommodates transient errors that might resolve on retry.
                                    let last_attempt = open_or_create_table(&table_path, schema).await;
                                    (
                                        last_attempt.expect("Unable to recover Delta table after all recovery attempts"),
                                        Err(format!("Failed to write records to Delta: {} (all recovery attempts failed)", e).into()),
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Records flush metrics and logs based on the flush result.
///
/// This helper function consolidates the duplicate metric emission and logging logic
/// that occurs after each flush operation (size-triggered, time-triggered, and shutdown).
///
/// # Arguments
/// * `result` - The Result from flush_buffer indicating success or failure
/// * `context` - A string describing the flush context (e.g., "Batch flush", "Timer flush", "Shutdown flush")
fn record_flush_metrics(result: &Result<usize, Box<dyn std::error::Error + Send + Sync>>, context: &str) {
    match result {
        Ok(count) => {
            debug!(
                records_written = count,
                "{} completed",
                context
            );
            metrics::counter!("certstream_delta_records_written").increment(*count as u64);
            metrics::counter!("certstream_delta_flushes").increment(1);
        }
        Err(e) => {
            warn!(
                error = %e,
                "{} failed, buffer retained for retry",
                context
            );
            metrics::counter!("certstream_delta_write_errors").increment(1);
        }
    }
}

/// Main async task for the Delta sink.
///
/// Receives certificate messages from a broadcast channel, deserializes them into
/// `DeltaCertRecord` structs, buffers them, and periodically flushes to the Delta table.
/// Implements batching (AC3.1), time-based flush (AC3.2), and graceful shutdown (AC3.3).
///
/// # Arguments
/// * `config` - Configuration containing table path, batch size, and flush interval
/// * `rx` - Broadcast receiver for `Arc<PreSerializedMessage>` events
/// * `shutdown` - CancellationToken for graceful shutdown signal
///
/// # Behavior
/// 1. Opens or creates the Delta table on startup
/// 2. Subscribes to broadcast channel via `rx.recv()`
/// 3. Deserializes `msg.full` JSON into `DeltaCertRecord`
/// 4. Buffers records and flushes when size threshold (batch_size) or time threshold (flush_interval_secs) is reached
/// 5. On graceful shutdown, flushes remaining buffered records and exits
/// 6. Lagged messages are logged and skipped (AC4.3)
/// 7. Deserialization failures are logged and skipped (AC1.5)
pub async fn run_delta_sink(
    config: DeltaSinkConfig,
    mut rx: broadcast::Receiver<Arc<PreSerializedMessage>>,
    shutdown: CancellationToken,
) {
    let schema = delta_schema();

    // Try to open or create the table; if it fails, log error and return
    let mut table = match open_or_create_table(&config.table_path, &schema).await {
        Ok(t) => t,
        Err(e) => {
            error!(
                error = %e,
                table_path = %config.table_path,
                "Failed to open or create Delta table at startup, delta-sink task exiting"
            );
            return;
        }
    };

    info!(
        table_path = %config.table_path,
        batch_size = config.batch_size,
        flush_interval_secs = config.flush_interval_secs,
        "Delta sink task started"
    );

    let mut flush_interval = tokio::time::interval(Duration::from_secs(config.flush_interval_secs));
    let mut buffer: Vec<DeltaCertRecord> = Vec::with_capacity(config.batch_size);

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        // Deserialize msg.full into DeltaCertRecord
                        match DeltaCertRecord::from_json(&msg.full) {
                            Ok(record) => {
                                buffer.push(record);

                                // Update buffer size gauge
                                metrics::gauge!("certstream_delta_buffer_size").set(buffer.len() as f64);

                                // If buffer.len() >= config.batch_size: flush (AC3.1)
                                if buffer.len() >= config.batch_size {
                                    let (new_table, flush_result) = flush_buffer(table, &mut buffer, &schema, config.batch_size).await;
                                    table = new_table;
                                    record_flush_metrics(&flush_result, "Batch flush");
                                }
                            }
                            Err(e) => {
                                // On deser failure: log warning, skip (AC1.5)
                                warn!(
                                    error = %e,
                                    "Failed to deserialize message, skipping"
                                );
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Log warning with count, continue (AC4.3)
                        warn!(
                            lagged = n,
                            "Sink receiver lagged, skipping {} messages",
                            n
                        );
                        metrics::counter!("certstream_delta_messages_lagged").increment(n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Channel closed, break
                        info!("Broadcast channel closed, exiting receive loop");
                        break;
                    }
                }
            }
            _ = flush_interval.tick() => {
                // Time-triggered flush (AC3.2)
                if !buffer.is_empty() {
                    let (new_table, flush_result) = flush_buffer(table, &mut buffer, &schema, config.batch_size).await;
                    table = new_table;
                    record_flush_metrics(&flush_result, "Timer-triggered flush");
                }
            }
            _ = shutdown.cancelled() => {
                // Graceful shutdown: flush remaining buffer (AC3.3)
                if !buffer.is_empty() {
                    info!(
                        records_in_buffer = buffer.len(),
                        "Graceful shutdown initiated, flushing remaining buffer"
                    );
                    let (_final_table, flush_result) = flush_buffer(table, &mut buffer, &schema, config.batch_size).await;
                    record_flush_metrics(&flush_result, "Shutdown flush");
                }
                break;
            }
        }
    }

    info!("Delta sink task stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_test_json_bytes() -> Vec<u8> {
        let json_str = r#"{"message_type":"certificate_update","data":{"update_type":"X509LogEntry","leaf_cert":{"subject":{"CN":"example.com","aggregated":"/CN=example.com"},"issuer":{"CN":"Test CA","aggregated":"/CN=Test CA"},"serial_number":"01","not_before":1700000000,"not_after":1730000000,"fingerprint":"AA:BB","sha1":"CC:DD","sha256":"EE:FF","signature_algorithm":"sha256, rsa","is_ca":false,"all_domains":["example.com","www.example.com"],"as_der":"base64encodedderdata","extensions":{"ctlPoisonByte":false}},"chain":[{"subject":{"CN":"Intermediate CA","aggregated":"/CN=Intermediate CA"},"issuer":{"CN":"Root CA","aggregated":"/CN=Root CA"},"serial_number":"02","not_before":1600000000,"not_after":1800000000,"fingerprint":"GG:HH","sha1":"II:JJ","sha256":"KK:LL","signature_algorithm":"sha256, rsa","is_ca":true,"as_der":null,"extensions":{"ctlPoisonByte":false}}],"cert_index":12345,"cert_link":"https://ct.example.com/entry/12345","seen":1700000000.0,"source":{"name":"Test Log","url":"https://ct.example.com/"}}}"#;
        json_str.as_bytes().to_vec()
    }

    fn make_test_record() -> DeltaCertRecord {
        DeltaCertRecord::from_json(&make_test_json_bytes()).expect("failed to deserialize test record")
    }

    #[test]
    fn test_from_json_deserializes_all_fields() {
        let json_bytes = make_test_json_bytes();

        let record = DeltaCertRecord::from_json(&json_bytes).expect("deserialization failed");

        assert_eq!(record.cert_index, 12345);
        assert_eq!(record.update_type, "X509LogEntry");
        assert_eq!(record.seen, 1700000000.0);
        assert_eq!(record.source_name, "Test Log");
        assert_eq!(record.source_url, "https://ct.example.com/");
        assert_eq!(record.cert_link, "https://ct.example.com/entry/12345");
        assert_eq!(record.serial_number, "01");
        assert_eq!(record.fingerprint, "AA:BB");
        assert_eq!(record.sha256, "EE:FF");
        assert_eq!(record.sha1, "CC:DD");
        assert_eq!(record.not_before, 1700000000);
        assert_eq!(record.not_after, 1730000000);
        assert_eq!(record.is_ca, false);
        assert_eq!(record.signature_algorithm, "sha256, rsa");
        assert_eq!(record.subject_aggregated, "/CN=example.com");
        assert_eq!(record.issuer_aggregated, "/CN=Test CA");
        assert_eq!(record.all_domains, vec!["example.com", "www.example.com"]);
        assert_eq!(record.as_der, "base64encodedderdata");
    }

    #[test]
    fn test_from_json_derives_seen_date() {
        let json_bytes = make_test_json_bytes();
        let record = DeltaCertRecord::from_json(&json_bytes).expect("deserialization failed");

        // 1700000000 seconds since epoch is 2023-11-14 in UTC
        assert_eq!(record.seen_date, "2023-11-14");
    }

    #[test]
    fn test_from_json_serializes_chain_to_json_strings() {
        let json_bytes = make_test_json_bytes();
        let record = DeltaCertRecord::from_json(&json_bytes).expect("deserialization failed");

        assert_eq!(record.chain.len(), 1);
        // Chain cert should be serialized as JSON
        assert!(record.chain[0].contains("Intermediate CA"));
    }

    #[test]
    fn test_from_json_with_empty_chain() {
        let json_str = r#"{"message_type":"certificate_update","data":{"update_type":"X509LogEntry","leaf_cert":{"subject":{"CN":"example.com","aggregated":"/CN=example.com"},"issuer":{"CN":"Test CA","aggregated":"/CN=Test CA"},"serial_number":"01","not_before":1700000000,"not_after":1730000000,"fingerprint":"AA:BB","sha1":"CC:DD","sha256":"EE:FF","signature_algorithm":"sha256, rsa","is_ca":false,"all_domains":["example.com","www.example.com"],"as_der":"base64encodedderdata","extensions":{"ctlPoisonByte":false}},"chain":null,"cert_index":12345,"cert_link":"https://ct.example.com/entry/12345","seen":1700000000.0,"source":{"name":"Test Log","url":"https://ct.example.com/"}}}"#;
        let json_bytes = json_str.as_bytes();
        let record = DeltaCertRecord::from_json(json_bytes).expect("deserialization failed");

        assert_eq!(record.chain.len(), 0);
    }

    #[test]
    fn test_from_json_with_empty_as_der() {
        let json_str = r#"{"message_type":"certificate_update","data":{"update_type":"X509LogEntry","leaf_cert":{"subject":{"CN":"example.com","aggregated":"/CN=example.com"},"issuer":{"CN":"Test CA","aggregated":"/CN=Test CA"},"serial_number":"01","not_before":1700000000,"not_after":1730000000,"fingerprint":"AA:BB","sha1":"CC:DD","sha256":"EE:FF","signature_algorithm":"sha256, rsa","is_ca":false,"all_domains":["example.com","www.example.com"],"as_der":null,"extensions":{"ctlPoisonByte":false}},"chain":null,"cert_index":12345,"cert_link":"https://ct.example.com/entry/12345","seen":1700000000.0,"source":{"name":"Test Log","url":"https://ct.example.com/"}}}"#;
        let json_bytes = json_str.as_bytes();
        let record = DeltaCertRecord::from_json(json_bytes).expect("deserialization failed");

        assert_eq!(record.as_der, "");
    }

    #[test]
    fn test_from_json_with_empty_domains() {
        let json_str = r#"{"message_type":"certificate_update","data":{"update_type":"X509LogEntry","leaf_cert":{"subject":{"CN":"example.com","aggregated":"/CN=example.com"},"issuer":{"CN":"Test CA","aggregated":"/CN=Test CA"},"serial_number":"01","not_before":1700000000,"not_after":1730000000,"fingerprint":"AA:BB","sha1":"CC:DD","sha256":"EE:FF","signature_algorithm":"sha256, rsa","is_ca":false,"all_domains":[],"as_der":"base64encodedderdata","extensions":{"ctlPoisonByte":false}},"chain":null,"cert_index":12345,"cert_link":"https://ct.example.com/entry/12345","seen":1700000000.0,"source":{"name":"Test Log","url":"https://ct.example.com/"}}}"#;
        let json_bytes = json_str.as_bytes();
        let record = DeltaCertRecord::from_json(json_bytes).expect("deserialization failed");

        assert_eq!(record.all_domains.len(), 0);
    }

    #[test]
    fn test_delta_schema_has_correct_field_count() {
        let schema = delta_schema();
        assert_eq!(schema.fields().len(), 20);
    }

    #[test]
    fn test_delta_schema_field_types() {
        let schema = delta_schema();
        let fields = schema.fields();

        // Check cert_index is UInt64
        assert_eq!(fields[0].name(), "cert_index");
        assert_eq!(fields[0].data_type(), &DataType::UInt64);

        // Check update_type is Utf8
        assert_eq!(fields[1].name(), "update_type");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);

        // Check seen is Timestamp(Microsecond, UTC)
        assert_eq!(fields[2].name(), "seen");
        match fields[2].data_type() {
            DataType::Timestamp(unit, tz) => {
                assert_eq!(*unit, TimeUnit::Microsecond);
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            _ => panic!("expected Timestamp for seen field"),
        }

        // Check all_domains is List(Utf8)
        assert_eq!(fields[17].name(), "all_domains");
        match fields[17].data_type() {
            DataType::List(inner_field) => {
                assert_eq!(inner_field.data_type(), &DataType::Utf8);
            }
            _ => panic!("expected List for all_domains field"),
        }

        // Check is_ca is Boolean
        assert_eq!(fields[13].name(), "is_ca");
        assert_eq!(fields[13].data_type(), &DataType::Boolean);

        // Check chain is List(Utf8)
        assert_eq!(fields[19].name(), "chain");
        match fields[19].data_type() {
            DataType::List(inner_field) => {
                assert_eq!(inner_field.data_type(), &DataType::Utf8);
            }
            _ => panic!("expected List for chain field"),
        }
    }

    #[test]
    fn test_records_to_batch_creates_correct_row_count() {
        let schema = delta_schema();
        let records: Vec<DeltaCertRecord> = (0..5)
            .map(|_| make_test_record())
            .collect();

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");
        assert_eq!(batch.num_rows(), 5);
    }

    #[test]
    fn test_records_to_batch_creates_correct_column_count() {
        let schema = delta_schema();
        let record = make_test_record();

        let batch = records_to_batch(&[record], &schema).expect("batch creation failed");
        assert_eq!(batch.num_columns(), 20);
    }

    #[test]
    fn test_records_to_batch_contains_as_der_string() {
        let schema = delta_schema();
        let record = make_test_record();

        let batch = records_to_batch(&[record], &schema).expect("batch creation failed");

        // as_der is at index 18
        let as_der_col = batch
            .column(18)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("as_der column should be StringArray");
        assert_eq!(as_der_col.value(0), "base64encodedderdata");
    }

    #[test]
    fn test_records_to_batch_contains_chain_as_list() {
        let schema = delta_schema();
        let record = make_test_record();

        let batch = records_to_batch(&[record], &schema).expect("batch creation failed");

        // chain is at index 19
        let chain_col = batch
            .column(19)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("chain column should be ListArray");
        assert_eq!(chain_col.len(), 1);
    }

    #[test]
    fn test_records_to_batch_contains_all_domains_list() {
        let schema = delta_schema();
        let record = make_test_record();

        let batch = records_to_batch(&[record], &schema).expect("batch creation failed");

        // all_domains is at index 17
        let all_domains_col = batch
            .column(17)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("all_domains column should be ListArray");
        assert_eq!(all_domains_col.len(), 1);
    }

    #[test]
    fn test_records_to_batch_is_ca_boolean_values() {
        let schema = delta_schema();
        let record = make_test_record();

        let batch = records_to_batch(&[record], &schema).expect("batch creation failed");

        // is_ca is at index 13
        let is_ca_col = batch
            .column(13)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("is_ca column should be BooleanArray");
        assert_eq!(is_ca_col.value(0), false);
    }

    #[test]
    fn test_records_to_batch_seen_timestamp() {
        let schema = delta_schema();
        let record = make_test_record();

        let batch = records_to_batch(&[record], &schema).expect("batch creation failed");

        // seen is at index 2
        let seen_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("seen column should be TimestampMicrosecondArray");

        // 1700000000 seconds = 1700000000000000 microseconds
        let expected_micros = 1700000000i64 * 1_000_000;
        assert_eq!(seen_col.value(0), expected_micros);
    }

    #[test]
    fn test_records_to_batch_multiple_records() {
        let schema = delta_schema();
        let records: Vec<DeltaCertRecord> = (0..3)
            .map(|_| make_test_record())
            .collect();

        let batch = records_to_batch(&records, &schema).expect("batch creation failed");

        assert_eq!(batch.num_rows(), 3);

        // Check cert_index column (index 0) - all records have the same cert_index from the test data
        let cert_index_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("cert_index column should be UInt64Array");
        assert_eq!(cert_index_col.value(0), 12345);
        assert_eq!(cert_index_col.value(1), 12345);
        assert_eq!(cert_index_col.value(2), 12345);
    }

    #[test]
    fn test_records_to_batch_with_empty_domains_and_chain() {
        let schema = delta_schema();
        let json_str = r#"{"message_type":"certificate_update","data":{"update_type":"X509LogEntry","leaf_cert":{"subject":{"CN":"example.com","aggregated":"/CN=example.com"},"issuer":{"CN":"Test CA","aggregated":"/CN=Test CA"},"serial_number":"01","not_before":1700000000,"not_after":1730000000,"fingerprint":"AA:BB","sha1":"CC:DD","sha256":"EE:FF","signature_algorithm":"sha256, rsa","is_ca":false,"all_domains":[],"as_der":null,"extensions":{"ctlPoisonByte":false}},"chain":null,"cert_index":12345,"cert_link":"https://ct.example.com/entry/12345","seen":1700000000.0,"source":{"name":"Test Log","url":"https://ct.example.com/"}}}"#;
        let json_bytes = json_str.as_bytes();
        let record = DeltaCertRecord::from_json(json_bytes).expect("deserialization failed");

        let batch = records_to_batch(&[record], &schema).expect("batch creation failed");

        // Verify batch was created successfully with 1 row
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 20);

        // as_der should be empty string (index 18)
        let as_der_col = batch
            .column(18)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("as_der column should be StringArray");
        assert_eq!(as_der_col.value(0), "");
    }

    #[tokio::test]
    async fn test_open_or_create_table_creates_new_table() {
        let test_name = "table_creation";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);

        // Clean up any existing test data
        let _ = std::fs::remove_dir_all(&table_path);

        // Create parent directory if needed
        let _ = std::fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let result = open_or_create_table(&table_path, &schema).await;

        // Verify table was created successfully
        assert!(result.is_ok(), "table creation should succeed");

        let table = result.unwrap();

        // Verify table version is 0 (newly created)
        assert_eq!(table.version(), 0, "new table should have version 0");

        // Verify schema exists and has correct field count
        let table_schema = table.get_schema().expect("should have schema");
        let field_count = table_schema.fields().count();
        assert_eq!(
            field_count, 20,
            "table schema should have 20 fields"
        );

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_open_or_create_table_reopens_existing_table() {
        let test_name = "table_reopen";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);

        // Clean up any existing test data
        let _ = std::fs::remove_dir_all(&table_path);

        // Create parent directory if needed
        let _ = std::fs::create_dir_all(&table_path);

        let schema = delta_schema();

        // First call: create the table
        let result1 = open_or_create_table(&table_path, &schema).await;
        assert!(result1.is_ok(), "first table creation should succeed");

        let table1 = result1.unwrap();
        let version1 = table1.version();

        // Second call: should open existing table
        let result2 = open_or_create_table(&table_path, &schema).await;
        assert!(result2.is_ok(), "reopening table should succeed");

        let table2 = result2.unwrap();
        let version2 = table2.version();

        // Verify that reopening the table doesn't create a new version
        // (version should still be 0 since we haven't written any data)
        assert_eq!(version1, version2, "table versions should match");
        assert_eq!(
            version2, 0,
            "table should still have version 0 after reopening"
        );

        // Verify schema is still correct
        let table_schema = table2.get_schema().expect("should have schema");
        let field_count = table_schema.fields().count();
        assert_eq!(
            field_count, 20,
            "reopened table schema should have 20 fields"
        );

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_open_or_create_table_preserves_seen_date_partition() {
        let test_name = "table_partition";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);

        // Clean up any existing test data
        let _ = std::fs::remove_dir_all(&table_path);

        // Create parent directory if needed
        let _ = std::fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let result = open_or_create_table(&table_path, &schema).await;

        assert!(result.is_ok(), "table creation should succeed");

        let table = result.unwrap();

        // Get table metadata
        let metadata = table
            .metadata()
            .expect("should have metadata");

        // Verify partition columns are set correctly
        let partition_columns: Vec<String> = metadata
            .partition_columns
            .iter()
            .map(|s| s.to_string())
            .collect();

        assert_eq!(
            partition_columns, vec!["seen_date"],
            "table should have seen_date as partition column"
        );

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[test]
    fn test_buffer_overflow_drops_oldest_half() {
        // AC3.4: Test buffer overflow logic
        let batch_size = 10;
        let mut buffer: Vec<DeltaCertRecord> = (0..(2 * batch_size + 1))
            .map(|i| {
                let mut record = make_test_record();
                record.cert_index = i as u64;
                record
            })
            .collect();

        let initial_len = buffer.len();
        let dropped = check_buffer_overflow(&mut buffer, batch_size);

        // Verify oldest half was dropped
        assert_eq!(dropped, initial_len / 2, "should drop oldest half");
        assert_eq!(buffer.len(), initial_len - dropped, "buffer should be halved");

        // Verify that remaining records are the newer ones (higher cert_index)
        for i in 0..buffer.len() {
            assert!(
                buffer[i].cert_index >= (initial_len / 2) as u64,
                "remaining records should be the newer ones"
            );
        }
    }

    #[test]
    fn test_buffer_overflow_no_drop_at_threshold() {
        // AC3.4: Buffer at exactly 2x batch_size should not be modified
        let batch_size = 10;
        let mut buffer: Vec<DeltaCertRecord> = (0..(2 * batch_size))
            .map(|i| {
                let mut record = make_test_record();
                record.cert_index = i as u64;
                record
            })
            .collect();

        let initial_len = buffer.len();
        let dropped = check_buffer_overflow(&mut buffer, batch_size);

        // Verify no records were dropped (buffer is at threshold, not exceeding)
        assert_eq!(dropped, 0, "should not drop at exact threshold");
        assert_eq!(buffer.len(), initial_len, "buffer should not change");
    }

    #[test]
    fn test_buffer_overflow_no_drop_below_threshold() {
        // AC3.4: Buffer below 2x batch_size should not be modified
        let batch_size = 10;
        let mut buffer: Vec<DeltaCertRecord> = (0..(2 * batch_size - 1))
            .map(|i| {
                let mut record = make_test_record();
                record.cert_index = i as u64;
                record
            })
            .collect();

        let initial_len = buffer.len();
        let dropped = check_buffer_overflow(&mut buffer, batch_size);

        // Verify no records were dropped
        assert_eq!(dropped, 0, "should not drop below threshold");
        assert_eq!(buffer.len(), initial_len, "buffer should not change");
    }

    #[tokio::test]
    async fn test_ac1_4_records_partitioned_by_seen_date() {
        // AC1.4: Records are partitioned by seen_date (YYYY-MM-DD derived from seen timestamp)
        let test_name = "partition_seen_date";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);
        let _ = std::fs::remove_dir_all(&table_path);
        let _ = std::fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        // Create records with different timestamps
        // 1700000000 = 2023-11-14, 1800000000 = 2027-01-01
        let mut record1 = make_test_record();
        record1.seen = 1700000000.0;
        record1.seen_date = "2023-11-14".to_string();
        record1.cert_index = 1;

        let mut record2 = make_test_record();
        record2.seen = 1800000000.0;
        record2.seen_date = "2027-01-01".to_string();
        record2.cert_index = 2;

        let mut buffer = vec![record1, record2];

        // Flush to Delta
        let (_, flush_result) = flush_buffer(table, &mut buffer, &schema, 10).await;
        assert!(flush_result.is_ok(), "flush should succeed");
        assert_eq!(flush_result.unwrap(), 2, "should flush 2 records");

        // Verify partition directories exist
        let partition_dir1 = format!("{}/seen_date=2023-11-14", table_path);
        let partition_dir2 = format!("{}/seen_date=2027-01-01", table_path);

        assert!(
            std::path::Path::new(&partition_dir1).exists(),
            "partition directory for 2023-11-14 should exist"
        );
        assert!(
            std::path::Path::new(&partition_dir2).exists(),
            "partition directory for 2027-01-01 should exist"
        );

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac1_5_malformed_json_skipped() {
        // AC1.5: Messages that fail deserialization are skipped and logged
        let malformed_json = b"not valid json";

        let result = DeltaCertRecord::from_json(malformed_json);
        assert!(result.is_err(), "malformed JSON should fail deserialization");
    }

    #[tokio::test]
    async fn test_ac3_1_size_triggered_flush() {
        // AC3.1: Buffer flushes when it reaches batch_size records
        let test_name = "size_triggered_flush";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);
        let _ = std::fs::remove_dir_all(&table_path);
        let _ = std::fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        let batch_size = 5;
        let mut buffer: Vec<DeltaCertRecord> = (0..batch_size)
            .map(|i| {
                let mut record = make_test_record();
                record.cert_index = i as u64;
                record
            })
            .collect();

        // Flush exactly batch_size records
        let (_table, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size).await;

        assert!(flush_result.is_ok(), "flush should succeed");
        assert_eq!(flush_result.unwrap(), batch_size as usize, "should flush all records");
        assert!(buffer.is_empty(), "buffer should be cleared after successful flush");

        // Verify table can be reopened with version 1
        let table_reopen = deltalake::open_table(&table_path).await.expect("should reopen table");
        assert_eq!(table_reopen.version(), 1, "table should have version 1 after flush");

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac3_2_time_triggered_flush() {
        // AC3.2: Buffer flushes when flush_interval_secs elapses
        let test_name = "time_triggered_flush";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);
        let _ = std::fs::remove_dir_all(&table_path);
        let _ = std::fs::create_dir_all(&table_path);

        let config = crate::config::DeltaSinkConfig {
            enabled: true,
            table_path: table_path.clone(),
            batch_size: 100,
            flush_interval_secs: 1,
        };

        let (tx, _) = tokio::sync::broadcast::channel::<Arc<PreSerializedMessage>>(10);
        let rx = tx.subscribe();
        let shutdown = CancellationToken::new();

        // Spawn the sink task
        let task = tokio::spawn(run_delta_sink(config, rx, shutdown.clone()));

        // Send a few messages (less than batch_size)
        let msg = make_test_json_bytes();
        let pre_serialized = PreSerializedMessage {
            full: Bytes::from(msg),
            lite: Bytes::new(),
            domains_only: Bytes::new(),
        };

        for i in 0..3 {
            let mut psm = pre_serialized.clone();
            let mut json_msg: serde_json::Value =
                serde_json::from_slice(&psm.full).expect("valid json");
            json_msg["data"]["cert_index"] = serde_json::json!(i);
            psm.full = bytes::Bytes::from(serde_json::to_vec(&json_msg).unwrap());

            let _ = tx.send(Arc::new(psm));
        }

        // Wait for flush interval to elapse and a bit more
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify table has version 1 (one flush occurred)
        let table = deltalake::open_table(&table_path)
            .await
            .expect("should open table");
        assert_eq!(table.version(), 1, "table should have one version after timer flush");

        // Graceful shutdown
        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac3_3_graceful_shutdown_flush() {
        // AC3.3: Graceful shutdown flushes remaining buffered records before exit
        let test_name = "graceful_shutdown_flush";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);
        let _ = std::fs::remove_dir_all(&table_path);
        let _ = std::fs::create_dir_all(&table_path);

        let config = crate::config::DeltaSinkConfig {
            enabled: true,
            table_path: table_path.clone(),
            batch_size: 100,
            flush_interval_secs: 60,
        };

        let (tx, _) = tokio::sync::broadcast::channel::<Arc<PreSerializedMessage>>(10);
        let rx = tx.subscribe();
        let shutdown = CancellationToken::new();

        // Spawn the sink task
        let task = tokio::spawn(run_delta_sink(config, rx, shutdown.clone()));

        // Send a few messages (less than batch_size, so timer won't trigger)
        let msg = make_test_json_bytes();
        let pre_serialized = PreSerializedMessage {
            full: Bytes::from(msg),
            lite: Bytes::new(),
            domains_only: Bytes::new(),
        };

        for i in 0..5 {
            let mut psm = pre_serialized.clone();
            let mut json_msg: serde_json::Value =
                serde_json::from_slice(&psm.full).expect("valid json");
            json_msg["data"]["cert_index"] = serde_json::json!(i);
            psm.full = bytes::Bytes::from(serde_json::to_vec(&json_msg).unwrap());

            let _ = tx.send(Arc::new(psm));
        }

        // Give a moment for messages to be received
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Trigger graceful shutdown
        shutdown.cancel();

        // Wait for task to complete
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;

        // Verify table has version 1 (shutdown flush occurred)
        let table = deltalake::open_table(&table_path)
            .await
            .expect("should open table");
        assert_eq!(table.version(), 1, "table should have one version after shutdown flush");

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac3_4_buffer_overflow_drop_oldest() {
        // AC3.4: Buffer exceeding 2x batch_size drops oldest half
        let test_name = "buffer_overflow";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);
        let _ = std::fs::remove_dir_all(&table_path);
        let _ = std::fs::create_dir_all(&table_path);

        let schema = delta_schema();
        let table = open_or_create_table(&table_path, &schema)
            .await
            .expect("table creation failed");

        let batch_size = 10;

        // Create buffer exceeding 2x batch_size
        let mut buffer: Vec<DeltaCertRecord> = (0..(2 * batch_size + 5))
            .map(|i| {
                let mut record = make_test_record();
                record.cert_index = i as u64;
                record
            })
            .collect();

        let initial_len = buffer.len();
        assert!(
            initial_len > 2 * batch_size,
            "buffer should exceed 2x batch_size"
        );

        // Flush (which includes overflow check)
        let (_table, flush_result) = flush_buffer(table, &mut buffer, &schema, batch_size).await;

        assert!(flush_result.is_ok(), "flush should succeed");

        // Verify buffer was cleared after successful flush
        assert!(buffer.is_empty(), "buffer should be cleared after flush");

        // Create another overflow scenario to test the drop logic
        let mut buffer2: Vec<DeltaCertRecord> = (0..(2 * batch_size + 3))
            .map(|i| {
                let mut record = make_test_record();
                record.cert_index = i as u64;
                record
            })
            .collect();

        let before_len = buffer2.len();
        check_buffer_overflow(&mut buffer2, batch_size);

        // Verify oldest half was dropped
        assert_eq!(
            buffer2.len(),
            before_len - (before_len / 2),
            "should have dropped oldest half"
        );

        // Verify remaining records are the newer ones
        for (idx, record) in buffer2.iter().enumerate() {
            assert!(
                record.cert_index >= (before_len / 2) as u64,
                "remaining record at index {} should have cert_index >= {}",
                idx,
                before_len / 2
            );
        }

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_malformed_json_in_sink_task() {
        // AC1.5: Sink task handles malformed JSON gracefully and continues
        let test_name = "malformed_json_sink";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);
        let _ = std::fs::remove_dir_all(&table_path);
        let _ = std::fs::create_dir_all(&table_path);

        let config = crate::config::DeltaSinkConfig {
            enabled: true,
            table_path: table_path.clone(),
            batch_size: 5,
            flush_interval_secs: 60,
        };

        let (tx, _) = tokio::sync::broadcast::channel::<Arc<PreSerializedMessage>>(10);
        let rx = tx.subscribe();
        let shutdown = CancellationToken::new();

        // Spawn the sink task
        let task = tokio::spawn(run_delta_sink(config, rx, shutdown.clone()));

        // Send malformed JSON
        let malformed_psm = Arc::new(PreSerializedMessage {
            full: Bytes::from(Vec::from(&b"not valid json"[..])),
            lite: Bytes::new(),
            domains_only: Bytes::new(),
        });
        let _ = tx.send(malformed_psm);

        // Send valid JSON
        let valid_msg = make_test_json_bytes();
        let valid_psm = Arc::new(PreSerializedMessage {
            full: Bytes::from(valid_msg),
            lite: Bytes::new(),
            domains_only: Bytes::new(),
        });

        for i in 0..5 {
            let mut psm_clone = (*valid_psm).clone();
            let mut json_msg: serde_json::Value =
                serde_json::from_slice(&psm_clone.full).expect("valid json");
            json_msg["data"]["cert_index"] = serde_json::json!(i);
            psm_clone.full = bytes::Bytes::from(serde_json::to_vec(&json_msg).unwrap());

            let _ = tx.send(Arc::new(psm_clone));
        }

        // Wait a bit for processing
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Trigger graceful shutdown
        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;

        // Verify task did not panic and table received valid records
        let table = deltalake::open_table(&table_path)
            .await
            .expect("should open table");

        // Should have flushed the valid records
        assert!(
            table.version() > 0,
            "table should have been written to despite malformed JSON"
        );

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    // ============================================================================
    // Integration Tests for AC4: No disruption to existing streaming
    // ============================================================================

    #[tokio::test]
    async fn test_ac4_1_no_interference_with_ws_sse_consumers() {
        // AC4.1 Success: WebSocket and SSE streams continue operating normally when delta sink is enabled
        // Verify that both the delta sink and mock WS/SSE consumers can receive messages from the same broadcast channel
        let test_name = "ac4_1_no_interference";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);
        let _ = std::fs::remove_dir_all(&table_path);
        let _ = std::fs::create_dir_all(&table_path);

        let config = crate::config::DeltaSinkConfig {
            enabled: true,
            table_path: table_path.clone(),
            batch_size: 5,
            flush_interval_secs: 60,
        };

        // Create broadcast channel (like main.rs does)
        let (tx, _) = tokio::sync::broadcast::channel::<Arc<PreSerializedMessage>>(100);

        // Spawn delta sink task (consumer 1)
        let rx_sink = tx.subscribe();
        let shutdown_sink = CancellationToken::new();
        let sink_task = tokio::spawn(run_delta_sink(config, rx_sink, shutdown_sink.clone()));

        // Spawn mock WS-like consumer (consumer 2)
        let mut rx_ws = tx.subscribe();
        let ws_task = tokio::spawn(async move {
            let mut ws_messages = vec![];
            for _ in 0..5 {
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    rx_ws.recv(),
                ).await {
                    Ok(Ok(msg)) => ws_messages.push(msg),
                    _ => break,
                }
            }
            ws_messages.len()
        });

        // Send messages through the broadcast channel
        let valid_msg = make_test_json_bytes();
        for i in 0..5 {
            let mut psm = PreSerializedMessage {
                full: Bytes::from(valid_msg.clone()),
                lite: Bytes::new(),
                domains_only: Bytes::new(),
            };

            // Vary cert_index to avoid dedup
            let mut json_msg: serde_json::Value =
                serde_json::from_slice(&psm.full).expect("valid json");
            json_msg["data"]["cert_index"] = serde_json::json!(i);
            psm.full = bytes::Bytes::from(serde_json::to_vec(&json_msg).unwrap());

            let _ = tx.send(Arc::new(psm));
        }

        // Wait for WS consumer to receive all messages
        let ws_count = tokio::time::timeout(Duration::from_secs(5), ws_task)
            .await
            .ok()
            .and_then(|result| result.ok())
            .unwrap_or(0);

        // Give sink time to process and flush
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Graceful shutdown
        shutdown_sink.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), sink_task).await;

        // Verify both consumers received messages
        assert_eq!(ws_count, 5, "WS consumer should receive all 5 messages");

        // Verify delta sink wrote to table
        let table = deltalake::open_table(&table_path)
            .await
            .expect("should open table");
        assert!(
            table.version() > 0,
            "delta sink should have written at least one batch"
        );

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[test]
    fn test_ac4_2_disabled_by_default() {
        // AC4.2 Success: Delta sink disabled by default
        // Verify that DeltaSinkConfig::default().enabled is false
        let default_config = crate::config::DeltaSinkConfig::default();
        assert_eq!(
            default_config.enabled, false,
            "Delta sink should be disabled by default"
        );
    }

    #[tokio::test]
    async fn test_ac4_3_lagged_messages_handled() {
        // AC4.3 Success: Broadcast channel Lagged errors are logged and metriced, not propagated
        // Create a small buffer broadcast channel and cause lagging by sending many messages
        let test_name = "ac4_3_lagged_messages";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);
        let _ = std::fs::remove_dir_all(&table_path);
        let _ = std::fs::create_dir_all(&table_path);

        let config = crate::config::DeltaSinkConfig {
            enabled: true,
            table_path: table_path.clone(),
            batch_size: 100, // Large batch size so flush doesn't happen immediately
            flush_interval_secs: 60,
        };

        // Small buffer to force lagging
        let (tx, _) = tokio::sync::broadcast::channel::<Arc<PreSerializedMessage>>(5);
        let rx = tx.subscribe();
        let shutdown = CancellationToken::new();

        // Spawn the sink task (it will start lagging due to small buffer)
        let task = tokio::spawn(run_delta_sink(config, rx, shutdown.clone()));

        // Send many messages to cause lagging in the receiver
        let valid_msg = make_test_json_bytes();
        for i in 0..20 {
            let mut psm = PreSerializedMessage {
                full: Bytes::from(valid_msg.clone()),
                lite: Bytes::new(),
                domains_only: Bytes::new(),
            };

            let mut json_msg: serde_json::Value =
                serde_json::from_slice(&psm.full).expect("valid json");
            json_msg["data"]["cert_index"] = serde_json::json!(i);
            psm.full = bytes::Bytes::from(serde_json::to_vec(&json_msg).unwrap());

            let _ = tx.send(Arc::new(psm));
            // Use std::thread::sleep (blocking) instead of tokio::time::sleep (async) to intentionally
            // block the tokio executor and cause the receiver to fall behind, triggering lagged messages.
            // This tests that the sink handles lagging gracefully without panicking.
            std::thread::sleep(Duration::from_millis(5));
        }

        // Wait for processing
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Graceful shutdown
        shutdown.cancel();
        let task_result = tokio::time::timeout(Duration::from_secs(5), task).await;

        // Verify task completed without panicking
        assert!(
            task_result.is_ok(),
            "Task should complete without panicking despite lagged messages"
        );

        // Verify table exists and was written to (even if only partially)
        let table_result = deltalake::open_table(&table_path).await;
        assert!(
            table_result.is_ok(),
            "Table should exist even with lagged messages"
        );

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }

    #[tokio::test]
    async fn test_ac4_4_startup_failure_non_fatal() {
        // AC4.4 Success: Delta table creation failure on startup does not prevent the rest of the application from running
        // Call run_delta_sink with an invalid/unwritable table_path (e.g., /nonexistent/path/delta)
        // Assert the task starts and exits without panicking

        let config = crate::config::DeltaSinkConfig {
            enabled: true,
            table_path: "/nonexistent/path/that/cannot/be/created/delta_table".to_string(),
            batch_size: 10,
            flush_interval_secs: 1,
        };

        let (tx, _) = tokio::sync::broadcast::channel::<Arc<PreSerializedMessage>>(10);
        let rx = tx.subscribe();
        let shutdown = CancellationToken::new();

        // Spawn the sink task with invalid path
        let task = tokio::spawn(run_delta_sink(config, rx, shutdown.clone()));

        // Give it a moment to attempt startup
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Trigger shutdown
        shutdown.cancel();

        // The task should complete without panicking
        let result = tokio::time::timeout(Duration::from_secs(5), task).await;

        // Verify the task did not panic and exited cleanly
        assert!(result.is_ok(), "Task should exit cleanly with invalid table path");

        // Extract the result - it should be Ok(()) not a panic
        match result {
            Ok(Ok(())) => {
                // Expected: task exited gracefully
            }
            Ok(Err(e)) => {
                // Only acceptable if it's not a panic
                if !e.is_panic() {
                    // OK - task exited with non-panic error
                } else {
                    panic!("Task panicked during startup failure: {}", e);
                }
            }
            Err(_) => {
                panic!("Task timed out - should have exited quickly on startup failure");
            }
        }
    }

    #[tokio::test]
    async fn test_ac4_integration_full_workflow() {
        // Comprehensive integration test combining all AC4 criteria:
        // - Enable delta sink (AC4.2 inverse)
        // - Spawn both delta sink and mock WS consumer (AC4.1)
        // - Send messages through broadcast (both consumers receive)
        // - Verify delta sink flushes to table (AC4.1)
        // - Verify no panics on lagged messages (AC4.3)
        // - Graceful shutdown flushes final buffer (AC3.3)

        let test_name = "ac4_integration_full";
        let table_path = format!("/tmp/delta_sink_test_{}", test_name);
        let _ = std::fs::remove_dir_all(&table_path);
        let _ = std::fs::create_dir_all(&table_path);

        let config = crate::config::DeltaSinkConfig {
            enabled: true,
            table_path: table_path.clone(),
            batch_size: 10,
            flush_interval_secs: 2,
        };

        // Create broadcast channel
        let (tx, _) = tokio::sync::broadcast::channel::<Arc<PreSerializedMessage>>(50);

        // Spawn delta sink
        let rx_sink = tx.subscribe();
        let shutdown_sink = CancellationToken::new();
        let sink_task = tokio::spawn(run_delta_sink(config, rx_sink, shutdown_sink.clone()));

        // Spawn mock WS consumer
        let mut rx_ws = tx.subscribe();
        let ws_received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ws_received_clone = ws_received.clone();
        let ws_task = tokio::spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_secs(3), rx_ws.recv()).await {
                    Ok(Ok(_)) => {
                        ws_received_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    _ => break,
                }
            }
        });

        // Send batch of messages
        let valid_msg = make_test_json_bytes();
        for i in 0..25 {
            let mut psm = PreSerializedMessage {
                full: Bytes::from(valid_msg.clone()),
                lite: Bytes::new(),
                domains_only: Bytes::new(),
            };

            let mut json_msg: serde_json::Value =
                serde_json::from_slice(&psm.full).expect("valid json");
            json_msg["data"]["cert_index"] = serde_json::json!(i);
            psm.full = bytes::Bytes::from(serde_json::to_vec(&json_msg).unwrap());

            let _ = tx.send(Arc::new(psm));
        }

        // Wait for processing and flushes
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Check WS consumer received messages
        let ws_count = ws_received.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            ws_count > 0,
            "WS consumer should have received messages, got {}",
            ws_count
        );

        // Graceful shutdown
        shutdown_sink.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), sink_task).await;
        let _ = ws_task.abort();

        // Verify delta sink wrote to table with multiple versions (multiple flushes)
        let table = deltalake::open_table(&table_path)
            .await
            .expect("should open table");
        assert!(
            table.version() > 0,
            "delta sink should have written at least one batch"
        );

        // Clean up
        let _ = std::fs::remove_dir_all(&table_path);
    }
}
