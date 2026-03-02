use std::collections::HashMap;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine};
use deltalake::arrow::array::{Array, BinaryArray, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray, UInt64Array};
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::{open_table, DeltaTableError};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::ct::parse_certificate;

/// Helper struct to track field mismatches and collect sample diffs.
#[derive(Debug, Clone)]
pub struct SampleDiff {
    pub cert_index: u64,
    pub source_url: String,
    pub fields: Vec<FieldDiff>,
}

/// Individual field difference for a sample mismatch.
#[derive(Debug, Clone)]
pub struct FieldDiff {
    pub field_name: String,
    pub stored: String,
    pub reparsed: String,
}

/// Audit report containing all audit results and statistics.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AuditReport {
    pub total_records: u64,
    pub mismatch_record_count: u64,
    pub unparseable_count: u64,
    pub partition_count: u64,
    pub field_mismatch_counts: HashMap<String, u64>,
    pub sample_diffs: Vec<SampleDiff>,
}

impl Default for AuditReport {
    fn default() -> Self {
        AuditReport {
            total_records: 0,
            mismatch_record_count: 0,
            unparseable_count: 0,
            partition_count: 0,
            field_mismatch_counts: HashMap::new(),
            sample_diffs: Vec::new(),
        }
    }
}

/// Helper function to extract domains from a ListArray at a given row index.
fn extract_domains_from_list(list_array: &ListArray, row: usize) -> Vec<String> {
    let values = list_array.value(row);
    let string_values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("Failed to downcast ListArray value to StringArray"));
    (0..string_values.len())
        .map(|i| string_values.value(i).to_string())
        .collect()
}

/// Validate that a date string matches the expected YYYY-MM-DD format.
/// Returns true if the format is valid, false otherwise.
fn is_valid_date_format(date: &str) -> bool {
    if date.len() != 10 {
        return false;
    }
    let bytes = date.as_bytes();
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[0..4].iter().all(|b| b.is_ascii_digit())
        && bytes[5..7].iter().all(|b| b.is_ascii_digit())
        && bytes[8..10].iter().all(|b| b.is_ascii_digit())
}

/// Build a SQL WHERE clause fragment for filtering by seen_date partition.
/// Returns an empty string if no date bounds are provided, or a string
/// starting with " WHERE " containing the appropriate conditions.
/// Returns Err if any provided date does not match YYYY-MM-DD format.
pub fn date_filter_clause(from_date: &Option<String>, to_date: &Option<String>) -> Result<String, String> {
    let mut conditions = Vec::new();

    if let Some(from) = from_date {
        if !is_valid_date_format(from) {
            return Err(format!("invalid --from-date format '{}', expected YYYY-MM-DD", from));
        }
        conditions.push(format!("seen_date >= '{}'", from));
    }
    if let Some(to) = to_date {
        if !is_valid_date_format(to) {
            return Err(format!("invalid --to-date format '{}', expected YYYY-MM-DD", to));
        }
        conditions.push(format!("seen_date <= '{}'", to));
    }

    if conditions.is_empty() {
        Ok(String::new())
    } else {
        Ok(format!(" WHERE {}", conditions.join(" AND ")))
    }
}

/// Return the 19-column Arrow schema for the metadata-only Delta table.
/// Identical to delta_schema() with the `as_der` column removed.
pub fn metadata_schema() -> Arc<Schema> {
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
        Field::new(
            "chain",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]))
}

/// Helper function to print a reparse audit report with the given header text.
/// Used by both the shutdown path and the normal completion path.
fn print_reparse_report(
    header: &str,
    total_records: u64,
    partition_count: u64,
    mismatch_record_count: u64,
    unparseable_count: u64,
    field_mismatch_counts: &HashMap<String, u64>,
    sample_diffs: &[SampleDiff],
) {
    let percentage = if total_records > 0 {
        mismatch_record_count as f64 / total_records as f64 * 100.0
    } else {
        0.0
    };

    println!("\n{}", header);
    println!("{}", "=".repeat(header.len()));
    println!("Records scanned: {}", total_records);
    println!("Partitions scanned: {}", partition_count);
    println!(
        "Records with mismatches: {} ({:.1}%)",
        mismatch_record_count, percentage
    );
    println!("Unparseable records: {}", unparseable_count);
    println!();

    if !field_mismatch_counts.is_empty() {
        println!("Field breakdown:");
        let mut field_names: Vec<_> = field_mismatch_counts.keys().collect();
        field_names.sort();
        for field_name in field_names {
            let count = field_mismatch_counts[field_name];
            println!("  {}: {} mismatches", field_name, count);
        }
        println!();
    }

    if !sample_diffs.is_empty() {
        println!(
            "Sample mismatches ({} of {}):",
            sample_diffs.len(),
            mismatch_record_count
        );
        for sample in sample_diffs {
            println!(
                "  [cert_index={}, source={}]",
                sample.cert_index, sample.source_url
            );
            for field_diff in &sample.fields {
                println!(
                    "    {}: stored=\"{}\" reparsed=\"{}\"",
                    field_diff.field_name, field_diff.stored, field_diff.reparsed
                );
            }
        }
    }
    println!();
}

pub async fn run_reparse_audit(
    config: Config,
    from_date: Option<String>,
    to_date: Option<String>,
    shutdown: CancellationToken,
) -> (i32, AuditReport) {
    info!(
        table_path = %config.delta_sink.table_path,
        from_date = ?from_date,
        to_date = ?to_date,
        "Starting reparse audit"
    );

    // Step 1: Open the Delta table
    let table = match open_table(&config.delta_sink.table_path).await {
        Ok(t) => t,
        Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
            error!(
                table_path = %config.delta_sink.table_path,
                "source Delta table does not exist"
            );
            return (1, AuditReport::default());
        }
        Err(e) => {
            error!(error = %e, "failed to open source Delta table");
            return (1, AuditReport::default());
        }
    };

    // Step 2: Create DataFusion session context
    let ctx = SessionContext::new();

    // Register table as ct_main
    if let Err(e) = ctx.register_table("ct_main", Arc::new(table)) {
        error!(error = %e, "Failed to register Delta table in DataFusion");
        return (1, AuditReport::default());
    }

    // Step 3: Build the SQL query
    let base_sql = r#"SELECT cert_index, source_url, seen_date, as_der, subject_aggregated, issuer_aggregated,
           all_domains, signature_algorithm, is_ca, not_before, not_after, serial_number
    FROM ct_main"#;

    let date_filter = match date_filter_clause(&from_date, &to_date) {
        Ok(clause) => clause,
        Err(e) => {
            error!(error = %e, "Invalid date filter");
            return (1, AuditReport::default());
        }
    };

    let sql = format!("{}{}", base_sql, date_filter);

    // Step 4: Execute the query and get a stream
    let df = match ctx.sql(&sql).await {
        Ok(df) => df,
        Err(e) => {
            error!(error = %e, "Failed to execute SQL query");
            return (1, AuditReport::default());
        }
    };

    let mut stream = match df.execute_stream().await {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "Failed to get record batch stream");
            return (1, AuditReport::default());
        }
    };

    // Initialize tracking variables
    let mut total_records: u64 = 0;
    let mut mismatch_record_count: u64 = 0;
    let mut unparseable_count: u64 = 0;
    let mut partition_count: u64 = 0;
    let mut field_mismatch_counts: HashMap<String, u64> = HashMap::new();
    let mut sample_diffs: Vec<SampleDiff> = Vec::new();
    let mut any_rows_seen = false;

    // Step 5 & 6: Iterate through batches
    while let Some(batch_result) = stream.next().await {
        if shutdown.is_cancelled() {
            warn!("reparse audit interrupted by shutdown signal");
            print_reparse_report(
                "Reparse Audit Report (Partial - Interrupted)",
                total_records,
                partition_count,
                mismatch_record_count,
                unparseable_count,
                &field_mismatch_counts,
                &sample_diffs,
            );
            let report = AuditReport {
                total_records,
                mismatch_record_count,
                unparseable_count,
                partition_count,
                field_mismatch_counts,
                sample_diffs,
            };
            return (1, report);
        }

        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                error!(error = %e, "Error reading batch from stream");
                return (1, AuditReport {
                    total_records,
                    mismatch_record_count,
                    unparseable_count,
                    partition_count,
                    field_mismatch_counts,
                    sample_diffs,
                });
            }
        };

        if batch.num_rows() > 0 {
            any_rows_seen = true;
            partition_count += 1;
        }

        // Get column indices
        let cert_index_col = batch.column_by_name("cert_index").unwrap();
        let source_url_col = batch.column_by_name("source_url").unwrap();
        let as_der_col = batch.column_by_name("as_der").unwrap();
        let subject_agg_col = batch.column_by_name("subject_aggregated").unwrap();
        let issuer_agg_col = batch.column_by_name("issuer_aggregated").unwrap();
        let all_domains_col = batch.column_by_name("all_domains").unwrap();
        let sig_algo_col = batch.column_by_name("signature_algorithm").unwrap();
        let is_ca_col = batch.column_by_name("is_ca").unwrap();
        let not_before_col = batch.column_by_name("not_before").unwrap();
        let not_after_col = batch.column_by_name("not_after").unwrap();
        let serial_col = batch.column_by_name("serial_number").unwrap();

        // Note: cert_index may be stored as Int64 or UInt64 depending on Delta Lake type handling
        let cert_index_array_uint = cert_index_col.as_any().downcast_ref::<UInt64Array>();
        let cert_index_array_int = cert_index_col.as_any().downcast_ref::<Int64Array>();

        if cert_index_array_uint.is_none() && cert_index_array_int.is_none() {
            warn!("cert_index column has unexpected type: {:?}", cert_index_col.data_type());
            continue;
        }
        let source_url_array = match source_url_col.as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("source_url column has unexpected type: {:?}", source_url_col.data_type());
                continue;
            }
        };
        let subject_agg_array = match subject_agg_col.as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("subject_aggregated column has unexpected type: {:?}", subject_agg_col.data_type());
                continue;
            }
        };
        let issuer_agg_array = match issuer_agg_col.as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("issuer_aggregated column has unexpected type: {:?}", issuer_agg_col.data_type());
                continue;
            }
        };
        let all_domains_list = match all_domains_col.as_any().downcast_ref::<ListArray>() {
            Some(arr) => arr,
            None => {
                warn!("all_domains column has unexpected type: {:?}", all_domains_col.data_type());
                continue;
            }
        };
        let sig_algo_array = match sig_algo_col.as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("signature_algorithm column has unexpected type: {:?}", sig_algo_col.data_type());
                continue;
            }
        };
        let is_ca_array = match is_ca_col.as_any().downcast_ref::<BooleanArray>() {
            Some(arr) => arr,
            None => {
                warn!("is_ca column has unexpected type: {:?}", is_ca_col.data_type());
                continue;
            }
        };
        let not_before_array = match not_before_col.as_any().downcast_ref::<Int64Array>() {
            Some(arr) => arr,
            None => {
                warn!("not_before column has unexpected type: {:?}", not_before_col.data_type());
                continue;
            }
        };
        let not_after_array = match not_after_col.as_any().downcast_ref::<Int64Array>() {
            Some(arr) => arr,
            None => {
                warn!("not_after column has unexpected type: {:?}", not_after_col.data_type());
                continue;
            }
        };
        let serial_array = match serial_col.as_any().downcast_ref::<StringArray>() {
            Some(arr) => arr,
            None => {
                warn!("serial_number column has unexpected type: {:?}", serial_col.data_type());
                continue;
            }
        };

        // Iterate rows in this batch
        for row in 0..batch.num_rows() {
            total_records += 1;

            let cert_index = if let Some(arr) = cert_index_array_uint {
                arr.value(row)
            } else {
                cert_index_array_int.unwrap().value(row) as u64
            };
            let source_url = source_url_array.value(row).to_string();

            // Extract as_der from the batch
            let der_bytes = match as_der_col.data_type() {
                DataType::Binary => {
                    // Binary column type
                    let binary_array = as_der_col.as_any().downcast_ref::<BinaryArray>().unwrap();
                    if binary_array.is_null(row) {
                        unparseable_count += 1;
                        continue;
                    }
                    binary_array.value(row).to_vec()
                }
                DataType::Utf8 => {
                    // Utf8 column type (base64 encoded)
                    let utf8_array = as_der_col.as_any().downcast_ref::<StringArray>().unwrap();
                    if utf8_array.is_null(row) {
                        unparseable_count += 1;
                        continue;
                    }
                    let base64_str = utf8_array.value(row);
                    if base64_str.is_empty() {
                        unparseable_count += 1;
                        continue;
                    }
                    match STANDARD.decode(base64_str) {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            unparseable_count += 1;
                            continue;
                        }
                    }
                }
                _ => {
                    warn!("Unexpected as_der column type: {:?}", as_der_col.data_type());
                    unparseable_count += 1;
                    continue;
                }
            };

            // Parse the certificate
            let leaf = match parse_certificate(&der_bytes, false) {
                Some(leaf) => leaf,
                None => {
                    unparseable_count += 1;
                    continue;
                }
            };

            // Compare fields
            let mut mismatches_in_row: Vec<FieldDiff> = Vec::new();

            // subject_aggregated comparison
            let stored_subject = subject_agg_array.value(row);
            let reparsed_subject = leaf.subject.aggregated.as_deref().unwrap_or("");
            if stored_subject != reparsed_subject {
                field_mismatch_counts
                    .entry("subject_aggregated".to_string())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                mismatches_in_row.push(FieldDiff {
                    field_name: "subject_aggregated".to_string(),
                    stored: stored_subject.to_string(),
                    reparsed: reparsed_subject.to_string(),
                });
            }

            // issuer_aggregated comparison
            let stored_issuer = issuer_agg_array.value(row);
            let reparsed_issuer = leaf.issuer.aggregated.as_deref().unwrap_or("");
            if stored_issuer != reparsed_issuer {
                field_mismatch_counts
                    .entry("issuer_aggregated".to_string())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                mismatches_in_row.push(FieldDiff {
                    field_name: "issuer_aggregated".to_string(),
                    stored: stored_issuer.to_string(),
                    reparsed: reparsed_issuer.to_string(),
                });
            }

            // all_domains comparison
            let mut stored_domains: Vec<String> = extract_domains_from_list(all_domains_list, row);
            let mut reparsed_domains: Vec<String> = leaf.all_domains.to_vec();
            stored_domains.sort();
            reparsed_domains.sort();
            if stored_domains != reparsed_domains {
                field_mismatch_counts
                    .entry("all_domains".to_string())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                mismatches_in_row.push(FieldDiff {
                    field_name: "all_domains".to_string(),
                    stored: format!("{:?}", stored_domains),
                    reparsed: format!("{:?}", reparsed_domains),
                });
            }

            // signature_algorithm comparison
            let stored_sig_algo = sig_algo_array.value(row);
            if stored_sig_algo != leaf.signature_algorithm {
                field_mismatch_counts
                    .entry("signature_algorithm".to_string())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                mismatches_in_row.push(FieldDiff {
                    field_name: "signature_algorithm".to_string(),
                    stored: stored_sig_algo.to_string(),
                    reparsed: leaf.signature_algorithm.clone(),
                });
            }

            // is_ca comparison
            let stored_is_ca = is_ca_array.value(row);
            if stored_is_ca != leaf.is_ca {
                field_mismatch_counts
                    .entry("is_ca".to_string())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                mismatches_in_row.push(FieldDiff {
                    field_name: "is_ca".to_string(),
                    stored: stored_is_ca.to_string(),
                    reparsed: leaf.is_ca.to_string(),
                });
            }

            // not_before comparison
            let stored_not_before = not_before_array.value(row);
            if stored_not_before != leaf.not_before {
                field_mismatch_counts
                    .entry("not_before".to_string())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                mismatches_in_row.push(FieldDiff {
                    field_name: "not_before".to_string(),
                    stored: stored_not_before.to_string(),
                    reparsed: leaf.not_before.to_string(),
                });
            }

            // not_after comparison
            let stored_not_after = not_after_array.value(row);
            if stored_not_after != leaf.not_after {
                field_mismatch_counts
                    .entry("not_after".to_string())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                mismatches_in_row.push(FieldDiff {
                    field_name: "not_after".to_string(),
                    stored: stored_not_after.to_string(),
                    reparsed: leaf.not_after.to_string(),
                });
            }

            // serial_number comparison
            let stored_serial = serial_array.value(row);
            if stored_serial != leaf.serial_number {
                field_mismatch_counts
                    .entry("serial_number".to_string())
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                mismatches_in_row.push(FieldDiff {
                    field_name: "serial_number".to_string(),
                    stored: stored_serial.to_string(),
                    reparsed: leaf.serial_number.clone(),
                });
            }

            // If there were mismatches, increment counter and collect sample
            if !mismatches_in_row.is_empty() {
                mismatch_record_count += 1;
                if sample_diffs.len() < 10 {
                    sample_diffs.push(SampleDiff {
                        cert_index,
                        source_url,
                        fields: mismatches_in_row,
                    });
                }
            }
        }
    }

    // Step 5 check: If no rows were seen, print informational message and return 0
    if !any_rows_seen {
        println!("No records found in the specified date range");
        let report = AuditReport {
            total_records: 0,
            mismatch_record_count: 0,
            unparseable_count: 0,
            partition_count: 0,
            field_mismatch_counts,
            sample_diffs,
        };
        return (0, report);
    }

    // Step 7: Print the report
    print_reparse_report(
        "Reparse Audit Report",
        total_records,
        partition_count,
        mismatch_record_count,
        unparseable_count,
        &field_mismatch_counts,
        &sample_diffs,
    );

    // Step 8: Return 0 (audit always exits 0 unless infrastructure failure)
    let report = AuditReport {
        total_records,
        mismatch_record_count,
        unparseable_count,
        partition_count,
        field_mismatch_counts,
        sample_diffs,
    };
    (0, report)
}

pub async fn run_extract_metadata(
    config: Config,
    output_path: String,
    from_date: Option<String>,
    to_date: Option<String>,
    shutdown: CancellationToken,
) -> i32 {
    use deltalake::protocol::SaveMode;
    use deltalake::DeltaOps;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use crate::delta_sink::open_or_create_table;

    info!(
        table_path = %config.delta_sink.table_path,
        output_path = %output_path,
        from_date = ?from_date,
        to_date = ?to_date,
        "Starting metadata extraction"
    );

    // Step 1: Open the source Delta table
    let source_table = match open_table(&config.delta_sink.table_path).await {
        Ok(t) => {
            info!("Source table opened successfully");
            t
        }
        Err(DeltaTableError::NotATable(_)) | Err(DeltaTableError::InvalidTableLocation(_)) => {
            error!(
                table_path = %config.delta_sink.table_path,
                "source Delta table does not exist"
            );
            return 1;
        }
        Err(e) => {
            error!(error = %e, "failed to open source Delta table");
            return 1;
        }
    };

    // Step 2: Create DataFusion session context and register the source table
    let ctx = SessionContext::new();
    if let Err(e) = ctx.register_table("ct_main", Arc::new(source_table)) {
        error!(error = %e, "Failed to register source Delta table in DataFusion");
        return 1;
    }
    info!("Source table registered in DataFusion");

    // Step 3: Build the SQL query selecting 19 metadata columns (all except as_der)
    // Cast fields to ensure they match the expected schema:
    // - cert_index: use CAST or just let it be, seems DataFusion returns Int64
    // - seen_date: cast to VARCHAR to prevent dictionary encoding which Delta can't handle for partitions
    let base_sql = r#"SELECT cert_index, update_type, seen, CAST(seen_date AS VARCHAR) as seen_date, source_name, source_url,
           cert_link, serial_number, fingerprint, sha256, sha1, not_before,
           not_after, is_ca, signature_algorithm, subject_aggregated,
           issuer_aggregated, all_domains, chain
    FROM ct_main"#;

    let date_filter = match date_filter_clause(&from_date, &to_date) {
        Ok(clause) => {
            clause
        }
        Err(e) => {
            error!(error = %e, "Invalid date filter");
            return 1;
        }
    };

    let sql = format!("{}{}", base_sql, date_filter);
    info!("SQL query: {}", sql);

    // Step 4: Execute the query to get a stream
    let df = match ctx.sql(&sql).await {
        Ok(df) => {
            info!("SQL query executed successfully");
            df
        }
        Err(e) => {
            error!(error = %e, "Failed to execute SQL query");
            return 1;
        }
    };

    let mut stream = match df.execute_stream().await {
        Ok(s) => {
            info!("Record batch stream obtained");
            s
        }
        Err(e) => {
            error!(error = %e, "Failed to get record batch stream");
            return 1;
        }
    };

    // Step 5: Open or create the output Delta table
    // First, ensure the output directory exists
    if let Err(e) = std::fs::create_dir_all(&output_path) {
        error!(error = %e, output_path = %output_path, "Failed to create output directory");
        return 1;
    }

    // Use the metadata schema to create the output table with the correct structure
    let mut output_table = match open_or_create_table(&output_path, &metadata_schema()).await {
        Ok(t) => {
            t
        }
        Err(e) => {
            error!(error = %e, output_path = %output_path, "Failed to open or create output table");
            return 1;
        }
    };

    // Step 6: Build WriterProperties with zstd compression
    let writer_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(config.delta_sink.compression_level)
                .expect("compression level validated at startup"),
        ))
        .build();

    // Step 7: Iterate through batches, accumulate, and write to output table
    let mut total_records_written: u64 = 0;
    let mut any_rows_seen = false;
    let mut pending_batches: Vec<RecordBatch> = Vec::new();
    let mut pending_rows: u64 = 0;
    let flush_threshold: u64 = config.delta_sink.offline_batch_size as u64;

    while let Some(batch_result) = stream.next().await {
        if shutdown.is_cancelled() {
            // Flush any pending batches before exiting
            if !pending_batches.is_empty() {
                if let Ok(_new_table) = DeltaOps(output_table)
                    .write(pending_batches)
                    .with_save_mode(SaveMode::Append)
                    .with_writer_properties(writer_props.clone())
                    .await
                {
                    total_records_written += pending_rows;
                    info!("Flushed {} records before shutdown", pending_rows);
                }
            }
            warn!("metadata extraction interrupted by shutdown signal");
            info!(records_written = total_records_written, "partial extraction left intact at {}", output_path);
            return 1;
        }

        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                error!(error = %e, "Error reading batch from stream");
                return 1;
            }
        };

        if batch.num_rows() > 0 {
            any_rows_seen = true;
            pending_rows += batch.num_rows() as u64;
            pending_batches.push(batch);

            // Flush when accumulated enough rows
            if pending_rows >= flush_threshold {
                match DeltaOps(output_table)
                    .write(pending_batches)
                    .with_save_mode(SaveMode::Append)
                    .with_writer_properties(writer_props.clone())
                    .await
                {
                    Ok(new_table) => {
                        output_table = new_table;
                        total_records_written += pending_rows;
                        info!("Written {} records (total: {})", pending_rows, total_records_written);
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to write batch to output table");
                        return 1;
                    }
                }
                pending_batches = Vec::new();
                pending_rows = 0;
            }
        }
    }

    // Flush remaining batches
    if !pending_batches.is_empty() {
        match DeltaOps(output_table)
            .write(pending_batches)
            .with_save_mode(SaveMode::Append)
            .with_writer_properties(writer_props.clone())
            .await
        {
            Ok(_new_table) => {
                total_records_written += pending_rows;
                info!("Written {} records (total: {})", pending_rows, total_records_written);
            }
            Err(e) => {
                error!(error = %e, "Failed to write final batch to output table");
                return 1;
            }
        }
    }

    // Step 8: Check if any records were extracted
    if !any_rows_seen || total_records_written == 0 {
        info!("No records found in the specified date range");
        return 0;
    }

    info!(
        total_records = total_records_written,
        "Metadata extraction completed successfully"
    );
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_filter_clause_both_dates() {
        let from_date = Some("2026-02-01".to_string());
        let to_date = Some("2026-02-28".to_string());
        let result = date_filter_clause(&from_date, &to_date);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            " WHERE seen_date >= '2026-02-01' AND seen_date <= '2026-02-28'"
        );
    }

    #[test]
    fn test_date_filter_clause_from_date_only() {
        let from_date = Some("2026-02-01".to_string());
        let to_date = None;
        let result = date_filter_clause(&from_date, &to_date);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), " WHERE seen_date >= '2026-02-01'");
    }

    #[test]
    fn test_date_filter_clause_to_date_only() {
        let from_date = None;
        let to_date = Some("2026-02-28".to_string());
        let result = date_filter_clause(&from_date, &to_date);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), " WHERE seen_date <= '2026-02-28'");
    }

    #[test]
    fn test_date_filter_clause_no_dates() {
        let from_date = None;
        let to_date = None;
        let result = date_filter_clause(&from_date, &to_date);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_date_filter_clause_invalid_from_date_too_short() {
        let from_date = Some("2026-02-1".to_string());
        let to_date = None;
        let result = date_filter_clause(&from_date, &to_date);
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("invalid --from-date format"));
        assert!(err_msg.contains("YYYY-MM-DD"));
    }

    #[test]
    fn test_date_filter_clause_invalid_to_date_wrong_format() {
        let from_date = None;
        let to_date = Some("2026/02/28".to_string());
        let result = date_filter_clause(&from_date, &to_date);
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("invalid --to-date format"));
        assert!(err_msg.contains("YYYY-MM-DD"));
    }

    #[test]
    fn test_date_filter_clause_invalid_date_non_digits() {
        let from_date = Some("202a-02-01".to_string());
        let to_date = None;
        let result = date_filter_clause(&from_date, &to_date);
        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_schema_field_count() {
        let schema = metadata_schema();
        assert_eq!(schema.fields().len(), 19, "metadata_schema should have 19 fields");
    }

    #[test]
    fn test_metadata_schema_no_as_der() {
        let schema = metadata_schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            !field_names.contains(&"as_der"),
            "metadata_schema should not contain as_der field"
        );
    }

    #[test]
    fn test_metadata_schema_has_chain() {
        let schema = metadata_schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"chain"),
            "metadata_schema should contain chain field"
        );
    }

    #[test]
    fn test_metadata_schema_field_order() {
        let schema = metadata_schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let expected = vec![
            "cert_index",
            "update_type",
            "seen",
            "seen_date",
            "source_name",
            "source_url",
            "cert_link",
            "serial_number",
            "fingerprint",
            "sha256",
            "sha1",
            "not_before",
            "not_after",
            "is_ca",
            "signature_algorithm",
            "subject_aggregated",
            "issuer_aggregated",
            "all_domains",
            "chain",
        ];
        assert_eq!(
            field_names, expected,
            "metadata_schema field order should match expected order (delta_schema minus as_der)"
        );
    }

    #[test]
    fn test_metadata_schema_correct_types() {
        let schema = metadata_schema();
        let cert_index_field = schema.field_with_name("cert_index").unwrap();
        assert_eq!(cert_index_field.data_type(), &DataType::UInt64);

        let seen_field = schema.field_with_name("seen").unwrap();
        assert_eq!(
            seen_field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );

        let is_ca_field = schema.field_with_name("is_ca").unwrap();
        assert_eq!(is_ca_field.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_parse_certificate_returns_none_for_garbage() {
        // AC1.5: garbage bytes should return None
        let garbage = vec![0xFF, 0xFF, 0xFF, 0xFF];
        let result = parse_certificate(&garbage, false);
        assert!(result.is_none(), "Should return None for garbage bytes");
    }

    #[test]
    fn test_base64_decode_invalid_base64() {
        // AC1.4: Invalid base64 should fail to decode
        let invalid_base64 = "not_valid_base64!!!";
        let result = STANDARD.decode(invalid_base64);
        assert!(result.is_err(), "Invalid base64 should fail to decode");
    }

    #[test]
    fn test_base64_decode_valid_base64() {
        // AC1.4: Valid base64 should decode successfully
        let valid_base64 = STANDARD.encode(b"test data");
        let result = STANDARD.decode(valid_base64);
        assert!(result.is_ok(), "Valid base64 should decode successfully");
        assert_eq!(result.unwrap(), b"test data".to_vec());
    }

    // Integration tests for reparse audit using real Delta tables
    use crate::delta_sink::{records_to_batch, DeltaCertRecord, delta_schema};
    use crate::models::LeafCert;
    use chrono::Utc;
    use deltalake::DeltaOps;
    use deltalake::protocol::SaveMode;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use std::fs;
    use std::path::Path;

    /// Helper to create a test certificate using rcgen and parse it to get expected values
    fn create_test_cert() -> (Vec<u8>, LeafCert) {
        use rcgen::{CertificateParams, KeyPair};

        let mut params = CertificateParams::new(vec!["example.com".to_string(), "www.example.com".to_string()])
            .expect("Failed to create certificate params");
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.distinguished_name.push(rcgen::DnType::CommonName, "example.com");
        params.is_ca = rcgen::IsCa::NoCa;

        let key_pair = KeyPair::generate().expect("Failed to generate key pair");
        let cert = params
            .self_signed(&key_pair)
            .expect("Failed to create self-signed cert");
        let der_bytes = cert.der().to_vec();

        let leaf = parse_certificate(&der_bytes, false).expect("Failed to parse test cert");
        (der_bytes, leaf)
    }

    /// Helper to create a minimal Config for testing
    fn create_test_config(table_path: &str) -> crate::config::Config {
        use std::net::IpAddr;

        crate::config::Config {
            host: "127.0.0.1".parse::<IpAddr>().unwrap(),
            port: 9100,
            log_level: "info".to_string(),
            buffer_size: 100,
            ct_logs_url: "https://www.gstatic.com/ct/log_list/v3/log_list.json".to_string(),
            tls_cert: None,
            tls_key: None,
            custom_logs: vec![],
            static_logs: vec![],
            protocols: Default::default(),
            ct_log: Default::default(),
            connection_limit: Default::default(),
            rate_limit: Default::default(),
            api: Default::default(),
            auth: Default::default(),
            hot_reload: Default::default(),
            delta_sink: crate::config::DeltaSinkConfig {
                enabled: true,
                table_path: table_path.to_string(),
                batch_size: 1,
                flush_interval_secs: 60,
                compression_level: 9,
                heavy_column_compression_level: 15,
                offline_batch_size: 100000,
            },
            query_api: Default::default(),
            zerobus_sink: Default::default(),
            config_path: None,
        }
    }

    /// Helper to create a DeltaCertRecord from a parsed LeafCert
    fn create_record_from_leaf(
        cert_index: u64,
        source_url: String,
        seen_date: String,
        der_bytes: &[u8],
        leaf: &LeafCert,
    ) -> DeltaCertRecord {
        DeltaCertRecord {
            cert_index,
            update_type: "added".to_string(),
            seen: Utc::now().timestamp() as f64,
            seen_date,
            source_name: "test_log".to_string(),
            source_url,
            cert_link: "https://example.com/cert/1".to_string(),
            serial_number: leaf.serial_number.clone(),
            fingerprint: leaf.fingerprint.clone(),
            sha256: leaf.sha256.clone(),
            sha1: leaf.sha1.clone(),
            not_before: leaf.not_before,
            not_after: leaf.not_after,
            is_ca: leaf.is_ca,
            signature_algorithm: leaf.signature_algorithm.clone(),
            subject_aggregated: leaf.subject.aggregated.as_deref().unwrap_or("").to_string(),
            issuer_aggregated: leaf.issuer.aggregated.as_deref().unwrap_or("").to_string(),
            all_domains: leaf.all_domains.to_vec(),
            as_der: der_bytes.to_vec(),
            chain: vec![],
        }
    }

    /// Helper to clean up test directories
    fn cleanup_test_dir(path: &str) {
        if Path::new(path).exists() {
            let _ = fs::remove_dir_all(path);
        }
    }

    /// Helper to write test records to a Delta table
    async fn write_test_records(table_path: &str, records: Vec<DeltaCertRecord>) {
        use crate::delta_sink::open_or_create_table;

        // Create the directory if it doesn't exist
        fs::create_dir_all(table_path).expect("Failed to create test directory");

        let schema = delta_schema();
        let batch = records_to_batch(&records, &schema).expect("Failed to create batch");

        // Create or open Delta table
        let table = open_or_create_table(table_path, &schema)
            .await
            .expect("Failed to open/create table");

        // Write batch to table
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(9).expect("compression level 9 is valid"),
            ))
            .build();

        DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .with_writer_properties(writer_props)
            .await
            .expect("Failed to write to table");
    }

    #[tokio::test]
    async fn test_ac1_1_audit_completes_with_zero_mismatches() {
        // AC1.1: Audit completes against a Delta table and reports zero mismatches when parsing code has not changed
        let test_dir = "/tmp/delta_table_ops_test_ac1_1";
        cleanup_test_dir(test_dir);

        let (der_bytes, leaf) = create_test_cert();
        let record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &der_bytes, &leaf);

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (exit_code, report) = run_reparse_audit(config, None, None, shutdown).await;

        assert_eq!(exit_code, 0, "Audit should exit with code 0");
        assert_eq!(report.mismatch_record_count, 0, "AC1.1: Should have 0 mismatches");
        assert_eq!(report.unparseable_count, 0, "AC1.1: Should have 0 unparseable records");
        assert_eq!(report.partition_count, 1, "AC1.1: Should have 1 partition");
        cleanup_test_dir(test_dir);
    }

    #[tokio::test]
    async fn test_ac1_2_audit_detects_field_mismatches() {
        // AC1.2: Audit correctly identifies field-level mismatches when parsing code produces different output
        let test_dir = "/tmp/delta_table_ops_test_ac1_2";
        cleanup_test_dir(test_dir);

        let (der_bytes, leaf) = create_test_cert();
        let mut record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &der_bytes, &leaf);

        // Deliberately alter the subject_aggregated field to create a mismatch
        record.subject_aggregated = "wrong_subject".to_string();

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (exit_code, report) = run_reparse_audit(config, None, None, shutdown).await;

        assert_eq!(exit_code, 0, "Audit should exit with code 0 (reports mismatches, doesn't fail)");
        assert_eq!(report.mismatch_record_count, 1, "AC1.2: Should detect 1 mismatch");
        assert!(report.field_mismatch_counts.contains_key("subject_aggregated"), "AC1.2: subject_aggregated mismatch should be detected");
        cleanup_test_dir(test_dir);
    }

    #[tokio::test]
    async fn test_ac1_3_audit_reports_multiple_mismatches() {
        // AC1.3: Audit report shows per-field mismatch counts and up to 10 sample diffs
        let test_dir = "/tmp/delta_table_ops_test_ac1_3";
        cleanup_test_dir(test_dir);

        let (der_bytes, leaf) = create_test_cert();
        let mut records = vec![];

        // Create 15 records with mismatches (> 10)
        for i in 1..=15 {
            let mut record = create_record_from_leaf(
                i,
                format!("https://ct.example.com/log{}", i),
                "2026-02-27".to_string(),
                &der_bytes,
                &leaf,
            );
            // Alter subject_aggregated for each record to create mismatches
            record.subject_aggregated = format!("wrong_subject_{}", i);
            records.push(record);
        }

        write_test_records(test_dir, records).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (exit_code, report) = run_reparse_audit(config, None, None, shutdown).await;

        assert_eq!(exit_code, 0, "Audit should exit with code 0");
        assert_eq!(report.mismatch_record_count, 15, "AC1.3: Should detect 15 mismatches");
        assert_eq!(report.sample_diffs.len(), 10, "AC1.3: Should have exactly 10 sample diffs");
        cleanup_test_dir(test_dir);
    }

    #[tokio::test]
    async fn test_ac1_4_audit_counts_invalid_base64_as_unparseable() {
        // AC1.4: Records with invalid base64 in as_der (Utf8 format) are counted as unparseable, not mismatches
        let test_dir = "/tmp/delta_table_ops_test_ac1_4";
        cleanup_test_dir(test_dir);

        let (_, leaf) = create_test_cert();
        let mut record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &[0xFF], &leaf);

        // Set invalid/garbage bytes in as_der field (not a valid DER certificate)
        record.as_der = b"not_valid_der!!!".to_vec();

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (exit_code, report) = run_reparse_audit(config, None, None, shutdown).await;

        assert_eq!(exit_code, 0, "Audit should exit with code 0");
        assert_eq!(report.unparseable_count, 1, "AC1.4: Invalid base64 should be counted as unparseable");
        assert_eq!(report.mismatch_record_count, 0, "AC1.4: Invalid base64 should not create mismatches");
        cleanup_test_dir(test_dir);
    }

    #[tokio::test]
    async fn test_ac1_5_audit_counts_unparseable_certificates() {
        // AC1.5: Records where parse_certificate() returns None are counted as unparseable
        let test_dir = "/tmp/delta_table_ops_test_ac1_5";
        cleanup_test_dir(test_dir);

        let (_, leaf) = create_test_cert();
        let mut record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &[0xFF], &leaf);

        // Set garbage bytes that parse_certificate will reject
        record.as_der = vec![0xFF, 0xFF, 0xFF, 0xFF];

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (exit_code, report) = run_reparse_audit(config, None, None, shutdown).await;

        assert_eq!(exit_code, 0, "Audit should exit with code 0");
        assert_eq!(report.unparseable_count, 1, "AC1.5: Garbage bytes should be counted as unparseable");
        assert_eq!(report.mismatch_record_count, 0, "AC1.5: Unparseable record should not create mismatches");
        cleanup_test_dir(test_dir);
    }

    #[tokio::test]
    async fn test_ac1_6_audit_exits_with_empty_date_range() {
        // AC1.6: Audit against an empty date range exits 0 with informational message
        let test_dir = "/tmp/delta_table_ops_test_ac1_6";
        cleanup_test_dir(test_dir);

        let (der_bytes, leaf) = create_test_cert();
        let record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &der_bytes, &leaf);

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        // Query for a date range that doesn't match the data (2026-03-01 to 2026-03-31)
        let (exit_code, report) = run_reparse_audit(
            config,
            Some("2026-03-01".to_string()),
            Some("2026-03-31".to_string()),
            shutdown,
        )
        .await;

        assert_eq!(exit_code, 0, "Audit should exit with code 0 for empty date range");
        assert_eq!(report.total_records, 0, "AC1.6: Should have zero total records");
        cleanup_test_dir(test_dir);
    }

    // Integration tests for metadata extraction using real Delta tables

    #[tokio::test]
    async fn test_ac2_1_extract_metadata_output_schema_has_19_columns_no_as_der() {
        // AC2.1: Output Delta table contains exactly 19 columns (all except as_der)
        let test_dir = "/tmp/delta_table_ops_test_ac2_1_source";
        let output_dir = "/tmp/delta_table_ops_test_ac2_1_output";
        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);

        let (der_bytes, leaf) = create_test_cert();
        let record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &der_bytes, &leaf);

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();

        // Debug: verify source table exists
        let source_check = open_table(test_dir).await;
        assert!(source_check.is_ok(), "Source table should exist before extraction");

        let exit_code = run_extract_metadata(config, output_dir.to_string(), None, None, shutdown).await;

        assert_eq!(exit_code, 0, "AC2.1: Extraction should succeed");

        // Open the output table via DataFusion and verify schema has 19 columns
        let output_table = open_table(output_dir).await.expect("Failed to open output table");
        let ctx = SessionContext::new();
        ctx.register_table("output", Arc::new(output_table)).expect("Failed to register output table");

        // Use SQL to verify the schema
        let sql = "SELECT * FROM output LIMIT 0";
        let df = ctx.sql(sql).await.expect("Failed to execute schema query");
        let schema = df.schema();

        // Verify 19 columns
        assert_eq!(schema.fields().len(), 19, "AC2.1: Output schema should have 19 fields");

        // Verify no as_der field
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(!field_names.contains(&"as_der"), "AC2.1: Output should not contain as_der field");

        // Verify all expected fields are present
        let expected_fields = vec![
            "cert_index", "update_type", "seen", "seen_date", "source_name", "source_url",
            "cert_link", "serial_number", "fingerprint", "sha256", "sha1", "not_before",
            "not_after", "is_ca", "signature_algorithm", "subject_aggregated",
            "issuer_aggregated", "all_domains", "chain",
        ];
        for expected in &expected_fields {
            assert!(field_names.contains(expected), "AC2.1: Field {} should be present", expected);
        }

        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);
    }

    #[tokio::test]
    async fn test_ac2_2_extract_metadata_preserves_all_field_values() {
        // AC2.2: All metadata field values in output match source table exactly
        let test_dir = "/tmp/delta_table_ops_test_ac2_2_source";
        let output_dir = "/tmp/delta_table_ops_test_ac2_2_output";
        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);

        let (der_bytes, leaf) = create_test_cert();
        let record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &der_bytes, &leaf);

        let subject_aggregated = record.subject_aggregated.clone();
        let not_before = record.not_before;
        let not_after = record.not_after;
        let is_ca = record.is_ca;
        let source_url = record.source_url.clone();

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let exit_code = run_extract_metadata(config, output_dir.to_string(), None, None, shutdown).await;

        assert_eq!(exit_code, 0, "AC2.2: Extraction should succeed");

        // Query the output table to verify field values
        let output_table = open_table(output_dir).await.expect("Failed to open output table");
        let ctx = SessionContext::new();
        ctx.register_table("output", Arc::new(output_table)).expect("Failed to register output table");

        let sql = "SELECT cert_index, subject_aggregated, not_before, not_after, is_ca, source_url FROM output";
        let df = ctx.sql(sql).await.expect("Failed to execute SQL");
        let batches = df.collect().await.expect("Failed to collect batches");

        assert!(!batches.is_empty(), "AC2.2: Output table should have records");
        let batch = &batches[0];

        // Extract values from batch
        // Note: cert_index might be Int64 from DataFusion even though schema says UInt64
        let cert_index_col = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("cert_index should be Int64");
        let subject_col = batch.column(1).as_any().downcast_ref::<StringArray>().expect("subject_aggregated should be Utf8");
        let not_before_col = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("not_before should be Int64");
        let not_after_col = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("not_after should be Int64");
        let is_ca_col = batch.column(4).as_any().downcast_ref::<BooleanArray>().expect("is_ca should be Boolean");
        let source_url_col = batch.column(5).as_any().downcast_ref::<StringArray>().expect("source_url should be Utf8");

        // Verify values match source record
        assert_eq!(cert_index_col.value(0) as u64, 1, "AC2.2: cert_index should match");
        assert_eq!(subject_col.value(0), subject_aggregated, "AC2.2: subject_aggregated should match");
        assert_eq!(not_before_col.value(0), not_before, "AC2.2: not_before should match");
        assert_eq!(not_after_col.value(0), not_after, "AC2.2: not_after should match");
        assert_eq!(is_ca_col.value(0), is_ca, "AC2.2: is_ca should match");
        assert_eq!(source_url_col.value(0), source_url, "AC2.2: source_url should match");

        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);
    }

    #[tokio::test]
    async fn test_ac2_3_extract_metadata_output_is_partitioned_by_seen_date() {
        // AC2.3: Output table is partitioned by seen_date
        let test_dir = "/tmp/delta_table_ops_test_ac2_3_source";
        let output_dir = "/tmp/delta_table_ops_test_ac2_3_output";
        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);

        let (der_bytes, leaf) = create_test_cert();
        let record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &der_bytes, &leaf);

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let exit_code = run_extract_metadata(config, output_dir.to_string(), None, None, shutdown).await;

        assert_eq!(exit_code, 0, "AC2.3: Extraction should succeed");

        // Open the output table and check partition columns
        let output_table = open_table(output_dir).await.expect("Failed to open output table");
        let metadata = output_table.metadata().expect("Failed to get table metadata");
        let partition_cols = &metadata.partition_columns;

        assert!(!partition_cols.is_empty(), "AC2.3: Output table should have partition columns");
        assert_eq!(partition_cols[0], "seen_date", "AC2.3: First partition column should be seen_date");

        // Verify directory structure includes seen_date=YYYY-MM-DD/ subdirectories
        let path = std::path::Path::new(output_dir);
        let entries: Vec<_> = fs::read_dir(path)
            .expect("Failed to read output directory")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter(|e| e.file_name().to_string_lossy().starts_with("seen_date="))
            .collect();

        assert!(!entries.is_empty(), "AC2.3: Output directory should have seen_date= subdirectories");

        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);
    }

    #[tokio::test]
    async fn test_ac2_4_extract_metadata_preserves_chain_column() {
        // AC2.4: Chain column is preserved with full chain certificate metadata JSON
        let test_dir = "/tmp/delta_table_ops_test_ac2_4_source";
        let output_dir = "/tmp/delta_table_ops_test_ac2_4_output";
        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);

        let (der_bytes, leaf) = create_test_cert();
        let mut record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &der_bytes, &leaf);

        // Add chain values
        record.chain = vec![
            "{\"subject\": \"CN=intermediate\"}".to_string(),
            "{\"subject\": \"CN=root\"}".to_string(),
        ];

        let expected_chain_0 = "{\"subject\": \"CN=intermediate\"}".to_string();
        let expected_chain_1 = "{\"subject\": \"CN=root\"}".to_string();

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let exit_code = run_extract_metadata(config, output_dir.to_string(), None, None, shutdown).await;

        assert_eq!(exit_code, 0, "AC2.4: Extraction should succeed");

        // Query the output table to verify chain values
        let output_table = open_table(output_dir).await.expect("Failed to open output table");
        let ctx = SessionContext::new();
        ctx.register_table("output", Arc::new(output_table)).expect("Failed to register output table");

        let sql = "SELECT chain FROM output";
        let df = ctx.sql(sql).await.expect("Failed to execute SQL");
        let batches = df.collect().await.expect("Failed to collect batches");

        assert!(!batches.is_empty(), "AC2.4: Output table should have records");
        let batch = &batches[0];
        let chain_col = batch.column(0).as_any().downcast_ref::<ListArray>().unwrap();

        let chain_values = chain_col.value(0);
        let chain_str_array = chain_values.as_any().downcast_ref::<StringArray>().unwrap();

        // Verify chain values match
        assert_eq!(chain_str_array.len(), 2, "AC2.4: Chain should have 2 items");
        assert_eq!(chain_str_array.value(0), expected_chain_0, "AC2.4: First chain item should match");
        assert_eq!(chain_str_array.value(1), expected_chain_1, "AC2.4: Second chain item should match");

        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);
    }

    #[tokio::test]
    async fn test_ac2_5_extract_metadata_fails_with_missing_source_table() {
        // AC2.5: Missing source table exits 1 with error message
        let test_dir = "/tmp/delta_table_ops_test_ac2_5_nonexistent";
        let output_dir = "/tmp/delta_table_ops_test_ac2_5_output";
        cleanup_test_dir(output_dir);

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let exit_code = run_extract_metadata(config, output_dir.to_string(), None, None, shutdown).await;

        assert_eq!(exit_code, 1, "AC2.5: Should exit with code 1 when source table doesn't exist");

        cleanup_test_dir(output_dir);
    }

    #[tokio::test]
    async fn test_ac2_6_extract_metadata_empty_date_range_exits_zero() {
        // AC2.6: Extraction against empty date range exits 0 with informational message
        let test_dir = "/tmp/delta_table_ops_test_ac2_6_source";
        let output_dir = "/tmp/delta_table_ops_test_ac2_6_output";
        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);

        let (der_bytes, leaf) = create_test_cert();
        let record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-01-01".to_string(), &der_bytes, &leaf);

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();
        // Query for a future date range that doesn't match (2099-01-01)
        let exit_code = run_extract_metadata(
            config,
            output_dir.to_string(),
            Some("2099-01-01".to_string()),
            None,
            shutdown,
        )
        .await;

        assert_eq!(exit_code, 0, "AC2.6: Should exit with code 0 for empty date range");

        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);
    }

    #[tokio::test]
    async fn test_ac4_4_reparse_audit_shutdown() {
        // AC4.4: Reparse audit responds to shutdown by stopping cleanly and returning exit code 1
        let test_dir = "/tmp/delta_table_ops_test_ac4_4_reparse_shutdown";
        cleanup_test_dir(test_dir);

        let (der_bytes, leaf) = create_test_cert();
        let record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &der_bytes, &leaf);

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();

        // Cancel the token before calling the function to simulate shutdown signal
        shutdown.cancel();

        let (exit_code, _report) = run_reparse_audit(config, None, None, shutdown).await;

        assert_eq!(exit_code, 1, "AC4.4: Audit should exit with code 1 on shutdown");

        cleanup_test_dir(test_dir);
    }

    #[tokio::test]
    async fn test_ac4_4_metadata_extraction_shutdown() {
        // AC4.4: Metadata extraction responds to shutdown by stopping cleanly and returning exit code 1
        let test_dir = "/tmp/delta_table_ops_test_ac4_4_extract_shutdown_source";
        let output_dir = "/tmp/delta_table_ops_test_ac4_4_extract_shutdown_output";
        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);

        let (der_bytes, leaf) = create_test_cert();
        let record = create_record_from_leaf(1, "https://ct.example.com/log1".to_string(), "2026-02-27".to_string(), &der_bytes, &leaf);

        write_test_records(test_dir, vec![record]).await;

        let config = create_test_config(test_dir);
        let shutdown = tokio_util::sync::CancellationToken::new();

        // Cancel the token before calling the function to simulate shutdown signal
        shutdown.cancel();

        let exit_code = run_extract_metadata(config, output_dir.to_string(), None, None, shutdown).await;

        assert_eq!(exit_code, 1, "AC4.4: Metadata extraction should exit with code 1 on shutdown");

        cleanup_test_dir(test_dir);
        cleanup_test_dir(output_dir);
    }
}
