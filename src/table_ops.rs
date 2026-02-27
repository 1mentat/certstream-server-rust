use std::sync::Arc;

use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::Config;

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
}
