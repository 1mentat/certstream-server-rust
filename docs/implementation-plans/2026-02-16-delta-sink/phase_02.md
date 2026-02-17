# Delta Sink Implementation Plan - Phase 2: Arrow Schema and Record Conversion

**Goal:** Define the Arrow schema, `DeltaCertRecord` deserialization struct, and conversion from JSON bytes to `RecordBatch`

**Architecture:** The delta sink deserializes the `full` JSON bytes from `PreSerializedMessage.full` (which is a serialized `CertificateMessage`) into a flat `DeltaCertRecord` struct, then converts a batch of records into columnar Arrow arrays for Delta table writes.

**Tech Stack:** deltalake (re-exported arrow types), serde for JSON deserialization

**Scope:** 5 phases from original design (phases 1-5)

**Codebase verified:** 2026-02-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-sink.AC1: CT entries are written to Delta table with correct schema
- **delta-sink.AC1.1 Success:** `full` JSON bytes from broadcast channel deserialize into `DeltaCertRecord` with all fields populated
- **delta-sink.AC1.2 Success:** Arrow RecordBatch contains correct column types (Timestamp for `seen`, List(Utf8) for `all_domains`, Boolean for `is_ca`, etc.)
- **delta-sink.AC1.3 Success:** `as_der` and `chain` fields are present in the Delta table (base64 string and JSON-serialized list respectively)

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Create delta_sink.rs module with DeltaCertRecord struct

**Files:**
- Create: `src/delta_sink.rs`
- Modify: `src/main.rs` (add `mod delta_sink;` declaration)

**Implementation:**

Create `src/delta_sink.rs` with a `DeltaCertRecord` struct that deserializes from the `full` JSON format. The `full` JSON is a serialized `CertificateMessage` (see `src/models/certificate.rs:9-13`) with this nested structure:

```json
{
  "message_type": "certificate_update",
  "data": {
    "update_type": "X509LogEntry",
    "leaf_cert": { "serial_number": "...", "fingerprint": "...", ... },
    "chain": [...],
    "cert_index": 12345,
    "cert_link": "...",
    "seen": 1700000000.0,
    "source": { "name": "...", "url": "..." }
  }
}
```

The `DeltaCertRecord` is a **flat** struct that uses `#[serde(rename)]` and nested deserialization to flatten this hierarchy. It does NOT need to capture every field — only the columns defined in the Delta table schema.

Key type mappings from source structs (`src/models/certificate.rs`):
- `data.cert_index`: `u64` (line 21)
- `data.update_type`: `Cow<'static, str>` -> deserialize as `String` (line 17)
- `data.seen`: `f64` (line 23)
- `data.source.name`: `Arc<str>` -> deserialize as `String` (line 143)
- `data.source.url`: `Arc<str>` -> deserialize as `String` (line 144)
- `data.cert_link`: `String` (line 22)
- `data.chain`: `Option<Vec<ChainCert>>` (line 20) -> serialize each to JSON string
- `leaf_cert.serial_number`: `String` (line 110)
- `leaf_cert.fingerprint`: `String` (line 113)
- `leaf_cert.sha256`: `String` (line 115)
- `leaf_cert.sha1`: `String` (line 114)
- `leaf_cert.not_before`: `i64` (line 111)
- `leaf_cert.not_after`: `i64` (line 112)
- `leaf_cert.is_ca`: `bool` (line 117)
- `leaf_cert.signature_algorithm`: `String` (line 116)
- `leaf_cert.subject.aggregated`: `Option<String>` (line 42)
- `leaf_cert.issuer.aggregated`: `Option<String>` (line 42)
- `leaf_cert.all_domains`: `DomainList` (SmallVec) (line 118) -> `Vec<String>`
- `leaf_cert.as_der`: `Option<String>` (line 120)

Rather than using deeply nested serde `#[serde(rename)]` (which is fragile), deserialize into `CertificateMessage` directly (it already implements `Deserialize`) and then extract fields:

```rust
use crate::models::CertificateMessage;

pub struct DeltaCertRecord {
    pub cert_index: u64,
    pub update_type: String,
    pub seen: f64,
    pub seen_date: String,  // derived YYYY-MM-DD
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
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let msg: CertificateMessage = serde_json::from_slice(bytes)?;
        // Extract and flatten fields from msg
        // Convert seen (f64 seconds) to seen_date (YYYY-MM-DD)
        // Serialize chain certs individually to JSON strings
        // Use empty string defaults for Option<String> fields
        // ...
    }
}
```

The `seen_date` partition column is derived: convert `seen` (f64 seconds since epoch) to a date string. Use `chrono` (already in Cargo.toml) for this.

Also add `mod delta_sink;` to `src/main.rs` alongside the other module declarations. Place it alphabetically between `dedup` and `health` in the module declarations section (lines 1-13).

**Note:** Until Phase 4/5 when the module is fully wired, the compiler may emit dead_code warnings for `DeltaCertRecord` fields and functions. This is expected. Do not add `#[allow(dead_code)]` — the warnings will resolve naturally as later phases use the code.

**Verification:**

Run: `cargo build 2>&1 | tail -3`
Expected: Build succeeds

**Commit:** `feat: add DeltaCertRecord struct with JSON deserialization`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Define Arrow schema and records_to_batch conversion

**Verifies:** delta-sink.AC1.2, delta-sink.AC1.3

**Files:**
- Modify: `src/delta_sink.rs`

**Implementation:**

Add the Arrow schema definition and `records_to_batch()` function to `src/delta_sink.rs`.

Use arrow types re-exported from deltalake: `deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit}` and `deltalake::arrow::array::*`.

The schema must match the design's Delta Table Schema:

| Column | Arrow Type |
|--------|-----------|
| `cert_index` | `DataType::UInt64` |
| `update_type` | `DataType::Utf8` |
| `seen` | `DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))` |
| `seen_date` | `DataType::Utf8` |
| `source_name` | `DataType::Utf8` |
| `source_url` | `DataType::Utf8` |
| `cert_link` | `DataType::Utf8` |
| `serial_number` | `DataType::Utf8` |
| `fingerprint` | `DataType::Utf8` |
| `sha256` | `DataType::Utf8` |
| `sha1` | `DataType::Utf8` |
| `not_before` | `DataType::Int64` |
| `not_after` | `DataType::Int64` |
| `is_ca` | `DataType::Boolean` |
| `signature_algorithm` | `DataType::Utf8` |
| `subject_aggregated` | `DataType::Utf8` |
| `issuer_aggregated` | `DataType::Utf8` |
| `all_domains` | `DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))` |
| `as_der` | `DataType::Utf8` |
| `chain` | `DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))` |

Create a function `delta_schema() -> Arc<Schema>` that returns this schema.

Create `records_to_batch(records: &[DeltaCertRecord], schema: &Arc<Schema>) -> Result<RecordBatch, ArrowError>`:
- Build each column as an Arrow array from the records vector
- For Utf8 columns: use `StringArray::from(vec_of_strings)`
- For UInt64: `UInt64Array::from(vec_of_u64)`
- For Int64: `Int64Array::from(vec_of_i64)`
- For Boolean: `BooleanArray::from(vec_of_bool)`
- For Timestamp: `TimestampMicrosecondArray::from(vec_of_i64).with_timezone("UTC")` — convert f64 seconds to i64 microseconds
- For List(Utf8): use `ListBuilder<StringBuilder>` for `all_domains` and `chain`
- Build `RecordBatch::try_new(schema.clone(), columns)`

**Verification:**

Run: `cargo build 2>&1 | tail -3`
Expected: Build succeeds

**Commit:** `feat: add Arrow schema definition and records_to_batch conversion`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Unit tests for DeltaCertRecord and records_to_batch

**Verifies:** delta-sink.AC1.1, delta-sink.AC1.2, delta-sink.AC1.3

**Files:**
- Modify: `src/delta_sink.rs` (add `#[cfg(test)] mod tests` block)

**Testing:**

Add inline `#[cfg(test)] mod tests` following the project pattern (see `src/config.rs:599-803`, `src/models/certificate.rs:265-402`).

Tests must verify each AC listed above:
- **delta-sink.AC1.1:** Deserialize a sample `full` JSON (constructed from `CertificateMessage` using the same pattern as `make_test_message()` in `src/models/certificate.rs:272-308`) into `DeltaCertRecord`. Assert all fields are populated correctly: cert_index, update_type, seen, source_name, source_url, cert_link, serial_number, fingerprint, sha256, sha1, not_before, not_after, is_ca, signature_algorithm, subject_aggregated, issuer_aggregated, all_domains, as_der.
- **delta-sink.AC1.2:** Convert a batch of `DeltaCertRecord`s to a `RecordBatch`. Assert: the batch has the correct number of rows, the schema field count matches, specific column types are correct (check that `seen` column is `Timestamp(Microsecond, UTC)`, `all_domains` is `List(Utf8)`, `is_ca` is `Boolean`, etc.).
- **delta-sink.AC1.3:** Create a record with `as_der` set to a base64 string and `chain` with serialized JSON chain certs. Assert these values are present in the RecordBatch (read back from the StringArray and ListArray respectively).

Also test edge cases: empty `as_der` (None -> empty string), empty `chain` (None -> empty list), empty `all_domains`.

**Verification:**

Run: `cargo test delta_sink 2>&1 | tail -10`
Expected: All delta_sink tests pass

**Commit:** `test: add unit tests for DeltaCertRecord and Arrow conversion`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->
