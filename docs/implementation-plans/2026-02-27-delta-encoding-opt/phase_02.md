# Delta Encoding Optimization — Phase 2: Schema Change — `as_der` to Binary

**Goal:** Store raw DER bytes instead of base64-encoded strings in the Delta table, eliminating ~33% size overhead on the largest column.

**Architecture:** `DeltaCertRecord.as_der` changes from `String` to `Vec<u8>`. The base64 decoding happens in `DeltaCertRecord::from_message()` — the entry point where `LeafCert` data enters the Delta storage path. `LeafCert.as_der` and `parse_certificate()` remain unchanged to preserve the JSON wire format for WebSocket/SSE consumers. The ZeroBus sink re-encodes to base64 when populating its protobuf string field.

**Design deviation:** The design plan specifies changing `LeafCert.as_der` from `Option<String>` to `Option<Vec<u8>>` and removing `STANDARD.encode()` from `parser.rs`. Investigation revealed this would break JSON serialization for WebSocket/SSE consumers (Vec<u8> serializes as `[1,2,3,...]` in JSON, not as a base64 string). Since all Delta/ZeroBus paths go through `DeltaCertRecord::from_message()` anyway, decoding base64 there achieves the same result (Binary in Delta, AC2.1) without requiring custom serde modules or risking wire format changes. All three ACs are satisfied.

**Tech Stack:** Rust, base64 crate (STANDARD engine), Arrow BinaryArray, deltalake

**Scope:** 4 phases from original design (phase 2 of 4)

**Codebase verified:** 2026-02-27

---

## Acceptance Criteria Coverage

This phase implements and tests:

### delta-encoding-opt.AC2: as_der schema change
- **delta-encoding-opt.AC2.1 Success:** as_der stored as Binary (raw DER bytes) in Delta table, not base64 Utf8
- **delta-encoding-opt.AC2.2 Success:** Round-trip: write DER bytes → read back → bytes match original
- **delta-encoding-opt.AC2.3 Success:** ZeroBus sink correctly base64-encodes raw bytes for protobuf string field

### delta-encoding-opt.AC5: No regression
- **delta-encoding-opt.AC5.2 Success:** All existing tests pass after changes
- **delta-encoding-opt.AC5.3 Success:** ZeroBus sink continues to function (protobuf as_der field populated correctly)

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
## Subcomponent A: Delta Storage Type Change

<!-- START_TASK_1 -->
### Task 1: Change DeltaCertRecord.as_der to Vec<u8> and update conversion logic

**Verifies:** delta-encoding-opt.AC2.1

**Files:**
- Modify: `src/delta_sink.rs:1-18` (add base64 import)
- Modify: `src/delta_sink.rs:43` (struct field type)
- Modify: `src/delta_sink.rs:72-128` (from_message method)
- Modify: `src/delta_sink.rs:134-169` (delta_schema — change as_der to Binary)
- Modify: `src/delta_sink.rs:383-387` (records_to_batch — change StringArray to BinaryArray)

**Implementation:**

**1. Add base64 import** (`src/delta_sink.rs`, add near the top imports):

```rust
use base64::{engine::general_purpose::STANDARD, Engine};
```

**2. Change struct field** (`src/delta_sink.rs:43`):

```rust
pub as_der: Vec<u8>,
```

**3. Update from_message()** — find the line in `from_message` that sets `as_der`:

Currently (around line 128 in the struct literal):
```rust
as_der: msg.data.leaf_cert.as_der.clone().unwrap_or_default(),
```

Replace with:
```rust
as_der: msg
    .data
    .leaf_cert
    .as_der
    .as_deref()
    .and_then(|s| STANDARD.decode(s).ok())
    .unwrap_or_default(),
```

This decodes the base64 string from LeafCert into raw bytes. If the base64 string is missing or invalid, defaults to empty `Vec<u8>`.

**4. Change delta_schema()** (`src/delta_sink.rs:162`):

Replace:
```rust
Field::new("as_der", DataType::Utf8, false),
```

With:
```rust
Field::new("as_der", DataType::Binary, false),
```

**5. Update records_to_batch()** (`src/delta_sink.rs:383-387`):

Replace:
```rust
// Build as_der column (Utf8)
let as_der: StringArray = records
    .iter()
    .map(|r| Some(r.as_der.as_str()))
    .collect();
```

With:
```rust
// Build as_der column (Binary)
let as_der: BinaryArray = records
    .iter()
    .map(|r| Some(r.as_der.as_slice()))
    .collect();
```

**Verification:**
Run: `cargo build`
Expected: Compiles (some tests will fail — fixed in Task 3)

**Commit:** `feat: change as_der from String/Utf8 to Vec<u8>/Binary in Delta storage`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update ZeroBus sink to base64-encode as_der for protobuf

**Verifies:** delta-encoding-opt.AC2.3, delta-encoding-opt.AC5.3

**Files:**
- Modify: `src/zerobus_sink.rs:1-17` (add base64 import)
- Modify: `src/zerobus_sink.rs:49` (from_delta_cert as_der mapping)

**Implementation:**

**1. Add base64 import** (`src/zerobus_sink.rs`, add after existing imports):

```rust
use base64::{engine::general_purpose::STANDARD, Engine};
```

**2. Update from_delta_cert()** (`src/zerobus_sink.rs:49`):

Replace:
```rust
as_der: record.as_der.clone(),
```

With:
```rust
as_der: STANDARD.encode(&record.as_der),
```

The protobuf `CertRecord.as_der` field remains `string` type. The raw bytes from `DeltaCertRecord` are base64-encoded back to a string for the protobuf wire format. This maintains the existing protobuf contract.

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat: base64-encode as_der in ZeroBus protobuf conversion`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Update all tests for Vec<u8> as_der

**Verifies:** delta-encoding-opt.AC2.1, delta-encoding-opt.AC2.2, delta-encoding-opt.AC5.2, delta-encoding-opt.AC5.3

**Files:**
- Modify: `src/delta_sink.rs` (test module, lines 791-1106)
- Modify: `src/zerobus_sink.rs` (test module, lines 256-414)
- Modify: `src/backfill.rs` (test helpers, lines 1347-1353)

**Testing:**

Tests must be updated to reflect the type change from String to Vec<u8>. The key changes are:

**delta_sink.rs test updates:**

1. **`make_test_json_bytes()` (line 792):** Change `"as_der":"base64encodedderdata"` in the JSON to a valid base64 string, e.g., `"as_der":"AQID"` (which decodes to bytes `[1, 2, 3]`). All other test JSON strings containing `"as_der":"base64encodedderdata"` must also be updated to use `"AQID"` or the equivalent valid base64.

2. **`test_from_json_deserializes_all_fields` (line 823):** Change assertion from `assert_eq!(record.as_der, "base64encodedderdata")` to `assert_eq!(record.as_der, vec![1u8, 2, 3])`.

3. **`test_from_json_with_empty_as_der` (line 860):** Change assertion from `assert_eq!(record.as_der, "")` to `assert_eq!(record.as_der, Vec::<u8>::new())` (empty Vec since `as_der: null` in JSON → None → empty bytes).

4. **`test_from_message_matches_from_json` (line 905):** No logic change needed — `assert_eq!(record_from_json.as_der, record_from_message.as_der)` still works since both produce `Vec<u8>`.

5. **`test_records_to_batch_contains_as_der_string` (line 982-995):** Rename to `test_records_to_batch_contains_as_der_binary`. Change downcast from `StringArray` to `BinaryArray`. Change assertion from `assert_eq!(as_der_col.value(0), "base64encodedderdata")` to `assert_eq!(as_der_col.value(0), &[1u8, 2, 3])`.

6. **`test_records_to_batch_with_empty_domains_and_chain` (line 1087-1106):** Change downcast from `StringArray` to `BinaryArray`. Change assertion from `assert_eq!(as_der_col.value(0), "")` to `assert_eq!(as_der_col.value(0), &[] as &[u8])`.

7. **`test_delta_schema_field_types` (if it exists):** Verify `as_der` field is now `DataType::Binary`.

**zerobus_sink.rs test updates:**

1. **`make_test_record()` (line 256-278):** Change `as_der: "base64_encoded_der_data".to_string()` to `as_der: vec![1u8, 2, 3]` (or another test byte vector).

2. **`test_from_delta_cert_all_fields` (line 311):** Change assertion from `assert_eq!(cert_record.as_der, "base64_encoded_der_data")` to `assert_eq!(cert_record.as_der, STANDARD.encode(&[1u8, 2, 3]))` — verify the base64 re-encoding.

3. **`test_cert_record_round_trip` (line 412):** Still works — `assert_eq!(decoded.as_der, cert_record.as_der)` checks protobuf round-trip of the string field.

4. **`test_delta_cert_record_to_proto_encoding` and `test_delta_cert_record_from_json_valid`:** Update any test DeltaCertRecord literals or JSON fixtures that reference as_der as a String to use valid base64 or Vec<u8>.

**backfill.rs test updates:**

1. **`make_test_record()` (line 1347-1353):** This function creates DeltaCertRecord via `from_json`. The JSON it uses contains `"as_der":"base64encodedderdata"`. Update to valid base64 (e.g., `"as_der":"AQID"`). Assertions that check `record.as_der` need to compare against `Vec<u8>` values.

**New test — round-trip Binary verification (AC2.2):**

Add a test in delta_sink.rs that:
1. Creates a DeltaCertRecord with known `as_der` bytes (e.g., `vec![0xDE, 0xAD, 0xBE, 0xEF]`)
2. Writes it to a Delta table via `flush_buffer`
3. Reads it back via DataFusion SQL
4. Asserts the read-back bytes match the original

Follow the pattern of `test_flush_buffer_writes_zstd_compression` (lines 1972-2056) for writing and reading back from a Delta table.

**Verification:**
Run: `cargo test`
Expected: All tests pass (existing updated + new round-trip test)

**Commit:** `test: update all tests for Binary as_der and add round-trip verification`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->
