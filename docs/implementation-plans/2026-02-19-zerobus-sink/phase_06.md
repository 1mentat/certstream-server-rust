# ZeroBus Sink Implementation Plan - Phase 6: Integration Testing

**Goal:** End-to-end validation with a real Databricks workspace, gated behind a feature flag to avoid running without credentials.

**Architecture:** An integration test in `tests/` creates a ZeroBus SDK connection, ingests test records, and verifies they appear in the Databricks table. The test is gated behind `#[cfg(feature = "integration")]` so it only runs when explicitly enabled with `cargo test --features integration`. A manual test procedure documents how to verify live sink and backfill paths.

**Tech Stack:** databricks-zerobus-ingest-sdk 0.1, cargo test features

**Scope:** 6 phases from original design (phase 6 of 6)

**Codebase verified:** 2026-02-19

---

## Acceptance Criteria Coverage

This phase implements and tests:

### zerobus-sink.AC1: Live sink streams records to Databricks
- **zerobus-sink.AC1.1 Success:** Records from the broadcast channel are ingested into the Databricks Delta table via ZeroBus SDK

### zerobus-sink.AC3: Backfill writes historical records via ZeroBus
- **zerobus-sink.AC3.1 Success:** `--backfill --sink zerobus --from N` ingests records from index N to state file ceiling into Databricks

---

<!-- START_TASK_1 -->
### Task 1: Add "integration" feature to Cargo.toml

**Files:**
- Modify: `Cargo.toml` (add `integration` to `[features]` section at line 10)

**Implementation:**

Add to the `[features]` section (after `simd` at line 11):
```toml
integration = []
```

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Compiles without errors

**Commit:** `chore: add integration test feature flag`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create integration test for ZeroBus SDK connection and record ingestion

**Verifies:** zerobus-sink.AC1.1, zerobus-sink.AC3.1

**Files:**
- Modify: `src/zerobus_sink.rs` (add `#[cfg(all(test, feature = "integration"))]` module at end of file)

**Implementation:**

The project is a binary-only crate (`src/main.rs`, no `src/lib.rs`), so integration tests in `tests/` cannot import crate types. Place the integration test inside `src/zerobus_sink.rs` as a feature-gated test module. This gives direct access to all types without needing a `lib.rs` shim.

Add at the end of `src/zerobus_sink.rs` (after the existing `#[cfg(test)] mod tests` block):

```rust
#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    use super::*;
    use crate::delta_sink::DeltaCertRecord;
    use databricks_zerobus_ingest_sdk::{
        StreamConfigurationOptions, TableProperties, ZerobusSdk,
    };
    use prost::Message;

    fn test_record() -> DeltaCertRecord {
        DeltaCertRecord {
            cert_index: 999999,
            update_type: "X509LogEntry".to_string(),
            seen: 1700000000.0,
            seen_date: "2023-11-14".to_string(),
            source_name: "integration-test".to_string(),
            source_url: "https://test.example.com".to_string(),
            cert_link: "".to_string(),
            serial_number: "AA:BB:CC".to_string(),
            fingerprint: "test-fingerprint".to_string(),
            sha256: "test-sha256".to_string(),
            sha1: "test-sha1".to_string(),
            not_before: 1700000000,
            not_after: 1731536000,
            is_ca: false,
            signature_algorithm: "SHA256withRSA".to_string(),
            subject_aggregated: "CN=test.example.com".to_string(),
            issuer_aggregated: "CN=Test CA".to_string(),
            all_domains: vec!["test.example.com".to_string()],
            as_der: "".to_string(),
            chain: vec![],
        }
    }

    #[tokio::test]
    async fn test_zerobus_ingest_record() {
        let endpoint = match std::env::var("ZEROBUS_TEST_ENDPOINT") {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Skipping integration test: ZEROBUS_TEST_ENDPOINT not set");
                return;
            }
        };
        let uc_url = std::env::var("ZEROBUS_TEST_UC_URL")
            .expect("ZEROBUS_TEST_UC_URL required");
        let table_name = std::env::var("ZEROBUS_TEST_TABLE_NAME")
            .expect("ZEROBUS_TEST_TABLE_NAME required");
        let client_id = std::env::var("ZEROBUS_TEST_CLIENT_ID")
            .expect("ZEROBUS_TEST_CLIENT_ID required");
        let client_secret = std::env::var("ZEROBUS_TEST_CLIENT_SECRET")
            .expect("ZEROBUS_TEST_CLIENT_SECRET required");

        let sdk = ZerobusSdk::new(
            endpoint.clone(),
            uc_url.clone(),
        )
        .expect("Failed to create ZeroBus SDK");

        let table_properties = TableProperties {
            table_name: table_name.clone(),
            descriptor_proto: cert_record_descriptor_proto(),
        };

        let mut stream = sdk
            .create_stream(
                table_properties,
                client_id,
                client_secret,
                Some(StreamConfigurationOptions::default()),
            )
            .await
            .expect("Failed to create stream");

        let record = test_record();
        let cert_record = proto::CertRecord::from_delta_cert(&record);
        let encoded = cert_record.encode_to_vec();

        // ingest_record returns ack future on success; await it to confirm ingestion
        let ack_future = stream
            .ingest_record(encoded)
            .await
            .expect("Failed to queue record for ingestion");
        ack_future.await.expect("Failed to get ingestion acknowledgement");

        stream.flush().await.expect("Failed to flush");
        stream.close().await.expect("Failed to close");

        println!("Integration test passed: record ingested into {}", table_name);
    }
}
```

The test requires environment variables for Databricks credentials:
- `ZEROBUS_TEST_ENDPOINT`
- `ZEROBUS_TEST_UC_URL`
- `ZEROBUS_TEST_TABLE_NAME`
- `ZEROBUS_TEST_CLIENT_ID`
- `ZEROBUS_TEST_CLIENT_SECRET`

**Verification:**
Run: `cargo test --features integration` (with credentials set) in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Test passes, record ingested

Run: `cargo test` (without feature flag)
Expected: Integration test is not compiled or run

**Commit:** `test: add ZeroBus integration test with feature gate`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Document manual test procedure

**Files:**
- Create: `docs/implementation-plans/2026-02-19-zerobus-sink/manual-test-procedure.md`

**Implementation:**

Document the manual verification steps for both live sink and backfill paths:

```markdown
# ZeroBus Sink Manual Test Procedure

## Prerequisites
- Databricks workspace with Unity Catalog enabled
- OAuth M2M application with ingest permissions
- Target table created in Unity Catalog

## Environment Setup
export CERTSTREAM_ZEROBUS_ENABLED=true
export CERTSTREAM_ZEROBUS_ENDPOINT="<zerobus-endpoint>"
export CERTSTREAM_ZEROBUS_UNITY_CATALOG_URL="<uc-url>"
export CERTSTREAM_ZEROBUS_TABLE_NAME="catalog.schema.table"
export CERTSTREAM_ZEROBUS_CLIENT_ID="<client-id>"
export CERTSTREAM_ZEROBUS_CLIENT_SECRET="<client-secret>"

## Test 1: Live Sink
1. Start server: `cargo run`
2. Verify log output: "zerobus sink enabled, streaming to: catalog.schema.table"
3. Wait for CT records to flow (visible in server logs)
4. Query Databricks table to verify records appear
5. Send SIGINT, verify "zerobus sink shutdown complete" in logs

## Test 2: Backfill
1. Ensure state file exists with log entries
2. Run: `cargo run -- --backfill --sink zerobus --from 0`
3. Verify records ingested (check Databricks table)
4. Verify exit code 0

## Test 3: Error Cases
1. `cargo run -- --backfill --sink zerobus` (no --from) → error message
2. Set CERTSTREAM_ZEROBUS_ENABLED=false, run --sink zerobus → error message
3. `cargo run -- --backfill --sink badname` → error listing valid sinks

## Integration Test
ZEROBUS_TEST_ENDPOINT="<endpoint>" \
ZEROBUS_TEST_UC_URL="<uc-url>" \
ZEROBUS_TEST_TABLE_NAME="catalog.schema.table" \
ZEROBUS_TEST_CLIENT_ID="<id>" \
ZEROBUS_TEST_CLIENT_SECRET="<secret>" \
cargo test --features integration
```

**Verification:**
Run: `cat docs/implementation-plans/2026-02-19-zerobus-sink/manual-test-procedure.md`
Expected: Document exists and is readable

**Commit:** `docs: add ZeroBus sink manual test procedure`
<!-- END_TASK_3 -->
