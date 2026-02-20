# ZeroBus Sink Manual Test Procedure

## Prerequisites
- Databricks workspace with Unity Catalog enabled
- OAuth M2M application with ingest permissions
- Target table created in Unity Catalog

## Environment Setup
```bash
export CERTSTREAM_ZEROBUS_ENABLED=true
export CERTSTREAM_ZEROBUS_ENDPOINT="<zerobus-endpoint>"
export CERTSTREAM_ZEROBUS_UNITY_CATALOG_URL="<uc-url>"
export CERTSTREAM_ZEROBUS_TABLE_NAME="catalog.schema.table"
export CERTSTREAM_ZEROBUS_CLIENT_ID="<client-id>"
export CERTSTREAM_ZEROBUS_CLIENT_SECRET="<client-secret>"
```

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
```bash
ZEROBUS_TEST_ENDPOINT="<endpoint>" \
ZEROBUS_TEST_UC_URL="<uc-url>" \
ZEROBUS_TEST_TABLE_NAME="catalog.schema.table" \
ZEROBUS_TEST_CLIENT_ID="<id>" \
ZEROBUS_TEST_CLIENT_SECRET="<secret>" \
cargo test --features integration
```
