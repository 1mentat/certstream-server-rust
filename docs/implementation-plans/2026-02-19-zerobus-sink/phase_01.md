# ZeroBus Sink Implementation Plan - Phase 1: Protobuf Schema & Build Pipeline

**Goal:** Define the certificate record protobuf schema, set up compile-time code generation via prost-build, and implement the conversion between DeltaCertRecord and the generated CertRecord type.

**Architecture:** A `.proto` file defines the 20-column CertRecord message. `prost-build` compiles it at build time into a Rust struct with `prost::Message` derive. A conversion function maps `DeltaCertRecord` to `CertRecord`, and a helper extracts `DescriptorProto` from the compiled file descriptor set for later use by the ZeroBus SDK's `TableProperties`.

**Tech Stack:** prost 0.13, prost-types 0.13, prost-build 0.13

**Scope:** 6 phases from original design (phase 1 of 6)

**Codebase verified:** 2026-02-19

---

## Acceptance Criteria Coverage

This phase implements and tests:

### zerobus-sink.AC2: Full 20-column schema as Protobuf
- **zerobus-sink.AC2.1 Success:** All 20 fields of `DeltaCertRecord` are represented in the protobuf `CertRecord` message and round-trip correctly
- **zerobus-sink.AC2.2 Success:** `DescriptorProto` is constructable from the compiled proto and accepted by `TableProperties`

---

<!-- START_SUBCOMPONENT_A (tasks 1-4) -->
<!-- START_TASK_1 -->
### Task 1: Add prost dependencies to Cargo.toml

**Files:**
- Modify: `Cargo.toml:13-52` (add dependencies and build-dependencies sections)

**Implementation:**

Add `prost` and `prost-types` to `[dependencies]` (after line 49, before `[dev-dependencies]`):

```toml
prost = "0.13"
prost-types = "0.13"
```

Add a new `[build-dependencies]` section (after `[dev-dependencies]`, before `[profile.release]`):

```toml
[build-dependencies]
prost-build = "0.13"
```

**Verification:**
Run: `cargo check` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Downloads and compiles prost crates without errors

**Commit:** `chore: add prost dependencies for protobuf support`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create proto/cert_record.proto

**Files:**
- Create: `proto/cert_record.proto`

**Implementation:**

Create the `proto/` directory and write the proto file. All 20 fields must match `DeltaCertRecord` (defined in `src/delta_sink.rs:22-43`) exactly:

```proto
syntax = "proto3";

package certstream;

// Mirrors the 20-column DeltaCertRecord struct (src/delta_sink.rs:22-43).
// Used as the wire format for ZeroBus SDK ingestion into Databricks.
message CertRecord {
  uint64 cert_index = 1;
  string update_type = 2;
  double seen = 3;
  string seen_date = 4;
  string source_name = 5;
  string source_url = 6;
  string cert_link = 7;
  string serial_number = 8;
  string fingerprint = 9;
  string sha256 = 10;
  string sha1 = 11;
  int64 not_before = 12;
  int64 not_after = 13;
  bool is_ca = 14;
  string signature_algorithm = 15;
  string subject_aggregated = 16;
  string issuer_aggregated = 17;
  repeated string all_domains = 18;
  string as_der = 19;
  repeated string chain = 20;
}
```

Field type mapping rationale:
- `cert_index: u64` → `uint64` (direct)
- `seen: f64` → `double` (direct; microsecond timestamp conversion happens in Arrow layer, not proto)
- `not_before/not_after: i64` → `int64` (direct)
- `is_ca: bool` → `bool` (direct)
- `all_domains/chain: Vec<String>` → `repeated string` (direct)
- All `String` fields → `string` (direct)

**Verification:**
Run: `ls proto/cert_record.proto`
Expected: File exists

**Commit:** `feat: add protobuf schema for CertRecord`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Create build.rs with prost-build

**Files:**
- Create: `build.rs` (project root, same level as Cargo.toml)

**Implementation:**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    let descriptor_path = std::path::Path::new(&out_dir).join("cert_record_descriptor.bin");

    let mut config = prost_build::Config::new();
    config.file_descriptor_set_path(&descriptor_path);
    config.compile_protos(&["proto/cert_record.proto"], &["proto/"])?;

    Ok(())
}
```

This compiles `cert_record.proto` into a Rust module at `$OUT_DIR/certstream.rs` (named after the proto package). It also writes a binary file descriptor set to `$OUT_DIR/cert_record_descriptor.bin` for runtime `DescriptorProto` extraction.

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Build succeeds. Generated file exists in target build output.

**Commit:** `feat: add build.rs for prost-build proto compilation`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Verify protobuf build pipeline end-to-end

**Files:** None (verification only)

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Build succeeds without errors. The prost-build step generates `certstream.rs` in the target output directory.

Run: `find target -name "certstream.rs" -path "*/out/*" | head -1`
Expected: A file path is returned, confirming the generated code exists.
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 5-6) -->
<!-- START_TASK_5 -->
### Task 5: Create src/zerobus_sink.rs with conversion and descriptor functions

**Verifies:** zerobus-sink.AC2.1, zerobus-sink.AC2.2

**Files:**
- Create: `src/zerobus_sink.rs`
- Modify: `src/main.rs:1` (add `mod zerobus_sink;` declaration)

**Implementation:**

Add `mod zerobus_sink;` to `src/main.rs` in the module declarations block (alphabetical order, after `mod websocket;` or in sorted position).

Create `src/zerobus_sink.rs` with:

1. **Proto module inclusion** — include the prost-generated code:
```rust
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/certstream.rs"));
}
```

2. **`CertRecord::from_delta_cert()`** — converts a `DeltaCertRecord` reference to a `CertRecord`:
```rust
use crate::delta_sink::DeltaCertRecord;

impl proto::CertRecord {
    pub fn from_delta_cert(record: &DeltaCertRecord) -> Self {
        Self {
            cert_index: record.cert_index,
            update_type: record.update_type.clone(),
            seen: record.seen,
            seen_date: record.seen_date.clone(),
            source_name: record.source_name.clone(),
            source_url: record.source_url.clone(),
            cert_link: record.cert_link.clone(),
            serial_number: record.serial_number.clone(),
            fingerprint: record.fingerprint.clone(),
            sha256: record.sha256.clone(),
            sha1: record.sha1.clone(),
            not_before: record.not_before,
            not_after: record.not_after,
            is_ca: record.is_ca,
            signature_algorithm: record.signature_algorithm.clone(),
            subject_aggregated: record.subject_aggregated.clone(),
            issuer_aggregated: record.issuer_aggregated.clone(),
            all_domains: record.all_domains.clone(),
            as_der: record.as_der.clone(),
            chain: record.chain.clone(),
        }
    }
}
```

3. **`cert_record_descriptor_proto()`** — extracts the DescriptorProto from the compiled file descriptor set:
```rust
use prost::Message;

pub fn cert_record_descriptor_proto() -> prost_types::DescriptorProto {
    let descriptor_bytes = include_bytes!(concat!(
        env!("OUT_DIR"),
        "/cert_record_descriptor.bin"
    ));
    let file_descriptor_set =
        prost_types::FileDescriptorSet::decode(&descriptor_bytes[..])
            .expect("Failed to decode file descriptor set");
    file_descriptor_set
        .file
        .into_iter()
        .next()
        .expect("No file descriptors in set")
        .message_type
        .into_iter()
        .next()
        .expect("No message types in file descriptor")
}
```

**Verification:**
Run: `cargo build` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: Compiles without errors

**Commit:** `feat: add CertRecord conversion and DescriptorProto builder`
<!-- END_TASK_5 -->

<!-- START_TASK_6 -->
### Task 6: Write tests for field mapping and DescriptorProto

**Verifies:** zerobus-sink.AC2.1, zerobus-sink.AC2.2

**Files:**
- Modify: `src/zerobus_sink.rs` (add `#[cfg(test)] mod tests` block at end of file)

**Testing:**

Tests must verify each AC listed above:
- zerobus-sink.AC2.1: Create a `DeltaCertRecord` with known values for all 20 fields, convert to `CertRecord` via `from_delta_cert()`, and assert every field matches. Must cover scalar fields (u64, f64, i64, bool), string fields, and repeated string fields (`all_domains`, `chain`).
- zerobus-sink.AC2.2: Call `cert_record_descriptor_proto()`, assert the returned `DescriptorProto` has `name == "CertRecord"` and contains exactly 20 fields.

Follow the inline `#[cfg(test)] mod tests` pattern used throughout the codebase (e.g., `src/delta_sink.rs:772`). Use a helper function like `make_test_record()` to create a `DeltaCertRecord` with deterministic values. Use `assert_eq!` for all field comparisons.

Additional test: encode a `CertRecord` to bytes via `prost::Message::encode_to_vec()` and decode it back, verifying the round-trip preserves all field values.

**Verification:**
Run: `cargo test zerobus` in `/data/mentat/certstream-server-rust/.worktrees/zerobus-sink/`
Expected: All tests pass

**Commit:** `test: add CertRecord field mapping and DescriptorProto tests`
<!-- END_TASK_6 -->
<!-- END_SUBCOMPONENT_B -->
