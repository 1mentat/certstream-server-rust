# S3 Write Resilience: Provider-Aware Storage Defaults — Human Test Plan

## Prerequisites
- Rust toolchain installed (edition 2024)
- `cargo test` passing (511 tests, 0 failures)
- Access to a terminal with the working directory at the project root
- A minimal config YAML file for `--validate-config` smoke tests (can use `config.example.yaml` as base)

## Phase 1: Env Var Provider Setting (AC3.2)

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `src/config.rs` and navigate to `Config::load()` around line 890-935. | Two code paths for S3 env vars: YAML-overlay branch (~line 912) and env-var-triggered construction branch (~line 929). |
| 2 | In the YAML-overlay branch (line 912-914), verify the code reads `env::var("CERTSTREAM_STORAGE_S3_PROVIDER")` and assigns `s3.provider = Some(v)`. | Pattern matches all other S3 env vars in the same block. |
| 3 | In the env-var-triggered construction branch (line 929), verify `S3StorageConfig` struct literal includes `provider: env::var("CERTSTREAM_STORAGE_S3_PROVIDER").ok()`. | Appears alongside `conditional_put`, `allow_http`, and other optional fields. |
| 4 | Create a minimal config YAML (no `storage.s3` section) and run: `CERTSTREAM_STORAGE_S3_ENDPOINT="https://fly.storage.tigris.dev" CERTSTREAM_STORAGE_S3_REGION="auto" CERTSTREAM_STORAGE_S3_ACCESS_KEY_ID="test" CERTSTREAM_STORAGE_S3_SECRET_ACCESS_KEY="test" CERTSTREAM_STORAGE_S3_PROVIDER="tigris" cargo run -- --validate-config` | No parse errors related to the `provider` field or S3 storage parsing. |
| 5 | Run the same command but with `CERTSTREAM_STORAGE_S3_PROVIDER="minio"`. | Same result: no parse errors related to the provider field. |

## Phase 2: Info-Level Log Message (AC3.4)

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `src/config.rs` and navigate to `resolve_storage_options` around line 603-617. | A `match provider` block with arms for `Tigris | R2 | MinIO` and `Aws | Generic`. |
| 2 | Verify inside the `Tigris | R2 | MinIO` arm, within the `if !opts.contains_key("conditional_put")` block, there is an `info!(...)` macro call. | Message: "Provider detected as {:?} from {}, setting conditional_put=etag". |
| 3 | Verify the `info!()` call is inside the `if !opts.contains_key("conditional_put")` guard, NOT outside it. | Log only appears when auto-default is actually applied. |
| 4 | (Optional) If you have a Tigris-endpoint config, run with `RUST_LOG=info cargo run -- --validate-config` and check for the log line containing "setting conditional_put=etag". | The log line should appear once for each S3 location resolved where conditional_put was auto-applied. |

## Phase 3: Config Example File

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `config.example.yaml`. | File exists and is readable. |
| 2 | Search for `provider` in the `storage.s3` section. | Commented-out `provider` field with explanation of available values (tigris, aws, r2, minio) and auto-detection note. |

## End-to-End: Provider Detection Through Named Target Resolution

1. Examine test `test_resolve_target_tigris_target_gets_auto_conditional_put` in `src/config.rs`.
2. Trace the call chain: `config.resolve_target("tigris")` -> `resolve_storage_options()` -> `detect_s3_provider()` -> detects Tigris -> auto-sets `conditional_put=etag` -> returns in `ResolvedTarget.storage_options`.
3. Confirm the test verifies the final output (`resolved.storage_options.get("conditional_put")` equals `"etag"`).
4. Examine `test_resolve_target_global_storage_fallback_gets_provider_defaults` for the same pipeline via global storage fallback.
5. Both tests should be in the passing set (511/511 pass).

## End-to-End: Backward Compatibility

1. Run `cargo test` from the project root.
2. Verify 511 tests pass, 0 fail.
3. Confirm all changes to existing tests are limited to adding `provider: None` to `S3StorageConfig` struct literals -- no behavioral changes.

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 - Explicit provider priority | `test_detect_s3_provider_explicit_tigris`, `_aws`, `_r2`, `_minio`, `_overrides_url` | -- |
| AC1.2 - URL pattern matching | `test_detect_s3_provider_tigris_url`, `_aws_url`, `_r2_url` | -- |
| AC1.3 - Unknown -> Generic | `test_detect_s3_provider_generic_url`, `_explicit_unknown` | -- |
| AC1.4 - Case insensitive | `test_detect_s3_provider_case_insensitive`, `_explicit_case_insensitive` | -- |
| AC2.1 - Tigris auto conditional_put | `test_resolve_storage_options_tigris_auto_conditional_put`, `test_resolve_target_tigris_target_gets_auto_conditional_put` | -- |
| AC2.2 - R2 auto conditional_put | `test_resolve_storage_options_r2_auto_conditional_put` | -- |
| AC2.3 - MinIO auto conditional_put | `test_resolve_storage_options_minio_explicit_provider_auto_conditional_put` | -- |
| AC2.4 - AWS/Generic no auto defaults | `test_resolve_storage_options_aws_no_auto_conditional_put`, `_generic_no_auto_conditional_put` | -- |
| AC2.5 - Explicit never overridden | `test_resolve_storage_options_explicit_conditional_put_not_overridden` | -- |
| AC3.1 - provider field in struct | All tests compile with `provider` field | -- |
| AC3.2 - PROVIDER env var | (code inspection) | Phase 1 steps 1-5 |
| AC3.3 - Named target provider field | `test_resolve_target_tigris_target_gets_auto_conditional_put` | -- |
| AC3.4 - Info log on auto-default | -- | Phase 2 steps 1-4 |
| AC4.1 - Existing configs unchanged | `test_resolve_storage_options_local_unaffected_by_provider` + 511 tests passing | -- |
| AC4.2 - Global explicit preserved | `test_resolve_target_global_explicit_conditional_put_preserved` | -- |
| AC4.3 - Target fallback to global | `test_resolve_target_global_storage_fallback_gets_provider_defaults` | -- |
| AC4.4 - All existing tests pass | Full `cargo test`: 511 passed, 0 failed | E2E backward compat steps 1-4 |
