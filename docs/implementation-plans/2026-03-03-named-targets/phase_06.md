# Named Targets Implementation Plan — Phase 6: Integration Testing and Cleanup

**Goal:** Verify end-to-end flows work with named targets, remove dead code from old CLI patterns, and update CLAUDE.md to reflect new CLI and config structure.

**Architecture:** Add integration tests exercising config->CLI->resolve->operation flows, sweep all source files for remaining references to old flag patterns, and update project documentation.

**Tech Stack:** Rust, cargo test

**Scope:** 6 phases from original design (this is phase 6 of 6)

**Codebase verified:** 2026-03-05

---

## Acceptance Criteria Coverage

This phase implements and tests:

### named-targets.AC7: Integration and cleanup
- **named-targets.AC7.1 Success:** Full config -> CLI -> resolve -> migrate flow works end-to-end
- **named-targets.AC7.2 Success:** No dead code from old `--output`/`--staging-path`/`--source <path>` patterns remains
- **named-targets.AC7.3 Success:** CLAUDE.md updated to reflect new CLI and config contracts

---

## Codebase Verification Findings

Dead code and references to old patterns that need cleanup:

**src/cli.rs:**
- ✓ Old CliArgs fields (staging_path, migrate_output, migrate_source, output_path) should be removed in Phase 3. Phase 6 verifies they're gone.
- ✓ ~20 tests in cli.rs that reference old flags should be updated in Phase 3. Phase 6 verifies none remain.

**src/main.rs:**
- ✓ `validate_staging_path_uri()` function at lines 485-492 — should be removed in Phase 4. Phase 6 verifies it's gone.
- ✓ Old error messages referencing `--output`, `--staging-path` — should be updated in Phases 3-4. Phase 6 verifies.

**src/config.rs:**
- ✓ `test_validate_staging_path_uri_format` test at line 2216 — should be removed or updated.

**CLAUDE.md sections to update:**
- ✓ Commands section (lines 24-32): 6 command examples with old flags
- ✓ Architecture section (lines 65-71): mode descriptions with old flag names
- ✓ Backfill Contracts (lines 154-170): CLI flags description
- ✓ Merge Contracts (lines 173-182): CLI flags description
- ✓ Migrate Contracts (lines 185-197): CLI flags description
- ✓ Metadata Extraction Contracts (lines 215-226): CLI flags description
- ✓ CLI Shared Flag Contracts section: needs updating for --target/--source

**No existing end-to-end tests** found that combine config loading + CLI parsing + target resolution + operation execution in a single test.

---

<!-- START_TASK_1 -->
### Task 1: Add integration test for config -> resolve -> operation flow

**Verifies:** named-targets.AC7.1

**Files:**
- Modify: `src/config.rs` (add integration test to `#[cfg(test)] mod tests` block)

**Implementation:**

Add an integration test that exercises the full flow: define targets in a Config, resolve them, and verify the resolved values are correct for passing to operation functions.

**Testing:**

Test must verify:
- named-targets.AC7.1: Construct a Config with a `targets` map containing entries for "source" and "target" with specific table_paths, compression levels, and storage configs. Call `config.resolve_target("source")` and `config.resolve_target("target")`. Verify the resolved targets have:
  - Correct table_paths (normalized via parse_table_uri)
  - Correct storage_options (resolved from per-target or global storage)
  - Correct compression_level (from target config or delta_sink fallback)
  - Correct heavy_column_compression_level (from target config or delta_sink fallback)
  - Correct offline_batch_size (from target config or delta_sink fallback)

Also test a round-trip: construct a ResolvedTarget from resolve_target(), then verify it can be used with `open_or_create_table()` to create a temp Delta table at the resolved path.

Follow existing test patterns: use `file://` paths with temp directories, clean up after test.

**Verification:**

Run: `cargo test config::tests::test_target_integration`
Expected: Integration test passes

**Commit:** `test: add end-to-end integration test for named target resolution`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Remove dead code from old CLI patterns

**Verifies:** named-targets.AC7.2

**Files:**
- Verify and clean: `src/cli.rs` (confirm no old field references remain)
- Verify and clean: `src/main.rs` (confirm `validate_staging_path_uri` removed, no old error messages)
- Verify and clean: `src/config.rs` (remove `test_validate_staging_path_uri_format` if still present)
- Verify and clean: all source files for any remaining `--output`, `--staging-path`, `--source <path>` references

**Implementation:**

This is a verification and cleanup task. After Phases 3-5, most dead code should already be removed. This task verifies completeness:

1. **Search all .rs files** for string literals and identifiers referencing old patterns:
   - `staging_path` (as a field/variable name, not in comments about the feature)
   - `migrate_output`
   - `migrate_source`
   - `output_path` (as the old CLI field name)
   - `--output` (in string literals)
   - `--staging-path` (in string literals)
   - `validate_staging_path_uri`

2. **For each reference found**: determine if it's dead code (should be removed) or legitimate (new code referencing the concept). Remove dead code.

3. **Check test modules** for tests that still construct old field names or reference removed CliArgs fields.

4. **Verify no commented-out code** from old patterns was left behind (avoid `// removed: staging_path` style comments).

**Verification:**

Run: `cargo build`
Expected: Clean build with no warnings about unused imports or dead code

Run: `cargo test`
Expected: All tests pass

Run: `grep -rn "staging_path\|migrate_output\|migrate_source\|output_path\|validate_staging_path_uri" src/`
Expected: No matches, with these known exceptions to ignore if found:
- `output_path` in `table_ops.rs` test names or comments that describe the parameter being replaced (these should be renamed during Phase 5)
- `staging_path` in `detect_gaps` documentation comments describing the old dual-table behavior (should be updated)
- Any grep match inside a `// Removed:` or changelog-style comment should be deleted (we don't keep commented-out code)
If legitimate matches remain, investigate and remove. Do NOT leave dead references.

**Commit:** `chore: remove dead code from old CLI flag patterns`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Update CLAUDE.md for new CLI and config contracts

**Verifies:** named-targets.AC7.3

**Files:**
- Modify: `CLAUDE.md`

**Implementation:**

Update the following CLAUDE.md sections to reflect the new named targets CLI and config:

**Commands section** — Replace old command examples:
- `cargo run -- --backfill --staging-path file:///tmp/staging` → `cargo run -- --backfill --target staging`
- `cargo run -- --merge --staging-path file:///tmp/staging` → `cargo run -- --merge --source staging --target main`
- `cargo run -- --migrate --output file:///tmp/output` → `cargo run -- --migrate --source main --target output`
- `cargo run -- --migrate --output file:///tmp/output --source file:///tmp/source --from 2026-02-01 --to 2026-02-28` → `cargo run -- --migrate --source archive --target output --from 2026-02-01 --to 2026-02-28`
- `cargo run -- --extract-metadata --output file:///tmp/metadata` → `cargo run -- --extract-metadata --source main --target metadata`
- Similar updates for extract-metadata with date filters

**Architecture section** — Update mode descriptions:
- Backfill: mention `--target` instead of `--staging-path`
- Merge: mention `--source` and `--target` instead of `--staging-path`
- Migrate: mention `--source` and `--target` instead of `--output` and `--source <path>`
- Extract-metadata: mention `--source` and `--target` instead of `--output`

**Add Named Targets section** — Add a new section after "Storage Config Contracts" documenting:
- `TargetConfig` struct fields
- `ResolvedTarget` struct fields
- `targets:` config section format
- `resolve_target()` behavior (lookup + inheritance from delta_sink)
- `--target` and `--source` CLI flags
- Env var pattern: `CERTSTREAM_TARGETS_<NAME>_<FIELD>`

**Backfill Contracts** — Update CLI flags to reference `--target` instead of `--staging-path`

**Merge Contracts** — Update CLI flags to reference `--source` and `--target`

**Migrate Contracts** — Update CLI flags to reference `--source` and `--target`

**Reparse Audit Contracts** — Add `--source` requirement

**Metadata Extraction Contracts** — Update to reference `--source` and `--target`

**CLI Shared Flag Contracts** — Add notes about `--target` and `--source` flag semantics

**Verification:**

Run: `grep -n "staging-path\|--output" CLAUDE.md`
Expected: No old flag references remain (except possibly in change history notes)

**Commit:** `docs: update CLAUDE.md for named targets CLI and config contracts`

<!-- END_TASK_3 -->
