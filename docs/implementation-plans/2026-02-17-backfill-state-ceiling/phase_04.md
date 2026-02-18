# Backfill State Ceiling Implementation Plan - Phase 4

**Goal:** Update CLAUDE.md project documentation to reflect the new backfill behavior (state file ceiling, no frontier gaps).

**Architecture:** Edit the Backfill Contracts section of CLAUDE.md to replace tree_size references with state file ceiling, remove frontier gap references, and document the state file dependency.

**Tech Stack:** Markdown documentation

**Scope:** 4 phases from original design (phase 4 of 4)

**Codebase verified:** 2026-02-18

---

## Acceptance Criteria Coverage

This phase is infrastructure (documentation update). **Verifies: None** — verified by reading the updated file.

---

<!-- START_TASK_1 -->
### Task 1: Update CLAUDE.md Backfill Contracts section

**Files:**
- Modify: `CLAUDE.md` (Backfill Contracts section, approximately lines 56-72)

**Implementation:**

Replace the first 7 bullet points of the Backfill Contracts section (from `**CLI flags**` through `**Historical rules**`). The edit boundary is: start at `## Backfill Contracts`, replace through the `**Historical rules**` bullet, stop before the `**Architecture**` bullet.

**Current text to replace** (lines 56-62 of CLAUDE.md):
```markdown
## Backfill Contracts
- **CLI flags**: `--backfill` activates backfill mode; `--from <INDEX>` sets historical start; `--logs <FILTER>` filters logs by substring
- **Entry point**: `backfill::run_backfill(config, backfill_from, backfill_logs, shutdown)` called from main, returns exit code (i32)
- **Two modes**: catch-up (no `--from`, fills gaps from existing Delta data) and historical (`--from N`, backfills from index N to tree_size)
- **Gap detection**: `detect_gaps(table_path, logs, backfill_from)` queries Delta table via DataFusion SQL, finds internal gaps (LEAD window function) and frontier gaps
- **Catch-up rules**: only backfills logs already present in Delta table; logs not in table are skipped
- **Historical rules**: backfills all logs from `--from` index; logs not in Delta get full range
```

**Replace with:**
```markdown
## Backfill Contracts
- **CLI flags**: `--backfill` activates backfill mode; `--from <INDEX>` sets historical start; `--logs <FILTER>` filters logs by substring
- **Entry point**: `backfill::run_backfill(config, backfill_from, backfill_logs, shutdown)` called from main, returns exit code (i32)
- **State file dependency**: backfill loads `StateManager` from `config.ct_log.state_file` (default: `certstream_state.json`) and uses each log's `current_index` as the per-log ceiling. Logs not in the state file are skipped with a warning. If no logs have state file entries, backfill exits with code 0.
- **Two modes**: catch-up (no `--from`, fills internal gaps within existing Delta data) and historical (`--from N`, backfills from index N to ceiling)
- **Gap detection**: `detect_gaps(table_path, logs, backfill_from)` queries Delta table via DataFusion SQL, finds internal gaps only (LEAD window function). The second element of the `logs` tuple is the ceiling (from state file `current_index`).
- **Catch-up rules**: only backfills logs already present in Delta table; logs not in table are skipped; only internal gaps are filled (no frontier gaps)
- **Historical rules**: backfills all logs from `--from` index to ceiling; logs not in Delta get full range from `--from` to ceiling
```

All remaining bullets after `**Historical rules**` stay unchanged:
- **Architecture**: mpsc channel from N fetcher tasks to 1 writer task...
- **Fetcher retry**: rate-limit errors get exponential backoff...
- **Writer reuses**: `delta_sink::flush_buffer()` and `delta_sink::open_or_create_table()`...
- **Exit code**: 0 if all fetchers and writer succeed...
- **Graceful shutdown**: CancellationToken checked per batch...

**Verification:**
Read the updated CLAUDE.md and confirm:
1. No references to `tree_size` in Backfill Contracts (except as historical context if any)
2. No references to "frontier gap"
3. State file dependency is documented
4. Ceiling terminology is used consistently

**Commit:** `docs: update CLAUDE.md backfill contracts for state file ceiling`

<!-- END_TASK_1 -->
