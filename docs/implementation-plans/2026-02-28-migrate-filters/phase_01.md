# Migrate Filters — Phase 1: CLI Parsing and Validation

**Goal:** Add `--source`, `--to`, and date-aware `--from` CLI flags so that `--migrate` can target a specific source table and date range without editing config files.

**Architecture:** The `--from` flag currently parses to `u64` inline in `cli.rs` for backfill index usage. To support YYYY-MM-DD dates for migrate and u64 indexes for backfill, `backfill_from` becomes `Option<String>` (raw storage). The backfill dispatch in `main.rs` parses String→u64 at the call site. Two new fields (`migrate_source`, `to`) are added to `CliArgs`. A `validate_date_format()` helper validates YYYY-MM-DD format for the migrate dispatch.

**Tech Stack:** Rust, std::env, manual CLI arg parsing (no clap)

**Scope:** 2 phases (phase 1 of 2)

**Codebase verified:** 2026-02-28

---

## Acceptance Criteria Coverage

This phase implements and tests:

### migrate-filters.AC1: CLI parsing for new flags
- **migrate-filters.AC1.1 Success:** `--from` stores raw string; backfill dispatch parses to u64
- **migrate-filters.AC1.2 Success:** `--source <PATH>` parsed and stored in `migrate_source`
- **migrate-filters.AC1.3 Success:** `--to <DATE>` parsed and stored in `to`
- **migrate-filters.AC1.4 Success:** `validate_date_format()` accepts YYYY-MM-DD, rejects invalid
- **migrate-filters.AC1.5 Success:** Help text includes new flags under MIGRATION OPTIONS

### migrate-filters.AC2: Dispatch logic (partial)
- **migrate-filters.AC2.3 Success:** `validate_backfill_sink_command` unchanged; backfill dispatch parses String→u64

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
## Subcomponent A: CliArgs Changes

<!-- START_TASK_1 -->
### Task 1: Change backfill_from to Option<String> and add new fields

**Verifies:** migrate-filters.AC1.1, migrate-filters.AC1.2, migrate-filters.AC1.3

**Files:**
- Modify: `src/cli.rs:5-20` (CliArgs struct)
- Modify: `src/cli.rs:37-50` (parse_args — parsing loop)
- Modify: `src/cli.rs:52-67` (parse_args — struct construction)

**Implementation:**

**1. Update CliArgs struct** (`src/cli.rs:5-20`):

Change `backfill_from` from `Option<u64>` to `Option<String>`. Add `migrate_source: Option<String>` and `to: Option<String>`:

```rust
#[derive(Debug, Clone)]
pub struct CliArgs {
    pub validate_config: bool,
    pub dry_run: bool,
    pub export_metrics: bool,
    pub show_version: bool,
    pub show_help: bool,
    pub backfill: bool,
    pub backfill_from: Option<String>,
    pub backfill_logs: Option<String>,
    pub staging_path: Option<String>,
    pub backfill_sink: Option<String>,
    pub merge: bool,
    pub migrate: bool,
    pub migrate_output: Option<String>,
    pub migrate_source: Option<String>,
    pub to: Option<String>,
}
```

**2. Update parsing loop** (`src/cli.rs:37-50`):

Add variables for the new fields and change `--from` to store the raw string:

```rust
let mut backfill_from = None;
let mut backfill_logs = None;
let mut staging_path = None;
let mut backfill_sink = None;
let mut migrate_output = None;
let mut migrate_source = None;
let mut to = None;

for (i, arg) in args.iter().enumerate() {
    if arg == "--from" && i + 1 < args.len() {
        backfill_from = Some(args[i + 1].clone());
    } else if arg == "--logs" && i + 1 < args.len() {
        backfill_logs = Some(args[i + 1].clone());
    } else if arg == "--staging-path" && i + 1 < args.len() {
        staging_path = Some(args[i + 1].clone());
    } else if arg == "--sink" && i + 1 < args.len() {
        backfill_sink = Some(args[i + 1].clone());
    } else if arg == "--output" && i + 1 < args.len() {
        migrate_output = Some(args[i + 1].clone());
    } else if arg == "--source" && i + 1 < args.len() {
        migrate_source = Some(args[i + 1].clone());
    } else if arg == "--to" && i + 1 < args.len() {
        to = Some(args[i + 1].clone());
    }
}
```

**3. Update struct construction** (`src/cli.rs:52-67`):

Add the new fields to the Self literal:

```rust
Self {
    // ... existing fields ...
    migrate: args.iter().any(|a| a == "--migrate"),
    migrate_output,
    migrate_source,
    to,
}
```

**Verification:**
Run: `cargo build`
Expected: Compilation fails because `backfill_from` type changed from `u64` to `String` — callers in `main.rs` and `cli.rs::validate_backfill_sink_command` need updating. This is expected and addressed in Task 2 and Phase 2.

**Commit:** `feat: add --source, --to flags and change --from to store raw string`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update validate_backfill_sink_command and main.rs dispatch for String backfill_from

**Verifies:** migrate-filters.AC2.3

**Files:**
- Modify: `src/cli.rs:129-169` (validate_backfill_sink_command signature and body)
- Modify: `src/main.rs:133-164` (backfill dispatch — parse String to u64)

**Implementation:**

**1. Keep validate_backfill_sink_command signature unchanged** (`src/cli.rs:129-169`):

The `validate_backfill_sink_command` function takes `Option<u64>` for `backfill_from` — it only checks `is_none()` for the zerobus case. Keep this function's signature as `Option<u64>` because it's semantically correct: it validates whether a backfill index was provided.

**2. Update main.rs backfill dispatch** (`src/main.rs:133-164`):

Before calling `validate_backfill_sink_command` and `run_backfill`, parse the String to u64. The backfill_from field is now `Option<String>`, so parse it:

```rust
if cli_args.backfill {
    // Parse --from value to u64 for backfill mode
    let backfill_from: Option<u64> = match &cli_args.backfill_from {
        Some(s) => match s.parse::<u64>() {
            Ok(v) => Some(v),
            Err(_) => {
                eprintln!("Error: --from value '{}' is not a valid integer for backfill mode", s);
                std::process::exit(1);
            }
        },
        None => None,
    };

    // Validate --sink flag
    if let Err(error_msg) = cli::validate_backfill_sink_command(
        cli_args.backfill_sink.as_deref(),
        backfill_from,
        config.zerobus_sink.enabled,
    ) {
        eprintln!("{}", error_msg);
        std::process::exit(1);
    }

    // ... tracing setup ...

    let exit_code = backfill::run_backfill(
        config,
        cli_args.staging_path,
        backfill_from,
        cli_args.backfill_logs,
        cli_args.backfill_sink,
        shutdown_token,
    )
    .await;

    std::process::exit(exit_code);
}
```

**Verification:**
Run: `cargo build`
Expected: Compiles without errors (the type mismatch from Task 1 is resolved)

**Commit:** `fix: parse --from String to u64 at backfill dispatch site`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->
## Subcomponent B: Date Validation and Help Text

<!-- START_TASK_3 -->
### Task 3: Add validate_date_format() function

**Verifies:** migrate-filters.AC1.4

**Files:**
- Modify: `src/cli.rs` (add function after `validate_backfill_sink_command`, before `#[cfg(test)]`)

**Implementation:**

Add a public function that validates YYYY-MM-DD format:

```rust
/// Validates that a date string is in YYYY-MM-DD format with valid month/day ranges.
///
/// # Arguments
/// * `date` - The date string to validate
/// * `flag_name` - The flag name for error messages (e.g., "--from", "--to")
///
/// # Returns
/// * `Ok(())` if the date is valid
/// * `Err(String)` with a descriptive error message if invalid
pub fn validate_date_format(date: &str, flag_name: &str) -> Result<(), String> {
    if date.len() != 10 {
        return Err(format!(
            "Error: {} value '{}' is not a valid date. Expected format: YYYY-MM-DD",
            flag_name, date
        ));
    }

    let parts: Vec<&str> = date.split('-').collect();
    if parts.len() != 3 || parts[0].len() != 4 || parts[1].len() != 2 || parts[2].len() != 2 {
        return Err(format!(
            "Error: {} value '{}' is not a valid date. Expected format: YYYY-MM-DD",
            flag_name, date
        ));
    }

    let year: u32 = parts[0].parse().map_err(|_| {
        format!(
            "Error: {} value '{}' is not a valid date. Expected format: YYYY-MM-DD",
            flag_name, date
        )
    })?;
    let month: u32 = parts[1].parse().map_err(|_| {
        format!(
            "Error: {} value '{}' is not a valid date. Expected format: YYYY-MM-DD",
            flag_name, date
        )
    })?;
    let day: u32 = parts[2].parse().map_err(|_| {
        format!(
            "Error: {} value '{}' is not a valid date. Expected format: YYYY-MM-DD",
            flag_name, date
        )
    })?;

    if year < 2000 || year > 2099 {
        return Err(format!(
            "Error: {} value '{}' has invalid year. Expected 2000-2099",
            flag_name, date
        ));
    }
    if !(1..=12).contains(&month) {
        return Err(format!(
            "Error: {} value '{}' has invalid month. Expected 01-12",
            flag_name, date
        ));
    }
    if !(1..=31).contains(&day) {
        return Err(format!(
            "Error: {} value '{}' has invalid day. Expected 01-31",
            flag_name, date
        ));
    }

    Ok(())
}
```

**Verification:**
Run: `cargo build`
Expected: Compiles without errors

**Commit:** `feat: add validate_date_format() for YYYY-MM-DD validation`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Update help text and add CLI tests

**Verifies:** migrate-filters.AC1.1, migrate-filters.AC1.2, migrate-filters.AC1.3, migrate-filters.AC1.4, migrate-filters.AC1.5

**Files:**
- Modify: `src/cli.rs:100-102` (MIGRATION OPTIONS in print_help)
- Modify: `src/cli.rs:171-433` (test module — update existing + add new tests)

**Implementation:**

**1. Update help text** (`src/cli.rs:100-102`):

Replace the current MIGRATION OPTIONS section:

```rust
println!("MIGRATION OPTIONS:");
println!("    --migrate              Migrate existing Delta table to new schema");
println!("    --output <PATH>        Output path for migrated table (required with --migrate)");
println!("    --source <PATH>        Source table path (default: config delta_sink.table_path)");
println!("    --from <DATE>          Start date filter (YYYY-MM-DD, inclusive)");
println!("    --to <DATE>            End date filter (YYYY-MM-DD, inclusive)");
```

Note: `--from` is shared between backfill (index) and migrate (date). The BACKFILL OPTIONS section already documents `--from <INDEX>`. Update it to clarify:

```rust
println!("BACKFILL OPTIONS:");
println!("    --backfill             Activate backfill mode (requires state file)");
println!("    --from <INDEX>         Override start index for all logs (integer)");
```

**2. Update existing tests that assert on `backfill_from`** (`src/cli.rs` test module):

Tests that currently assert `Some(12345u64)` or `Some(100u64)` need to change to `Some("12345".to_string())` or `Some("100".to_string())`:

- `test_sink_with_from_parameter` (line 228): `assert_eq!(parsed.backfill_from, Some("12345".to_string()))`
- `test_sink_with_staging_path` (line 258): `assert_eq!(parsed.backfill_from, Some("100".to_string()))`

**3. Add new tests:**

```rust
#[test]
fn test_source_flag_parsed_correctly() {
    let args = vec![
        "certstream".to_string(),
        "--migrate".to_string(),
        "--output".to_string(),
        "/tmp/out".to_string(),
        "--source".to_string(),
        "/data/old-table".to_string(),
    ];
    let parsed = CliArgs::parse_args(&args);
    assert_eq!(parsed.migrate_source, Some("/data/old-table".to_string()));
}

#[test]
fn test_source_flag_none_when_not_provided() {
    let args = vec!["certstream".to_string(), "--migrate".to_string()];
    let parsed = CliArgs::parse_args(&args);
    assert_eq!(parsed.migrate_source, None);
}

#[test]
fn test_to_flag_parsed_correctly() {
    let args = vec![
        "certstream".to_string(),
        "--migrate".to_string(),
        "--to".to_string(),
        "2024-06-30".to_string(),
    ];
    let parsed = CliArgs::parse_args(&args);
    assert_eq!(parsed.to, Some("2024-06-30".to_string()));
}

#[test]
fn test_to_flag_none_when_not_provided() {
    let args = vec!["certstream".to_string()];
    let parsed = CliArgs::parse_args(&args);
    assert_eq!(parsed.to, None);
}

#[test]
fn test_from_stores_date_string() {
    // --from now stores raw string, not parsed u64
    let args = vec![
        "certstream".to_string(),
        "--from".to_string(),
        "2024-01-15".to_string(),
    ];
    let parsed = CliArgs::parse_args(&args);
    assert_eq!(parsed.backfill_from, Some("2024-01-15".to_string()));
}

#[test]
fn test_from_stores_integer_string() {
    // --from with an integer value stores as string
    let args = vec![
        "certstream".to_string(),
        "--from".to_string(),
        "12345".to_string(),
    ];
    let parsed = CliArgs::parse_args(&args);
    assert_eq!(parsed.backfill_from, Some("12345".to_string()));
}

#[test]
fn test_migrate_all_flags_together() {
    let args = vec![
        "certstream".to_string(),
        "--migrate".to_string(),
        "--output".to_string(),
        "/tmp/out".to_string(),
        "--source".to_string(),
        "/data/source".to_string(),
        "--from".to_string(),
        "2024-01-01".to_string(),
        "--to".to_string(),
        "2024-01-31".to_string(),
    ];
    let parsed = CliArgs::parse_args(&args);
    assert!(parsed.migrate);
    assert_eq!(parsed.migrate_output, Some("/tmp/out".to_string()));
    assert_eq!(parsed.migrate_source, Some("/data/source".to_string()));
    assert_eq!(parsed.backfill_from, Some("2024-01-01".to_string()));
    assert_eq!(parsed.to, Some("2024-01-31".to_string()));
}

#[test]
fn test_validate_date_format_valid() {
    assert!(validate_date_format("2024-01-15", "--from").is_ok());
    assert!(validate_date_format("2025-12-31", "--to").is_ok());
    assert!(validate_date_format("2000-01-01", "--from").is_ok());
}

#[test]
fn test_validate_date_format_invalid() {
    assert!(validate_date_format("01-15-2024", "--from").is_err());
    assert!(validate_date_format("2024/01/15", "--from").is_err());
    assert!(validate_date_format("2024-1-5", "--from").is_err());
    assert!(validate_date_format("abc", "--from").is_err());
    assert!(validate_date_format("2024-13-01", "--from").is_err());
    assert!(validate_date_format("2024-01-32", "--from").is_err());
    assert!(validate_date_format("", "--from").is_err());
}
```

**Verification:**
Run: `cargo test --lib cli`
Expected: All tests pass (existing updated + new)

**Commit:** `test: add CLI tests for --source, --to, --from as string, and date validation`
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_B -->
