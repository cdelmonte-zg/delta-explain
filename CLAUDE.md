# CLAUDE.md

## Project Overview

`delta-explain` is a CLI that makes Delta Lake file pruning visible. Given a Delta
table and a `WHERE` predicate, it shows which files would be eliminated by partition
pruning (directory level, on partition columns) and by data skipping (file level, on
min/max statistics) — and which files survive each phase.

It is **not** a query engine: it never reads parquet data, never executes joins or
projections. It reads the Delta log directly via `delta_kernel`, classifies the
predicate, and reports what the kernel's scan planner would do. The output is
designed to be both human-readable (text) and CI-friendly (JSON, exit codes,
`--min-pruning` / `--assert-stats` assertions).

Design references live alongside the code:

- `VISION.md` — public-facing roadmap (v0.1 → v0.5)
- `DELTA-EXPLAIN-ROADMAP.md` (in the parent directory) — internal P0/P1 step list

## Build & Test Commands

```bash
# Build
cargo build

# Run all tests
cargo test

# Run a single test by name
cargo test test_name_here

# Format, lint, and run tests (pre-commit check)
cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test
```

No workspace, no feature flags, no nextest. Single binary crate.

## Architecture

The pipeline is a thin sequence of pure-ish modules driven by `main.rs`:

- **`predicate_parser.rs`** — SQL string → `delta_kernel::expressions::Predicate`.
  Used to feed the kernel's scan builder when we want it to actually execute pruning.
- **`predicate_analyzer.rs`** — SQL string → `PredicateAnalysis` (top-level AND split,
  classification of each fragment as `partition_safe` / `stats_safe` / `unsplittable`,
  `Confidence` derivation, encoded notes like `UNSPLITTABLE_OR`).
- **`stats.rs`** — direct reads from the Delta log JSON: `metaData.partitionColumns`,
  per-file Add stats. We bypass the kernel here because we need the *raw* data to
  display, not the kernel's planner-internal view.
- **`report.rs`** — owns `PruningReport`, `PhaseResult`, and the two output writers
  (`print_text`, `print_json`). All formatting concerns live here.
- **`main.rs`** — orchestrates: parse CLI args, build the kernel engine, collect files,
  run analyzer, run kernel scan once per phase, build report, print, exit.

Why `predicate_parser` and `predicate_analyzer` are separate: the parser produces a
type the kernel can consume; the analyzer produces metadata we report to the user.
Same input string, two independent representations — coupling them would entangle
"what the kernel sees" with "what the user reads".

## Testing

- `tests/cli.rs` — end-to-end scenarios via `assert_cmd`. Hits the actual binary,
  asserts on stdout/stderr/exit code.
- `tests/partition_columns.rs` — focused regression tests for the `partitionColumns`
  metadata read path.
- `tests/partial_stats.rs` — exercises the `stats.mode = "partial"` path against
  `fixtures/test-table-partial-stats`.
- `tests/semantic_regression.rs` — one canonical end-to-end JSON check per
  "minimum case" from the roadmap (Step 1.5).
- **Use `rstest`** when several cases share the same flow and differ only in inputs
  or expected outputs. Prefer one parameterized test with `#[case]` over four near-duplicate
  `#[test]` functions. When parameters are independent (cartesian product), `#[values]`.
- **Substring assertions** (`predicate::str::contains`) over exact equality for text output.
  The text format will keep evolving — substring assertions stay green when we add
  cosmetic details.
- **JSON assertions** via `serde_json::Value` and path indexing: `json["analysis"]["confidence"]`.
  Don't assert on whole-document equality.
- Tests live in `tests/`; unit tests in `src/<module>.rs` under `#[cfg(test)] mod tests`.

### Fixtures

Real Delta tables (not synthetic blobs) live under `fixtures/`:

- `test-table` — partitioned by `country` (DE/US/IT), full stats. Canonical fixture.
- `test-table-flat` — non-partitioned variant, same schema.
- `test-table-empty` — zero data files, partition metadata only.
- `test-table-partial-stats` — partitioned, half the files have `stats` stripped.

Tests build paths via a `fixture()` helper that prefixes
`{CARGO_MANIFEST_DIR}/fixtures/`. Don't hardcode absolute fixture paths in new tests.

The fixtures are checked into the repo, so day-to-day development never needs to
regenerate them. When the schema or data must change, `fixtures/create_test_table.py`
rewrites them (Python deps pinned in `fixtures/requirements.txt`; the script skips
any directory that already exists). See README's *Development* section for the
exact venv + invocation sequence.

## Code Style

- `cargo fmt` is the source of truth (Rust 2024 edition, default 100-column width).
- **No `unwrap()` / `expect()` / `panic!()` / `unreachable!()` in production code.**
  Use `Result` and propagate. Acceptable in tests.
- No emoji or unicode emoji-substitutes (special arrows, checkmarks) in code, comments,
  or CLI output. Use ASCII (`->`, `=>`).
- Doc comments only for public APIs and only when they say something the signature
  doesn't already convey.
- Prefer descriptive test names over test doc comments. `#[test] fn verbose_shows_kept_and_dropped`
  beats `/// Test that verbose mode shows kept and dropped files`.
- Comments explain *why*, not *what*. If a comment restates the code, delete it.
- No `// removed`, `// kept for backwards compat`, `// added in PR #X` style comments.

## Pull Requests / Commit Messages

Conventional commits, lowercase after the prefix, no period:

```
feat: surface predicate classification and confidence in CLI output
fix: read partitionColumns from Delta log metadata instead of inferring from files
refactor: split predicate module into parser and analyzer
chore(deps): bump rand from 0.9.2 to 0.9.4
```

Allowed types: `feat`, `fix`, `refactor`, `chore`, `docs`, `perf`, `test`, `ci`.
Breaking changes carry `!` (e.g. `feat!: change JSON schema`).

The body, when needed, focuses on the *why* — context, motivation, trade-offs — not
the diff (the diff is right there).

## Output Schema Stability

The JSON output is a **stable contract** from v0.2.0 onwards. It carries an
explicit `schema_version` (currently `"0.1.0"`) and changes follow SemVer
relative to that field — additive changes bump the minor, breaking changes
bump the major.

When introducing a JSON change, decide first whether it is additive (new
optional field) or breaking (rename/remove/restructure). Breaking changes
require a `schema_version` bump and a CHANGELOG entry under "Breaking".

## Roadmap & Milestones

- **FASE 0 (soft launch)** — shipped in v0.2.0 (May 2026), tied to a MERGE
  article that cites `delta-explain` as a "companion tool to make file
  elimination visible from metadata".
- **FASE 1 (MVP)** — confidence model, stable JSON schema, full e2e test
  matrix. Shipped together with FASE 0 as v0.2.0.
- Later phases (2–5) cover checkpoint Parquet support, real stats coverage,
  type coercions, finer CI assertions, predicate analysis improvements,
  diagnostics, and compare mode. See `DELTA-EXPLAIN-ROADMAP.md` for the
  canonical list and current status.
