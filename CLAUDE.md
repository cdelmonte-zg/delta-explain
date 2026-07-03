# CLAUDE.md

## Project Overview

`delta-explain` is a CLI that makes Delta Lake file pruning visible. Given a Delta
table and a `WHERE` predicate, it shows which files would be eliminated by partition
pruning (directory level, on partition columns) and by data skipping (file level, on
min/max statistics), and which files survive each phase.

It is **not** a query engine: it never reads parquet data, never executes joins or
projections. It reads the Delta log directly via `delta_kernel`, classifies the
predicate, and reports what the kernel's scan planner would do. The output is
designed to be both human-readable (text) and CI-friendly (JSON, exit codes,
`--min-pruning` / `--assert-stats` assertions).

Design references live alongside the code:

- `VISION.md`: public-facing roadmap (v0.1 → v0.5)
- `DELTA-EXPLAIN-ROADMAP.md` (in the parent directory): internal P0/P1 step list

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

No workspace, no feature flags, no nextest. One package, two targets: the
`delta_explain` library (analysis machinery) and the `delta-explain` binary
(CLI layer). The Rust API is internal and unstable; the stable contracts are
the CLI surface and the versioned JSON output schema.

## Architecture

The pipeline is a thin sequence of pure-ish library modules consumed by the
CLI in `main.rs`:

- **`predicate_ast.rs`**: SQL string → owned `Pred` AST, via a converter from
  sqlparser (the only module coupled to sqlparser types). The AST vocabulary is
  the language of pruning: column-op-literal comparisons, junctions, null checks,
  `IN`/`BETWEEN` sugar. Anything outside it becomes an `Unsupported` leaf carrying
  the raw fragment and a reason; the converter is total, consumers decide severity.
- **`predicate_analyzer.rs`**: `Pred` → `PredicateAnalysis` (top-level AND split,
  classification of each fragment as `partition_safe` / `stats_safe` / `unsplittable`,
  `Confidence` derivation, encoded notes like `UNSPLITTABLE_OR`). `classify` also
  returns the partition-safe subtree so the CLI can lower it without re-parsing.
- **`kernel_bridge.rs`**: the only module that names the kernel's expression
  vocabulary. Lowers `Pred` to `delta_kernel::expressions::Predicate` with
  schema-driven literal coercion (temporal, decimal, narrow ints, nested leaf
  types), and maps every kernel operator to a `Capability` tier through
  exhaustive matches with no catch-all arm, so a kernel bump that widens an
  enum breaks compilation instead of silently gapping support.
- **`scan.rs`**: kernel-backed metadata scans. `scan_baseline` collects the file
  listing and the per-file stats from one `scan_metadata` pass (the stats ride on
  the scan rows via `include_all_stats_columns`, checkpoint Parquet included);
  `collect_files` runs the per-phase predicate scans; `partition_columns_from_files`
  is the checkpoint-only fallback for partition columns.
- **`stats.rs`**: the statistics domain. `FileStats` types, stats-JSON parsing and
  nested flattening to dotted leaf keys, plus the JSON `metaData.partitionColumns`
  reader (primary source; the scan fallback covers fully checkpointed logs).
- **`attribution.rs`**: survivor sets → chained, labeled pruning phases. Pure.
- **`gates.rs`**: `--min-pruning` / `--assert-stats` → assertion records, overall
  result, failure messages. Pure.
- **`report.rs`**: the computed model (`PruningReport`, `PhaseResult`, `FileInfo`).
- **`render.rs`**: all presentation, text and JSON (including `schema_version`).
- **`error.rs`**: the crate error enum (thiserror); kernel errors pass through
  transparently.
- **`main.rs`**: CLI layer: parse args, build the kernel engine, baseline scan,
  analyzer, per-phase scans, `build_phases`, gates, render, exit code.

Why the AST sits between sqlparser and everything else: one parse, two
interpreters. `kernel_bridge` produces the type the kernel consumes; the
analyzer produces the metadata we report to the user. Both read the same owned
`Pred`, so "what the kernel sees" and "what the user reads" can never drift,
while neither module touches sqlparser types.

## Testing

Integration tests are one binary: `tests/integration/main.rs`, one module per
feature area. Shared helpers live in `tests/integration/common/mod.rs`:
`cmd()` (the binary under test), `fixture(name)` (checked-in fixture paths),
and `LogBuilder` (synthesizes a Delta log in a temp dir: arbitrary file
counts, partition layouts, stats gaps, protocol reader/writer features).

- **Placement rule**: `cli` holds CLI-surface tests only (arguments, exit
  codes, output shape). The semantics of a feature go in that feature's
  module (`nested_stats`, `checkpoint_stats`, `temporal_coercions`, ...);
  new feature areas get a new module, declared in `main.rs`.
- **Fixture rule**: prefer `LogBuilder` over a new checked-in fixture; check
  a fixture in only when the log shape cannot be synthesized (checkpoint
  Parquet, real-writer output). `fixtures/README.md` is the registry: every
  table's layout, stats coverage, and consuming tests.
- `semantic_regression`: one canonical end-to-end JSON check per "minimum
  case" from the roadmap; grows one case per shipped feature.
- `synthetic_log`: LogBuilder-backed scenarios, including the 1000-file
  scale smoke.
- The differential harness (`examples/differential`, Spark ground truth) is
  the survivor-set oracle; it runs outside `cargo test` (see its README).
- **Use `rstest`** when several cases share the same flow and differ only in inputs
  or expected outputs. Prefer one parameterized test with `#[case]` over four near-duplicate
  `#[test]` functions. When parameters are independent (cartesian product), `#[values]`.
- **Substring assertions** (`predicate::str::contains`) over exact equality for text output.
  The text format will keep evolving, substring assertions stay green when we add
  cosmetic details.
- **JSON assertions** via `serde_json::Value` and path indexing: `json["analysis"]["confidence"]`.
  Don't assert on whole-document equality.
- Tests live in `tests/`; unit tests in `src/<module>.rs` under `#[cfg(test)] mod tests`.

### Fixtures

Real Delta tables (not synthetic blobs) live under `fixtures/`:

- `test-table`: partitioned by `country` (DE/US/IT), full stats. Canonical fixture.
- `test-table-flat`: non-partitioned variant, same schema (hand-crafted stats).
- `test-table-empty`: zero data files, partition metadata only.
- `test-table-partial-stats`: partitioned, half the files have `stats` stripped.
- `test-table-nested` / `test-table-stats-budget`: struct columns with per-leaf
  stats; the budget variant lowers `dataSkippingNumIndexedCols` to 4.
- `test-table-checkpointed` / `-struct` / `-part`: checkpoint-only logs (JSON
  commits removed): flat with JSON stats, flat with `stats_parsed` only
  (hand-rewritten checkpoint), and partitioned.
- `users` / `users-flat`: demo tables generated from the canonical data.

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

The body, when needed, focuses on the *why*, context, motivation, trade-offs, not
the diff (the diff is right there).

## Output Schema Stability

The JSON output is a **stable contract** from v0.2.0 onwards. It carries an
explicit `schema_version` (currently `"0.1.0"`) and changes follow SemVer
relative to that field, additive changes bump the minor, breaking changes
bump the major.

When introducing a JSON change, decide first whether it is additive (new
optional field) or breaking (rename/remove/restructure). Breaking changes
require a `schema_version` bump and a CHANGELOG entry under "Breaking".

## Roadmap & Milestones

- **FASE 0 (soft launch)**: shipped in v0.2.0 (May 2026), tied to a MERGE
  article that cites `delta-explain` as a "companion tool to make file
  elimination visible from metadata".
- **FASE 1 (MVP)**: confidence model, stable JSON schema, full e2e test
  matrix. Shipped together with FASE 0 as v0.2.0.
- Later phases (2–5) cover checkpoint Parquet support, real stats coverage,
  type coercions, finer CI assertions, predicate analysis improvements,
  diagnostics, and compare mode. See `DELTA-EXPLAIN-ROADMAP.md` for the
  canonical list and current status.
