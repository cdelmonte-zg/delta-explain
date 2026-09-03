# Development guide

This document is the contributor-facing map of the codebase: how to build and
test, how the modules fit together, and the conventions changes are expected to
follow.

## Project overview

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

- `VISION.md`: the public roadmap
- `docs/semantics.md`: the public contract (guarantees, degradation rules,
  exit codes); `schemas/report-v0.5.schema.json` is the formal JSON contract,
  enforced by `tests/integration/json_contract.rs`
- `docs/adr/`: architecture decision records, the *why* behind module
  boundaries and contracts. Its README states the bar a decision must meet
  to earn one (crosses module boundaries or constrains future work, AND
  rejected a plausible alternative); architectural changes that meet it
  should land with their ADR

## Build & test commands

```bash
# Build
cargo build

# Run all tests
cargo test

# Run a single test by name
cargo test test_name_here

# Format, lint, and run tests (pre-commit check)
cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test

# Python wrapper tests (pytest comes from python/requirements-dev.txt; the
# repo .venv has it. DX_BIN defaults to target/release - point it at the
# debug build after a plain `cargo build`)
DX_BIN=$PWD/target/debug/delta-explain .venv/bin/python -m pytest python/tests/
```

No workspace, no nextest. One Cargo feature exists: `debug-ir`, an internal
developer diagnostic. It is off by default, never in release artifacts, and
not part of the public CLI surface; its output format is unstable by design.
`cargo test --features debug-ir` includes its test module, and CI runs one
leg with the feature enabled.

One package, two targets: the
`delta_explain` library (analysis machinery) and the `delta-explain` binary
(CLI layer). The Rust API is internal and unstable; the stable contracts are
the CLI surface and the versioned JSON output schema.

There is also a Python package (`pip install delta-explain`): maturin `bin`
bindings ship the compiled binary inside platform wheels, and
`python/delta_explain/` is a pure-Python wrapper that shells out to it and
returns the JSON as a `Report`. It is a client of the CLI+schema contract,
never a second API. Its tests (`python/tests/test_wrapper.py`) run against
the real binary in every CI matrix leg, and a `wheel-smoke` CI job proves
the full pip contract (maturin build, clean-venv install, bundled-binary
discovery). Version is single-sourced from `Cargo.toml`.

CI beyond the per-PR pipeline: `validation.yml` runs weekly and on
dispatch - the Spark differential harness over MinIO plus an Azurite
end-to-end smoke of the az:// path. Releases publish wheels to PyPI via
trusted publishing (environment `pypi`).

## Architecture

The pipeline is a chain of layers, each a directory (or single file) under
`src/`, consumed top-down by the CLI:

```
CLI (main.rs)
 -> storage       object-store + kernel engine construction
 -> table         snapshot open: log metadata, baseline scan, features
 -> execution     orchestrator: analysis + gates + report
      -> analysis    the predicate pipeline (parse, classify, prune)
      -> gates       CI assertions over the finished analysis
      -> report      the computed model
 -> presentation  report + gates -> presentable model -> text/JSON
```

- **`main.rs`**: the CLI layer and nothing else: parse args, `storage::open`,
  `table::open`, `execution::execute`, `presentation::build`, render, exit
  code (gate failures map to a nonzero exit; broken pipes exit clean).
- **`table_uri.rs`**: path or URL -> the table `Url` handed to storage and
  the kernel.
- **`storage/`**: resolves the object store and builds the kernel engine.
  `options.rs` merges the credential sources into `KEY=VALUE` options with
  explicit precedence (`--option` wins), `environment.rs` maps `--env-creds`
  environment variables, `aws_profile.rs` resolves static AWS profile
  credentials (SSO and `credential_process` produce an actionable error).
- **`table.rs`**: opens the snapshot into a `TableState`: reads the JSON log
  metadata, refuses catalog-managed tables (their truth lives outside the
  filesystem log), runs the baseline scan, resolves partition columns
  (log `metaData` primary, scan fallback for checkpoint-only logs), and
  detects protocol features.
- **`metadata/`**: what the Delta log says about the table. `log.rs` reads
  the JSON commits (`partitionColumns`, reader/writer features, the
  `delta.clustering` domain; the kernel exposes no public accessor for
  system domains). `scan.rs` is the kernel-backed baseline: one
  `scan_metadata` pass collecting the file listing and per-file stats
  (checkpoint Parquet included). `stats.rs` owns `FileStats`, stats-JSON
  parsing and nested flattening to dotted leaf keys. `features.rs` is
  detect-and-declare for features that distort or reframe the numbers
  (deletion vectors, column mapping, liquid clustering); it feeds the JSON
  `table_features` block and table-level warnings, never how pruning is
  computed.
- **`analysis/`**: the predicate pipeline, from SQL string to survivor sets.
  - `predicate/`: the owned `Pred` vocabulary (ADR 0001, ADR 0008).
    `parser.rs` is the only code coupled to sqlparser types; the converter
    is total, anything outside the vocabulary becomes an `Unsupported` leaf
    carrying the raw fragment and a reason. `normalize.rs` holds the
    rewrites (De Morgan, the string-gated prefix-LIKE range rewrite, OR
    factoring); `display.rs` renders `Pred` back to SQL-shaped text.
  - `predicate_analyzer.rs`: top-level AND split and classification of each
    fragment as `partition_safe` / `partition_exact` / `stats_safe` /
    `unsplittable`, keeping the classified subtrees so nothing re-parses.
  - `kernel/`: explicit lowering of already-classified `Pred` fragments to
    `delta_kernel::expressions::Predicate` (ADR 0008: the kernel vocabulary
    is a lowering target, not a semantic input), with schema-driven literal
    coercion in `literal.rs` (temporal, decimal, narrow ints, nested leaf
    types; `PrimitiveType` stays exhaustively matched with no catch-all).
  - `partition_eval.rs`: the third interpreter (ADR 0006). Evaluates a
    `Pred` against a file's literal partition values under a four-valued
    logic: SQL `Null` drops exactly, evaluator ignorance (`Unknown`) keeps
    conservatively. Its vocabulary is `Pred` minus `Unsupported`; growing
    it means growing the AST first, never a second parser.
  - `partition_pruning.rs` / `scan_pruning.rs`: the per-phase survivor
    computations (partition-literal evaluation and kernel predicate scans);
    `attribution.rs` turns survivor sets into chained, labeled phases and
    owns the phase-name constants; `confidence.rs` derives the reported
    confidence; `model.rs` is the `AnalysisResult` the rest consumes.
- **`execution.rs`**: the orchestrator: run the analysis (if a predicate was
  given), evaluate the gates over it, assemble the report. No I/O of its
  own, no presentation.
- **`gates/`**: `--min-pruning` / `--assert-stats` -> assertion records and
  the overall result. One module per gate over a shared `context.rs`;
  adding a gate is additive. Pure.
- **`report.rs`**: the computed model the presentation layer consumes.
- **`diagnostics/`**: `warnings.rs` derives the warning records with their
  stable codes (`PARTITION_EVAL_GAP`, ...) from analysis and table state;
  `explain.rs` is `--explain-why` (ADR 0007), a deterministic rules engine
  producing `Diagnosis` records with stable codes; no ML, nothing
  predicted. Pure.
- **`presentation/`**: everything shown to a user. `build.rs` folds report,
  gates and baseline into the presentation model (`model.rs`); `files.rs`
  handles per-file detail behind `--verbose` and `--limit`; `render/`
  holds the two renderers, `text.rs` and `json.rs` (the JSON carries
  `schema_version`).
- **`instrumentation/`**: the observer seam. `observer.rs` defines the
  `Instrumentation` trait the pipeline reports into (`NoOpInstrumentation`
  by default); `debug_ir.rs` (behind the `debug-ir` feature) implements it
  as the `--debug-ir` dump. The pipeline never knows whether anyone is
  listening.
- **`error.rs`**: the crate error enum (thiserror); kernel errors pass
  through transparently.

Why the AST sits between sqlparser and everything else: one parse, three
interpreters. `analysis/kernel` lowers to the type the kernel consumes; the
analyzer produces the metadata we report to the user; `partition_eval`
decides what the partition literals decide. All read the same owned `Pred`,
so "what the kernel sees", "what the user reads" and "what gets evaluated"
can never drift, while no module but the converter touches sqlparser types.
The owned vocabulary is also the capability boundary (ADR 0008): a kernel
release that widens its predicate enums cannot silently expand the pruning
language, because nothing reaches the kernel that the analysis did not
explicitly classify and lower.

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

## Code style

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

## Pull requests / commit messages

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

## Output schema stability

The JSON output is a **stable contract** from v0.2.0 onwards. It carries an
explicit `schema_version` and changes follow SemVer relative to that field:
additive changes bump the minor, breaking changes bump the major.

When introducing a JSON change, decide first whether it is additive (new
optional field) or breaking (rename/remove/restructure). Breaking changes
require a `schema_version` bump and a CHANGELOG entry under "Breaking".

## Roadmap

`VISION.md` is the public roadmap; `docs/semantics.md` states what is
guaranteed today. When a milestone ships, both move together with the
CHANGELOG entry.
