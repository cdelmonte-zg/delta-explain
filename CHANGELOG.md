# Changelog

All notable changes to `delta-explain` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and from v0.2.0 onwards the project follows [Semantic Versioning](https://semver.org/).
The JSON output carries an explicit `schema_version` field whose changes also
follow SemVer relative to that field.

## [0.2.0] — 2026-05-08

This release closes the FASE 0 / FASE 1 set in the roadmap and freezes the
JSON output as a stable contract. It is **breaking** for anyone parsing the
previous JSON shape.

### Breaking

- **JSON schema v0.1.0**. The output now carries top-level `schema_version`
  (`"0.1.0"`), `tool_version`, `elapsed_ms`, an `analysis` block, a `stats`
  block (with categorical `mode` ∈ {exact, partial, absent}), an
  `assertions` array, and a `result` field (`pass` / `fail` / `null`).
- `stats_coverage` is gone — replaced by `stats`, which includes the new
  `mode` field.
- `phases[].files[]` (the per-file detail array) has been removed from the
  JSON output. Per-file information remains available in the text output
  via `--verbose`. The stable JSON schema is summary-only.
- Assertions are now evaluated *before* the JSON is printed, so the
  document always reflects their outcome and the top-level `result`.

### Added

- **Predicate analyzer** (`src/predicate_analyzer.rs`). Splits top-level AND
  predicates into `partition_safe`, `stats_safe`, and `unsplittable`
  fragments and emits coded notes (e.g. `UNSPLITTABLE_OR`) when fragments
  cannot be separated.
- **Confidence model**. Each run reports an overall `confidence`
  (`exact` / `conservative` / `incomplete`) plus a per-phase confidence
  tag, derived from which buckets the predicate populated.
- **Predicate Analysis** section in the text output, plus a `Warnings!`
  section listing analyzer notes by code.
- **`test-table-partial-stats` fixture** for exercising the
  `stats.mode = "partial"` code path (4 files; 2 with stats, 2 without).
- **Semantic regression suite** (`tests/semantic_regression.rs`) — six
  canonical tests, one per minimum case in the roadmap.
- `CLAUDE.md` documenting project conventions for contributors and AI
  assistants.

### Fixed

- `stats::read_stats_async` was inserting an empty `FileStats` for every
  Add action regardless of whether the action carried a `stats` field, so
  `stats_coverage` reported every file as having stats. Files without
  log-level stats are now skipped at read time.

## [0.1.1] — 2026-04-13

### Fixed

- `partitionColumns` are now read from the Delta log `metaData` action
  instead of being inferred from file paths. Adds an empty-table fixture
  and 16 regression tests.
