# Changelog

All notable changes to `delta-explain` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and from v0.2.0 onwards the project follows [Semantic Versioning](https://semver.org/).
The JSON output carries an explicit `schema_version` field whose changes also
follow SemVer relative to that field.

## [Unreleased]

### Added

- **`--at-version <N>` (time travel).** Analyze the table at a historical
  version instead of the latest: the kernel replays the log only up to that
  version, so pruning is computed against the historical layout. Enables
  before/after comparisons around OPTIMIZE or a careless rewrite.
- **Temporal and narrow-type literal coercion.** Predicates on `DATE`,
  `TIMESTAMP`, `TIMESTAMP_NTZ`, `DECIMAL`, `SHORT`, and `BYTE` columns now
  resolve their literals against the Delta schema, so
  `event_date = '2026-07-02'` prunes date partitions instead of being kept
  conservatively as a string-vs-date mismatch. Quoted strings coerce by
  column type; the explicit `DATE '...'` / `TIMESTAMP '...'` forms are also
  accepted (for TIMESTAMP, RFC 3339 offsets normalize to UTC and bare dates
  mean midnight UTC; TIMESTAMP_NTZ is wall-clock and rejects offset-bearing
  literals as ambiguous). Decimal literals must fit the column scale; rounding a predicate
  bound silently would change its meaning. New `test-table-temporal`
  fixture: date-partitioned, with timestamp, decimal, and narrow-int
  per-file ranges.

- **`--profile <name>` (AWS shared config).** Resolves static credentials,
  session token, and region from `~/.aws/credentials` and `~/.aws/config`,
  the same files the AWS CLI reads. Closes the laptop gap where
  `--env-creds` fell through to instance-only providers. SSO,
  `credential_process`, and role-assumption profiles are not resolved; the
  error points at `aws configure export-credentials`.

## [0.3.0] — 2026-07-02

Per-file statistics now come from delta-kernel's log replay instead of a
direct read of the JSON commits. The JSON `schema_version` is unchanged
(`0.1.0`): the document shape is identical, but `stats` coverage now includes
files whose `add` action survives only inside checkpoint Parquet.

### Changed

- **Per-file statistics are sourced from the kernel scan.** The verbose view
  reads the `stats` payload the kernel carries on each scan row, produced by
  the same log replay that drives the pruning counts, checkpoint Parquet
  included. On long-lived tables after log cleanup, files referenced only by
  a checkpoint now show real min/max statistics instead of `[no stats]`.
  Both checkpoint layouts are covered: `add.stats` JSON and structured
  `stats_parsed` (`delta.checkpoint.writeStatsAsJson=false`); the scan
  requests the parsed stats schema so the kernel falls back to
  `COALESCE(add.stats, ToJson(add.stats_parsed))`. A malformed stats payload
  now counts as missing statistics instead of passing as an empty entry.
- **`--assert-stats` no longer false-positives on checkpointed logs.** A file
  is flagged only when its `add` action genuinely carries no statistics. On
  tables whose older commits were consolidated into a checkpoint, the
  assertion previously failed even though statistics existed; it now passes.
  If a pipeline relied on that failure as a proxy for "the log was
  checkpointed", it will now (correctly) pass.
- **delta-kernel-rs 0.20 → 0.24** (and `object_store` 0.12 → 0.13). No
  behavioral change beyond the stats sourcing above; the full test suite
  passes unchanged on the new kernel.

### Added

- **`test-table-checkpointed` and `test-table-checkpointed-struct` fixtures**:
  three appends, a checkpoint at v2, every JSON commit removed — the shape
  `delta.logRetentionDuration` cleanup produces. The struct variant carries
  stats only as the `stats_parsed` column (hand-rewritten checkpoint, since
  deltalake always writes JSON stats). Integration tests lock in stats
  display, pruning, and `--assert-stats` behavior on both layouts.

### Fixed

- **Partition columns on fully checkpointed logs.** Partition columns are
  identified from the `metaData` action in the JSON commits, which no longer
  exists after log cleanup consolidates everything into a checkpoint; the
  two-phase attribution collapsed into a single data-skipping phase (totals
  stayed correct). When no `metaData` is found, partition columns now fall
  back to the `partitionValues` keys the kernel replays out of the
  checkpoint: protocol data per `add` action, not an inference from paths.
  New `test-table-checkpointed-part` fixture locks the two-phase attribution
  in on a checkpoint-only partitioned log.

## [0.2.3] — 2026-06-20

Data skipping on nested (struct) columns addressed by dotted paths
(`profile.age`). The JSON `schema_version` is unchanged (`0.1.0`): per-column
statistics are surfaced only in verbose text output, so this is additive.

### Added

- **Nested stats display.** Per-file statistics for struct columns are now
  flattened to dotted leaf keys (`profile.age: 25..35, profile.score: 75.3..92`)
  in `--verbose` output, instead of the raw struct object blob. Works at any
  nesting depth (`profile.geo.zip`). New `test-table-nested` fixture exercises
  the path; `test-table-stats-budget` documents the ceiling — each nested leaf
  counts toward `dataSkippingNumIndexedCols`, so a wide struct can starve later
  columns of statistics.
- **Runnable cloud examples.** `examples/minio-s3/` and `examples/gcs/` provide
  end-to-end notebooks and Docker Compose stacks so contributors can reproduce
  the S3 and GCS paths locally without a cloud account.

### Fixed

- **Type coercion on nested columns.** A predicate comparing a nested
  double/long/float leaf to an integer literal (e.g. `profile.score > 90`) no
  longer aborts with `Invalid comparison operation: Float64 > Int32`; the
  literal now coerces to the nested leaf type resolved from the schema.
- **S3/GCS bucket-prefix resolution.** `delta-explain` now reads a Delta table
  at any bucket sub-prefix, not only at the bucket root. Previously, tables
  located under a prefix (e.g. `s3://my-bucket/warehouse/sales/`) were never
  resolved and the tool exited with a storage error. Fixes #3.

## [0.2.2] — 2026-05-09

This release adds binary distribution channels and aligns the README with
the companion deep-dive article. The binary itself is unchanged from v0.2.1;
the JSON `schema_version` remains `0.1.0`.

### Added

- **`--version` / `-V` flag** on the CLI. Reports the binary version
  (derived from `CARGO_PKG_VERSION` at build time). Useful for support
  reports and for verifying which release a CI runner picked up.
- **Pre-built binaries** on every tagged release for six targets: Linux
  x86_64 (glibc and musl), Linux aarch64, macOS x86_64 and aarch64,
  Windows x86_64. Each archive ships with an SHA256 checksum.
- **Debian/Ubuntu `.deb` packages** (`amd64` and `arm64`) built via
  `cargo-deb` and uploaded as Release assets. Install with
  `sudo dpkg -i delta-explain_<version>_<arch>.deb`.
- **Homebrew tap** at `cdelmonte-zg/homebrew-tap`. Install with
  `brew install cdelmonte-zg/tap/delta-explain`. The formula is
  regenerated automatically with fresh SHA256s on every tagged release.
- **Scoop bucket** at `cdelmonte-zg/scoop-bucket`. Install with
  `scoop bucket add cdelmonte-zg https://github.com/cdelmonte-zg/scoop-bucket && scoop install delta-explain`.
  Manifests are auto-updated by Scoop's Excavator workflow (every three hours).
- **MSRV declared**: `rust-version = "1.88"` in `Cargo.toml` (let-chains).
- `LICENSE` file (MIT) at the repository root, required by `cargo-deb`
  and other distribution channels.

### Documentation

- README rewritten for clarity and alignment with the deep-dive article:
  - Install section reorganised: Homebrew, Scoop, `.deb`, pre-built
    binaries listed alongside crates.io, Git, and Docker.
  - Current limitations rewritten as headline + paragraph. Corrected
    the previous slip on `stats.mode` (per-table coverage, not
    per-predicate reachability). Clarified that older JSON commits are
    removed by Delta's log cleanup (governed by
    `delta.logRetentionDuration`), independently from `VACUUM`.
  - "How it works" step 3 now scans the original predicate, covering
    the `unsplittable` case correctly. The analyzer's vocabulary is
    threaded through to the scan steps.
  - `conservative` confidence now mentions overlapping ranges, not
    just missing stats.
  - JSON output framed as pre-1.0 (additive minor, breaking major),
    not "stable contract from v0.2.0".
  - New `[!IMPORTANT]` callout in Cloud storage explaining that
    `--env-creds` does not currently pick up `AWS_*` env vars or
    `AWS_PROFILE` on a developer laptop. On EC2/ECS/EKS/GKE/AKS the
    default credential chain works as expected.

## [0.2.1] — 2026-05-08

### Documentation

- README refreshed for the v0.2.0 schema: the main example now shows the
  `Predicate Analysis` block and per-phase `[confidence]` tags, the JSON
  output section documents the v0.1.0 schema fields (`analysis`, `stats`,
  `assertions`, `result`, version metadata), and the OR-mixed limitation
  is rephrased to reflect that those predicates are now explicitly
  classified as `unsplittable` rather than silently downgraded.

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
