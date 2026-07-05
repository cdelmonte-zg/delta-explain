# Changelog

All notable changes to `delta-explain` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and from v0.2.0 onwards the project follows [Semantic Versioning](https://semver.org/).
The JSON output carries an explicit `schema_version` field whose changes also
follow SemVer relative to that field.

## [Unreleased]

### Added

- Prefix `LIKE` patterns now prune instead of degrading (#72):
  normalization rewrites `col LIKE 'abc%'` into the lexicographic range
  `col >= 'abc' AND col < 'abd'` (and a wildcard-free pattern into plain
  equality), so the fragment classifies and prunes through the ordinary
  column rules on both the partition and the stats axis. The rewrite is
  exact under SQL three-valued semantics over binary code-point order.
  Non-prefix patterns, `NOT LIKE`, and `ESCAPE` forms keep the
  conservative degradation path. No JSON schema change: the fragment
  strings simply show the rewritten form.

### Fixed

- A consumer closing stdout mid-report (`delta-explain ... | head`, a
  crashed `jq`) made the process panic with a backtrace (exit 101),
  violating the error contract. The write error is now handled: output
  stops, stderr stays clean, and the exit code still reflects the gate
  verdict. Text rendering moved to fallible writes and JSON rendering is
  now separated from printing along the way.

## [0.4.0] — 2026-07-03

This cycle added no single big feature; it gave the tool a public contract,
and then verified it. The predicate pipeline was rebuilt on an owned AST
with normalization and diagnostic degradation; protocol features are
detected and declared instead of silently distorting numbers; the JSON
output is now a formal, CI-enforced schema (`schema_version` 0.1.0 ->
0.2.0, additive); the semantics, the validation story, and the release
process are written down; scale is measured and reproducible; the test
suite runs on Linux, macOS, and Windows with dependency audits; a weekly
Validation workflow runs the Spark differential oracle and an Azure
emulator smoke; and `pip install delta-explain` ships the CLI inside
platform wheels with a thin Python API. Real-cloud verification on AWS,
GCS, and Azure found and fixed two release-blocking bugs before any user
could (`az://` paths and `--env-creds` were both broken); the negative-path
and edge-case sweeps found three more. Positioning, meant literally:
production-usable as a conservative Delta metadata diagnostic and CI
guardrail, not yet a fully production-grade general-purpose Delta
observability product.

### Added

- **`pip install delta-explain`.** Platform wheels ship the compiled CLI
  binary plus a thin Python module wrapping the JSON contract: `explain()`
  returns a `Report` (mapping plus typed accessors), gate failures come
  back as `passed == False`, runtime errors raise `DeltaExplainError` with
  the CLI's message - the exit-code contract of docs/semantics.md, in
  Python types. No Rust toolchain involved; the module is a client of the
  CLI-and-schema contract, not a second API surface.
- **Scheduled validation workflow.** The checks too slow for per-PR CI now
  run weekly and on demand: the Spark differential harness in fresh mode,
  and an Azurite end-to-end smoke of the az:// path (the automated version
  of the manual check that caught the az:// regression).

### Fixed

- **Azure tables work again.** The direct log reader derived its path by
  re-parsing the table URL into a second object store, which on `az://`
  requires the account credentials it does not have: every Azure run
  failed with "Account must be specified". The prefix now derives from
  the URL path alone (the real store, already scoped and authenticated,
  was in hand the whole time). Verified end to end against Azurite,
  including gates, normalization, and per-file JSON; S3 (differential
  harness) and real GCS re-verified unaffected.
- **`--env-creds` reads the environment now.** The flag passed an
  `allow_env` option that no layer ever consumed: object_store's parse
  path silently drops unknown keys and never consults the environment,
  so the flag was a no-op that fell through to the instance-metadata
  chain - accidentally working on EC2, hanging toward IMDS anywhere
  else. The well-known variables (AWS key/secret/session token/region/
  endpoint, Azure account name/key, Google service-account paths) are
  now read by the CLI itself and injected as explicit store options,
  layered under `--profile`, flags, and `--option`. This also makes the
  GitHub Action's `env-creds` input work outside AWS runners with
  instance roles. Verified against real S3 (both auth chains, gates and
  diagnostics).

### Added

- **The managed-table boundary, declared.** Catalog-managed tables
  (`catalogManaged` / `catalogOwned-preview` reader features) now fail
  before the kernel scan with a plain explanation: their latest commits
  live in the catalog, not the filesystem log, so a filesystem-only
  analysis cannot be trusted. In-commit timestamps are declared in
  `table_features.in_commit_timestamps` (no warning: nothing the tool
  reports depends on them), and writer features outside the known-benign
  set earn an `UNRECOGNIZED_TABLE_FEATURE` warning listing them, so a
  table from a newer writer is flagged instead of silently half-read.
  Additive schema change within 0.2.0 (two new `table_features` fields).

- **The public contract, written down and enforced.** `docs/semantics.md`
  states what delta-explain guarantees (conservative survivor sets,
  degradation with diagnostics, confidence meaning, exit-code table) and
  what it does not do (no planner simulation, no runtime prediction, no
  feature compensation). `schemas/report-v0.2.schema.json` is a formal
  JSON Schema of the report, strict on purpose, and the integration suite
  validates every emitted document shape against it; `docs/json-schema.md`
  explains each field, the verbose-only fields, and the stable note codes.
  The README gains a documentation pointer and a measured "Performance
  notes" section (200k files: ~1.6 s, ~320 MB, linear).
- **Exotic log shapes proven.** The test matrix now covers log compaction
  (synthesized `<start>.<end>.compacted.json` files: no double counting,
  identical pruning, time travel through the compacted range), classic
  multi-part checkpoints, and V2 UUID-named checkpoints with parquet
  sidecars (two new Spark-written fixtures,
  `fixtures/create_exotic_checkpoints.py` regenerates them). The kernel
  handled all of them already; the matrix makes that a tested guarantee
  instead of an assumption.

- **Detect-and-declare for protocol features.** The report now declares the
  table features that distort or reframe its numbers, without changing how
  pruning is computed: deletion vectors (enabled flag and count of files
  actually carrying vectors; when present, a `DELETION_VECTORS` warning says
  record counts include soft-deleted rows), column mapping
  (`COLUMN_MAPPING`: the log stores physical column names, verbose
  statistics may display them), and liquid clustering (`LIQUID_CLUSTERING`
  with the clustering columns, read from the `delta.clustering` domain;
  delta-kernel 0.24 exposes no public accessor for system domains, so on a
  fully checkpointed log clustering goes undetected, the same blind spot as
  partition columns). JSON gains an additive `table_features` block; table
  warnings share the text "Warnings!" section with analyzer notes and now
  print even when no predicate is given.

- **Per-file detail in JSON (`--verbose`) and `--limit`.** With `--verbose`,
  the JSON document gains a `files` array (per file: `path`, `size_bytes`,
  `partition_values`, `num_records`, `has_stats`, `kept`, and `pruned_by`,
  the phase that eliminated it) plus a `files_truncated` flag; `--limit N`
  caps per-file listings in both the JSON array and the text phase
  listings, with a `... and N more files` tail note. Without `--verbose`
  no `files` array is emitted and the existing summary fields are
  unchanged, while `schema_version` moves to `0.2.0` because the verbose
  schema gained additive fields. This is what makes the output usable on
  large tables (a 200k-file verbose listing is 25 MB of text) and gives
  machine consumers the survivor set without parsing text: the differential
  harness now reads `files[].kept` instead of scraping `[KEPT]` lines.

- **Predicate normalization.** Predicates are normalized before
  classification: negations push down to the leaves (De Morgan,
  three-valued-logic safe), and conjuncts common to every OR branch factor
  out of the OR. `NOT (country = 'DE' OR age > 30)` now splits into a
  partition-pruning phase and a data-skipping phase instead of being one
  unsplittable fragment, and
  `(country = 'DE' AND x) OR (country = 'DE' AND y)` exposes
  `country = 'DE'` as partition-safe. Rewrites preserve SQL semantics, so
  survivor sets do not change; what improves is attribution and confidence.
- **`IS [NOT] DISTINCT FROM` (null-safe comparison).** Maps to the kernel's
  native `Distinct` predicate, which is evaluated exactly over partition
  values; file statistics cannot prune on it, so on data columns it keeps
  files conservatively. This closes the last gap between the SQL surface
  and the predicate language of delta-kernel 0.24.
- **Diagnostic degradation for unsupported expressions.** Constructs outside
  the pruning language (function calls, arithmetic, LIKE, subqueries,
  column-to-column comparisons) no longer abort the command. The fragment is
  reported as unsplittable with an `UNSUPPORTED_EXPRESSION` warning
  explaining why, it conservatively keeps all files, and the remaining
  conjuncts still prune. Malformed SQL still fails. JSON change is additive
  (a new note code), `schema_version` unchanged.

### Fixed

- **`--format` rejects unknown values.** A typo like `--format jsn` silently
  fell back to text output; in a pipeline that means jq parsing a text
  report. It is now a usage error listing the accepted values.
- **`--min-pruning` without `--where` is a usage error.** It previously ran
  and failed as "total pruning 0.0% is below threshold", hiding the actual
  mistake; the help text always documented the requirement, now the parser
  enforces it.
- **Stats payloads that are valid JSON but not an object count as missing.**
  A stats blob like `"some string"` or `[1,2]` produced a present-but-empty
  entry: the file passed `--assert-stats` and `stats.mode` stayed `exact`
  while contributing nothing to skipping. Such payloads now count as missing
  statistics, like unparseable JSON already did.

### Changed

- The predicate pipeline is rebuilt around an owned minimal AST: one parse,
  two interpreters (kernel lowering with schema-driven coercion, and
  classification). Reported fragments now render from the normalized AST,
  so cosmetic differences can appear (`!=` prints as `<>`, flipped
  comparisons normalize to column-on-the-left, negations appear pushed
  down). Behavior parity was validated against the differential harness:
  survivor sets are identical to v0.3.0 on the full predicate matrix.

## [0.3.0] — 2026-07-02

One release for the whole cycle: the kernel-backed statistics pipeline plus
the production-readiness sprint. The JSON `schema_version` is unchanged
(`0.1.0`).

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
- **GitHub Action.** The repo doubles as a composite action:
  `uses: cdelmonte-zg/delta-explain@<ref>` installs a released binary and
  runs the gate with CLI-mirroring inputs, failing the job when an
  assertion fails and exposing `pruning-pct`, `final-files`, and `result`
  as step outputs. An action-smoke workflow exercises pass and fail paths
  against the committed fixtures.
- **Differential harness** (`examples/differential`): MinIO + Spark stack
  where Spark computes, per predicate, the files that actually contain
  matching rows, and the harness asserts delta-explain's survivor set covers
  them. Sound on the full ten-predicate matrix (equality, ranges, AND, the
  unsplittable OR, IN, BETWEEN, NOT, float bounds, empty result), and on
  the reference layout exact: every kept file contains a match.
- **`test-table-checkpointed` and `test-table-checkpointed-struct` fixtures**:
  three appends, a checkpoint at v2, every JSON commit removed — the shape
  `delta.logRetentionDuration` cleanup produces. The struct variant carries
  stats only as the `stats_parsed` column (hand-rewritten checkpoint, since
  deltalake always writes JSON stats). Integration tests lock in stats
  display, pruning, and `--assert-stats` behavior on both layouts.

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
