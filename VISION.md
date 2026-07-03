# Vision

`delta-explain` is a metadata-level diagnostic tool for Delta Lake. It makes file elimination visible without executing queries.

This document outlines the planned evolution from the current prototype to a production-ready tool.

## Principles

These apply across all releases:

- **Not an optimizer.** No query planner simulation, no execution time prediction.
- **Metadata only.** Reads the transaction log and file statistics. Never touches data files.
- **Value comes from the semantic model, not from feature count.** Each release should deepen understanding of *why* pruning works or fails, not just count files.

## v0.1: Soft launch (shipped April 2026)

The tool works end-to-end for the common case: given a predicate and a Delta table, it shows how many files are eliminated by partition pruning and data skipping.

**What works:**
- Partition pruning and data skipping phases, reported separately
- SQL predicates: comparisons, AND/OR/NOT, IN, BETWEEN, IS [NOT] NULL
- Per-file verbose output showing kept/dropped with reason
- JSON output for programmatic consumption
- CI assertions (`--min-pruning`, `--assert-stats`)
- Local and cloud storage (S3, Azure, GCS)

## v0.2: Confidence and classification (shipped May 2026)

Released as the v0.2.0 → v0.2.3 series. The tool now explains *why* pruning worked or failed, not just the file counts.

- **Confidence model**: each result tagged as `exact`, `conservative`, or `incomplete` depending on stats completeness and predicate separability
- **Predicate classification**: each clause explicitly labeled as `partition_safe`, `stats_safe`, or `unsplittable`, with coded notes explaining why
- **Stable JSON schema** (v0.1.0): versioned, documented, with analysis/notes/assertions blocks
- **Expanded test fixtures**: tables with partial stats; OR-mixed predicates classified as `unsplittable` and covered by the existing canonical fixture
- **Distribution**: crates.io, Docker (multi-arch), pre-built binaries for six targets, `.deb` packages, Homebrew tap, Scoop bucket

The architecture and design rationale behind v0.2 are written up in detail in the companion deep-dive: [delta-explain: Making Delta Lake Pruning Visible](https://cdelmonte.dev/deep-dives/delta-explain-making-delta-pruning-visible/).

## v0.3: Checkpoint support and type hardening (shipped July 2026)

Works reliably on production tables that have been checkpointed and vacuumed. The whole analysis rides on the kernel's log replay, which reads checkpoint Parquet natively: one source of truth for counts, per-file statistics, and assertions.

- **Checkpoint coverage**: per-file statistics come from the stats payload the kernel carries on each scan row, JSON commits and checkpoint Parquet alike, in both layouts (`add.stats` JSON and structured `stats_parsed`). No more `[no stats]` on long-lived tables, no more `--assert-stats` false positives.
- **Partition columns on checkpoint-only logs**: the `metaData` action in the JSON commits is the primary source; when log cleanup has removed it, partition columns fall back to the kernel-replayed `partitionValues` keys, preserving the two-phase attribution.
- **delta-kernel-rs 0.24**, `object_store` 0.13.
- **Internal architecture**: lib/bin split; `scan`, `attribution`, `gates`, `render`, and `error` modules; the attribution arithmetic and the CI gates are pure and unit-tested.

Shipped in the same v0.3.0 release: type hardening and the production sprint. Date and timestamp coercion was the gate to real tables: most production tables are partitioned or clustered by date, and a predicate that cannot be typed cannot prune.

- **Type coercions**: `DATE`, `TIMESTAMP` (UTC-normalized), `TIMESTAMP_NTZ` (wall-clock, offsets rejected as ambiguous), `DECIMAL` (exact scale), and narrow integers, resolved against the Delta schema; `DATE '...'` / `TIMESTAMP '...'` literal forms accepted
- **Time travel**: `--at-version <N>`
- Alongside: `--profile` (AWS shared config), the composite GitHub Action, and the first differential harness

## v0.4: Predicate analysis, production tables, the public contract (on main, next release)

Everything below is merged and tested on main; v0.4.0 ships it as one release. Goal: reduce false negatives for common patterns without becoming an optimizer, be honest on tables that look like production, and make the contract externally verifiable. The substrate comes first: an owned minimal predicate AST, produced by a small converter from sqlparser (one parse, two interpreters: kernel lowering and classification). Owning SQL lexing forever is the wrong trade, so sqlparser stays as the front end; the converter is the only module coupled to it, and everything outside the pruning language collapses into an explainable `Unsupported` leaf at that boundary. The rewrites below operate on the owned AST.

- **Light normalization**: flatten nested ANDs, push negations down to the leaves (De Morgan, three-valued-logic safe)
- **OR factoring**: factor conjuncts common to every OR branch out of the OR, so `(col = 'A' AND x) OR (col = 'A' AND y)` exposes `col = 'A'` as a partition-safe top-level conjunct (a single-column OR like `col = 'A' OR col = 'B'` already classifies as partition-safe)
- **Unsplittable explanations**: for each unsplittable fragment, explain *why* it couldn't be classified (mixed columns, function calls, etc.), and degrade unsupported expressions to a conservative keep-all with a diagnostic warning instead of a fatal error

That is the complexity ceiling for the predicate analyzer: anything beyond crosses into optimizer territory. Also in v0.4: `IS [NOT] DISTINCT FROM` (the kernel's native null-safe comparison), and NULL literals typed from the column.

Production-table honesty (formerly the v0.5 plan, delivered in the same cycle):

- **Detect and declare protocol features**: deletion vectors (record counts overcount and the report says so), column mapping (physical vs logical names), liquid clustering. Declared in a `table_features` JSON block with warnings; the numbers are never silently compensated.
- **Per-file machine-readable output**: `--verbose --format json` emits a `files[]` array (`kept`, `pruned_by`, sizes, partition values), capped by `--limit`; the differential harness consumes it instead of scraping text.
- **Scale, measured**: 200k files in ~1.6 s / ~320 MB, linear; numbers and the ~1M-file memory ceiling documented in the README.
- **Exotic log shapes, proven**: log compaction, classic multi-part checkpoints, and V2 UUID-named checkpoints with sidecars are covered by the test matrix (the kernel handled them; now that is a guarantee, not an assumption).

And the public contract, stabilized:

- **`docs/semantics.md`**: what the tool guarantees, what it deliberately does not do, degradation rules, exit codes.
- **A formal JSON Schema** (`schemas/report-v0.2.schema.json`) validated by the integration suite against every emitted document shape, plus the field-by-field reference in `docs/json-schema.md`.
- **`RELEASE.md`** and a repeatable three-minute quickstart (`examples/quickstart/`).

### Road to 0.4.0

The release ships when the remaining professionalization items land, in this order: a "validated against" story (what real tables and writers the tool is exercised on, and the honest gap: Databricks/Unity-Catalog managed features such as coordinated commits and in-commit timestamps are not yet detected), CI test jobs on macOS and Windows (binaries for those targets are shipped today but tested only on Linux), reproducible benchmark tooling, and supply-chain checks (dependabot, cargo-audit).

**After 0.4.0 the plan is deliberate: a stabilization period of bug fixing and validation only, no new surface.** The roadmap below resumes after that.

## v0.5: Diagnostic layer (planned)

Goal: shift from "file counter" to "pruning advisor".

- **Diagnostic notes**: messages like "partition pruning unavailable because predicate does not reference partition columns" or "data skipping weak because string min/max ranges are wide"
- **`--explain-why` mode**: synthesized output: what enabled pruning, what blocked it, what would improve it
- **`--engine-profile`**: emulate a specific engine's pruning strength (`max` | `datafusion` | `spark` | `kernel`) so a CI gate asserts what *your* engine will do, not the metadata's theoretical best. The known divergences (IN-list strategies) are documented in the README today; this makes them selectable.

## Adoption track

- **AWS shared-config profiles**: shipped in v0.3.0 (`--profile`; SSO and `credential_process` produce an actionable error pointing at `aws configure export-credentials`)
- **GitHub Action**: shipped in v0.3.0 as a composite action at the repo root, one-line `uses:` with CLI-mirroring inputs and step outputs
- Next: full SSO resolution if demand shows up, and richer Action outputs

## Trust track

- **Differential testing**: the same predicates over the same tables through a reference engine, asserting the survivor set covers every file with matching rows. The harness in `examples/differential` (MinIO + Spark 4.1) runs a thirteen-predicate matrix including normalized forms (De Morgan, factored ORs) and null-safe comparisons: sound on all, exact on that layout. It now consumes the JSON `files[]` contract instead of scraping text. Next: a scheduled CI job, more layouts, and the "validated against" documentation.

## Ongoing: the kernel track

delta-explain builds on delta-kernel-rs, so some improvements arrive by adopting a newer kernel rather than writing code here:

- **Void schema type**: tables containing a `void` column are unreadable today; support landed on kernel main after 0.24 and ships with the next release.
- **thrift advisory (issue #9)**: drops out of the dependency tree once a kernel release moves to parquet 59+.
- **Public partition-columns accessor**: if the kernel exposes one, it replaces the checkpoint-only fallback and also covers the empty-table edge.
- **Public accessor for system metadata domains**: `delta.clustering` is read from the JSON commits today because the kernel rejects system domains on its public API; an accessor would also close the checkpoint-only clustering blind spot.
- **Data skipping on the kernel's binary `In`**: today it evaluates conservatively; if a kernel release adds skipping, our IN-as-OR expansion becomes an optimization choice instead of a necessity.

## Future: Compare mode

- Same predicate across two tables (flat vs partitioned, before vs after compaction)
- Side-by-side output with delta highlighted

## Future: Library surface

The 0.3 refactor split the crate into a library and a thin CLI; the analysis (scan, attribution, gates) is already callable as Rust. The natural next step, if demand shows up, is a small stable facade and Python bindings: a pruning check as a function call in a notebook or a pytest fixture, instead of a subprocess. The CLI and the JSON schema stay the primary contracts until then.
