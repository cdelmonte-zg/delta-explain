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

## v0.4: Smarter predicate analysis (planned)

Goal: reduce false negatives for common patterns, without becoming an optimizer. The substrate comes first: an owned minimal predicate AST, produced by a small converter from sqlparser (one parse, two interpreters: kernel lowering and classification). Owning SQL lexing forever is the wrong trade, so sqlparser stays as the front end; the converter is the only module coupled to it, and everything outside the pruning language collapses into an explainable `Unsupported` leaf at that boundary. The rewrites below operate on the owned AST.

- **Light normalization**: flatten nested ANDs, push negations down to the leaves (De Morgan, three-valued-logic safe)
- **OR factoring**: factor conjuncts common to every OR branch out of the OR, so `(col = 'A' AND x) OR (col = 'A' AND y)` exposes `col = 'A'` as a partition-safe top-level conjunct (a single-column OR like `col = 'A' OR col = 'B'` already classifies as partition-safe)
- **Unsplittable explanations**: for each unsplittable fragment, explain *why* it couldn't be classified (mixed columns, function calls, etc.), and degrade unsupported expressions to a conservative keep-all with a diagnostic warning instead of a fatal error

This is the complexity ceiling for the predicate analyzer. Anything beyond this crosses into optimizer territory.

## v0.5: Production tables (planned)

Goal: honest and usable on tables that look like production, not like fixtures.

- **Detect and declare protocol features**: deletion vectors (today record counts silently overcount), column mapping (logical vs physical names can silently void data skipping), liquid clustering (`clusteringProvider`). First detect and warn, then support. A tool that says "I cannot attribute this correctly" is credible; one that prints wrong numbers is not.
- **Scale**: a large synthetic fixture (tens of thousands of files), time and memory benchmarks in the README, and output that survives large tables: `--limit`, a summary mode, top surviving files by size.
- **Time travel**: `--at-version <N>` via the kernel snapshot builder; enables before/after OPTIMIZE comparisons and feeds Compare mode. Shipped ahead of schedule in the pre-webinar sprint.
- **Exotic log shapes**: multi-part and V2/UUID-named checkpoints, log compaction. The kernel handles them; the test matrix should prove delta-explain does too.

## v0.6: Diagnostic layer (planned)

Goal: shift from "file counter" to "pruning advisor".

- **Diagnostic notes**: messages like "partition pruning unavailable because predicate does not reference partition columns" or "data skipping weak because string min/max ranges are wide"
- **`--explain-why` mode**: synthesized output: what enabled pruning, what blocked it, what would improve it

## Adoption track

- **AWS shared-config profiles**: shipped in v0.3.0 (`--profile`; SSO and `credential_process` produce an actionable error pointing at `aws configure export-credentials`)
- **GitHub Action**: shipped in v0.3.0 as a composite action at the repo root, one-line `uses:` with CLI-mirroring inputs and step outputs
- Next: full SSO resolution if demand shows up, and richer Action outputs

## Trust track

- **Differential testing**: the same predicates over the same tables through a reference engine, asserting the survivor set covers every file with matching rows. First harness shipped in `examples/differential` (MinIO + Spark 4.1): sound, and on that layout exact, across a ten-predicate matrix. Next: more layouts, more types, a scheduled CI job.

## Ongoing: the kernel track

delta-explain builds on delta-kernel-rs, so some improvements arrive by adopting a newer kernel rather than writing code here:

- **Void schema type**: tables containing a `void` column are unreadable today; support landed on kernel main after 0.24 and ships with the next release.
- **thrift advisory (issue #9)**: drops out of the dependency tree once a kernel release moves to parquet 59+.
- **Public partition-columns accessor**: if the kernel exposes one, it replaces the checkpoint-only fallback and also covers the empty-table edge.

## Future: Compare mode

- Same predicate across two tables (flat vs partitioned, before vs after compaction)
- Side-by-side output with delta highlighted

## Future: Library surface

The 0.3 refactor split the crate into a library and a thin CLI; the analysis (scan, attribution, gates) is already callable as Rust. The natural next step, if demand shows up, is a small stable facade and Python bindings: a pruning check as a function call in a notebook or a pytest fixture, instead of a subprocess. The CLI and the JSON schema stay the primary contracts until then.
