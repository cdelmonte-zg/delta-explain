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

## v0.3: Checkpoint support (shipped July 2026)

Works reliably on production tables that have been checkpointed and vacuumed. The whole analysis rides on the kernel's log replay, which reads checkpoint Parquet natively: one source of truth for counts, per-file statistics, and assertions.

- **Checkpoint coverage**: per-file statistics come from the stats payload the kernel carries on each scan row, JSON commits and checkpoint Parquet alike, in both layouts (`add.stats` JSON and structured `stats_parsed`). No more `[no stats]` on long-lived tables, no more `--assert-stats` false positives.
- **Partition columns on checkpoint-only logs**: the `metaData` action in the JSON commits is the primary source; when log cleanup has removed it, partition columns fall back to the kernel-replayed `partitionValues` keys, preserving the two-phase attribution.
- **delta-kernel-rs 0.24**, `object_store` 0.13.
- **Internal architecture**: lib/bin split; `scan`, `attribution`, `gates`, `render`, and `error` modules; the attribution arithmetic and the CI gates are pure and unit-tested.

## v0.4: Type hardening and smarter predicate analysis (planned)

Goal: reduce false negatives for common patterns, without becoming an optimizer.

- **Type coercions**: correct handling of decimal, date, timestamp, boolean, and narrow-integer comparisons against column types
- **Light normalization**: flatten nested ANDs, push down simple negations, simplify constant expressions, treat IN as OR-on-same-column
- **OR factoring on single column**: recognize `(col = 'A' OR col = 'B')` as partition-safe when `col` is a partition column
- **Unsplittable explanations**: for each unsplittable fragment, explain *why* it couldn't be classified (mixed columns, function calls, etc.)

This is the complexity ceiling for the predicate analyzer. Anything beyond this crosses into optimizer territory.

## v0.5: Diagnostic layer (planned)

Goal: shift from "file counter" to "pruning advisor".

- **Diagnostic notes**: messages like "partition pruning unavailable because predicate does not reference partition columns" or "data skipping weak because string min/max ranges are wide"
- **`--explain-why` mode**: synthesized output: what enabled pruning, what blocked it, what would improve it

## Future: Compare mode

- Same predicate across two tables (flat vs partitioned, before vs after compaction)
- Side-by-side output with delta highlighted
