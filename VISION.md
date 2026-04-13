# Vision

`delta-explain` is a metadata-level diagnostic tool for Delta Lake. It makes file elimination visible without executing queries.

This document outlines the planned evolution from the current prototype to a production-ready tool.

## Principles

These apply across all releases:

- **Not an optimizer.** No query planner simulation, no execution time prediction.
- **Metadata only.** Reads the transaction log and file statistics. Never touches data files.
- **Value comes from the semantic model, not from feature count.** Each release should deepen understanding of *why* pruning works or fails, not just count files.

## v0.1 — Soft launch (current)

The tool works end-to-end for the common case: given a predicate and a Delta table, it shows how many files are eliminated by partition pruning and data skipping.

**What works:**
- Partition pruning and data skipping phases, reported separately
- SQL predicates: comparisons, AND/OR/NOT, IN, BETWEEN, IS [NOT] NULL
- Per-file verbose output showing kept/dropped with reason
- JSON output for programmatic consumption
- CI assertions (`--min-pruning`, `--assert-stats`)
- Local and cloud storage (S3, Azure, GCS)

**Known limitations:**
- Statistics from checkpoint Parquet files not yet supported
- No confidence model (the tool doesn't tell you how complete its analysis is)
- Predicate classification is implicit (no structured explanation of what went where)

## v0.2 — Confidence and classification

Goal: the tool explains *why* pruning worked or failed, not just the file counts.

- **Confidence model** — each result tagged as `exact`, `conservative`, or `incomplete` depending on stats completeness and predicate separability
- **Predicate classification** — each clause explicitly labeled as `partition_safe`, `stats_safe`, or `unsplittable`, with coded notes explaining why
- **Stable JSON schema** (v0.1.0) — versioned, documented, with analysis/notes/assertions blocks
- **Expanded test fixtures** — tables with partial stats, unsplittable OR predicates

## v0.3 — Checkpoint support and type hardening

Goal: work reliably on production tables that have been checkpointed and vacuumed.

- **Checkpoint Parquet support** — read statistics from `_last_checkpoint` and Parquet checkpoint files, not just JSON commits
- **Real stats coverage** — distinguish files without stats, files with partial stats, and predicate columns not covered by available statistics
- **Type coercions** — correct handling of decimal, date, timestamp, and boolean comparisons against column types

## v0.4 — Smarter predicate analysis

Goal: reduce false negatives for common patterns, without becoming an optimizer.

- **Light normalization** — flatten nested ANDs, push down simple negations, simplify constant expressions, treat IN as OR-on-same-column
- **OR factoring on single column** — recognize `(col = 'A' OR col = 'B')` as partition-safe when `col` is a partition column
- **Unsplittable explanations** — for each unsplittable fragment, explain *why* it couldn't be classified (mixed columns, function calls, etc.)

This is the complexity ceiling for the predicate analyzer. Anything beyond this crosses into optimizer territory.

## v0.5 — Diagnostic layer

Goal: shift from "file counter" to "pruning advisor".

- **Diagnostic notes** — messages like "partition pruning unavailable because predicate does not reference partition columns" or "data skipping weak because string min/max ranges are wide"
- **`--explain-why` mode** — synthesized output: what enabled pruning, what blocked it, what would improve it

## Future — Compare mode

- Same predicate across two tables (flat vs partitioned, before vs after compaction)
- Side-by-side output with delta highlighted
