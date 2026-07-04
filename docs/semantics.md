# Semantics: what delta-explain guarantees

This is the contract. Everything else in the documentation is commentary on
these statements.

> `delta-explain` reports metadata-level file pruning over a Delta snapshot.
>
> - It does **not** simulate a query planner.
> - It does **not** predict runtime.
> - It does **not** compensate for deletion vectors, column mapping, or
>   clustering: it detects and declares them.
> - It reports **conservative survivor sets**: a file any engine needs is
>   never reported as pruned.
> - When it cannot reason about a predicate fragment, it **keeps files and
>   emits a diagnostic**, never a silent wrong number.

## The pipeline

Every invocation follows the same linear sequence; there is no hidden
concurrency and no adaptive behavior:

1. **Baseline scan** — one kernel log replay (JSON commits + checkpoint
   Parquet) enumerates the snapshot's files and their statistics.
2. **Parse and normalize** — the `--where` predicate is parsed once into an
   owned AST, then normalized: negations push down to the leaves
   (De Morgan), and conjuncts common to every `OR` branch factor out of the
   `OR`. Both rewrites preserve SQL three-valued semantics, so they can
   change attribution and confidence but never the survivor set.
3. **Classify** — each top-level `AND` conjunct is routed to one bucket:
   `partition_safe` (references partition columns only), `stats_safe`
   (references no partition column), or `unsplittable` (mixes both, or
   contains an unsupported construct).
4. **Phase scans** — the partition-safe fragment runs as its own kernel
   scan (Phase 1); the full predicate, conservatively stripped of
   unsupported fragments, runs as the final scan.
5. **Attribution** — the survivor-set difference between phases is
   attributed: directory-level partition pruning first, min/max data
   skipping second.
6. **Gates and rendering** — `--min-pruning` / `--assert-stats` evaluate,
   and the report renders as text or JSON.

## Soundness

The one hard guarantee: **the survivor set is a superset of the files that
contain matching rows**. This is a contract verified on the current
support surface (the constructs and table shapes in the test matrix and
the differential harness), not a theorem about arbitrary future features:
when the tool meets something outside that surface, it degrades or refuses
loudly rather than extend the claim silently. Pruning may be less aggressive than an engine's
(and usually is not), but a pruned file never contains a match.

This is validated continuously by the differential harness
(`examples/differential`): Spark computes, per predicate, the files that
actually contain matching rows, and the harness asserts the survivor set
covers them, on a matrix that includes rewritten forms (`NOT` over mixed
`OR`, factored `OR`-of-`AND`s) and null-safe comparisons.

## Confidence

Confidence labels how precisely the elimination can be *explained*, never
whether it is correct (it always is, in the conservative direction):

- **exact** — the phase's elimination is precise: partition values are
  compared directly, every dropped file provably contains no match.
- **conservative** — sound but possibly loose: min/max ranges can overlap a
  predicate's bound without the file containing a matching row. Files may
  be kept in excess, never dropped in excess.
- **incomplete** — part of the predicate could not be attributed to a
  single phase (mixed `OR`, or an unsupported construct). The totals are
  still sound; what is lost is the clean attribution, and a diagnostic note
  says why.

The global `confidence` is the least informative label across fragments.

## Degradation rules

Constructs outside the pruning language — function calls, arithmetic,
`LIKE`, subqueries, column-to-column comparisons — do not abort the run:

- under a top-level `AND`, the unsupported fragment is dropped from the
  scan predicate (keeping more files, never fewer) and the sibling
  conjuncts still prune;
- an `OR` or `NOT` touching an unsupported fragment is dropped whole,
  because its truth value cannot be bounded;
- the fragment is reported as `unsplittable`, confidence degrades to
  `incomplete`, and an `UNSUPPORTED_EXPRESSION` note carries the reason.

These rules hold even when every column the fragment references is a
partition column. Partition values are exact literals per file, so an
engine can evaluate any deterministic predicate on them directly; this
report does not (one pruning language governs both phases), so it may
understate the partition-side opportunity for such fragments. Direct
evaluation over partition literals is a tracked extension (#75).

Malformed SQL is different: it is a user error and fails the run.

## Table features: declared, not compensated

Protocol features that distort or reframe the numbers are detected and
declared in `table_features`, with warnings, but the numbers are not
adjusted:

- **Deletion vectors** — record counts include soft-deleted rows on files
  that carry a vector (`DELETION_VECTORS` warns with the file count;
  enabled-but-unused stays silent because the numbers are correct).
- **Column mapping** — the log stores physical column names; verbose
  statistics may display them (`COLUMN_MAPPING`). Kernel pruning itself
  resolves the mapping.
- **Liquid clustering** — declared with the clustering columns
  (`LIQUID_CLUSTERING`); layout is managed by clustering, not directory
  partitions, and data skipping still applies. Undetectable on fully
  checkpointed logs (no public kernel accessor for system metadata
  domains).

## Engines may prune less than this report shows

The report reflects what the metadata makes possible, computed with the
strongest sound techniques in production use. Engines make different
choices; the known divergences are documented in the README's *Current
limitations* (notably `IN`-list strategies). The report never overstates
correctness — only, potentially, the pruning a specific engine will
realize.

## Exit codes and the error contract

| Condition | Exit code | stdout | stderr |
|---|---|---|---|
| Success (all gates pass or no gates) | 0 | report | empty |
| Assertion failure (`--min-pruning`, `--assert-stats`) | 1 | report (with `result: "fail"`) | `ASSERTION FAILED: ...` |
| Runtime error (bad predicate, unreadable table, storage) | 1 | **empty** | `Error: ...` |
| Usage error (unknown flag value, missing required flag) | 2 | empty | clap diagnostics |

Two properties are load-bearing for CI:

- **stdout is either a complete report or empty.** A failure never emits a
  partial document, so `delta-explain ... | jq ...` cannot parse garbage.
- **Panics are a bug by policy**, enforced by compiler lints
  (`clippy::unwrap_used` and friends are denied in production code): any
  defect must surface as exit 1 with a clean message, not exit 101 with a
  backtrace.
- **A consumer that stops reading does not crash the run.** When stdout
  closes mid-report (`delta-explain ... | head`, a crashed `jq`), the
  remaining output is skipped, stderr stays clean, and the exit code
  still reflects the gates.

## Scope: table protocol, not file format

delta-explain explains Delta-level file elimination only: partition pruning
and file-level data skipping. Parquet row-group predicate pushdown
(filtering *inside* surviving files on row-group footer statistics) is a
file-format concern, deliberately out of scope; it may appear later as a
separate mode.

## Known blind spots

- Statistics exist only for the first `delta.dataSkippingNumIndexedCols`
  leaf columns; predicates past the budget classify as `stats-safe` but
  cannot prune, and `stats.mode` reflects table coverage, not per-predicate
  reachability.
- On a fully checkpointed log (no JSON commits): an *empty* partitioned
  table's partition columns are undetectable, and liquid clustering is
  undetectable.
