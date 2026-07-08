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

1. **Baseline scan**: one kernel log replay (JSON commits + checkpoint
   Parquet) enumerates the snapshot's files and their statistics.
2. **Parse and normalize**: the `--where` predicate is parsed once into an
   owned AST, then normalized: negations push down to the leaves
   (De Morgan), a literal-prefix `LIKE` on a string column rewrites to a
   lexicographic range (`country LIKE 'D%'` becomes
   `country >= 'D' AND country < 'E'`, in the binary code-point order
   that partition values and min/max statistics compare with; on any
   other column type `LIKE` matches the value cast to a string, which a
   range cannot express, so the fragment routes to the evaluator or
   degrades), and conjuncts common to every `OR` branch factor out of
   the `OR`. All rewrites preserve SQL three-valued semantics, so they
   can change attribution and confidence but never the survivor set.
3. **Classify**: each top-level `AND` conjunct is routed to one bucket:
   `partition_safe` (references partition columns only), `partition_exact`
   (outside the kernel's predicate language, any-shape `LIKE`, but with
   fully known semantics and every column a partition column), `stats_safe`
   (references no partition column), or `unsplittable` (mixes both, or
   contains an opaque construct).
4. **Phase scans**: the partition-safe fragment runs as its own kernel
   scan, and the partition-exact fragment is evaluated per file against
   the literal partition values; their intersection is Phase 1. The full
   predicate, conservatively stripped of unsupported fragments and
   intersected with the partition-exact verdict, runs as the final scan.
5. **Attribution**: the survivor-set difference between phases is
   attributed: directory-level partition pruning first, min/max data
   skipping second.
6. **Gates and rendering**: `--min-pruning` / `--assert-stats` evaluate,
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
`OR`, factored `OR`-of-`AND`s, prefix `LIKE` ranges), partition-literal
evaluation (non-prefix `LIKE` on partition columns) and null-safe
comparisons.

## Confidence

Confidence labels how precisely the elimination can be *explained*, never
whether it is correct (it always is, in the conservative direction):

- **exact**. The phase's elimination is precise: partition values are
  compared directly, every dropped file provably contains no match.
- **conservative**. Sound but possibly loose: min/max ranges can overlap a
  predicate's bound without the file containing a matching row. Files may
  be kept in excess, never dropped in excess.
- **incomplete**. Part of the predicate could not be attributed to a
  single phase (mixed `OR`, or an unsupported construct). The totals are
  still sound; what is lost is the clean attribution, and a diagnostic note
  says why.

The global `confidence` is the least informative label across fragments.

## Diagnostics (`--explain-why`)

`--explain-why` turns the report into advice: a set of diagnoses, each a
**deterministic** function of the report the tool already computed (the
classification, the stats coverage, the partition columns, the per-phase
pruning). No prediction, no model - the *why* is as trustworthy as the
numbers it explains, and reproducible enough to gate on. Each diagnosis
carries a stable `code`, a `severity`, a message, and a suggestion where
one applies; the stable codes are `NO_PARTITION_FILTER`,
`WEAK_DATA_SKIPPING`, `STATS_ABSENT`, and `UNSUPPORTED_FRAGMENT`. In JSON
the diagnoses appear as an additive `explain` array, present only with the
flag. Natural-language phrasing of a diagnosis, if wanted, belongs to a
consumer of that JSON (an LLM the user brings), never bundled in the tool.

## Degradation rules

Constructs outside the pruning language (function calls, arithmetic,
`LIKE` in any non-prefix shape [leading or embedded wildcards, `_`,
`NOT LIKE`, `ESCAPE`], subqueries, column-to-column comparisons) do not
abort the run:

- under a top-level `AND`, the unsupported fragment is dropped from the
  scan predicate (keeping more files, never fewer) and the sibling
  conjuncts still prune;
- an `OR` or `NOT` touching an unsupported fragment is dropped whole,
  because its truth value cannot be bounded;
- the fragment is reported as `unsplittable`, confidence degrades to
  `incomplete`, and an `UNSUPPORTED_EXPRESSION` note carries the reason.

The partition axis has one carve-out. Partition values are exact literals
per file, so a fragment whose semantics the tool fully knows (any-shape
`LIKE` today) and whose columns are all partition columns does not
degrade: it is evaluated per file against those literals
(`partition_exact`), it prunes in Phase 1, and confidence stays `exact`.
The fragment is constant across a file's rows and a row is selected only
when the predicate is TRUE, so a file whose values make it FALSE *or
NULL* is dropped exactly. When the evaluator must abstain instead of
deciding (e.g. `LIKE` over a timestamp column, whose cast-to-string
format is engine-dependent), the affected files are kept, confidence
downgrades to `conservative`, and a `PARTITION_EVAL_GAP` note counts
them. Opaque constructs (function calls, arithmetic, subqueries, whose
semantics the tool does not model) always degrade by the rules above,
partition columns or not.

Malformed SQL is different: it is a user error and fails the run.

## Table features: declared, not compensated

Protocol features that distort or reframe the numbers are detected and
declared in `table_features`, with warnings, but the numbers are not
adjusted:

- **Deletion vectors**: record counts include soft-deleted rows on files
  that carry a vector (`DELETION_VECTORS` warns with the file count;
  enabled-but-unused stays silent because the numbers are correct).
- **Column mapping**: the log stores physical column names; verbose
  statistics may display them (`COLUMN_MAPPING`). Kernel pruning itself
  resolves the mapping.
- **Liquid clustering**: declared with the clustering columns
  (`LIQUID_CLUSTERING`); layout is managed by clustering, not directory
  partitions, and data skipping still applies. Undetectable on fully
  checkpointed logs (no public kernel accessor for system metadata
  domains).

## Engines may prune less than this report shows

The report reflects what the metadata makes possible, computed with the
strongest sound techniques in production use. Engines make different
choices; the known divergences are documented in the README's *Current
limitations* (notably `IN`-list strategies). The report never overstates
correctness: only, potentially, the pruning a specific engine will
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
