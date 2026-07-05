# Reading a report

A text report has four parts. Take:

```text
$ delta-explain ./users -w "country = 'DE' AND age > 40"

Predicate Analysis:
  partition-safe: country = 'DE'
  stats-safe:     age > 40
  unsplittable:   -
  confidence:     conservative

Files in snapshot: 6

Phase 1: Partition pruning [exact]
  predicate:       country = 'DE'
  files remaining: 2  (-4, 67% pruned)
Phase 2: Data skipping (min/max statistics) [conservative]
  predicate:       age > 40
  files remaining: 1  (-1, 50% pruned)

Total reduction: 6 -> 1 files (83% pruned)
```

## Predicate Analysis

Each top-level `AND` conjunct is routed to the mechanism that can eliminate
files on it:

- **partition-safe** — references partition columns only; prunes at the
  directory level, exactly.
- **partition-exact** — outside the kernel's language (an any-shape `LIKE`) but
  all its columns are partition columns, so it is evaluated directly against the
  literal partition values.
- **stats-safe** — references non-partition columns; prunes on per-file min/max
  statistics.
- **unsplittable** — mixes both axes, or contains a construct the tool cannot
  reason about; applied conservatively (keeps all files) with a diagnostic.

## Confidence

Labels how precisely the elimination can be *explained*, never whether it is
correct (it always is, conservatively):

- **exact** — partition values compared directly; every dropped file provably
  has no match.
- **conservative** — min/max ranges can overlap a bound without a match, so
  files may be kept in excess, never dropped in excess.
- **incomplete** — part of the predicate could not be attributed to one phase;
  the totals stay sound, the attribution does not.

## Phases

The pruning is decomposed into chained phases — partition pruning first, then
data skipping — each showing the predicate fragment it honored and the files
remaining. It is a decomposition of *what the metadata makes possible*, not an
execution trace.

## Per-file detail

Add `--verbose` to see each file kept or dropped and why; in JSON mode it emits
a machine-readable `files[]` array. Cap large listings with `--limit`.

The precise guarantees behind every label are in
[What delta-explain guarantees](../reference/semantics.md).
