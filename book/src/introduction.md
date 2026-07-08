# delta-explain

**Make Delta Lake file pruning visible.**

You run a query with a filter. The engine reads some files. But *how many* were
actually eliminated, and *why*? `delta-explain` answers that, from metadata
alone, without running a query engine.

Given a Delta table and a `WHERE` predicate, it reads the transaction log
directly (via [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs),
never the parquet data) and shows, step by step, which files partition pruning
and data skipping would eliminate, and which survive.

```text
$ delta-explain ./users -w "country = 'DE' AND age > 40"

Predicate Analysis:
  partition-safe: country = 'DE'
  stats-safe:     age > 40
  confidence:     conservative

Phase 1: Partition pruning [exact]
  files remaining: 2  (-4, 67% pruned)
Phase 2: Data skipping (min/max statistics) [conservative]
  files remaining: 1  (-1, 50% pruned)

Total reduction: 6 -> 1 files (83% pruned)
```

## What it is for

- **Understand pruning.** See how a predicate splits across the two elimination
  mechanisms, and, with [`--explain-why`](guides/explain-why.md), get told
  *why* a fragment did not prune and what to change.
- **Guard it in CI.** `--min-pruning` and `--assert-stats` turn a silent layout
  regression (a rewrite that drops partitioning, a query that scans everything)
  into a failed build. See [Gating pruning in CI](guides/ci-gating.md).
- **Build on it.** A [versioned JSON contract](reference/json-schema.md), a
  [Python wrapper](guides/python.md), and a self-contained
  [report viewer](guides/viewer.md).

## What it is not

`delta-explain` is not a query engine and not an optimizer. It never reads
data files, never predicts runtime, and never overstates: **a file any engine
needs is never reported as pruned.** When it cannot reason about a fragment it
keeps files and says so, rather than guessing. The full contract (guarantees,
degradation rules, exit codes) is in
[What delta-explain guarantees](reference/semantics.md).

## Positioning, meant literally

Production-usable as a conservative Delta metadata diagnostic and CI guardrail;
not yet a fully production-grade general-purpose Delta observability product.
The [roadmap](project/roadmap.md) tracks where that line moves next.
