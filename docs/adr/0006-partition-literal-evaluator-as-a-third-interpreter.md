# 0006. A partition-literal evaluator as a third interpreter over the same AST

Date: 2026-07-05
Status: accepted (issue #75; refined from the proposed version during
implementation - see the note at the end)

## Context

Partition values are exact literals per file, so an engine evaluates any
deterministic predicate on partition columns directly against them -
`LIKE` in any shape, eventually functions. delta-explain routes such
fragments to `unsplittable` because one pruning language governs both
phases, and docs/semantics.md documents the resulting understatement on
the partition axis. Evaluating "the fragment" naively would mean
re-parsing raw SQL or embedding an engine: the same two-parsers trap ADR
0005 closed.

## Decision

A third interpreter over the same owned AST, next to the analyzer and
the kernel bridge: a tree-walking evaluator (`partition_eval`) that
evaluates a `Pred` against a file's literal `partitionValues` under a
four-valued logic - Kleene's `True` / `False` / `Null` extended with
`Unknown` for the evaluator's own ignorance. The two non-True SQL
outcomes drop a file *exactly*: the fragment is constant across the
file's rows, and SQL selects a row only when the predicate is TRUE, so
NULL excludes it as surely as FALSE. `Unknown` (an unparseable partition
value) keeps the file and downgrades confidence with a
`PARTITION_EVAL_GAP` note; the conservative guarantee holds by
construction. The evaluator's vocabulary is exactly `Pred` minus
`Unsupported`: widening what it can evaluate means widening the AST
first (ADR 0005), never growing a shadow grammar. Classification gains a
fourth outcome (`partition_exact`) for fragments whose columns are all
partition columns but which the kernel cannot lower (an additive JSON
change: `schema_version` 0.3.0); Phase 1 is the intersection of the
kernel's partition scan with the evaluator's survivors, and the final
scan intersects with them too, because the kernel never sees the exact
fragment and phase chaining must survive that.

## Alternatives considered

- **Embed a SQL engine over the raw fragments.** A second parser and a
  second semantics to keep in sync; the exact failure mode this
  architecture exists to prevent.
- **Extend the kernel's predicate language.** Not ours to extend, and
  the kernel has no `LIKE`; waiting on upstream leaves the partition
  axis understated indefinitely.
- **Special-case LIKE only.** Covers today's gap but rebuilds the same
  machinery for the next construct; the evaluator generalizes at the
  same cost.
- **Plain three-valued logic, dropping only on `False`** (the proposed
  version of this record). Conflates SQL NULL with the evaluator's
  ignorance: it would keep files an engine prunes (NULL partition values
  under `LIKE`) while still labeling the phase exact. Rejected during
  implementation in favor of the four-valued form.

## Consequences

- Three interpreters, one tree: the analyzer says what a fragment means
  for attribution, the bridge says what the kernel executes, the
  evaluator says what the partition literals decide. None can drift.
- NULL handling concentrates in one `Truth` type instead of being
  re-derived per construct; its junction tables are chosen to stay exact
  for the keep decision (under AND, `Null` absorbs `Unknown`; under OR
  it cannot).
- Every scan that runs downstream of classification must honor the exact
  fragment by intersection, since the kernel cannot: forgetting this
  breaks phase chaining, which is why the integration suite pins the
  chained totals.
- The differential harness gains partition-axis cases for constructs the
  kernel cannot express, since the oracle is the only proof that
  engine-side semantics were reproduced faithfully.

Refinement note: the proposed version prescribed three-valued logic with
"drop only on definitive False". Implementation showed that to be either
inexact or unsound depending on how NULL was folded; the accepted design
splits SQL NULL from evaluator ignorance instead. Recorded here rather
than in a superseding ADR because the record had not been accepted yet.
