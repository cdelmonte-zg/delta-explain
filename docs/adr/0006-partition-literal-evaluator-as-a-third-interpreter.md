# 0006. A partition-literal evaluator as a third interpreter over the same AST

Date: 2026-07-05
Status: proposed (issue #75)

## Context

Partition values are exact literals per file, so an engine evaluates any
deterministic predicate on partition columns directly against them -
`LIKE` in any shape, eventually functions. delta-explain routes such
fragments to `unsplittable` because one pruning language governs both
phases, and docs/semantics.md documents the resulting understatement on
the partition axis. Evaluating "the fragment" naively would mean
re-parsing raw SQL or embedding an engine: the same two-parsers trap ADR
0005 closed.

## Decision (proposed)

A third interpreter over the same owned AST, next to the analyzer and
the kernel bridge: a tree-walking evaluator (`partition_eval`) that
evaluates a `Pred` against a file's literal `partitionValues` under
Kleene three-valued logic. A file is dropped only on a definitive
`False`; `True` and `Unknown` keep it, so the conservative guarantee
holds by construction, not by discipline. Its vocabulary is exactly
`Pred` minus `Unsupported`: widening what it can evaluate means widening
the AST first (ADR 0005), never growing a shadow grammar. Classification
gains a fourth outcome for fragments whose columns are all partition
columns but which the kernel cannot lower (an additive JSON change, so a
minor `schema_version` bump); attribution keeps Phase 1 semantics by
intersecting the evaluator's survivors with the kernel's partition scan.

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

## Consequences

- Three interpreters, one tree: the analyzer says what a fragment means
  for attribution, the bridge says what the kernel executes, the
  evaluator says what the partition literals decide. None can drift.
- NULL handling concentrates in one Kleene `Truth` type instead of being
  re-derived per construct.
- The differential harness gains partition-axis cases for constructs the
  kernel cannot express, since the oracle is the only proof that
  engine-side semantics were reproduced faithfully.

This record becomes `accepted` when #75 lands.
