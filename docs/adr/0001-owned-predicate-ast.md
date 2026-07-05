# 0001. An owned predicate AST between sqlparser and every consumer

Date: 2026-07-05 (recorded retroactively; the decision predates the record)
Status: accepted

## Context

The `--where` predicate feeds two consumers with different needs: the
kernel bridge must produce the exact `delta_kernel::expressions::Predicate`
the scan planner executes, and the analyzer must produce the
classification and confidence the user reads. If the two interpret the
input independently, "what the kernel sees" and "what the user reads" can
drift apart - the worst possible failure for a tool whose whole job is to
report what pruning actually does. sqlparser's AST is also far wider than
the pruning language and changes shape across versions.

## Decision

Parse once, at a single boundary: `predicate_ast.rs` converts sqlparser's
AST into an owned `Pred` whose vocabulary is the language of pruning
(column-op-literal comparisons, junctions, null checks, `IN`/`BETWEEN`
sugar). The converter is total: anything outside the vocabulary becomes an
`Unsupported` leaf carrying the raw fragment and a reason, and consumers
decide severity. Every downstream module - the analyzer, the kernel
bridge, normalization rewrites, future evaluators - reads the same owned
tree; only `predicate_ast.rs` names sqlparser types.

## Alternatives considered

- **Each consumer walks the sqlparser AST directly.** Two interpreters of
  a foreign grammar drift independently, and every sqlparser upgrade
  touches every consumer.
- **Lower straight to the kernel predicate and derive the analysis from
  it.** The kernel vocabulary is narrower than what the report needs
  (display fidelity for `IN`/`BETWEEN`, the raw text of unsupported
  fragments) and couples the user-facing surface to kernel versions.

## Consequences

- Growing the language is deliberate: one converter arm, then exhaustive
  matches force every consumer to take a stance (see 0003 for the same
  principle applied to the kernel vocabulary, 0005 for the first
  vocabulary extension after the fact).
- Normalization rewrites are `Pred -> Pred` and benefit every consumer at
  once; none of them can disagree about what the predicate means.
- The cost is a third representation to maintain, accepted as the price
  of making drift structurally impossible rather than merely tested for.
