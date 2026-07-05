# 0002. Degrade conservatively instead of failing or guessing

Date: 2026-07-05 (recorded retroactively; the decision predates the record)
Status: accepted

## Context

Real predicates contain constructs the pruning language cannot express:
function calls, arithmetic, subqueries, column-to-column comparisons.
The tool is a CI guardrail; its one hard guarantee (docs/semantics.md) is
that the survivor set is a superset of the files containing matching
rows. A wrong number is strictly worse than a loose one, and a hard
failure on every unsupported fragment would make the tool unusable on
production predicates.

## Decision

Unsupported constructs degrade, loudly, in the conservative direction.
Under a top-level `AND` the unsupported fragment is stripped from the
scan predicate (keeping more files, never fewer) and the siblings still
prune; an `OR` or `NOT` touching one is dropped whole, because its truth
value cannot be bounded. The fragment is reported as `unsplittable`,
confidence degrades to `incomplete`, and an `UNSUPPORTED_EXPRESSION`
note carries the reason. Malformed SQL is different - a user error that
fails the run. The Spark differential harness is the standing oracle
that the survivor set stays a superset.

## Alternatives considered

- **Fail the run on any unsupported construct.** Sound but useless as a
  guardrail: one `LIKE` in a fifty-conjunct predicate would block the
  whole check.
- **Best-effort evaluation without marking the gap.** Produces silent
  wrong numbers, which forfeits the only guarantee the tool has.

## Consequences

- Every feature must preserve the superset guarantee; anything that
  cannot prove it degrades instead (the burden of proof is on pruning,
  never on keeping).
- The report may understate what an engine can prune; that direction of
  divergence is documented and is what ADRs 0005/0006 chip away at.
- Consumers can trust exit codes and totals in CI even when parts of the
  predicate are beyond the tool.
