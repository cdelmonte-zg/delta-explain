# 0005. LIKE enters the AST as a structural node with a normalization rewrite

Date: 2026-07-05
Status: accepted (issue #72, PR #85)

## Context

`col LIKE 'p%'` is the one construct outside the pruning language with an
exact translation into it: in the binary code-point order that both
partition values and min/max string statistics compare with, it is
equivalent to `col >= 'p' AND col < succ(p)`. As an `Unsupported` leaf
the fragment carried only its raw SQL text, so any attempt to use it -
the range rewrite, or the future evaluation over partition literals
(#75) - would have had to re-parse that text: a second parser, or a
second engine, with its own grammar and its own drift.

## Decision

Widen the IR instead of adding a parser. `Pred::Like` is a structural
node (column, pattern, negated flag); the single sqlparser converter
remains the only parser. The rewrite is a normalization pass alongside
De Morgan and OR factoring: literal-prefix patterns become the
lexicographic range, wildcard-free patterns become equality, and both
are exact under SQL three-valued semantics, so the survivor-set
guarantee is untouched. A `Like` that survives normalization (non-prefix
shape, `NOT LIKE`, `ESCAPE`) degrades exactly like an `Unsupported`
leaf, and the exhaustive matches of ADR 0001/0003 force every consumer
to say so explicitly.

## Alternatives considered

- **Rewrite inside the converter.** Loses the interplay with negation
  pushdown (`NOT name NOT LIKE 'D%'` only becomes rewritable after
  De Morgan) and buries a semantic rewrite in a syntactic translation
  layer.
- **Evaluate the raw fragment with an embedded SQL engine.** A second
  grammar to keep in sync, a heavyweight dependency, and semantics we do
  not control.
- **String-match the raw fragment in the analyzer.** Untyped pattern
  matching on rendered SQL - fragile against quoting, spacing, and
  nesting.

## Consequences

- The AST is confirmed as the place where the pruning language grows:
  new constructs get a typed node first, then every consumer is forced
  by the compiler to take a stance.
- The analysis output shows the rewritten form (`country >= 'D' AND
  country < 'E'`), consistent with the other normalization rewrites; no
  JSON schema change.
- The vocabulary now contains a node the kernel cannot lower; emission
  treats it as fatal and callers strip it, same as `Unsupported`. This
  is the enabling step for ADR 0006.
