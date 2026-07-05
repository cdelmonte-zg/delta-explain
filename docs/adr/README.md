# Architecture Decision Records

The stable home for the *why* behind the module boundaries and the
contracts. Division of labor with the rest of the documentation:

- `README.md` describes observable behavior;
- `docs/semantics.md` states **what** the tool guarantees (the contract);
- an ADR records **why** a load-bearing decision was made, what it
  rejected, and what it constrains.

## When a decision earns an ADR

Both conditions must hold:

1. the decision crosses module boundaries or constrains future work, and
2. a plausible alternative was considered and rejected.

Routine implementation choices do not qualify; commit messages and PR
bodies remain the right place for those.

## Format

One file per decision, `NNNN-short-title.md`, one page: Context,
Decision, Alternatives considered, Consequences. Records are immutable
once accepted: a change of mind produces a new ADR that supersedes the
old one, never an edit that rewrites history. Statuses: `proposed`,
`accepted`, `superseded by NNNN`.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](0001-owned-predicate-ast.md) | An owned predicate AST between sqlparser and every consumer | accepted |
| [0002](0002-conservative-degradation.md) | Degrade conservatively instead of failing or guessing | accepted |
| [0003](0003-kernel-capability-sentinel.md) | Exhaustive capability matches over the kernel vocabulary, no catch-all | accepted |
| [0004](0004-cli-and-json-schema-as-the-stable-contract.md) | The stable contracts are the CLI surface and the versioned JSON schema | accepted |
| [0005](0005-like-as-a-structural-node-with-a-normalization-rewrite.md) | LIKE enters the AST as a structural node with a normalization rewrite | accepted |
| [0006](0006-partition-literal-evaluator-as-a-third-interpreter.md) | A partition-literal evaluator as a third interpreter over the same AST | accepted |
| [0007](0007-explain-why-is-a-deterministic-diagnostic-engine.md) | `--explain-why` is a deterministic diagnostic engine, not an ML model | accepted |
