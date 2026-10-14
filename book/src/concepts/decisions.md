# Architecture decisions

The *why* behind the module boundaries and the contracts. Division of labor:
this book's [Reference](../reference/semantics.md) states **what** the tool
guarantees; an ADR records **why** a load-bearing decision was made, what it
rejected, and what it constrains.

A decision earns an ADR only when it crosses module boundaries or constrains
future work **and** it rejected a plausible alternative. Records are immutable
once accepted; a change of mind produces a new record that supersedes the old.

The full records live under
[`docs/adr/`](https://github.com/cdelmonte-zg/delta-explain/tree/main/docs/adr):

| ADR | Decision | Status |
|---|---|---|
| [0001](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0001-owned-predicate-ast.md) | An owned predicate AST between sqlparser and every consumer | accepted |
| [0002](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0002-conservative-degradation.md) | Degrade conservatively instead of failing or guessing | accepted |
| [0003](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0003-kernel-capability-sentinel.md) | Exhaustive capability matches over the kernel vocabulary, no catch-all | superseded by 0008 |
| [0004](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0004-cli-and-json-schema-as-the-stable-contract.md) | The stable contracts are the CLI surface and the versioned JSON schema | accepted |
| [0005](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0005-like-as-a-structural-node-with-a-normalization-rewrite.md) | LIKE enters the AST as a structural node with a normalization rewrite | accepted |
| [0006](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0006-partition-literal-evaluator-as-a-third-interpreter.md) | A partition-literal evaluator as a third interpreter over the same AST | accepted |
| [0007](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0007-explain-why-is-a-deterministic-diagnostic-engine.md) | `--explain-why` is a deterministic diagnostic engine, not an ML model | accepted |
| [0008](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0008-owned-predicate-vocabulary-as-capability-boundary.md) | Owned predicate vocabulary is the pruning capability boundary | accepted |
| [0009](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0009-catalog-name-resolution-with-vended-credentials.md) | Catalog names resolve to a location and the credentials to read it | accepted |

The recurring theme: the value is a *sound, explainable* answer, so every
decision protects that: one parse so interpreters cannot drift (0001),
loud degradation over silent guessing (0002), an owned vocabulary as the
capability boundary (0008, superseding the compile-time sentinel of 0003), a
versioned contract as the only stable surface (0004), a diagnostic layer that
is deterministic rather than predictive (0007), and one credential rule per
mode when a catalog resolves the table (0009).
