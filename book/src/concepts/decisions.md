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
| [0003](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0003-kernel-capability-sentinel.md) | Exhaustive capability matches over the kernel vocabulary, no catch-all | accepted |
| [0004](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0004-cli-and-json-schema-as-the-stable-contract.md) | The stable contracts are the CLI surface and the versioned JSON schema | accepted |
| [0005](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0005-like-as-a-structural-node-with-a-normalization-rewrite.md) | LIKE enters the AST as a structural node with a normalization rewrite | accepted |
| [0006](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0006-partition-literal-evaluator-as-a-third-interpreter.md) | A partition-literal evaluator as a third interpreter over the same AST | accepted |
| [0007](https://github.com/cdelmonte-zg/delta-explain/blob/main/docs/adr/0007-explain-why-is-a-deterministic-diagnostic-engine.md) | `--explain-why` is a deterministic diagnostic engine, not an ML model | accepted |

The recurring theme: the value is a *sound, explainable* answer, so every
decision protects that: one parse so interpreters cannot drift (0001),
loud degradation over silent guessing (0002), compile-time gaps over runtime
surprises (0003), a versioned contract as the only stable surface (0004), and a
diagnostic layer that is deterministic rather than predictive (0007).
