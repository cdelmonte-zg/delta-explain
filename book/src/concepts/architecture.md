# Architecture

The pipeline is a thin, linear sequence - no hidden concurrency, no adaptive
behavior:

1. **Baseline scan** - one kernel log replay (JSON commits + checkpoint Parquet)
   enumerates the snapshot's files and their statistics.
2. **Parse and normalize** - the `--where` predicate is parsed once into an
   owned AST, then normalized (De Morgan pushdown, the string-column prefix-LIKE
   range rewrite, OR factoring). Every rewrite preserves SQL three-valued
   semantics, so it can change attribution but never the survivor set.
3. **Classify** - each top-level `AND` conjunct is routed to `partition_safe`,
   `partition_exact`, `stats_safe`, or `unsplittable`.
4. **Phase scans** - the partition fragments run as a kernel scan intersected
   with the partition-literal evaluator; the full predicate, stripped of
   unsupported fragments, runs as the final scan.
5. **Attribution** - the survivor-set differences become chained, labeled phases.
6. **Diagnose, gate, render** - `--explain-why` diagnoses, gates evaluate, and
   the report renders as text or JSON.

## One parse, three interpreters

The owned predicate AST (`Pred`) sits between sqlparser and everything else, so
there is one parse and three independent interpreters that can never disagree:

- the **analyzer** produces the classification the user reads;
- the **kernel bridge** produces the predicate the kernel executes;
- the **partition-literal evaluator** decides what a file's partition values
  decide (for constructs the kernel cannot lower, like an any-shape `LIKE`).

Because all three read the same tree, "what the kernel sees", "what the user
reads", and "what gets evaluated" cannot drift. Growing the pruning language
means widening the AST, never adding a second parser - the decision recorded in
[ADR 0005](decisions.md) and generalized in [ADR 0006](decisions.md).

Diagnostics (`--explain-why`) are a separate advisor layer *over the finished
report*, not a fourth AST interpreter: a deterministic rules engine, by
deliberate design ([ADR 0007](decisions.md)).

## Why metadata-only is a feature

The tool never reads data files. That is what makes it fast (cost scales with
the number of `add` actions, not data volume), sound (it reports what the
metadata makes possible), and honest under maintenance operations: `VACUUM`
deletes data files that are already absent from the current snapshot, so it does
not affect what the tool reports.

The module-by-module map and the working conventions live in the repo's
[`CLAUDE.md`](https://github.com/cdelmonte-zg/delta-explain/blob/main/CLAUDE.md).
