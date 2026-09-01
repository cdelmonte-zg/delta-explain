# 0008. Owned predicate vocabulary is the pruning capability boundary

Date: 2026-09-01
Status: accepted
Supersedes: ADR 0003

## Context

ADR 0003 established exhaustive capability matches over delta-kernel's
predicate vocabulary.

In the original architecture, `kernel_bridge.rs` named the kernel
expression operators directly and assigned each of them a capability
tier such as `SkippingNative` or `LanguageOnly`. Exhaustive matches with
no catch-all arm acted as a compile-time sentinel: if delta-kernel
widened one of its operator enums, a dependency upgrade failed to
compile until delta-explain explicitly classified the new operator.

That mechanism was necessary because the kernel vocabulary participated
directly in delta-explain's semantic classification. A new external
operator could otherwise become part of the supported expression space
without a deliberate decision about whether it could contribute to data
skipping.

The rewritten architecture reverses this dependency.

delta-explain now owns a closed predicate vocabulary, `Pred`, which
defines the pruning language understood by the application. Parsing and
normalization produce this owned representation first. Analysis
classifies `Pred` fragments, and only fragments already accepted by that
analysis are later lowered into `delta_kernel::expressions::Predicate`.

The kernel predicate vocabulary is therefore no longer an input to
capability classification. It is an output target of explicit lowering.

Adding a new predicate operator to delta-kernel does not make that
operator available to delta-explain automatically. Supporting it
requires a deliberate change to delta-explain's own predicate model,
normalization or classification rules, and lowering implementation.

The compile-time risk described by ADR 0003 consequently no longer
exists at that boundary.

External enums can still be semantic inputs elsewhere. For example,
schema types from delta-kernel determine literal coercion. In those
cases exhaustive matching remains required: widening an external enum
that influences delta-explain semantics must continue to force an
explicit decision.

## Decision

The owned `Pred` vocabulary is the capability boundary for pruning
semantics.

delta-explain determines whether an expression is supported,
partition-safe, partition-exact, stats-safe, unsplittable, or
unsupported from its own predicate representation. Kernel predicate
operators are used only as an explicit lowering target after those
decisions have been made.

We therefore do not maintain a capability table over
`BinaryPredicateOp`, `UnaryPredicateOp`, or `JunctionPredicateOp` solely
to detect growth in delta-kernel's predicate vocabulary.

Instead:

- support for pruning constructs is added first to the owned `Pred`
  vocabulary;
- analysis decides how each `Pred` construct may participate in pruning;
- kernel lowering implements the explicitly supported mapping from
  `Pred` to `delta_kernel::expressions::Predicate`;
- unsupported or non-lowerable constructs never reach the kernel
  lowering path;
- widening delta-kernel's predicate operator enums alone does not
  require delta-explain to change;
- external enums that are semantic inputs to delta-explain remain
  exhaustively matched with no catch-all arm.

`PrimitiveType` is one such input. Literal coercion depends on the
column type supplied by delta-kernel, so matches over `PrimitiveType`
remain intentionally exhaustive. A new primitive type must break
compilation until its coercion semantics are considered explicitly.

## Alternatives considered

### Retain the ADR 0003 capability sentinel unchanged

We could continue mapping every kernel predicate operator to a
delta-explain capability tier even though the mapping is no longer
consumed by analysis.

Rejected because it would preserve a compile-time sentinel for a
dependency direction that no longer exists. The resulting capability
table would be dead semantic metadata whose primary purpose was
satisfying the previous architecture rather than protecting the current
one.

It could also suggest incorrectly that the set of kernel operators
defines the set of expressions delta-explain supports.

### Derive delta-explain capabilities from kernel operators

We could move classification back toward the external kernel vocabulary
and treat kernel operator support as the source of truth.

Rejected because representation support and pruning semantics are
different questions. The kernel may represent an expression without
providing useful file-statistics pruning for it, while delta-explain
also has semantics outside the kernel predicate language, such as exact
evaluation against partition literals.

The application therefore needs its own semantic vocabulary regardless
of what the kernel can represent.

### Use catch-all matches for external enums

Rejected wherever an external enum affects delta-explain semantics.
Catch-all arms would allow dependency upgrades to introduce values whose
behavior had never been considered.

Exhaustive matching remains the rule for external semantic inputs.

## Consequences

The supported pruning language has one owner: the `Pred` model and the
analysis operating on it.

A delta-kernel release that adds predicate operators does not by itself
require a delta-explain change and cannot silently expand the
expressions delta-explain claims to support.

Adding support for a new pruning construct becomes an explicit
delta-explain change spanning the appropriate parts of the owned
vocabulary, analysis, normalization or evaluation, and kernel lowering.

The kernel adapter becomes simpler: it translates decisions already made
by delta-explain rather than participating in capability policy.

Compile-time sentinels remain where dependency growth can actually alter
semantics. In particular, externally owned type vocabularies used during
coercion must continue to be exhaustively matched.

ADR 0003 remains a record of the protection required by the original
architecture, but its decision no longer applies to the
predicate-operator boundary after the architecture rewrite.
