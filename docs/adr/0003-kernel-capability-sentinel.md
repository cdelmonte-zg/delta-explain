# 0003. Exhaustive capability matches over the kernel vocabulary, no catch-all

Date: 2026-07-05 (recorded retroactively; the decision predates the record)
Status: accepted

## Context

delta-kernel evolves independently and its expression enums widen over
time. The report's claims rest on knowing, per operator, whether the
kernel's data-skipping evaluator actually prunes on it or merely
represents it. A kernel upgrade that adds an operator we silently
mis-tier would corrupt the report without failing any test.

## Decision

`kernel_bridge.rs` is the only module that names the kernel's expression
vocabulary, and it maps every kernel operator to a `Capability` tier
(`SkippingNative` vs `LanguageOnly`) through exhaustive matches with no
catch-all arm. When a kernel bump widens an enum, compilation breaks and
forces a deliberate tiering decision instead of a silent gap. A golden
unit test documents the expected tiers per kernel version.

## Alternatives considered

- **Catch-all arms with a default tier.** Compiles forever, silently
  wrong on the first widened enum - precisely the failure mode to
  prevent.
- **Runtime feature detection.** There is nothing to detect against: the
  kernel does not expose its skipping capabilities introspectively.

## Consequences

- Kernel upgrades have a single audit point, and the compiler is the
  auditor.
- The same principle is reused wherever an external enum drives a
  semantic decision (e.g. literal coercion over `PrimitiveType`), and it
  is the enforcement mechanism ADR 0001's vocabulary growth relies on.
- The cost is verbosity in match arms, accepted as the price of turning
  upgrade risk into compile errors.
