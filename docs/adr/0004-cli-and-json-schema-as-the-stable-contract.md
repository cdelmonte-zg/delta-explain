# 0004. The stable contracts are the CLI surface and the versioned JSON schema

Date: 2026-07-05 (recorded retroactively; the decision predates the record)
Status: accepted

## Context

The tool's consumers are CI pipelines, shell scripts, and a Python
wrapper. Internally, the analysis machinery sits on delta-kernel, which
moves fast; freezing the Rust API would either freeze the internals or
turn every kernel bump into a semver event.

## Decision

Stability lives at the process boundary. The JSON output carries an
explicit `schema_version` and follows SemVer relative to that field,
formalized in `schemas/report-v0.2.schema.json` and enforced by the
`json_contract` integration tests; exit codes and the
stdout-complete-or-empty property are part of the same contract
(docs/semantics.md). The Rust library API is internal and unstable. The
Python package is a client of the CLI-and-schema contract - platform
wheels ship the compiled binary, the module shells out and returns the
JSON as a `Report` - never a second API. The text output is explicitly
not frozen (tests use substring assertions).

## Alternatives considered

- **Stabilize the Rust crate API.** Freezes internals prematurely and
  chains the public surface to delta-kernel's churn.
- **Native Python bindings (PyO3).** A second API surface that can drift
  from the CLI, plus a heavier build matrix, for no capability the JSON
  contract does not already provide.

## Consequences

- Every JSON change is classified first: additive (minor bump of
  `schema_version`) or breaking (major bump plus a CHANGELOG entry under
  "Breaking").
- Internals refactor freely; the schema tests and the exit-code table
  are the regression net.
- Any future language wrapper follows the same pattern: wrap the binary,
  parse the schema, add no semantics.
