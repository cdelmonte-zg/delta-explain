# 0010. Read table metadata from the kernel Snapshot

Date: 2026-10-28  
Status: accepted

## Context

Up to 0.8.0, `delta-explain` read JSON commits itself to get partition columns, reader/writer features and the `delta.clustering` domain.

That meant an extra GET per commit on cloud storage, even when the kernel could start from a checkpoint. It also missed features when the old JSON commits had been deleted. In that case, the catalog-managed check and feature warnings did not run.

The kernel already reconstructs metadata and protocol for the `Snapshot`, and can load the clustering domain too. These APIs are available through `internal-api`, but they can change without notice, including in patch releases.

## Decision

Get table metadata from the kernel Snapshot.

Enable `internal-api` and use `table_configuration`, `partition_columns`, `reader_features`, `writer_features` and `get_domain_metadata_internal`. Remove `metadata/log.rs`.

Look up the clustering domain only when the protocol declares the clustering feature, so tables without clustering avoid that extra work.

Pin both kernel crates to `=0.30.0`. Every upgrade needs a build and test run; semver alone is not enough to assume compatibility.

## Alternatives considered

- **Keep our JSON reader.** We would need to handle checkpoints ourselves and maintain another interpretation of the log. ADR 0001 and 0008 already moved us away from this kind of duplication on the predicate side.
- **Wait for public APIs.** That leaves the bug unresolved and blocks #134, which needs `internal-api` too. Revisit this decision when public APIs cover what we need.
- **Use the kernel except for clustering.** That keeps the JSON reader and its storage reads around for a single field, even though the kernel already has an accessor for it.

## Consequences

Features are recovered correctly from checkpoint-only logs. Partition columns come from snapshot metadata, so the fallback that inferred them from files is gone.

Our separate JSON-log listing and per-commit reads disappear. The clustering lookup can still read the log through the kernel when needed.

Kernel upgrades may break the build, including patch upgrades. We accept that cost and document it in `DEVELOPMENT.md`.

The CLI and report formats stay the same, including the JSON schema and exit-code rules.