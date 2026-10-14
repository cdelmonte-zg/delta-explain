# 0009. Catalog names resolve to a location and the credentials to read it

Date: 2026-10-14
Status: accepted

## Context

A Delta table that lives in a catalog has a storage location, but the
catalog knows it and the user often does not. Until now `delta-explain`
took only a path or an object-store URL, so analyzing a catalog table
meant finding its location by hand and then supplying credentials for it
by hand, while the catalog could have answered both questions.

Two different things hide behind "catalog table". Name resolution is a
lookup: the catalog returns a `storage_location` and, on request,
temporary credentials scoped to it. Catalog-managed commits
(`delta.feature.catalogManaged`, Unity Catalog's own managed tables since
0.4) are a different protocol in which the catalog owns the log tail;
those tables stay refused (`table.rs`), and this record is only about the
lookup.

Unity Catalog exposes the lookup over REST, identically on Databricks and
on the open-source server: `GET /tables/{catalog.schema.table}` and
`POST /temporary-table-credentials`. The delta-kernel-rs Unity Catalog
crates that wrap the same calls are not published on crates.io
(release tooling pending upstream), and a git dependency is incompatible
with publishing `delta-explain` there.

## Decision

A `catalog` module sits between the CLI and the storage layer. The CLI
builds a `TableReference` (a path, or a name in a named catalog kind) and
`catalog::resolve` returns a `ResolvedTable`: the location `Url` the rest
of the pipeline already consumes, how storage must be opened, and where
the name came from (provenance). `main.rs` no longer parses the path
itself.

How storage is opened is an enum with two cases. `UserConfigured` keeps
the existing precedence chain (`--env-creds`, `--profile`, `--region`,
`--public`, `--option`). `CatalogVended` carries typed
`CatalogCredentials` (AWS key/secret/token, an Azure SAS, a GCP OAuth
token); the storage layer, not the resolver, translates them into
object-store configuration, merged over the user's `--region` and
`--option` so that endpoint, path style and region stay user-controlled.

In catalog mode the catalog is authoritative for credentials: `--env-creds`,
`--profile` and `--public` are rejected, and a vending failure is an
error that keeps the server's own `error_code` and `message`. GCP tokens
are declared unsupported for now: object_store has no configuration key
for a bearer token, so they need a credential provider rather than
key/value options.

The report shows what the resolution decided: a `Resolved:` line in the
text and an additive `catalog` object (`kind`, `name`, `location`) in the
JSON, `null` for a path (schema 0.5.0 -> 0.6.0). A consumer can tell a
catalog-resolved run from a path run and knows which location the numbers
describe.

## Alternatives considered

- **The delta-kernel-rs Unity Catalog crates.** Rejected for now: not on
  crates.io, and a git dependency blocks publishing. The REST surface is
  two calls; the module is written against it directly and can be swapped
  for the crates when they ship.
- **Fall back to user credentials when vending fails.** Rejected: two
  credential rules in one mode make the contract hard to state and hide
  misconfiguration (a catalog that cannot vend would silently read with
  whatever the user had in the environment). One rule per mode instead;
  a user who holds credentials uses the path mode with the location the
  catalog printed.
- **Vended credentials as `--option` strings inside the resolver.**
  Rejected: it works for AWS and Azure and not for GCP, and it couples
  the resolver to object_store's key names. The typed enum keeps the
  boundary and lets storage grow a provider for GCP.
- **Hive Metastore and Glue in the same step.** Deferred by decision:
  Thrift and SDK clients with their own version coupling, for a demand
  that does not exist yet. The `TableReference` enum leaves the door open.

## Consequences

- The local catalog stack (`examples/catalogs/`) exercises the full path
  on RustFS, whose STS credentials the Unity Catalog server is configured
  to vend, and on a real ADLS Gen2 account; the integration suite covers
  the CLI flow against a fake catalog server, so `cargo test` needs no
  cloud.
- Vended credentials are used for one invocation and never refreshed;
  the tool is a CLI, not a service, and their lifetime (an hour on Azure,
  twelve on the local stack) far exceeds a run.
- The error taxonomy gains two variants: transport failures with their
  full cause chain, and catalog errors with the server's body.
- Catalog-managed tables remain refused; supporting them is upstream
  work, tracked separately (#139).
