# Cloud storage

`delta-explain` reads `s3://`, `az://`, and `gs://` tables directly. Credentials
come in by environment, by profile, or by explicit options.

## Credentials

- **On cloud infrastructure** (EC2/ECS, EKS, AKS, GKE): with no explicit
  credentials the storage layer uses the provider's ambient chain (instance
  profile, Managed Identity, Workload Identity). Add `--env-creds` when the
  credentials live in environment variables instead.
- **On a laptop (AWS)**: `--profile <name>` resolves static keys, session token,
  and region from `~/.aws/credentials` / `~/.aws/config`. Profiles that rely on
  SSO, `credential_process`, or role assumption are not resolved; export them
  first and use `--env-creds`:
  ```bash
  eval $(aws configure export-credentials --profile corp --format env)
  delta-explain --env-creds s3://bucket/table -w "..."
  ```
- **Static keys** (S3-compatible stores, local dev): pass them via repeated `--option
  KEY=VALUE`, expanding from environment variables to keep secrets out of `argv`.

## Examples

```bash
# S3 with credentials from the environment
delta-explain --env-creds s3://bucket/path/to/table -w "date = '2024-01-01'"

# S3 public bucket
delta-explain --region us-east-1 --public s3://my-public-bucket/table -w "id > 100"

# Azure
delta-explain --env-creds az://container/table -w "region = 'eu-west-1'"

# GCS (Workload Identity on GKE, or service-account JSON via env)
delta-explain gs://bucket/table -w "day >= DATE '2026-01-01'"
```

The end-to-end walkthroughs for
[S3-compatible storage](https://github.com/cdelmonte-zg/delta-explain/tree/main/examples/s3-compatible)
and [GCS](https://github.com/cdelmonte-zg/delta-explain/tree/main/examples/gcs)
live in `examples/`.

## Unity Catalog

A table that lives in Unity Catalog (Databricks, or the open-source
server) can be named instead of located:

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<personal access token>
delta-explain --catalog unity sales.events.orders -w "day = DATE '2026-10-01'"
```

The catalog answers with the table's storage location and vends temporary
credentials for it, which is what the table is read with: no cloud
credentials of your own are involved, and `--env-creds`, `--profile` and
`--public` are rejected in this mode. `--region` and `--option` still
configure the transport, which matters for S3-compatible stores (endpoint,
path style). The report shows the resolved location (`Resolved:` in the
text, `catalog` in the JSON). AWS, Azure and GCP credentials are
supported. The
[catalog stack](https://github.com/cdelmonte-zg/delta-explain/tree/main/examples/catalogs)
in `examples/` runs the whole flow locally against the open-source
server, RustFS and (optionally) a real ADLS Gen2 account.

Valid `--option` keys pass through to the
[`object_store`](https://docs.rs/object_store/) builders; see upstream for the
per-backend list.
