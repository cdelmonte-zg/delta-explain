# CLI reference

The complete flag list, as printed by `delta-explain --help`:

```
delta-explain <PATH> [OPTIONS]

Arguments:
  <PATH>  Path to the Delta table (local path, s3://, az://, gs://), or with
          --catalog the table's name in that catalog (catalog.schema.table)

Options:
  -w, --where <PREDICATE>   Predicate (e.g. "age > 30 AND country = 'DE'")
  -v, --verbose             Show per-file details (kept/dropped with reason);
                            in JSON, adds the "files" array
      --limit <N>           Cap per-file listings at N entries
      --explain-why         Diagnose why the predicate pruned as it did, with
                            suggestions; in JSON, adds the "explain" array
      --format <FORMAT>     Output format: text (default) or json
      --min-pruning <PCT>   Fail if total pruning is below this percentage
      --assert-stats        Fail if any file is missing statistics
      --at-version <N>      Analyze the table at this version (time travel)
      --profile <NAME>      Static AWS credentials from ~/.aws/credentials (S3)
      --region <REGION>     AWS region (S3 / S3-compatible)
      --option <KEY=VALUE>  Object store config (repeatable)
      --env-creds           Read cloud credentials from environment variables
      --public              Access a public bucket (skip auth)
      --catalog <KIND>      Resolve PATH as a table name in this catalog
                            (unity: Unity Catalog via DATABRICKS_HOST and
                            DATABRICKS_TOKEN) and read it with the
                            credentials the catalog vends
```

Gate flags (`--min-pruning`, `--assert-stats`) are covered in
[Gating pruning in CI](../guides/ci-gating.md); credential flags
(`--env-creds`, `--profile`, `--option`, `--region`, `--public`) and
`--catalog` in [Cloud storage](../guides/cloud.md).
