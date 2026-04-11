# delta-explain

**Make Delta pruning visible.**

A CLI that shows how partition pruning and data skipping reduce the set of candidate files in a Delta table.

Most engineers use Delta Lake without ever seeing what gets skipped. This tool makes that boundary explicit.

## The problem

You run a query with a filter. The engine reads some files. But how many files were actually eliminated, and *why*?

Delta Lake uses two mechanisms to skip files before reading data:

- **Partition pruning** eliminates files at the directory level based on partition column values
- **Data skipping** eliminates files at the file level based on per-column min/max statistics

Both happen silently inside the query engine. If your partitioning strategy is wrong, or your table is missing statistics, you won't know until performance degrades.

## What this tool does

`delta-explain` reads the Delta log directly (no Spark, no DuckDB, no runtime) and shows, step by step, how a predicate narrows the set of candidate files.

```
$ delta-explain ./my-table -w "age > 40 AND country = 'DE'"

Delta table: ./my-table
Version:     5
Predicate:   age > 40 AND country = 'DE'

Files in snapshot: 6

Phase 1: Partition pruning
  predicate:       country = 'DE'
  files remaining: 2  (-4, 67% pruned)

Phase 2: Data skipping (min/max statistics)
  predicate:       age > 40
  files remaining: 1  (-1, 50% pruned)

Total reduction: 6 -> 1 files (83% pruned)
```

With `--verbose`, you see exactly *which* files are kept or dropped and *why*:

```
Phase 1: Partition pruning
  predicate:       country = 'DE'
  files remaining: 2  (-4, 67% pruned)

  [DROPPED] part-00000-48368dae.parquet  (1.1 KB  3 records)  partition(country=IT)  stats(age: 41..65)
  [DROPPED] part-00000-fcf95aac.parquet  (1.1 KB  5 records)  partition(country=IT)  stats(age: 22..38)
  [DROPPED] part-00000-eee5a3ec.parquet  (1.1 KB  3 records)  partition(country=US)  stats(age: 31..55)
  [DROPPED] part-00000-de2ffaef.parquet  (1.1 KB  4 records)  partition(country=US)  stats(age: 18..29)
  [KEPT   ] part-00000-a35083c1.parquet  (1.1 KB  4 records)  partition(country=DE)  stats(age: 40..60)
  [KEPT   ] part-00000-c34f1417.parquet  (1.1 KB  5 records)  partition(country=DE)  stats(age: 20..35)

Phase 2: Data skipping (min/max statistics)
  predicate:       age > 40
  files remaining: 1  (-1, 50% pruned)

  [KEPT   ] part-00000-a35083c1.parquet  (1.1 KB  4 records)  partition(country=DE)  stats(age: 40..60)
  [DROPPED] part-00000-c34f1417.parquet  (1.1 KB  5 records)  partition(country=DE)  stats(age: 20..35)
```

Files missing statistics are explicitly flagged as `[no stats]`.

## Install

From Git:

```bash
cargo install --git https://github.com/cdelmonte-zg/delta-explain
```

Or build locally:

```bash
git clone https://github.com/cdelmonte-zg/delta-explain.git
cd delta-explain
cargo install --path .
```

Once a release is published, `cargo install delta-explain` and `docker pull ghcr.io/cdelmonte-zg/delta-explain` will work too.

## Usage

```
delta-explain <PATH> [OPTIONS]

Arguments:
  <PATH>  Path to the Delta table (local path, s3://, az://, gs://)

Options:
  -w, --where <PREDICATE>   Predicate (e.g. "age > 30 AND country = 'DE'")
  -v, --verbose             Show per-file details (kept/dropped with reason)
      --format <FORMAT>     Output format: text (default) or json
      --min-pruning <PCT>   Fail if total pruning is below this percentage
      --assert-stats        Fail if any file is missing statistics
      --region <REGION>     AWS region (S3 only)
      --option <KEY=VALUE>  Object store config (repeatable)
      --env-creds           Get cloud credentials from environment
      --public              Access a public bucket (skip auth)
```

### Local table

```bash
delta-explain ./my-table -w "country = 'DE'"
delta-explain ./my-table -w "age > 30 AND country = 'IT'" --verbose
```

### Cloud storage

```bash
# S3 with environment credentials
delta-explain --env-creds s3://bucket/path/to/table -w "date = '2024-01-01'"

# S3 public bucket
delta-explain --region us-east-1 --public s3://my-public-bucket/table -w "id > 100"

# Azure
delta-explain --env-creds az://container/table -w "region = 'eu-west-1'"

# S3-compatible (MinIO, Akamai, etc.)
delta-explain --option AWS_ENDPOINT=https://minio.local:9000 --option AWS_ACCESS_KEY_ID=key --option AWS_SECRET_ACCESS_KEY=secret s3://bucket/table -w "col > 5"
```

## CI/CD mode

`delta-explain` doubles as an assertion tool in pipelines. After your ETL writes a Delta table, verify that the pruning layout is healthy.

### Assert minimum pruning

Fail the pipeline if a predicate doesn't eliminate enough files:

```bash
delta-explain s3://warehouse/events -w "date = '2024-01-15'" --min-pruning 90
```

Exit code 1 if total pruning is below 90%.

### Assert statistics coverage

Fail if any file in the table is missing min/max statistics:

```bash
delta-explain s3://warehouse/events --assert-stats
```

### JSON output for downstream processing

```bash
delta-explain ./my-table -w "country = 'DE'" --format json | jq '.total_pruning_pct'
```

The JSON output includes per-file status, stats coverage, and phase-level metrics.

### Docker in a pipeline (after first release)

```yaml
# GitHub Actions example
- name: Verify pruning after ETL
  run: |
    docker run --rm \
      -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
      ghcr.io/cdelmonte-zg/delta-explain \
      --env-creds s3://warehouse/events \
      -w "date = '2024-01-15'" \
      --min-pruning 90 --assert-stats --format json
```

Combine flags freely: `--min-pruning`, `--assert-stats`, `--format json`, and `--verbose` are all independent.

## How it works

`delta-explain` uses [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) as a library. It reads the Delta log directly and runs multiple scans with different predicates to isolate the effect of each pruning phase:

1. **Scan with no predicate** to count total files
2. **Scan with partition-only clauses** to measure partition pruning
3. **Scan with the full predicate** to measure data skipping on top

The per-file statistics (min/max values) are read directly from the Delta log JSON to show *why* each file was kept or dropped.

No query engine is involved. No data files are read. Only metadata.

### Scope

`delta-explain` explains Delta-level file elimination only: partition pruning and file-level data skipping. Parquet row-group predicate pushdown (filtering *inside* surviving files based on row-group footer statistics) is intentionally out of scope for the current version -- it operates at a different layer (file format, not table protocol) and will be available as a future `--parquet-pushdown` option.

## Predicate syntax

`delta-explain` accepts standard SQL WHERE-clause syntax, parsed via [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs).

```sql
-- Comparisons
age > 30
country = 'DE'
score >= 90.5

-- Logical operators
age > 30 AND country = 'DE'
country = 'DE' OR country = 'IT'
NOT country = 'US'

-- IN lists
country IN ('DE', 'IT', 'US')
country NOT IN ('US')

-- BETWEEN
age BETWEEN 20 AND 40

-- NULL checks
name IS NOT NULL
age IS NULL

-- Parentheses
(country = 'DE' OR country = 'IT') AND age > 30

-- Nested columns
payload.age > 30
```

Supported types: string, integer, long, float, double, boolean. This is a diagnostic tool -- subqueries, functions, and LIKE are not supported.

## License

MIT

## Author

[Christian Del Monte](https://github.com/cdelmonte-zg) -- built as a companion tool for understanding Delta Lake internals and as open-source observability for the Delta protocol.

Powered by [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs).
