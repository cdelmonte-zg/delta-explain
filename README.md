# delta-explain

**Make Delta pruning visible.**

A CLI that shows how partition pruning and data skipping reduce the set of candidate files in a Delta table.

Production-usable as a conservative Delta metadata diagnostic and CI guardrail — not yet a fully production-grade general-purpose Delta observability product. That line is meant literally: what the tool guarantees, and what it deliberately does not, is written down in [docs/semantics.md](docs/semantics.md).


**Documentation**: [three-minute quickstart](examples/quickstart/) - [what delta-explain guarantees (and what it does not)](docs/semantics.md) - [the JSON report, field by field](docs/json-schema.md) - [what it is validated against](docs/validation.md) - [current limitations](#current-limitations)

## The problem

You run a query with a filter. The engine reads some files. But how many files were actually eliminated, and *why*?

Delta Lake uses two mechanisms to skip files before reading data:

- **Partition pruning** eliminates files at the directory level based on partition column values
- **Data skipping** eliminates files at the file level based on per-column min/max statistics

Both happen silently during scan planning, below the query. If partitioning is wrong or stats are missing, you won't know until performance degrades.

## What this tool does

`delta-explain` uses [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) to read Delta metadata directly (no Spark, no DuckDB, no query execution engine) and shows, step by step, how a predicate narrows the set of candidate files.

```
$ delta-explain ./my-table -w "age > 40 AND country = 'DE'"

Delta table: ./my-table
Version:     5
Predicate:   age > 40 AND country = 'DE'

Predicate Analysis:
  partition-safe: country = 'DE'
  stats-safe:     age > 40
  unsplittable:   -
  confidence:     conservative

Files in snapshot: 6

Phase 1: Partition pruning [exact]
  predicate:       country = 'DE'
  files remaining: 2  (-4, 67% pruned)

Phase 2: Data skipping (min/max statistics) [conservative]
  predicate:       age > 40
  files remaining: 1  (-1, 50% pruned)

Total reduction: 6 -> 1 files (83% pruned)
```

The **Predicate Analysis** block shows how the predicate was split across the two pruning phases, and `confidence` labels how precisely the elimination can be explained (`exact` / `conservative` / `incomplete`). The precise definitions, the degradation rules, and what each label guarantees are in [docs/semantics.md](docs/semantics.md).

With `--verbose`, you see exactly *which* files are kept or dropped and *why*:

```
Phase 1: Partition pruning [exact]
  predicate:       country = 'DE'
  files remaining: 2  (-4, 67% pruned)

  [DROPPED] part-00000-48368dae.parquet  (1.1 KB  3 records)  partition(country=IT)  stats(age: 41..65)
  [DROPPED] part-00000-fcf95aac.parquet  (1.1 KB  5 records)  partition(country=IT)  stats(age: 22..38)
  [DROPPED] part-00000-eee5a3ec.parquet  (1.1 KB  3 records)  partition(country=US)  stats(age: 31..55)
  [DROPPED] part-00000-de2ffaef.parquet  (1.1 KB  4 records)  partition(country=US)  stats(age: 18..29)
  [KEPT   ] part-00000-a35083c1.parquet  (1.1 KB  4 records)  partition(country=DE)  stats(age: 40..60)
  [KEPT   ] part-00000-c34f1417.parquet  (1.1 KB  5 records)  partition(country=DE)  stats(age: 20..35)

```

(Use `--limit` to cap the listing on large tables; in JSON mode `--verbose` emits the machine-readable `files[]` array instead.) Files without a `stats` payload appear as `[no stats]`; statistics come from the kernel's log replay, checkpoint Parquet included, so `[no stats]` means the writer really recorded none.

## Install

### Homebrew (macOS, Linux)

```bash
brew tap cdelmonte-zg/tap
brew install delta-explain
```

### Scoop (Windows)

```powershell
scoop bucket add cdelmonte-zg https://github.com/cdelmonte-zg/scoop-bucket
scoop install delta-explain
```

### Debian / Ubuntu (`.deb`)

Download the `.deb` for your architecture from the [latest release](https://github.com/cdelmonte-zg/delta-explain/releases/latest) and install with `dpkg`:

```bash
wget https://github.com/cdelmonte-zg/delta-explain/releases/download/v0.5.0/delta-explain_0.5.0-1_amd64.deb
sudo dpkg -i delta-explain_0.5.0-1_amd64.deb
```

Available for `amd64` and `arm64`. Uninstall with `sudo apt remove delta-explain`.

### Pre-built binary (any OS, no package manager)

Download the archive for your platform from the [latest release](https://github.com/cdelmonte-zg/delta-explain/releases/latest), extract, and place on `$PATH`:

| Platform | Archive |
|---|---|
| Linux x86_64 (glibc) | `delta-explain-x86_64-unknown-linux-gnu.tar.gz` |
| Linux x86_64 (static, musl) | `delta-explain-x86_64-unknown-linux-musl.tar.gz` |
| Linux ARM64 | `delta-explain-aarch64-unknown-linux-gnu.tar.gz` |
| macOS Intel | `delta-explain-x86_64-apple-darwin.tar.gz` |
| macOS Apple Silicon | `delta-explain-aarch64-apple-darwin.tar.gz` |
| Windows x86_64 | `delta-explain-x86_64-pc-windows-msvc.zip` |

Each archive ships with a `.sha256` checksum. The musl build is statically linked and runs on any Linux distribution without glibc dependencies.

### From PyPI (Python, no Rust needed)

```bash
pip install delta-explain
```

The wheel ships the compiled binary (the `delta-explain` command works from the same environment) plus a thin Python API around the JSON contract:

```python
from delta_explain import explain

report = explain("s3://warehouse/events",
                 where="country = 'DE' AND age > 40",
                 min_pruning=80, env_creds=True)
report.passed              # gate outcome; False means the CLI would exit 1
report.total_pruning_pct
report["analysis"]["confidence"]
```

Gate failures come back as a report with `passed == False`; runtime errors raise `DeltaExplainError` with the CLI's message — the same exit-code contract as the command line, in Python types.

### From crates.io (requires Rust 1.88+)

```bash
cargo install delta-explain
```

### From Git (latest development version)

```bash
cargo install --git https://github.com/cdelmonte-zg/delta-explain
```

### Docker (amd64 + arm64)

```bash
docker pull ghcr.io/cdelmonte-zg/delta-explain
docker run --rm -v /path/to/table:/data ghcr.io/cdelmonte-zg/delta-explain /data -w "col > 10"
```

For pipelines, pin to a release tag (e.g., `:0.5.0`) or to a digest; `:latest` is for local exploration only.

## Usage

```
delta-explain <PATH> [OPTIONS]

Arguments:
  <PATH>  Path to the Delta table (local path, s3://, az://, gs://)

Options:
  -w, --where <PREDICATE>   Predicate (e.g. "age > 30 AND country = 'DE'")
  -v, --verbose             Show per-file details (kept/dropped with reason);
                            in JSON, adds the "files" array
      --limit <N>           Cap per-file listings at N entries
      --format <FORMAT>     Output format: text (default) or json
      --min-pruning <PCT>   Fail if total pruning is below this percentage
      --assert-stats        Fail if any file is missing statistics
      --at-version <N>      Analyze the table at this version (time travel)
      --profile <NAME>      Static AWS credentials from ~/.aws/credentials (S3)
      --region <REGION>     AWS region (S3 / S3-compatible)
      --option <KEY=VALUE>  Object store config (repeatable)
      --env-creds           Read cloud credentials from environment variables
      --public              Access a public bucket (skip auth)
```

### Local table

```bash
delta-explain ./my-table -w "country = 'DE'"
delta-explain ./my-table -w "age > 30 AND country = 'IT'" --verbose
```

### Cloud storage

**Credentials.** Three ways in, by environment:

- **On cloud infrastructure** (EC2/ECS, EKS, AKS, GKE): with no explicit credentials the storage layer falls back to the provider's ambient chain (instance profile, Managed Identity, Workload Identity) on its own; add `--env-creds` when the credentials live in environment variables instead (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`/`AWS_REGION`, `AZURE_STORAGE_ACCOUNT_NAME`/`AZURE_STORAGE_ACCOUNT_KEY`, `GOOGLE_APPLICATION_CREDENTIALS`).
- **On a developer laptop** (AWS): `--profile <name>` resolves static keys, session token, and region from `~/.aws/credentials` and `~/.aws/config`, the same files the AWS CLI reads (including the `AWS_SHARED_CREDENTIALS_FILE` / `AWS_CONFIG_FILE` overrides). Profiles that rely on SSO, `credential_process`, or role assumption are not resolved; export them first and use `--env-creds`:
  ```bash
  eval $(aws configure export-credentials --profile corp --format env)
  delta-explain --env-creds s3://bucket/table -w "..."
  ```
- **Static keys** (MinIO, local development): pass them via `--option`, expanding from environment variables to keep secrets out of argv. Valid `--option` keys are passed through to the [`object_store`](https://docs.rs/object_store/) builders; see upstream docs for the per-backend list.

```bash
# S3 with credentials from the environment
delta-explain --env-creds s3://bucket/path/to/table -w "date = '2024-01-01'"

# S3 public bucket
delta-explain --region us-east-1 --public s3://my-public-bucket/table -w "id > 100"

# Azure
delta-explain --env-creds az://container/table -w "region = 'eu-west-1'"

# GCS (Workload Identity on GKE, or service account JSON via env)
delta-explain --env-creds gs://bucket/table -w "date = '2024-01-01'"

# S3-compatible (MinIO, Akamai, etc.); endpoint via --option, key/secret expanded from env
delta-explain \
    --option AWS_ENDPOINT=https://minio.local:9000 \
    --option AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    --option AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    s3://bucket/table -w "col > 5"
```

## CI/CD mode

`delta-explain` doubles as an assertion tool in pipelines. After your ETL writes a Delta table, verify that the pruning layout is healthy.

`--min-pruning`, `--assert-stats`, `--format json`, and `--verbose` are independent. Without `--verbose` the JSON document is summary-only; with it, a per-file `files` array is included (cap it with `--limit` on large tables).

### GitHub Action

The repo doubles as a composite action, so the gate is one step. Pin the tag: the action downloads a released binary, so the ref you pin is the behavior you get.

```yaml
- uses: cdelmonte-zg/delta-explain@v0.5.0
  with:
    table: s3://warehouse/events
    where: "country = 'DE' AND age > 40"
    min-pruning: "60"
    assert-stats: "true"
    env-creds: "true"
```

Inputs mirror the CLI flags (`table`, `where`, `min-pruning`, `assert-stats`, `at-version`, `env-creds`, `profile`, plus `options` as one `KEY=VALUE` per line, and `version` to pin a release; default `latest`). The step fails when a gate fails, and exposes `pruning-pct`, `final-files`, and `result` as outputs for later steps:

```yaml
- name: Comment the pruning percentage
  run: echo "Pruning ${{ steps.gate.outputs.pruning-pct }}%"
```

### Assert minimum pruning

Fail the pipeline if a predicate doesn't eliminate enough files:

```bash
delta-explain s3://warehouse/events -w "date = '2024-01-15'" --min-pruning 90
```

Exit code 1 if total pruning is below 90%.

The threshold is per-invocation, applied to the current predicate against the current snapshot. Calibrate it against a baseline pruning percentage in dev (set the gate a few points below it); a flat threshold across heterogeneous partitions will misfire. Note also that 100% pruning can signal a broken or unexpectedly empty predicate, so pair `--min-pruning` with a sanity check on `final_files > 0` when the workload is expected to read data.

### Assert statistics coverage

Fail if any file in the table is missing min/max statistics:

```bash
delta-explain s3://warehouse/events --assert-stats
```

Statistics are resolved through the kernel's log replay, checkpoint Parquet included, so a file is flagged only when its `add` action genuinely carries no statistics. Long-lived tables whose older commits have been consolidated into a checkpoint do not produce false positives.

### Predicate parity

The pruning percentage `delta-explain` reports reflects the predicate you pass to `-w`. If the runtime query wraps a column in `LOWER`, `CAST`, or a UDF, the engine may prune less than the gate suggests. Use a CI predicate that is semantically equivalent to the runtime predicate and explicitly track that equivalence: a gate on `country = 'DE'` does not automatically validate a production query using `LOWER(country) = 'de'`.

### JSON output for downstream processing

```bash
delta-explain ./my-table -w "country = 'DE'" --format json | jq '.total_pruning_pct'
```

The JSON output is versioned independently from the CLI binary (`schema_version: "0.2.0"`). The schema is pre-1.0: additive changes bump the minor version, breaking changes bump the major version. Consumers should branch on stable field names (e.g. assertion names), tolerate unknown fields, and check `schema_version`.

The contract is formal: [`schemas/report-v0.3.schema.json`](schemas/report-v0.3.schema.json) is a JSON Schema that the integration suite validates every emitted document against, and [`docs/json-schema.md`](docs/json-schema.md) explains each field, the stable note codes, and the meaning of `confidence`, `kept`, and `pruned_by`.

Exit code is `0` when all assertions pass and `1` if any fails; the JSON `result` field carries the per-assertion outcome.

See [CHANGELOG.md](CHANGELOG.md) for the full schema notes.

### Docker in a pipeline

```yaml
# GitHub Actions example
- name: Verify pruning after ETL
  run: |
    docker run --rm \
      -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
      ghcr.io/cdelmonte-zg/delta-explain:0.5.0 \
      --env-creds s3://warehouse/events \
      -w "date = '2024-01-15'" \
      --min-pruning 90 --assert-stats --format json
```

## How it works

`delta-explain` replays Delta metadata through [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) and runs separate metadata scans (no predicate, partition-safe fragment, full predicate) to isolate each pruning phase's contribution. No query engine is involved, no data files are read: only metadata. The full pipeline, the soundness guarantee, and the attribution rules are in [docs/semantics.md](docs/semantics.md).

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

Also supported: `IS [NOT] DISTINCT FROM`, `DATE '...'` / `TIMESTAMP '...'` literal forms, schema-driven coercion (a quoted `'2026-07-01'` against a `DATE` column just works, including `DECIMAL` and narrow integers), and `LIKE`: prefix patterns (`country LIKE 'D%'`) prune on partition values and on string min/max statistics, and on partition columns every other shape (`'%son'`, `_`, `NOT LIKE`) prunes exactly too. Subqueries, functions, and non-prefix `LIKE` on data columns are outside the pruning language: they warn and keep files instead of failing (see [Current limitations](#current-limitations)).

## Performance notes

delta-explain reads only the Delta log, never the parquet data files, so its cost scales with the number of `add` actions, not with data volume. Measured at 200k files on Linux, local disk, in the three log shapes that matter (generate them yourself with `cargo run --release --example gen_scale_log`):

| Log shape (200k files) | Baseline | With predicate | Peak memory |
|---|---|---|---|
| single JSON commit | ~1.0 s | ~1.3 s | ~280 MB |
| 2000 JSON commits | ~1.4 s | ~2.2 s | ~320 MB |
| 2000 commits + parquet checkpoint | ~0.8 s | ~1.0 s | ~240 MB |

The most production-like shape (checkpointed) is also the fastest: the kernel reads one parquet checkpoint instead of replaying thousands of JSON commits. Scaling is linear at roughly 1.5 KB of resident memory per file, which extrapolates to ~1.5 GB at one million files; that is the current practical ceiling and it is a known limitation, not a hidden one. Predicate complexity is immaterial at this level: an `IN` list with 500 items over 200k files adds ~0.4 s.

Output is the dimension to manage on large tables: the compact JSON stays summary-only at any size, and per-file detail (`--verbose`, in both formats) should be capped with `--limit`.

## Current limitations

- **First N indexed leaf columns only.** Delta collects min/max statistics only for the first `delta.dataSkippingNumIndexedCols` leaf fields (default 32, configurable per-table; nested struct children count separately).

  Predicates on columns past this index are still classified as `stats-safe` but contribute no pruning, because the column's min/max never appears in the log. (`stats.mode` reflects per-table coverage of the indexed columns, not per-predicate reachability, so it can read `exact` even when the predicate column is unreachable by stats.)

- **No query planner simulation.** This tool shows metadata-level file elimination only. It does not predict query execution time or replicate engine-specific optimizer behavior.

- **OR-mixed predicates.** Predicate classification operates on top-level AND conjuncts, after normalization: negations push down to the leaves (De Morgan) and conjuncts common to every OR branch factor out of the OR, so `NOT (country = 'DE' OR age > 30)` splits into two attributable phases and `(country = 'DE' AND x) OR (country = 'DE' AND y)` exposes `country = 'DE'` as partition-safe. What remains is the irreducibly mixed OR (`country = 'DE' OR age > 30`): it is flagged as `unsplittable` per the rule above, never silently downgraded.

- **Computed expressions keep all files.** Function calls, arithmetic, subqueries, column-to-column comparisons, `LIKE ... ESCAPE`, and non-prefix `LIKE` on data columns (leading or embedded wildcards, `_`, `NOT LIKE`) are outside the pruning language; such fragments are reported with an `UNSUPPORTED_EXPRESSION` warning and conservatively keep every file, while sibling AND conjuncts still prune. (On partition columns, `LIKE` in any shape prunes exactly instead.) Most of these are file-level unskippable for any engine.

- **`IN` pruning strength varies by engine.** delta-explain expands `IN` lists into OR-of-equalities, the strongest sound form, with no size cap. Real engines differ: DataFusion-based engines (delta-rs) do the same expansion but stop skipping past 20 list items, and delta-spark evaluates an imprecise range test over the whole list (`min(values) <= col <= max(values)`), which keeps more files on sparse lists. On `IN`-heavy predicates a specific engine may therefore prune less than this report shows; the report reflects what the metadata makes possible, and it is always sound.

- **Protocol features are declared, not compensated.** Deletion vectors, column mapping, and liquid clustering are detected and reported in `table_features` with explicit warnings, but the numbers are not adjusted: record counts still include soft-deleted rows on files with deletion vectors, verbose statistics may show physical column names under column mapping, and clustering columns are informational. On a fully checkpointed log (no JSON commits) liquid clustering goes undetected, because delta-kernel exposes no public accessor for system metadata domains.

See [VISION.md](VISION.md) for planned improvements.

## Development

To build and test from a fresh clone:

```bash
git clone https://github.com/cdelmonte-zg/delta-explain
cd delta-explain
cargo build
cargo test
```

The integration tests under `tests/` rely on pre-built Delta tables checked into the repo under `fixtures/`. They are real Delta tables, not synthetic blobs, so the tests exercise the kernel's actual scan planner.

### Regenerating the fixtures

The fixtures only need to be regenerated when you change their schema or the data they contain, for ordinary development you can ignore this step entirely.

The generator is a small Python script (`fixtures/create_test_table.py`) that uses `pyarrow` and `deltalake` to write the tables. Set up a virtual environment and install the pinned dependencies:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r fixtures/requirements.txt
```

Then run the generator:

```bash
python fixtures/create_test_table.py
```

The script skips any fixture directory that already exists; delete the directory you want to regenerate first, then re-run.

## Deep dive

For a detailed walkthrough of the architecture, design decisions, and the reasoning behind the two-phase model, see the companion article: [delta-explain: Making Delta Lake Pruning Visible](https://cdelmonte.dev/projects/delta-explain-making-delta-pruning-visible/).

## License

MIT

## Author

[Christian Del Monte](https://github.com/cdelmonte-zg)

`delta-explain` is built on [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) and focuses on making Delta-level file elimination visible.
