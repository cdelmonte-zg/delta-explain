# delta-explain

**Make Delta pruning visible.**

A CLI that shows how partition pruning and data skipping reduce the set of candidate files in a Delta table.

Production-usable as a conservative Delta metadata diagnostic and CI guardrail, not yet a fully production-grade general-purpose Delta observability product. That line is meant literally: what the tool guarantees, and what it deliberately does not, is written down in [docs/semantics.md](docs/semantics.md).


**Documentation**: the full [documentation site](https://cdelmonte-zg.github.io/delta-explain/) (guides, reference, architecture) - or jump to the [three-minute quickstart](examples/quickstart/), [what delta-explain guarantees (and what it does not)](docs/semantics.md), [the JSON report, field by field](docs/json-schema.md), [what it is validated against](docs/validation.md), or [current limitations](#current-limitations).

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
  stats coverage:
    age [min_max]: 2/2 candidate files (100%)
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

The **Predicate Analysis** block shows how the predicate was split across the two pruning phases, and `confidence` labels how precisely the elimination can be explained (`exact` / `conservative` / `incomplete`). Under `stats-safe`, one line per column reports how many of the files entering the data-skipping phase actually carry the statistics that column needs (min/max for comparisons, null counts for `IS [NOT] NULL`): a file without them can never be skipped, so low coverage bounds what data skipping can do before any ranges are compared. The precise definitions, the degradation rules, and what each label guarantees are in [docs/semantics.md](docs/semantics.md).

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

```bash
brew tap cdelmonte-zg/tap && brew install delta-explain    # Homebrew (macOS, Linux)
pip install delta-explain                                  # PyPI: binary wheel + Python API
cargo install delta-explain                                # crates.io (Rust 1.88+)
```

The PyPI wheel ships the compiled binary (the `delta-explain` command works from the same environment) plus a thin Python API around the JSON contract:

```python
from delta_explain import explain

report = explain("s3://warehouse/events",
                 where="country = 'DE' AND age > 40",
                 min_pruning=80, env_creds=True)
report.passed              # gate outcome; False means the CLI would exit 1
report.total_pruning_pct
```

Every other route - pre-built binaries and `.deb` packages for six targets, Scoop, Docker (amd64 + arm64), from Git - is on the [install page](https://cdelmonte-zg.github.io/delta-explain/getting-started/install.html).

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
```

### Local table

```bash
delta-explain ./my-table -w "country = 'DE'"
delta-explain ./my-table -w "age > 30 AND country = 'IT'" --verbose
```

### On a real table

The repo ships `fixtures/taxi-nyc`, a small Delta table written from public
[NYC TLC yellow-taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
data, partitioned by pickup date - a realistic shape to see both pruning
axes on real column names:

```bash
# date is the partition column: directory-level pruning, exact
delta-explain fixtures/taxi-nyc -w "pickup_date = '2024-01-03'"

# date prunes partitions, fare prunes on min/max stats within them
delta-explain fixtures/taxi-nyc -w "pickup_date = '2024-01-03' AND fare_amount > 50"

# a predicate on a non-partition, non-clustered column (pickup zone):
# stats-safe, but the per-file ranges overlap, so nothing is eliminated -
# exactly the "why isn't this pruning" case the tool exists to show
delta-explain fixtures/taxi-nyc -w "PULocationID = 132" --verbose

# --explain-why turns that into advice:
delta-explain fixtures/taxi-nyc -w "PULocationID = 132" --explain-why
#   Why:
#     [NO_PARTITION_FILTER] ... filter on a partition column (pickup_date) ...
#     [WEAK_DATA_SKIPPING]  ... sort or cluster the table by that column ...
```

### Cloud storage

`s3://`, `az://`, and `gs://` URIs work with the provider's ambient credential chain (instance profile, Managed Identity, Workload Identity), environment variables (`--env-creds`), AWS shared-config profiles (`--profile`), or explicit `--option KEY=VALUE` pairs for S3-compatible stores:

```bash
delta-explain --env-creds s3://bucket/path/to/table -w "date = '2024-01-01'"
```

Per-provider recipes (Azure, GCS, MinIO, public buckets) and the credential resolution rules are in the [cloud storage guide](https://cdelmonte-zg.github.io/delta-explain/guides/cloud.html).

## CI/CD mode

`delta-explain` doubles as an assertion tool in pipelines. After your ETL writes a Delta table, verify that the pruning layout is healthy.

`--min-pruning`, `--assert-stats`, `--format json`, and `--verbose` are independent. Without `--verbose` the JSON document is summary-only; with it, a per-file `files` array is included (cap it with `--limit` on large tables).

### GitHub Action

The repo doubles as a composite action, so the gate is one step. Pin the tag: the action downloads a released binary, so the ref you pin is the behavior you get.

```yaml
- uses: cdelmonte-zg/delta-explain@v0.7.0
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

### Gates

`--min-pruning <PCT>` fails the run (exit 1) when total pruning falls below the threshold; `--assert-stats` fails it when any file in the snapshot is missing statistics:

```bash
delta-explain s3://warehouse/events -w "date = '2024-01-15'" --min-pruning 90 --assert-stats
```

How to calibrate the threshold, keep the CI predicate semantically equivalent to the runtime query, and run the same gate from Docker: [gating pruning in CI](https://cdelmonte-zg.github.io/delta-explain/guides/ci-gating.html).

### JSON output for downstream processing

```bash
delta-explain ./my-table -w "country = 'DE'" --format json | jq '.total_pruning_pct'
```

The JSON output is versioned independently from the CLI binary (`schema_version: "0.5.0"`). The schema is pre-1.0: additive changes bump the minor version, breaking changes bump the major version. Consumers should branch on stable field names (e.g. assertion names), tolerate unknown fields, and check `schema_version`.

The contract is formal: [`schemas/report-v0.5.schema.json`](schemas/report-v0.5.schema.json) is a JSON Schema that the integration suite validates every emitted document against, and [`docs/json-schema.md`](docs/json-schema.md) explains each field, the stable note codes, and the meaning of `confidence`, `kept`, and `pruned_by`.

Exit code is `0` when all assertions pass and `1` if any fails; the JSON `result` field carries the per-assertion outcome.

See [CHANGELOG.md](CHANGELOG.md) for the full schema notes.

## How it works

`delta-explain` replays Delta metadata through [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) and runs separate metadata scans (no predicate, partition-safe fragment, full predicate) to isolate each pruning phase's contribution. No query engine is involved, no data files are read: only metadata. The full pipeline, the soundness guarantee, and the attribution rules are in [docs/semantics.md](docs/semantics.md).

## Predicate syntax

Standard SQL WHERE-clause syntax, parsed via [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs): comparisons, `AND`/`OR`/`NOT`, `IN`, `BETWEEN`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, nested columns (`payload.age > 30`), typed and schema-coerced literals, and `LIKE` (prefix patterns prune on both axes; on partition columns every shape prunes exactly). The full list with examples is the [predicate syntax reference](https://cdelmonte-zg.github.io/delta-explain/reference/predicate-syntax.html). Subqueries, functions, and non-prefix `LIKE` on data columns are outside the pruning language: they warn and keep files instead of failing (see [Current limitations](#current-limitations)).

## Performance notes

delta-explain reads only the Delta log, never the parquet data files, so its cost scales with the number of `add` actions, not with data volume. Measured at 200k files on Linux, local disk, in the three log shapes that matter (generate them yourself with `cargo run --release --example gen_scale_log`):

| Log shape (200k files) | Baseline | With predicate | Peak memory |
|---|---|---|---|
| single JSON commit | ~1.0 s | ~1.3 s | ~280 MB |
| 2000 JSON commits | ~1.4 s | ~2.2 s | ~320 MB |
| 2000 commits + parquet checkpoint | ~0.8 s | ~1.0 s | ~240 MB |

The most production-like shape (checkpointed) is also the fastest: the kernel reads one parquet checkpoint instead of replaying thousands of JSON commits. Scaling is linear at roughly 1.5 KB of resident memory per file, which extrapolates to ~1.5 GB at one million files; that is the current practical ceiling and it is a known limitation, not a hidden one. Predicate complexity is immaterial at this level: an `IN` list with 500 items over 200k files adds ~0.4 s.

Data volume itself is invisible - what a big table costs is its log. A 10 TB profile (40k files x 256 MB, 31 stats columns, checkpointed; `--wide 30 --file-size-mb 256`) runs in ~1.5 s at ~470 MB; the pathological small-files version of the same 10 TB (160k files x 64 MB, same stats width) takes ~5 s at ~1.8 GB. Memory scales with files x stats leaves, so the one-million-file ceiling above assumes a narrow schema: wide production schemas reach it proportionally earlier, and `delta.dataSkippingNumIndexedCols` on the table bounds the width the log carries in the first place.

Output is the dimension to manage on large tables: the compact JSON stays summary-only at any size, and per-file detail (`--verbose`, in both formats) should be capped with `--limit`.

## Current limitations

- **First N indexed leaf columns only.** Delta collects min/max statistics only for the first `delta.dataSkippingNumIndexedCols` leaf fields (default 32, configurable per-table; nested struct children count separately).

  Predicates on columns past this index are still classified as `stats-safe` but contribute no pruning, because the column's min/max never appears in the log. The per-column `stats coverage` lines in the analysis make this visible per predicate column (a column past the budget shows zero covered files), while `stats.mode` keeps reflecting per-table coverage of the indexed columns, so it can read `exact` even when the predicate column is unreachable by stats.

- **No query planner simulation.** This tool shows metadata-level file elimination only. It does not predict query execution time or replicate engine-specific optimizer behavior.

- **OR-mixed predicates.** Predicate classification operates on top-level AND conjuncts, after normalization: negations push down to the leaves (De Morgan) and conjuncts common to every OR branch factor out of the OR, so `NOT (country = 'DE' OR age > 30)` splits into two attributable phases and `(country = 'DE' AND x) OR (country = 'DE' AND y)` exposes `country = 'DE'` as partition-safe. What remains is the irreducibly mixed OR (`country = 'DE' OR age > 30`): it is flagged as `unsplittable` per the rule above, never silently downgraded.

- **Computed expressions keep all files.** Function calls, arithmetic, subqueries, column-to-column comparisons, `LIKE ... ESCAPE`, and non-prefix `LIKE` on data columns (leading or embedded wildcards, `_`, `NOT LIKE`) are outside the pruning language; such fragments are reported with an `UNSUPPORTED_EXPRESSION` warning and conservatively keep every file, while sibling AND conjuncts still prune. (On partition columns, `LIKE` in any shape prunes exactly instead.) Most of these are file-level unskippable for any engine.

- **`IN` pruning strength varies by engine.** delta-explain expands `IN` lists into OR-of-equalities, the strongest sound form, with no size cap. Real engines differ: DataFusion-based engines (delta-rs) do the same expansion but stop skipping past 20 list items, and delta-spark evaluates an imprecise range test over the whole list (`min(values) <= col <= max(values)`), which keeps more files on sparse lists. On `IN`-heavy predicates a specific engine may therefore prune less than this report shows; the report reflects what the metadata makes possible, and it is always sound.

- **Protocol features are declared, not compensated.** Deletion vectors, column mapping, and liquid clustering are detected and reported in `table_features` with explicit warnings, but the numbers are not adjusted: record counts still include soft-deleted rows on files with deletion vectors, verbose statistics may show physical column names under column mapping, and clustering columns are informational. On a fully checkpointed log (no JSON commits) liquid clustering goes undetected, because delta-kernel exposes no public accessor for system metadata domains.

See [VISION.md](VISION.md) for planned improvements.

## Development

```bash
git clone https://github.com/cdelmonte-zg/delta-explain
cd delta-explain
cargo build && cargo test
```

The integration tests rely on real Delta tables checked into `fixtures/` (not synthetic blobs), so they exercise the kernel's actual scan planner. The full contributor guide - architecture, testing conventions, fixture regeneration - is [DEVELOPMENT.md](DEVELOPMENT.md).

## Deep dive

For a detailed walkthrough of the architecture, design decisions, and the reasoning behind the two-phase model, see the companion article: [delta-explain: Making Delta Lake Pruning Visible](https://cdelmonte.dev/projects/delta-explain-making-delta-pruning-visible/).

## License

MIT

## Author

[Christian Del Monte](https://github.com/cdelmonte-zg)

`delta-explain` is built on [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) and focuses on making Delta-level file elimination visible.
