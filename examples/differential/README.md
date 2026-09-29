# Differential testing: delta-explain vs Spark

The strongest claim a pruning-attribution tool can make is that its survivor
set agrees with what an engine actually needs. This harness checks exactly
that, on a real object store (MinIO locally, S3 in the weekly validation)
with a real engine (Spark + Delta), across two tables:

- **`users`**: synthetic, written by Spark (partitioned by country, age-banded
  files) - a controlled layout with predictable selectivity.
- **`taxi`**: real NYC TLC yellow-taxi data, written by Spark from a public
  parquet (partitioned by pickup date, fare-sorted files) - a real writer's
  layout and statistics, and the same soundness on data we did not shape.

For every (table, predicate):

1. **Spark computes the ground truth**: the set of files that actually
   *contain matching rows* (`input_file_name()` over the filtered table).
2. **delta-explain reports its survivor set**: the `kept` files of the final
   pruning phase, parsed from `--format json --verbose`.
3. **Soundness assertion**: ground truth ⊆ survivor set, for every predicate.
   A file with matching rows that delta-explain pruned would be a wrong
   answer. The reverse is legitimate: conservative min/max ranges keep files
   that turn out to contain no matches.

## Run it

```bash
docker compose up -d        # MinIO on :9010, Spark container (first run
                            # downloads Delta + hadoop-aws jars, ~1 min)
python3 run_differential.py # delta-explain on PATH, or DX_BIN=/path/to/bin
```

The tables are written once and reused across runs. After changing a layout
in `spark_ground_truth.py`, or on a stale MinIO volume, force a rewrite with
`DX_DIFF_FRESH=1 python3 run_differential.py`. The taxi table is built from a
public NYC TLC file downloaded once into `work/` (gitignored); set `TAXI_SRC`
to a local copy to skip the download.

The MinIO images come from `ghcr.io/cdelmonte-zg/minio` and `minio-mc`, a
private mirror of the last community-edition builds: MinIO archived the
project and no public registry serves its images any more. Pulling them
needs a GitHub token with `read:packages` and access to the packages
(`gh auth token | docker login ghcr.io -u <user> --password-stdin`). They
are frozen artifacts, fine for a local S3 API, not something to run in
production, and not redistributed for that reason. Any S3-compatible
server works in their place: point the `minio` service at it and create
the bucket by other means.

### Against real S3

The weekly Validation workflow runs the same harness against an AWS bucket
instead of MinIO, so the oracle does not depend on a container registry:

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=eu-central-1
docker compose up -d --no-deps spark
DX_DIFF_REAL_S3=1 DX_DIFF_S3_PREFIX=s3://my-bucket/diff \
  DX_DIFF_FRESH=1 python3 run_differential.py
```

delta-explain then reads with `--env-creds`, Spark writes through the
default AWS endpoint, and both tables land under the prefix. Nothing else
in the bucket is touched. The workflow empties the prefix before and after
each run; a local run leaves its tables in place.

The matrix covers equality and ranges on partition and data columns, AND/OR
mixes (including the `unsplittable` OR case), `IN`, `BETWEEN`, `NOT`, a
floating-point bound, an empty-result predicate, and `LIKE` in every shape -
prefix (rewritten to a range) and non-prefix (evaluated against partition
values), the latter checked against Spark's own `LIKE` on the real taxi
partition column.

## Results (2026-07-05, MinIO, Spark 4.1.2 + Delta 4.3)

```
=== users  (17 files) ===
country = 'DE'                                       5         5 YES
... (20 predicates)                                                YES
=== taxi  (34 files, real NYC TLC data) ===
pickup_date = '2024-01-03'                           5         5 YES
fare_amount > 60                                    11        11 YES
pickup_date = '2024-01-03' AND fare_amount > 60      2         2 YES
pickup_date LIKE '2024-01-0%'                       34        34 YES
pickup_date LIKE '%-03'                              5         5 YES
pickup_date NOT LIKE '2024-01-0%'                    0         0 YES
---------------------------------------------------------------------------
SOUND: on all 29 predicates across 2 tables.
```

Sound on every predicate, and on many of them exact - notably `LIKE '%-03'`
on the real partition column, where delta-explain's partition-literal
evaluator keeps exactly the five files Spark's own `LIKE` matches, and
`fare_amount > 60`, where data skipping on the real writer's statistics keeps
exactly the eleven matching files.

## What it does not check

Attribution (which phase gets credit) has no engine-side ground truth to
compare against: engines do not expose per-mechanism file elimination. The
harness checks the end result, soundness of the survivor set; attribution
correctness is covered by the unit and integration tests in the repo.
