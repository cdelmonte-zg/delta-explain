#!/usr/bin/env python3
"""Differential test: delta-explain's survivor set against Spark's reality.

For every (table, predicate) in the matrix:

1. Spark (in the container) computes which files actually CONTAIN matching
   rows, via input_file_name(): the ground truth.
2. delta-explain (on the host) reports which files SURVIVE pruning, read
   from the JSON per-file output (files[].kept).
3. Soundness assertion: ground truth is a subset of the survivor set. A file
   with matching rows that delta-explain pruned would be an unsound (wrong)
   answer. The reverse is fine: conservative min/max ranges legitimately keep
   files that turn out to contain no matches.

Two tables:
- `users`: synthetic, written by Spark (partitioned by country, age-banded).
- `taxi`: real NYC TLC yellow-taxi data, written by Spark from a public
  parquet (partitioned by pickup date). Validates the same soundness on a
  real writer's layout and statistics, and exercises the LIKE machinery
  against Spark on real partition values.

Usage:
    docker compose up -d          # in this directory (first Spark run
                                  # downloads jars, ~1 min)
    python3 run_differential.py   # delta-explain must be on PATH
"""
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORK = HERE / "work"
DX_BIN = os.environ.get("DX_BIN", "delta-explain")
CONTAINER = "dxdiff-spark"
DX_OPTIONS = [
    "--option", "aws_endpoint=http://localhost:9010",
    "--option", "aws_allow_http=true",
    "--option", "aws_access_key_id=minioadmin",
    "--option", "aws_secret_access_key=minioadmin",
    "--option", "aws_virtual_hosted_style_request=false",
    "--option", "aws_region=us-east-1",
]

# The public NYC TLC yellow-taxi file the taxi table is built from. Downloaded
# once into the shared work volume (gitignored); the container reads it from
# /home/jovyan/work. Set TAXI_SRC to a local copy to skip the download.
TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

TABLES = {
    "users": {
        "uri": "s3://diff/users",
        "predicates": [
            "country = 'DE'",
            "age > 55",
            "country = 'DE' AND age > 55",
            "country = 'DE' OR age > 60",
            "age BETWEEN 30 AND 40",
            "country IN ('DE', 'IT')",
            "score > 95.5",
            "NOT (country = 'DE')",
            "age > 55 AND score > 95.5",
            "age > 100",
            # normalization rewrites: De Morgan pushdown and OR factoring must
            # not change the survivor set
            "NOT (country = 'DE' OR age > 60)",
            "(country = 'DE' AND age > 55) OR (country = 'DE' AND score > 95.5)",
            # null-safe comparison, evaluated over partition values
            "country IS DISTINCT FROM 'DE'",
            # prefix LIKE rewrites to a lexicographic range during normalization
            "country LIKE 'D%'",
            "country LIKE 'D%' AND age > 55",
            "country LIKE 'DE'",
            # non-prefix LIKE on a partition column evaluates exactly against
            # the literal partition values
            "country LIKE '%E'",
            "country NOT LIKE 'D%'",
            "country LIKE '_E'",
            # the exact fragment must also constrain the final scan (chaining)
            "country LIKE '%E' AND age > 55",
        ],
    },
    "taxi": {
        "uri": "s3://diff/taxi",
        "predicates": [
            # pickup_date is the partition column: exact directory pruning
            "pickup_date = '2024-01-03'",
            "pickup_date IN ('2024-01-02', '2024-01-04')",
            "pickup_date > '2024-01-04'",
            # data skipping on real-writer min/max statistics
            "fare_amount > 60",
            "PULocationID = 132",
            # both axes at once
            "pickup_date = '2024-01-03' AND fare_amount > 60",
            # LIKE on the real partition column, against Spark's own LIKE:
            # prefix rewrites to a range, non-prefix evaluates exactly
            "pickup_date LIKE '2024-01-0%'",
            "pickup_date LIKE '%-03'",
            "pickup_date NOT LIKE '2024-01-0%'",
        ],
    },
}


def ensure_taxi_source():
    """Put the TLC parquet where the container can read it (work volume)."""
    local = os.environ.get("TAXI_SRC")
    dest = WORK / "taxi-src.parquet"
    if local:
        if not dest.exists():
            dest.symlink_to(Path(local).resolve())
        return
    if not dest.exists():
        print(f"downloading {TAXI_URL}")
        urllib.request.urlretrieve(TAXI_URL, dest)


def spark_ground_truth():
    ensure_taxi_source()
    # The config travels as a file on the shared volume: argv through docker
    # exec + bash -lc mangles SQL string-literal quoting.
    config = {name: t["predicates"] for name, t in TABLES.items()}
    with open(WORK / "predicates.json", "w") as f:
        json.dump(config, f)
    fresh = os.environ.get("DX_DIFF_FRESH", "")
    subprocess.run(
        [
            "docker", "exec", "-e", f"DX_DIFF_FRESH={fresh}",
            CONTAINER, "bash", "-lc",
            "cd /home/jovyan/work && $SPARK_HOME/bin/spark-submit "
            "--packages io.delta:delta-spark_2.13:4.3.0,"
            "org.apache.hadoop:hadoop-aws:3.4.2 "
            "spark_ground_truth.py",
        ],
        check=True,
    )
    with open(WORK / "ground_truth.json") as f:
        return json.load(f)


def dx_survivors(uri, predicate):
    """The kept files (survivor set) for one predicate, as basenames matching
    the ground truth's input_file_name() normalization."""
    proc = subprocess.run(
        [DX_BIN, uri, *DX_OPTIONS, "-w", predicate, "--format", "json", "--verbose"],
        capture_output=True, text=True, check=True,
    )
    report = json.loads(proc.stdout)
    files = report.get("files")
    if files is None:
        raise RuntimeError(f"no files array in output for {predicate!r}")
    return {f["path"].rsplit("/", 1)[-1] for f in files if f["kept"]}


def main():
    truth = spark_ground_truth()
    unsound = []
    n_predicates = 0

    for name, table in TABLES.items():
        results = truth[name]["results"]
        total = truth[name]["total_files"]
        print(f"\n=== {name}  ({total} files) ===")
        print(f"{'predicate':45} {'matches':>8} {'dx keeps':>9} sound")
        print("-" * 75)
        for entry in results:
            pred = entry["predicate"]
            matches = set(entry["match_files"])
            kept = dx_survivors(table["uri"], pred)
            missing = matches - kept
            ok = not missing
            if not ok:
                unsound.append((name, pred, sorted(missing)))
            n_predicates += 1
            print(f"{pred:45} {len(matches):>8} {len(kept):>9} {'YES' if ok else 'NO'}")

    print("\n" + "=" * 75)
    if unsound:
        print(f"UNSOUND on {len(unsound)} (table, predicate):")
        for name, pred, missing in unsound:
            print(f"  [{name}] {pred!r} pruned files that contain matches: {missing}")
        sys.exit(1)
    print(f"SOUND: delta-explain's survivor set covers every file with matching "
          f"rows,\non all {n_predicates} predicates across {len(TABLES)} tables.")


if __name__ == "__main__":
    main()
