#!/usr/bin/env python3
"""Differential test: delta-explain's survivor set against Spark's reality.

For every predicate in the matrix:

1. Spark (in the container) computes which files actually CONTAIN matching
   rows, via input_file_name(): the ground truth.
2. delta-explain (on the host) reports which files SURVIVE pruning, read
   from the JSON per-file output (files[].kept).
3. Soundness assertion: ground truth is a subset of the survivor set. A file
   with matching rows that delta-explain pruned would be an unsound (wrong)
   answer. The reverse is fine: conservative min/max ranges legitimately keep
   files that turn out to contain no matches.

Usage:
    docker compose up -d          # in this directory (first Spark run
                                  # downloads jars, ~1 min)
    python3 run_differential.py   # delta-explain must be on PATH
"""
import json
import os

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DX_BIN = os.environ.get("DX_BIN", "delta-explain")
CONTAINER = "dxdiff-spark"
TABLE_URI = "s3://diff/users"
DX_OPTIONS = [
    "--option", "aws_endpoint=http://localhost:9010",
    "--option", "aws_allow_http=true",
    "--option", "aws_access_key_id=minioadmin",
    "--option", "aws_secret_access_key=minioadmin",
    "--option", "aws_virtual_hosted_style_request=false",
    "--option", "aws_region=us-east-1",
]

PREDICATES = [
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
    # prefix LIKE rewrites to a lexicographic range during normalization;
    # the survivor set must stay a superset of Spark's matching files
    "country LIKE 'D%'",
    "country LIKE 'D%' AND age > 55",
    "country LIKE 'DE'",
    # non-prefix LIKE on a partition column evaluates exactly against the
    # literal partition values; the survivor set must still cover Spark's
    "country LIKE '%E'",
    "country NOT LIKE 'D%'",
    "country LIKE '_E'",
    # the exact fragment must also constrain the final scan (phase chaining)
    "country LIKE '%E' AND age > 55",
]


def spark_ground_truth():
    # The predicate list travels as a file on the shared volume: argv through
    # docker exec + bash -lc mangles SQL string-literal quoting.
    with open(HERE / "work" / "predicates.json", "w") as f:
        json.dump(PREDICATES, f)
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
    with open(HERE / "work" / "ground_truth.json") as f:
        return json.load(f)


def dx_survivors(predicate):
    """Run delta-explain with JSON per-file output and collect the kept
    files (the survivor set of the whole predicate). Basenames, matching
    the ground truth's input_file_name() normalization."""
    proc = subprocess.run(
        [DX_BIN, TABLE_URI, *DX_OPTIONS, "-w", predicate, "--format", "json", "--verbose"],
        capture_output=True,
        text=True,
        check=True,
    )
    report = json.loads(proc.stdout)
    files = report.get("files")
    if files is None:
        raise RuntimeError(f"no files array in output for {predicate!r}")
    return {f["path"].rsplit("/", 1)[-1] for f in files if f["kept"]}


def main():
    truth = spark_ground_truth()
    total = truth["total_files"]

    print(f"\n{'predicate':45} {'matches':>8} {'dx keeps':>9} sound")
    print("-" * 75)
    unsound = []
    for entry in truth["results"]:
        pred = entry["predicate"]
        matches = set(entry["match_files"])
        kept = dx_survivors(pred)
        missing = matches - kept
        ok = not missing
        if not ok:
            unsound.append((pred, sorted(missing)))
        print(f"{pred:45} {len(matches):>8} {len(kept):>9} {'YES' if ok else 'NO'}")

    print("-" * 75)
    if unsound:
        print(f"\nUNSOUND on {len(unsound)} predicate(s):")
        for pred, missing in unsound:
            print(f"  {pred!r} pruned files that contain matches: {missing}")
        sys.exit(1)
    print(f"\nSOUND: delta-explain's survivor set covers every file with "
          f"matching rows,\non all {len(truth['results'])} predicates "
          f"({total} files in the table).")


if __name__ == "__main__":
    main()
