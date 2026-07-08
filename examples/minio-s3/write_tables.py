#!/usr/bin/env python3
"""Write two Delta tables to the local MinIO 'lake' bucket so you can try
delta-explain against S3-compatible storage.

Both tables hold the same logical rows; only the physical layout differs:

  s3://lake/users        partitioned by country, files sorted into age bands
                         -> partition pruning + data skipping both apply
  s3://lake/users-flat   no partitioning, rows shuffled across files
                         -> every file spans everything, nothing can be skipped

Run after `docker compose up -d`:
    pip install -r requirements.txt
    python write_tables.py
"""
import random

import pyarrow as pa
from deltalake import write_deltalake

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": "http://127.0.0.1:9000",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
}

random.seed(42)
COUNTRIES = ["DE", "US", "IT"]
N = 6000
rows = [
    {"name": f"user{i}", "age": random.randint(18, 70),
     "country": random.choice(COUNTRIES), "score": round(random.uniform(60, 99), 1)}
    for i in range(N)
]


def table(rs):
    return pa.table({
        "name": pa.array([r["name"] for r in rs]),
        "age": pa.array([r["age"] for r in rs], pa.int32()),
        "country": pa.array([r["country"] for r in rs]),
        "score": pa.array([r["score"] for r in rs]),
    })


# s3://lake/users: partitioned by country; each file a narrow age band
first = True
for c in COUNTRIES:
    crows = sorted([r for r in rows if r["country"] == c], key=lambda r: r["age"])
    chunk = max(1, len(crows) // 4)
    for k in range(0, len(crows), chunk):
        write_deltalake("s3://lake/users", table(crows[k:k + chunk]),
                        partition_by=["country"],
                        mode="overwrite" if first else "append",
                        storage_options=STORAGE_OPTIONS)
        first = False
print("wrote s3://lake/users        (partitioned, age-banded)")

# s3://lake/users-flat: flat, shuffled into 6 files
random.shuffle(rows)
chunk = N // 6
write_deltalake("s3://lake/users-flat", table(rows[:chunk]),
                mode="overwrite", storage_options=STORAGE_OPTIONS)
for k in range(1, 6):
    write_deltalake("s3://lake/users-flat", table(rows[k * chunk:(k + 1) * chunk]),
                    mode="append", storage_options=STORAGE_OPTIONS)
print("wrote s3://lake/users-flat   (flat, shuffled)")
