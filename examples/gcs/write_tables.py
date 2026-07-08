#!/usr/bin/env python3
"""Write two Delta tables to a real Google Cloud Storage bucket so you can try
delta-explain against gs:// storage.

Same logical rows, two physical layouts (partitioned vs flat): the partitioned
one prunes well, the flat one barely prunes at all.

Auth: set GOOGLE_SERVICE_ACCOUNT to the path of a service-account JSON key with
write access to the bucket, and GCS_BUCKET to the bucket name. Run:

    pip install -r requirements.txt
    export GCS_BUCKET=your-bucket
    export GOOGLE_SERVICE_ACCOUNT=/path/to/key.json
    python write_tables.py
"""
import os
import random

import pyarrow as pa
from deltalake import write_deltalake

BUCKET = os.environ["GCS_BUCKET"]
KEY = os.environ["GOOGLE_SERVICE_ACCOUNT"]
STORAGE_OPTIONS = {"google_service_account": KEY}

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


users = f"gs://{BUCKET}/lake/users"
users_flat = f"gs://{BUCKET}/lake/users-flat"

# partitioned by country; each file a narrow age band
first = True
for c in COUNTRIES:
    crows = sorted([r for r in rows if r["country"] == c], key=lambda r: r["age"])
    chunk = max(1, len(crows) // 4)
    for k in range(0, len(crows), chunk):
        write_deltalake(users, table(crows[k:k + chunk]), partition_by=["country"],
                        mode="overwrite" if first else "append",
                        storage_options=STORAGE_OPTIONS)
        first = False
print(f"wrote {users}        (partitioned, age-banded)")

# flat, shuffled into 6 files
random.shuffle(rows)
chunk = N // 6
write_deltalake(users_flat, table(rows[:chunk]), mode="overwrite",
                storage_options=STORAGE_OPTIONS)
for k in range(1, 6):
    write_deltalake(users_flat, table(rows[k * chunk:(k + 1) * chunk]), mode="append",
                    storage_options=STORAGE_OPTIONS)
print(f"wrote {users_flat}   (flat, shuffled)")
