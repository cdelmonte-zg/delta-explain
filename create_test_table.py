"""Create test Delta tables for delta-explain integration tests.

Produces:
- ./test-table              partitioned, full stats (canonical fixture)
- ./test-table-partial-stats partitioned, half the files have no stats
                             (exercises the `stats.mode = "partial"` path)

Each table is regenerated only if its directory does not already exist,
so re-running this script after adding a new fixture is safe. Delete
the target directory by hand to force regeneration.
"""
import json
import os
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake

TABLE_PATH = "./test-table"
PARTIAL_TABLE_PATH = "./test-table-partial-stats"


def already_exists(path: str) -> bool:
    if Path(path).exists():
        print(f"Skipping {path}: directory already exists. Delete it to regenerate.")
        return True
    return False


# === test-table (canonical) ==============================================
# We write multiple batches to get separate parquet files with different min/max stats.
# Partition by country => 3 partitions: DE, US, IT

batches = [
    # DE partition - file 1: age 20-35
    pa.table({
        "name": pa.array(["Hans", "Greta", "Klaus", "Liesel", "Fritz"]),
        "age": pa.array([25, 30, 35, 20, 28], type=pa.int32()),
        "country": pa.array(["DE", "DE", "DE", "DE", "DE"]),
        "score": pa.array([88.5, 92.0, 75.3, 91.2, 83.1], type=pa.float64()),
    }),
    # DE partition - file 2: age 40-60
    pa.table({
        "name": pa.array(["Dieter", "Helga", "Wolfgang", "Ursula"]),
        "age": pa.array([45, 52, 60, 40], type=pa.int32()),
        "country": pa.array(["DE", "DE", "DE", "DE"]),
        "score": pa.array([70.0, 65.5, 88.9, 77.3], type=pa.float64()),
    }),
    # US partition - file 1: age 18-29
    pa.table({
        "name": pa.array(["Alice", "Bob", "Charlie", "Diana"]),
        "age": pa.array([22, 18, 29, 25], type=pa.int32()),
        "country": pa.array(["US", "US", "US", "US"]),
        "score": pa.array([95.0, 78.2, 85.6, 90.1], type=pa.float64()),
    }),
    # US partition - file 2: age 31-55
    pa.table({
        "name": pa.array(["Eve", "Frank", "Grace"]),
        "age": pa.array([31, 45, 55], type=pa.int32()),
        "country": pa.array(["US", "US", "US"]),
        "score": pa.array([82.0, 71.5, 93.4], type=pa.float64()),
    }),
    # IT partition - file 1: age 22-38
    pa.table({
        "name": pa.array(["Marco", "Giulia", "Luca", "Sofia", "Alessandro"]),
        "age": pa.array([22, 35, 28, 38, 30], type=pa.int32()),
        "country": pa.array(["IT", "IT", "IT", "IT", "IT"]),
        "score": pa.array([87.0, 91.5, 76.8, 84.2, 89.3], type=pa.float64()),
    }),
    # IT partition - file 2: age 41-65
    pa.table({
        "name": pa.array(["Giovanni", "Maria", "Roberto"]),
        "age": pa.array([41, 58, 65], type=pa.int32()),
        "country": pa.array(["IT", "IT", "IT"]),
        "score": pa.array([68.0, 72.5, 80.1], type=pa.float64()),
    }),
]

if not already_exists(TABLE_PATH):
    write_deltalake(
        TABLE_PATH,
        batches[0],
        partition_by=["country"],
        mode="overwrite",
    )
    for batch in batches[1:]:
        write_deltalake(
            TABLE_PATH,
            batch,
            partition_by=["country"],
            mode="append",
        )
    print(f"Test table created at {TABLE_PATH}")
    print(f"  Partitions: DE, US, IT")
    print(f"  Total batches written: {len(batches)} (each as separate file)")
    print(f"  Age ranges per file vary to enable data skipping")


# === test-table-partial-stats ============================================
#
# Purpose: exercise the `stats.mode = "partial"` code path. Half of the files
# carry per-column min/max statistics in the Delta log, the other half do not.
# Schema matches `test-table` for reuse in tests.

partial_batches = [
    # DE partition - file 1 (stats will be stripped after write)
    pa.table({
        "name": pa.array(["Hans", "Greta", "Klaus"]),
        "age": pa.array([25, 30, 35], type=pa.int32()),
        "country": pa.array(["DE", "DE", "DE"]),
        "score": pa.array([88.5, 92.0, 75.3], type=pa.float64()),
    }),
    # DE partition - file 2 (stats kept)
    pa.table({
        "name": pa.array(["Dieter", "Helga"]),
        "age": pa.array([45, 52], type=pa.int32()),
        "country": pa.array(["DE", "DE"]),
        "score": pa.array([70.0, 65.5], type=pa.float64()),
    }),
    # IT partition - file 1 (stats will be stripped after write)
    pa.table({
        "name": pa.array(["Marco", "Giulia"]),
        "age": pa.array([22, 35], type=pa.int32()),
        "country": pa.array(["IT", "IT"]),
        "score": pa.array([87.0, 91.5], type=pa.float64()),
    }),
    # IT partition - file 2 (stats kept)
    pa.table({
        "name": pa.array(["Giovanni", "Maria"]),
        "age": pa.array([41, 58], type=pa.int32()),
        "country": pa.array(["IT", "IT"]),
        "score": pa.array([68.0, 72.5], type=pa.float64()),
    }),
]

if not already_exists(PARTIAL_TABLE_PATH):
    write_deltalake(
        PARTIAL_TABLE_PATH,
        partial_batches[0],
        partition_by=["country"],
        mode="overwrite",
    )
    for batch in partial_batches[1:]:
        write_deltalake(
            PARTIAL_TABLE_PATH,
            batch,
            partition_by=["country"],
            mode="append",
        )

    # Strip the `stats` field from half the Add actions: commit 0 and commit 2.
    # Each append produces one Add per commit, so we rewrite those two commits
    # in-place.
    log_dir = Path(PARTIAL_TABLE_PATH) / "_delta_log"
    strip_versions = {"00000000000000000000.json", "00000000000000000002.json"}

    stripped = 0
    for fname in sorted(os.listdir(log_dir)):
        if fname not in strip_versions:
            continue
        path = log_dir / fname
        with open(path) as f:
            actions = [json.loads(line) for line in f if line.strip()]
        rewrote = False
        for action in actions:
            if "add" in action and "stats" in action["add"]:
                del action["add"]["stats"]
                rewrote = True
        if rewrote:
            with open(path, "w") as f:
                for action in actions:
                    f.write(json.dumps(action, separators=(",", ":")) + "\n")
            stripped += 1

    print(f"Partial-stats table created at {PARTIAL_TABLE_PATH}")
    print(f"  Files: {len(partial_batches)} ({stripped} without stats, {len(partial_batches) - stripped} with stats)")
    print(f"  stats.mode should be \"partial\"")
