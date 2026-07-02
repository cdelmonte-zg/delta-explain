"""Create test Delta tables for delta-explain integration tests and the demo.

Produces (next to this script):
- ./test-table              partitioned, full stats (canonical fixture)
- ./test-table-partial-stats partitioned, half the files have no stats
                             (exercises the `stats.mode = "partial"` path)
- ./users                   demo table: same canonical data as test-table,
                             under the name the live demo uses
                             (`delta-explain ./users ...`); expect 83% pruned
- ./users-flat              demo table: flat layout, copied from the committed
                             synthetic test-table-flat fixture; expect 33% pruned
- ./test-table-nested       partitioned, with a `profile` struct column whose
                             leaves (age, score, geo.zip — two levels deep) carry
                             min/max stats — exercises data skipping on nested
                             (dotted) columns at arbitrary depth
- ./test-table-stats-budget  dataSkippingNumIndexedCols=4: a 5-leaf struct eats
                             the stats budget, so the trailing root column `tail`
                             gets no stats and cannot be skipped
- ./test-table-checkpointed  long-lived table after log cleanup: every `add`
                             action lives only inside the checkpoint Parquet,
                             no JSON commits remain — per-file stats must come
                             from the kernel's log replay
- ./test-table-checkpointed-struct  same shape, but the checkpoint carries
                             stats only as the structured `stats_parsed`
                             column (add.stats JSON nulled out) — the
                             delta.checkpoint.writeStatsAsJson=false layout.
                             Hand-crafted: deltalake always writes JSON stats,
                             so the checkpoint is rewritten post-hoc

Each table is regenerated only if its directory does not already exist,
so re-running this script after adding a new fixture is safe. Delete
the target directory by hand to force regeneration.

Note on the flat layout: ./test-table-flat is a hand-crafted synthetic
fixture. Its Delta-log statistics (interleaved `country` ranges that are NOT
physically present in the parquet files) are what make data skipping
ineffective and produce the 33% result. It therefore CANNOT be rebuilt from a
dataframe via write_deltalake — that would recompute the stats and change the
number. It is committed to git as the source of truth, and ./users-flat is
materialised by copying it verbatim.
"""
import json
import os
import shutil
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake

HERE = Path(__file__).resolve().parent
TABLE_PATH = str(HERE / "test-table")
PARTIAL_TABLE_PATH = str(HERE / "test-table-partial-stats")
FLAT_TABLE_PATH = str(HERE / "test-table-flat")
USERS_PATH = str(HERE / "users")
USERS_FLAT_PATH = str(HERE / "users-flat")
NESTED_TABLE_PATH = str(HERE / "test-table-nested")
BUDGET_TABLE_PATH = str(HERE / "test-table-stats-budget")
CHECKPOINTED_TABLE_PATH = str(HERE / "test-table-checkpointed")


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


# === users (demo: partitioned, expect 83% pruned) ========================
# Same canonical data as test-table, under the name the live demo uses:
#   delta-explain ./users -w "country = 'DE' AND age > 40"
# Partitioned by country, so `country = 'DE'` prunes whole partitions and
# `age > 40` then drops one more file via data skipping.

if not already_exists(USERS_PATH):
    write_deltalake(
        USERS_PATH,
        batches[0],
        partition_by=["country"],
        mode="overwrite",
    )
    for batch in batches[1:]:
        write_deltalake(
            USERS_PATH,
            batch,
            partition_by=["country"],
            mode="append",
        )
    print(f"Demo table created at {USERS_PATH}")
    print(f"  Partitioned by country (DE, US, IT) — expect 83% pruned")


# === users-flat (demo: flat layout, expect 33% pruned) ===================
# The flat demo table is a hand-crafted synthetic fixture (see the module
# docstring): its Delta-log statistics are what produce the 33% result, so it
# cannot be rebuilt from a dataframe. We materialise ./users-flat by copying
# the committed ./test-table-flat verbatim, which preserves those stats.

if not already_exists(USERS_FLAT_PATH):
    if not Path(FLAT_TABLE_PATH).exists():
        raise SystemExit(
            f"ERROR: {FLAT_TABLE_PATH} is missing. It is a committed synthetic "
            f"fixture and cannot be regenerated here — restore it from git:\n"
            f"  git checkout -- fixtures/test-table-flat"
        )
    shutil.copytree(FLAT_TABLE_PATH, USERS_FLAT_PATH)
    print(f"Demo table created at {USERS_FLAT_PATH}")
    print(f"  Flat copy of test-table-flat (no partitions) — expect 33% pruned")


# === test-table-nested (struct column, nested data skipping) =============
# A `profile` struct with an int leaf (age), a double leaf (score), and a
# *second-level* struct `geo` with an int leaf (zip). Delta records min/max for
# every leaf at any depth, so data skipping works on the dotted columns
# profile.age, profile.score, and profile.geo.zip. Partitioned by country, two
# files per partition with disjoint nested ranges so each leaf can rule files out.


def nested_batch(names, ages, scores, zips, country):
    geo = pa.StructArray.from_arrays([pa.array(zips, type=pa.int32())], names=["zip"])
    profile = pa.StructArray.from_arrays(
        [pa.array(ages, type=pa.int32()), pa.array(scores, type=pa.float64()), geo],
        names=["age", "score", "geo"],
    )
    return pa.table({
        "name": pa.array(names),
        "country": pa.array([country] * len(names)),
        "profile": profile,
    })


nested_batches = [
    nested_batch(["Hans", "Greta", "Klaus"], [25, 30, 35], [75.3, 92.0, 88.5], [1000, 1001, 1002], "DE"),
    nested_batch(["Dieter", "Helga"], [45, 60], [65.5, 70.0], [2000, 2001], "DE"),
    nested_batch(["Alice", "Bob"], [22, 29], [78.2, 95.0], [3000, 3001], "US"),
    nested_batch(["Eve", "Frank"], [45, 55], [71.5, 82.0], [4000, 4001], "US"),
    nested_batch(["Marco", "Sofia"], [28, 38], [76.8, 89.3], [5000, 5001], "IT"),
    nested_batch(["Giovanni", "Roberto"], [41, 65], [68.0, 80.1], [6000, 6001], "IT"),
]

if not already_exists(NESTED_TABLE_PATH):
    write_deltalake(
        NESTED_TABLE_PATH,
        nested_batches[0],
        partition_by=["country"],
        mode="overwrite",
    )
    for batch in nested_batches[1:]:
        write_deltalake(
            NESTED_TABLE_PATH,
            batch,
            partition_by=["country"],
            mode="append",
        )
    print(f"Nested-struct table created at {NESTED_TABLE_PATH}")
    print(f"  6 files across DE/US/IT; profile.age / profile.score / profile.geo.zip carry min/max")


# === test-table-stats-budget (nested leaves exhaust the stats budget) =====
# Delta indexes only the first `delta.dataSkippingNumIndexedCols` *leaf* columns
# (default 32), and each nested struct leaf counts as one. Here the property is
# lowered to 4 to demonstrate the mechanism compactly: struct `s` has 5 leaves
# (a..e), so a..d consume the budget and `s.e` plus the trailing root column
# `tail` get NO statistics. A predicate on `tail` therefore cannot be skipped,
# even though its values would allow it -- the budget was eaten by the struct.

INDEXED_COLS = 4


def budget_batch(base):
    leaves = [pa.array([base + i, base + i + 1], type=pa.int32()) for i in range(5)]
    s = pa.StructArray.from_arrays(leaves, names=list("abcde"))
    return pa.table({"s": s, "tail": pa.array([base, base + 100], type=pa.int32())})


if not already_exists(BUDGET_TABLE_PATH):
    write_deltalake(
        BUDGET_TABLE_PATH,
        budget_batch(0),
        mode="overwrite",
        configuration={"delta.dataSkippingNumIndexedCols": str(INDEXED_COLS)},
    )
    write_deltalake(BUDGET_TABLE_PATH, budget_batch(1000), mode="append")
    print(f"Stats-budget table created at {BUDGET_TABLE_PATH}")
    print(f"  dataSkippingNumIndexedCols={INDEXED_COLS}; s.a..s.d have stats, s.e and tail do not")


# === test-table-checkpointed (adds live only in checkpoint Parquet) =======
# Simulates a long-lived production table after log cleanup: three appends,
# a checkpoint at v2, then every JSON commit is deleted — the same shape
# `delta.logRetentionDuration` cleanup produces. All surviving `add` actions
# (and their stats) live only inside the checkpoint Parquet, so per-file
# statistics are only reachable through the kernel's log replay; reading the
# JSON commits directly finds nothing. Unpartitioned on purpose: partition
# columns are still read from the JSON metaData action, which this fixture
# no longer has.


def checkpointed_batch(names, ages):
    return pa.table({
        "name": pa.array(names),
        "age": pa.array(ages, type=pa.int32()),
    })


if not already_exists(CHECKPOINTED_TABLE_PATH):
    from deltalake import DeltaTable

    write_deltalake(
        CHECKPOINTED_TABLE_PATH,
        checkpointed_batch(["Hans", "Greta"], [20, 30]),
        mode="overwrite",
    )
    write_deltalake(
        CHECKPOINTED_TABLE_PATH,
        checkpointed_batch(["Dieter", "Helga"], [40, 50]),
        mode="append",
    )
    write_deltalake(
        CHECKPOINTED_TABLE_PATH,
        checkpointed_batch(["Alice", "Bob"], [60, 70]),
        mode="append",
    )
    DeltaTable(CHECKPOINTED_TABLE_PATH).create_checkpoint()
    removed = 0
    for f in (Path(CHECKPOINTED_TABLE_PATH) / "_delta_log").glob("*.json"):
        f.unlink()
        removed += 1
    print(f"Checkpointed table created at {CHECKPOINTED_TABLE_PATH}")
    print(f"  3 files, checkpoint at v2, {removed} JSON commits removed")


# === test-table-checkpointed-struct (stats_parsed only, no JSON stats) ====
# Same shape as test-table-checkpointed, but the checkpoint carries statistics
# only as the structured `stats_parsed` column: the layout a writer with
# delta.checkpoint.writeStatsAsJson=false produces. deltalake (delta-rs)
# always writes add.stats JSON and ignores that property, so the checkpoint is
# rewritten post-hoc: parse each add.stats JSON into a stats_parsed struct,
# then null the JSON field. delta-explain must request the parsed stats schema
# from the kernel (include_all_stats_columns) for these files to show stats.

CHECKPOINTED_STRUCT_TABLE_PATH = str(HERE / "test-table-checkpointed-struct")


def rewrite_checkpoint_stats_as_struct(table_path: str):
    import pyarrow.parquet as pq

    cp_path = next((Path(table_path) / "_delta_log").glob("*.checkpoint.parquet"))
    tbl = pq.read_table(cp_path)
    add = tbl.column("add").combine_chunks()

    # stats_parsed leaf types mirror the table schema (age int32, name string);
    # nullCount leaves are always long.
    mm_t = pa.struct([("age", pa.int32()), ("name", pa.string())])
    nc_t = pa.struct([("age", pa.int64()), ("name", pa.int64())])
    sp_t = pa.struct([
        ("numRecords", pa.int64()),
        ("minValues", mm_t),
        ("maxValues", mm_t),
        ("nullCount", nc_t),
    ])

    new_rows = []
    for i in range(len(add)):
        row = add[i].as_py()
        if row is None:
            new_rows.append(None)
            continue
        stats = json.loads(row["stats"])
        row = dict(row)
        row["stats"] = None
        row["stats_parsed"] = {
            "numRecords": stats["numRecords"],
            "minValues": stats["minValues"],
            "maxValues": stats["maxValues"],
            "nullCount": stats["nullCount"],
        }
        new_rows.append(row)

    old_t = add.type
    fields = [old_t.field(i) for i in range(old_t.num_fields)]
    fields.append(pa.field("stats_parsed", sp_t))
    new_add = pa.array(new_rows, type=pa.struct(fields))

    idx = tbl.schema.get_field_index("add")
    new_tbl = tbl.set_column(idx, pa.field("add", new_add.type), new_add)
    pq.write_table(new_tbl, cp_path)


if not already_exists(CHECKPOINTED_STRUCT_TABLE_PATH):
    from deltalake import DeltaTable

    write_deltalake(
        CHECKPOINTED_STRUCT_TABLE_PATH,
        checkpointed_batch(["Hans", "Greta"], [20, 30]),
        mode="overwrite",
    )
    write_deltalake(
        CHECKPOINTED_STRUCT_TABLE_PATH,
        checkpointed_batch(["Dieter", "Helga"], [40, 50]),
        mode="append",
    )
    write_deltalake(
        CHECKPOINTED_STRUCT_TABLE_PATH,
        checkpointed_batch(["Alice", "Bob"], [60, 70]),
        mode="append",
    )
    DeltaTable(CHECKPOINTED_STRUCT_TABLE_PATH).create_checkpoint()
    removed = 0
    for f in (Path(CHECKPOINTED_STRUCT_TABLE_PATH) / "_delta_log").glob("*.json"):
        f.unlink()
        removed += 1
    rewrite_checkpoint_stats_as_struct(CHECKPOINTED_STRUCT_TABLE_PATH)
    print(f"Checkpointed-struct table created at {CHECKPOINTED_STRUCT_TABLE_PATH}")
    print(f"  3 files, checkpoint at v2, {removed} JSON commits removed, stats moved to stats_parsed")
