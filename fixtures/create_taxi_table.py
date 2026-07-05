#!/usr/bin/env python3
"""Write `taxi-nyc`: a small, real Delta table from NYC TLC yellow-taxi data.

Unlike the synthetic fixtures, this one is written by a real Delta writer
(deltalake) from a public production dataset, so its log carries the
partition layout and per-file statistics a real table has - the kind of
shape LogBuilder cannot synthesize. It is the recognizable table behind
the quickstart and the `taxi` integration tests.

Data source and attribution: NYC Taxi & Limousine Commission (TLC) trip
record data, January 2024 yellow taxi
(https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The TLC
publishes these files for public use under its data usage terms linked
on that page; this fixture redistributes a small derived subset for
testing. We keep a deterministic subset (the first N trips of the first
few days), a tidy column set, and partition by pickup date so the table
stays a few hundred KB - delta-explain never reads the parquet data, but
the fixture is checked in, so the data files must be small.

Regenerate (from the repo root, with the deltalake writer available):

    python fixtures/create_taxi_table.py           # downloads the source
    TAXI_SRC=/path/to/yellow_tripdata_2024-01.parquet \
        python fixtures/create_taxi_table.py       # or point at a local copy

The script refuses to overwrite an existing fixtures/taxi-nyc.
"""

import os
import shutil
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from deltalake import write_deltalake

HERE = Path(__file__).resolve().parent
DEST = HERE / "taxi-nyc"
SRC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

# Deterministic subset: the first few pickup dates, capped per day, so the
# table has several date partitions with a handful of files each.
KEEP_DATES = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
ROWS_PER_DATE = 800

# The columns worth keeping: an identity, the times, the geography
# (PULocationID zone), and the money columns that make fare/tip predicates
# meaningful. Names are the real TLC names, so a reader recognizes them.
COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]


def main() -> None:
    if DEST.exists():
        raise SystemExit(f"{DEST} already exists, refusing to overwrite")

    src = os.environ.get("TAXI_SRC")
    if src is None:
        src = str(HERE / ".taxi-src.parquet")
        if not Path(src).exists():
            print(f"downloading {SRC_URL}")
            urllib.request.urlretrieve(SRC_URL, src)

    table = pq.read_table(src, columns=COLUMNS)

    # Derive the pickup_date partition column and keep only the target days.
    dates = pc.strftime(table["tpep_pickup_datetime"], format="%Y-%m-%d")
    table = table.append_column("pickup_date", dates)
    table = table.filter(pc.is_in(table["pickup_date"], value_set=pa.array(KEEP_DATES)))

    # Cap each day deterministically (stable sort by pickup time, then head
    # per date), so the same rows are selected on every run. (The rows are
    # reproducible; the on-disk table is not byte-identical - deltalake
    # assigns random parquet file names and timestamps each write.)
    table = table.sort_by([("pickup_date", "ascending"), ("tpep_pickup_datetime", "ascending")])
    keep_idx = []
    seen: dict[str, int] = {}
    date_col = table["pickup_date"].to_pylist()
    for i, d in enumerate(date_col):
        n = seen.get(d, 0)
        if n < ROWS_PER_DATE:
            keep_idx.append(i)
            seen[d] = n + 1
    table = table.take(pa.array(keep_idx))

    write_deltalake(str(DEST), table, partition_by=["pickup_date"], mode="error")

    rows = table.num_rows
    size = sum(f.stat().st_size for f in DEST.rglob("*") if f.is_file())
    print(f"wrote {rows} rows across {len(KEEP_DATES)} date partitions to {DEST}")
    print(f"table size: {size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
