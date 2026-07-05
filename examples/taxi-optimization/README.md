# Tuning a Delta table with delta-explain

An executable notebook that uses `delta-explain` as the measuring
instrument for a table-layout optimization, on real NYC taxi data. It
walks the arc every table goes through - an unpartitioned pile, a wrong
partition column, the right one, and finally partitioning plus ordering
for data skipping - and at each step measures how much file pruning the
layout actually buys, before any query engine runs.

Every number in the notebook is produced by `delta-explain` on a real
Delta table the notebook writes; nothing is simulated.

## The result

| Layout | Files | date query | fare query | date + fare |
|---|---|---|---|---|
| 0. unpartitioned pile | 23 | 0% | 0% | 0% |
| 1. by `PULocationID` (wrong) | 240 | 3% | 16% | 18% |
| 2. by `pickup_date` (right) | 7 | 86% | 0% | 86% |
| 3. date partitions + fare-sorted files | 25 | 84% | 68% | 92% |

The lesson each row teaches: the baseline is hopeless; partitioning by a
high-cardinality column makes it *worse* (240 tiny files, still no date
pruning); the right partition column wins the date axis but not the fare
axis; and only layout-plus-ordering prunes on both - at a visible file-count
cost the tool lets you weigh instead of guess.

## Run it

Needs the `delta-explain` binary on `$PATH` (or `DX_BIN`), plus `deltalake`
and `pyarrow`:

```bash
pip install deltalake pyarrow jupyter
jupyter notebook examples/taxi-optimization/taxi-optimization.ipynb
```

The NYC TLC source (~48 MB, public domain) downloads once on first run; set
`TAXI_SRC=/path/to/yellow_tripdata_2024-01.parquet` to use a local copy. The
committed notebook already carries its executed outputs, so it reads without
running.
