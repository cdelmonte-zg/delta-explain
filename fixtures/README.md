# Fixture registry

Checked-in Delta tables used by the integration suite. Every table is a real
Delta log (JSON commits, some with checkpoint Parquet); the parquet data
files exist but are never read by delta-explain.

Prefer the synthetic route for new scenarios: `LogBuilder` in
`tests/integration/common/mod.rs` writes a temp-dir Delta log in a few lines
(arbitrary file counts, partition layouts, stats gaps, protocol features)
with nothing to check in. A checked-in fixture is worth it only when the log
shape cannot be synthesized trivially: checkpoint Parquet layouts, output of
real writers, hand-broken edge cases.

Regeneration: `fixtures/create_test_table.py` (deps in `requirements.txt`); `taxi-nyc` from `fixtures/create_taxi_table.py` (downloads a public NYC TLC file, or point `TAXI_SRC` at a local copy); the exotic checkpoint pair comes from `fixtures/create_exotic_checkpoints.py`, run inside the differential harness's Spark container (see its docstring);
the script skips directories that already exist. The checkpoint-only
variants were derived by hand from generated tables (JSON commits removed,
in one case the checkpoint rewritten); the script cannot rebuild those.

| Fixture | Layout | Stats | Used by (tests/integration/) |
|---|---|---|---|
| `test-table` | partitioned by `country` (DE/US/IT), 6 files, versions 0-5 | full | `cli`, `semantic_regression`, `time_travel`, `partition_columns`, `like_rewrite`, `partition_exact`, `json_contract`, `protocol_features`, `debug_ir` |
| `test-table-flat` | non-partitioned twin of `test-table`, 6 files | full (hand-crafted) | `cli`, `partition_columns` |
| `test-table-empty` | partition metadata only, zero data files | n/a | `partition_columns`, `semantic_regression` |
| `test-table-partial-stats` | partitioned, 4 files, 2 with `stats` stripped | partial | `partial_stats`, `semantic_regression` |
| `test-table-nested` | struct column `profile` (int + double leaves), 6 files | per-leaf | `nested_stats` |
| `test-table-stats-budget` | 5-leaf struct + root col, `dataSkippingNumIndexedCols=4`, 2 files | truncated by budget | `stats_budget` |
| `test-table-temporal` | partitioned by DATE, TIMESTAMP/DECIMAL/INT16/INT8 ranges, 6 files | full | `temporal_coercions` |
| `test-table-checkpointed` | checkpoint-only log (JSON commits removed), flat, `add.stats` JSON | full | `checkpoint_stats` |
| `test-table-checkpointed-struct` | checkpoint-only, `stats_parsed` struct only (hand-rewritten checkpoint) | full | `checkpoint_stats` |
| `test-table-checkpointed-part` | checkpoint-only, partitioned | full | `checkpoint_partition_columns` |
| `test-table-checkpoint-multipart` | classic checkpoint split in two parts (`delta.checkpoint.partSize=2`), 3 commits, Spark-written | full | `exotic_logs` |
| `test-table-checkpoint-v2` | V2 checkpoint: UUID-named JSON manifest + parquet sidecars in `_sidecars/` (`delta.checkpointPolicy=v2`), Spark-written | full | `exotic_logs` |
| `taxi-nyc` | real NYC TLC yellow-taxi data (deltalake-written), partitioned by `pickup_date`, 5 date partitions, 4000 rows | full (real writer) | `taxi` |

`taxi-nyc` data source: NYC Taxi & Limousine Commission (TLC) trip record data
([terms](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)); a small
derived subset is redistributed here for testing.
| `users` / `users-flat` | demo tables, same canonical data as `test-table` | full | README examples, not tests |
