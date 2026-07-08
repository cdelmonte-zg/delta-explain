# Tuning a table layout

A table layout is a bet on which queries matter. `delta-explain` makes the bet
measurable - *before* you run a query - so you can iterate on partitioning and
file layout with numbers instead of guesses.

The repo ships an executable notebook,
[`examples/taxi-optimization/`](https://github.com/cdelmonte-zg/delta-explain/blob/main/examples/taxi-optimization/taxi-optimization.ipynb),
that walks a real optimization on NYC taxi data. Every number in it is produced
by `delta-explain` on a real Delta table the notebook writes.

## The arc

Four layouts of the same trips, measured against three queries (a date filter, a
fare filter, both):

| Layout | Files | date | fare | date + fare |
|---|---|---|---|---|
| unpartitioned pile | 23 | 0% | 0% | 0% |
| partition by `PULocationID` (wrong) | 240 | 3% | 16% | 18% |
| partition by `pickup_date` (right) | 7 | 86% | 0% | 86% |
| date partitions + fare-sorted files | 25 | 84% | 68% | 92% |

The lessons the tool surfaces at each step:

1. **The baseline is hopeless** - every file spans the whole week, nothing
   prunes.
2. **Partitioning by the wrong (high-cardinality) column makes it worse** - 240
   tiny files, and the date query still does not prune.
3. **The right partition column** wins the date axis, and the tool shows exactly
   which queries it does *not* help.
4. **Partitioning plus ordering** prunes both axes - at a file-count cost the
   tool makes explicit instead of leaving to guesswork.

And the tool predicts step 4 from step 3: running `--explain-why` on the
date-partitioned layout with a fare query returns `WEAK_DATA_SKIPPING` - "order
by that column" - which is precisely the next move.

This is the shift from *file counter* to *pruning advisor*: for the queries you
care about, how many files does this layout eliminate, and what would improve
it?
