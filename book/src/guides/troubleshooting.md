# Why isn't my Delta query pruning files?

Your query filters on a column, but the engine still reads the whole table — the
scan is slow, or the cloud bill is bigger than it should be. This page is a
diagnostic playbook: find the symptom, get the cause, apply the fix. Every step
is something `delta-explain` measures from the transaction log, without running
a query.

## First: measure what is actually happening

Point `delta-explain` at the table with your predicate and read the total
pruning:

```bash
delta-explain ./your-table -w "your = 'predicate' AND ..."
```

If the total reduction is near **0%**, the query really is scanning everything.
Now ask *why*:

```bash
delta-explain ./your-table -w "..." --explain-why
```

The `Why:` section names the exact reason — one of the four cases below. Each is
a deterministic reading of your table's metadata, not a guess. (More on the
flag: [The `--explain-why` advisor](explain-why.md).)

## Symptom: partition pruning never runs

```text
[NO_PARTITION_FILTER] The table is partitioned by pickup_date, but the predicate
  filters on none of those columns, so partition pruning cannot run.
```

**Cause.** Partition pruning eliminates files at the directory level, but only
on the *partition* columns. If your `WHERE` clause doesn't reference one, the
directory structure can't help — every partition is a candidate.

**Fix.** Filter on a partition column when you can. If your queries genuinely
filter on a different column than the table is partitioned by, the table is
partitioned for the wrong workload — see [the wrong-column trap](#symptom-i-partitioned-it-and-it-got-worse)
below.

## Symptom: data skipping eliminates nothing

Two different causes wear the same symptom — the tool distinguishes them.

### The statistics are there, but the ranges overlap

```text
[WEAK_DATA_SKIPPING] Data skipping eliminated no files for 'fare_amount > 60':
  the per-file min/max ranges all overlap the predicate's bound.
```

**Cause.** Data skipping works by comparing your bound against each file's
recorded min/max. If every file spans the whole range of the column — because
the rows are not ordered by it — no file can be excluded. This is the classic
"stats exist but are useless" case.

**Fix.** Order the data so each file covers a narrow range of the column: sort
or cluster the data — for example with Z-order or liquid clustering where
available. Then a `fare_amount > 60` query touches only the high-fare files.

### There are no statistics to skip on

```text
[STATS_ABSENT] The table carries no file statistics, so data skipping cannot
  prune on 'fare_amount > 60'.
```

**Cause.** No min/max were recorded for that column — the writer collected none,
or the column is past the `delta.dataSkippingNumIndexedCols` budget (Delta
indexes only the first N leaf columns, 32 by default).

**Fix.** Ensure the writer records statistics for the columns you filter on. If
the column is past the indexed-columns budget, reorder the schema or raise
`delta.dataSkippingNumIndexedCols` so it is covered.

## Symptom: part of my predicate is "unsupported"

```text
[UNSUPPORTED_FRAGMENT] The fragment 'UPPER(name) = 'X'' is outside the pruning
  language and was applied conservatively, keeping all files.
```

**Cause.** Function calls, arithmetic on columns, subqueries, and column-to-
column comparisons cannot prune on file metadata — there is no min/max to
compare a computed value against. A mixed `partition OR data` fragment also
can't be split cleanly across the two mechanisms.

**Fix.** For a mixed `OR`, rewrite it so the partition and data filters are
independent conjuncts (`AND`) where the logic allows. For a function on a
column, Delta file metadata cannot prune the computed expression directly —
filter on the raw column instead, or precompute the value into a stored, indexed
column.

## Symptom: I partitioned it, and it got worse

A frequent mistake: partition by a high-cardinality column (a user id, a zone
id) and end up with thousands of tiny files — the small-files problem — *and*
still no pruning for the queries that matter.

Measured on real NYC-taxi data, the same trips laid out four ways against a date
filter, a fare filter, and both:

| Layout | Files | date | fare | date + fare |
|---|---|---|---|---|
| unpartitioned pile | 23 | 0% | 0% | 0% |
| partition by zone id (wrong) | 240 | 3% | 16% | 18% |
| partition by date (right) | 7 | 86% | 0% | 86% |
| date partitions + fare-sorted files | 25 | 84% | 68% | 92% |

**Fix.** Partition by a column with the right cardinality for how you actually
query (a date, not a 260-value zone id), and get file-level skipping by sorting
or clustering within the partitions. The full walkthrough is in
[Tuning a table layout](tuning.md).

## Then: keep it from regressing

Once a layout prunes, a later change — a rewrite that drops partitioning, a
query that loses its filter — can silently undo it. Gate the pruning in CI so
the build fails instead of the cloud bill rising:

```bash
delta-explain ./table -w "..." --min-pruning 80
```

See [Gating pruning in CI](ci-gating.md).
