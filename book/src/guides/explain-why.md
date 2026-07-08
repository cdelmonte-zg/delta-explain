# The `--explain-why` advisor

Counting files tells you *that* a predicate did not prune. `--explain-why` tells
you *why*, and what to change. (For a symptom-first playbook, see
[Why isn't my Delta query pruning files?](troubleshooting.md).)

```text
$ delta-explain fixtures/taxi-nyc -w "PULocationID = 132" --explain-why
  ...
Why:
  [NO_PARTITION_FILTER] The table is partitioned by pickup_date, but the
    predicate filters on none of those columns, so partition pruning cannot run.
    -> Filter on a partition column (pickup_date) to eliminate whole directories
       before data skipping.
  [WEAK_DATA_SKIPPING] Data skipping eliminated no files for 'PULocationID = 132':
    the per-file min/max ranges all overlap the predicate's bound.
    -> Ranges this wide usually mean the data is not sorted or clustered by that
       column; ordering by it may enable skipping.
```

## Deterministic, not a model

Each diagnosis is a **function of the report the tool already computed** - the
classification, the stats coverage, the partition columns, the per-phase
pruning. Nothing is predicted. That is deliberate: the *why* must be as
trustworthy as the numbers it explains, and reproducible enough to gate on in
CI. (An LLM, if you want prose, is a consumer of the JSON *outside* the tool,
never bundled - see [ADR 0007](../concepts/decisions.md).)

## The diagnosis codes

Stable identifiers; new codes are additive.

| Code | Means | Honest limit |
|---|---|---|
| `NO_PARTITION_FILTER` | The table is partitioned but the predicate filters on no partition column. | - |
| `WEAK_DATA_SKIPPING` | Stats are present on every file, but the min/max ranges all overlap the bound, so nothing is eliminated. | The overlap is observed; "sort or cluster" is a recommendation, not a proven layout claim. |
| `STATS_ABSENT` | A stats-safe fragment, but the table carries no statistics for its column, so data skipping cannot act. | - |
| `UNSUPPORTED_FRAGMENT` | A fragment outside the pruning language kept all files. | Reframes the classification note as advice. |

`WEAK_DATA_SKIPPING` fires only when *every* file has statistics: on a
partial-stats table the survivors may be kept for missing stats, not overlapping
ranges, and the tool will not assert a cause it cannot prove.

## In JSON

With the flag, the JSON document gains an additive `explain` array of
`{code, severity, message, suggestion}`. It is present only with `--explain-why`,
so existing consumers are unaffected. See
[The JSON report](../reference/json-schema.md).
