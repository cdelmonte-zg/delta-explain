# Quickstart: the three-minute tour

One script, five beats, each one thing delta-explain does. Repeatable:
it runs against the fixtures committed in this repo, plus one Delta log
the script synthesizes on the fly (delta-explain never reads parquet
data, so a log alone is a perfectly good table).

```bash
examples/quickstart/quickstart.sh
```

Uses the `delta-explain` on PATH, or `DX_BIN=/path/to/binary`, or falls
back to `cargo run --release`.

## The five beats

**1. Partition pruning.** `country = 'DE'` eliminates whole directories:
6 files become 2 (`67% pruned`), phase tagged `[exact]` because partition
values are compared directly.

**2. Data skipping.** `age > 60` prunes on per-file min/max statistics:
6 files become 1 (`83% pruned`), tagged `[conservative]` because ranges
can overlap a bound without containing a match.

**3. Degradation.** `country = 'DE' AND UPPER(name) = 'X'` contains a
function call, which no file-level statistic can evaluate. The command
does not fail: the partition filter still prunes to 2 files, the
unsupported fragment keeps them conservatively, and a
`[UNSUPPORTED_EXPRESSION]` warning says exactly what happened.

**4. CI gate.** `--min-pruning 60` passes (`"result": "pass"`, exit 0);
`--min-pruning 95` fails with the numbers that say why (`"actual": 66.6,
"threshold": 95`, exit 1). stdout is machine-readable JSON in both
cases; that is the whole CI story.

**5. Table features.** On a table where a file carries a deletion
vector, the report declares it instead of silently overcounting:
`[DELETION_VECTORS]: 1 of 2 files carry deletion vectors: record counts
include soft-deleted rows`.

## Where to go next

- What each number guarantees (and what the tool deliberately does not
  do): [docs/semantics.md](../../docs/semantics.md)
- The JSON contract behind beat 4, field by field:
  [docs/json-schema.md](../../docs/json-schema.md)
- Gate a pipeline with the GitHub Action: see the README's *CI/CD mode*.
