# Differential testing: delta-explain vs Spark

The strongest claim a pruning-attribution tool can make is that its survivor
set agrees with what an engine actually needs. This harness checks exactly
that, on a real object store (MinIO) with a real engine (Spark + Delta):

1. **Spark computes the ground truth**: for each predicate, the set of files
   that actually *contain matching rows* (`input_file_name()` over the
   filtered table).
2. **delta-explain reports its survivor set**: the `[KEPT]` files of the
   final pruning phase, parsed from `--verbose` output.
3. **Soundness assertion**: ground truth ⊆ survivor set, for every predicate.
   A file with matching rows that delta-explain pruned would be a wrong
   answer. The reverse is legitimate: conservative min/max ranges keep files
   that turn out to contain no matches.

## Run it

```bash
docker compose up -d        # MinIO on :9010, Spark container (first run
                            # downloads Delta + hadoop-aws jars, ~1 min)
python3 run_differential.py # delta-explain on PATH, or DX_BIN=/path/to/bin
```

The predicate matrix covers equality and ranges on partition and data
columns, AND/OR mixes (including the `unsplittable` OR case), `IN`,
`BETWEEN`, `NOT`, a floating-point bound, and an empty-result predicate.

## Results (2026-07-02, MinIO, Spark 4.1.2 + Delta 4.3, 60k rows / 17 files)

```
predicate                                      matches  dx keeps sound
---------------------------------------------------------------------------
country = 'DE'                                       5         5 YES
age > 55                                             8         8 YES
country = 'DE' AND age > 55                          2         2 YES
country = 'DE' OR age > 60                           9         9 YES
age BETWEEN 30 AND 40                                6         6 YES
country IN ('DE', 'IT')                             11        11 YES
score > 95.5                                        17        17 YES
NOT (country = 'DE')                                12        12 YES
age > 55 AND score > 95.5                            8         8 YES
age > 100                                            0         0 YES
---------------------------------------------------------------------------
SOUND: delta-explain's survivor set covers every file with matching rows.
```

On this layout the survivor set is not just sound but exact: every kept file
contains at least one matching row, on all ten predicates, including the
unsplittable OR and the empty-result bound.

## What it does not check

Attribution (which phase gets credit) has no engine-side ground truth to
compare against: engines do not expose per-mechanism file elimination. The
harness checks the end result, soundness of the survivor set; attribution
correctness is covered by the unit and integration tests in the repo.
