# Gating pruning in CI

The failure mode `delta-explain` was built to catch: a change quietly breaks
pruning (a table gets rewritten without partitioning, a query loses its
partition filter), nothing errors, and every downstream scan silently reads the
whole table. A gate turns that into a failed build.

## The flags

- `--min-pruning <PCT>`: exit 1 if total pruning is below the threshold.
- `--assert-stats`: exit 1 if any file is missing statistics.

```bash
delta-explain s3://lake/events -w "region = 'eu' AND ts > '2026-06-01'" \
  --min-pruning 80
```

On failure the report still prints (with `result: "fail"`) and stderr carries
`ASSERTION FAILED: ...` - the terminal shows both, and the exit code flips
to 1:

```
$ delta-explain ./table -w "country = 'DE' AND age > 40" --min-pruning 90
ASSERTION FAILED: total pruning 83.3% is below threshold 90.0%
Delta table: ./table
Version:     5
Predicate:   country = 'DE' AND age > 40

Predicate Analysis:
  partition-safe: country = 'DE'
  stats-safe:     age > 40
  stats coverage:
    age [min_max]: 2/2 candidate files (100%)
  unsplittable:   -
  confidence:     conservative

Files in snapshot: 6

Phase 1: Partition pruning [exact]
  predicate:       country = 'DE'
  files remaining: 2  (-4, 67% pruned)

Phase 2: Data skipping (min/max statistics) [conservative]
  predicate:       age > 40
  files remaining: 1  (-1, 50% pruned)

Total reduction: 6 -> 1 files (83% pruned)

$ echo $?
1
```

The exit-code contract is precise and stable; see the table in
[What delta-explain guarantees](../reference/semantics.md).

Statistics are resolved through the kernel's log replay, checkpoint Parquet
included, so `--assert-stats` flags a file only when its `add` action genuinely
carries no statistics: long-lived tables whose older commits have been
consolidated into a checkpoint do not produce false positives.

## Calibrating the threshold

The `--min-pruning` threshold is per-invocation, applied to the current
predicate against the current snapshot. Calibrate it against a baseline pruning
percentage in dev (set the gate a few points below it); a flat threshold across
heterogeneous partitions will misfire. Note also that 100% pruning can signal a
broken or unexpectedly empty predicate, so pair `--min-pruning` with a sanity
check on `final_files > 0` when the workload is expected to read data.

## Predicate parity

The pruning percentage `delta-explain` reports reflects the predicate you pass
to `-w`. If the runtime query wraps a column in `LOWER`, `CAST`, or a UDF, the
engine may prune less than the gate suggests. Use a CI predicate that is
semantically equivalent to the runtime predicate and explicitly track that
equivalence: a gate on `country = 'DE'` does not automatically validate a
production query using `LOWER(country) = 'de'`.

## In a pipeline (JSON)

```bash
delta-explain ./table -w "..." --min-pruning 80 --format json \
  | jq -e '.result == "pass"'
```

`stdout` is always a complete report or empty, so a downstream `jq` never parses
a partial document.

## GitHub Action

A composite action wraps the CLI with matching inputs. Pin the release tag:

```yaml
- uses: cdelmonte-zg/delta-explain@v0.7.0
  with:
    table: s3://lake/events
    where: "region = 'eu'"
    min-pruning: "80"
```

## Docker in a pipeline

The same gate without the composite action, from any CI system that can run a
container:

```yaml
- name: Verify pruning after ETL
  run: |
    docker run --rm \
      -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
      ghcr.io/cdelmonte-zg/delta-explain:0.7.0 \
      --env-creds s3://warehouse/events \
      -w "date = '2024-01-15'" \
      --min-pruning 90 --assert-stats --format json
```

## Attach a report artifact

Generate a verbose JSON report and render it with the
[report viewer](viewer.md) into a self-contained `report.html`, uploaded as a
run artifact, so a reviewer of a failed gate sees *which* phase did not prune
and *which* files survived, instead of an exit code.
