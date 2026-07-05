# Gating pruning in CI

The failure mode `delta-explain` was built to catch: a change quietly breaks
pruning — a table gets rewritten without partitioning, a query loses its
partition filter — nothing errors, and every downstream scan silently reads the
whole table. A gate turns that into a failed build.

## The flags

- `--min-pruning <PCT>` — exit 1 if total pruning is below the threshold.
- `--assert-stats` — exit 1 if any file is missing statistics.

```bash
delta-explain s3://lake/events -w "region = 'eu' AND ts > '2026-06-01'" \
  --min-pruning 80
```

On failure the report still prints (with `result: "fail"`) and stderr carries
`ASSERTION FAILED: ...`. The exit-code contract is precise and stable — see the
table in [What delta-explain guarantees](../reference/semantics.md).

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
- uses: cdelmonte-zg/delta-explain@v0.6.0
  with:
    table: s3://lake/events
    where: "region = 'eu'"
    min-pruning: "80"
```

## Attach a report artifact

Generate a verbose JSON report and render it with the
[report viewer](viewer.md) into a self-contained `report.html`, uploaded as a
run artifact — so a reviewer of a failed gate sees *which* phase did not prune
and *which* files survived, instead of an exit code.
