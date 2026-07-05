# Quickstart

Three minutes, no cloud account. The repo ships a runnable demo and a small
real table.

## On a real table (no setup)

The repo includes `fixtures/taxi-nyc`, a small Delta table written from public
[NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
yellow-taxi data, partitioned by pickup date:

```bash
# date is the partition column: directory-level pruning, exact
delta-explain fixtures/taxi-nyc -w "pickup_date = '2024-01-03'"

# date prunes partitions, fare prunes on min/max stats within them
delta-explain fixtures/taxi-nyc -w "pickup_date = '2024-01-03' AND fare_amount > 50"

# a predicate that does NOT prune - and the tool tells you why
delta-explain fixtures/taxi-nyc -w "PULocationID = 132" --explain-why
```

That last command is the point of the tool: the pickup-zone column is not
clustered, so data skipping cannot help, and `--explain-why` says so with a fix.
See [Why isn't it pruning?](../guides/explain-why.md).

## The full demo (Docker)

`examples/quickstart/` brings up a MinIO + demo-table stack and walks the gate
story end to end (a healthy table, a layout regression the gate catches, the
JSON contract). Follow its README:

```bash
cd examples/quickstart
docker compose up -d
# then the commands in examples/quickstart/README.md
```

## Next

- [Reading a report](../guides/reading-a-report.md) — what every line means.
- [Gating pruning in CI](../guides/ci-gating.md) — turn it into a build check.
