# From Python

`pip install delta-explain` ships the compiled binary plus a thin wrapper. It is
a **client of the CLI-and-JSON contract**, not a second API: it shells out and
returns the report.

```python
from delta_explain import explain

report = explain("s3://warehouse/events",
                 where="country = 'DE' AND age > 40",
                 min_pruning=80, env_creds=True)

report.passed              # False also makes the CLI exit 1 in CI
report.total_pruning_pct
report["analysis"]["confidence"]   # the report is also a Mapping
```

Keyword arguments mirror the CLI flags one to one (`verbose`, `limit`,
`explain_why`, `at_version`, `profile`, `region`, `public`, `options`, ...).

## Outcomes

- **Gate failure** (`--min-pruning` / `--assert-stats`) is *not* an error: it
  comes back as a `Report` with `passed == False`, mirroring the CLI's exit-code
  contract.
- **Runtime errors** (unreadable table, bad predicate, storage) raise
  `DeltaExplainError` with the CLI's message.

## Typed accessors

`Report` is a read-only `Mapping` (so `report["analysis"]["confidence"]` works)
plus convenience properties: `schema_version`, `total_files`, `final_files`,
`total_pruning_pct`, `result`, `passed`, `files` (with `verbose=True`), and
`explain` (with `explain_why=True`).
