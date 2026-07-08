# The report viewer

A single self-contained HTML page that renders one JSON report: the pruning
funnel, the predicate analysis, gates, the `--explain-why` diagnoses, and a
filterable per-file table that stays usable at hundreds of thousands of files
(the text listing does not). No dependencies, no network requests: it works
air-gapped and as a CI artifact.

It is a **client of the [versioned JSON contract](../reference/json-schema.md)**,
exactly like the Python wrapper: it adds no analysis of its own and renders any
saved report.

## Use

```bash
delta-explain ./table -w "country = 'DE' AND age > 40" \
  --format json --verbose > report.json
```

Then drop `report.json` onto
[`viewer/report-viewer.html`](https://github.com/cdelmonte-zg/delta-explain/blob/main/viewer/report-viewer.html),
or pick it with the button.

For a one-file artifact (e.g. to attach to a CI run), inject the report into the
page so it renders itself; the recipe is in
[`viewer/README.md`](https://github.com/cdelmonte-zg/delta-explain/blob/main/viewer/README.md).
The killer workflow: after a failed `--min-pruning` gate in CI, upload the
`report.html`: the reviewer sees which phase did not prune and which files
survived, instead of an exit code.

## Scale

A 200,000-file verbose report (~51 MB JSON) loads and renders in about a second,
because the table only ever mounts the rows in view. Toward a million files the
browser's JSON parse dominates; generate the report with `--limit` there.
