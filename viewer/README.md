# Report viewer

![The viewer rendering a report from fixtures/test-table: pruning
funnel, analysis buckets, gates, and the per-file table](screenshot.png)

A single self-contained HTML page that renders one delta-explain JSON
report: the pruning funnel, the predicate analysis, gates, warnings, and
a filterable per-file table that stays usable at hundreds of thousands
of files (the text listing does not). No dependencies, no network
requests: it works air-gapped and as a CI artifact.

The viewer is a **client of the versioned JSON contract**
(`schemas/report-v0.3.schema.json`), exactly like the Python wrapper:
it adds no analysis of its own, and it renders any saved report,
retroactively. See issue #89 and ADR 0004 for the boundary.

## Use

Generate a report and open the page in any browser:

```bash
delta-explain ./table -w "country = 'DE' AND age > 40" \
    --format json --verbose > report.json
```

Then drop `report.json` onto `report-viewer.html`, or pick it with the
button.

For a one-file artifact (e.g. to attach to a CI run), inject the report
into the page:

```bash
python3 - <<'EOF'
tpl = open('report-viewer.html').read()
doc = open('report.json').read().strip()
open('report.html', 'w').write(tpl.replace('/*REPORT_JSON*/', doc, 1))
EOF
```

`report.html` renders itself on open. The killer workflow: run the two
commands in CI after a failed `--min-pruning` gate and upload
`report.html` as an artifact - the reviewer sees which phase did not
prune and which files survived, instead of an exit code.

## What it shows

- header: table, snapshot version, elapsed, gate verdict;
- the funnel: baseline and each phase as chained bars, survivors solid,
  the pruned remainder hatched, confidence per phase; clicking a phase
  filters the file table to what it pruned;
- the analysis buckets (`partition-safe`, `partition-exact`,
  `stats-safe`, `unsplittable`), confidence, and every warning note;
- gates with threshold and actual;
- the per-file table (needs `--verbose`): path, partition values,
  records, stats presence, kept / pruned-by, with substring and
  outcome filters. Rows render through a windowed list, so a 200k-file
  report scrolls smoothly.

Reports from older schema minors render too (fields the schema did not
have yet are simply absent); a non-report JSON gets a plain error with
the command to generate a real one.

## Design notes

Colors are the data-viz reference palette verbatim (both modes,
`prefers-color-scheme`): one blue for surviving files everywhere, a
neutral hatch for the pruned remainder, reserved status colors for
pass/fail/warnings - never color alone, always paired with a label.
System font stacks only, since the page must not fetch anything.

The screenshot above regenerates from a real report, so it never drifts
from the page:

```bash
cargo run -q -- fixtures/test-table \
    -w "country LIKE '%E' AND age > 40 AND UPPER(name) = 'X'" \
    --format json --verbose --min-pruning 50 > /tmp/report.json
python3 - <<'EOF'
tpl = open('viewer/report-viewer.html').read()
doc = open('/tmp/report.json').read().strip()
open('/tmp/report.html', 'w').write(tpl.replace('/*REPORT_JSON*/', doc, 1))
EOF
chromium --headless --disable-gpu --window-size=1100,1500 \
    --screenshot=viewer/screenshot.png /tmp/report.html
```
