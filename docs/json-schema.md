# The JSON report, field by field

The goal of this page: **a consumer can trust the JSON without reading the
code**. The formal schema lives at
[`schemas/report-v0.5.schema.json`](https://github.com/cdelmonte-zg/delta-explain/blob/main/schemas/report-v0.5.schema.json) and
the integration suite validates every emitted document against it; this
page explains what the fields mean.

## Versioning

The document carries `schema_version`, versioned independently of the tool:
additive changes bump the minor (and ship a new schema file), breaking
changes bump the major. Consumers should:

- branch on stable field names and `assertions[].name`, not on positions;
- tolerate unknown fields (a newer minor may add some);
- check `schema_version` if they depend on a specific shape.

## Always present

| Field | Type | Meaning |
|---|---|---|
| `schema_version` | string | Schema of this document (`0.5.x`) |
| `tool_version` | string | delta-explain release |
| `elapsed_ms` | integer | Wall-clock analysis duration |
| `table` | string | Table path/URI as given |
| `version` | integer | Delta snapshot version analyzed |
| `predicate` | string \| null | The `--where` input, null without one |
| `total_files` | integer | Files before pruning |
| `final_files` | integer | Files surviving the last phase |
| `total_pruning_pct` | number | Overall reduction in percent |
| `analysis` | object \| null | Predicate classification (`partition_safe`, `partition_exact`, `stats_safe`, `stats_coverage`, `unsplittable`, `confidence`, `notes`), null without `--where` |
| `table_features` | object | Detect-and-declare block: deletion vectors, `column_mapping_mode`, `clustering_columns`, `in_commit_timestamps`, `unrecognized_writer_features`, and warning `notes` |
| `stats` | object | Statistics coverage: `mode` is `exact` / `partial` / `absent` |
| `phases` | array | Chained pruning phases, empty without `--where` |
| `assertions` | array | One entry per gate that ran, empty without gate flags |
| `result` | `"pass"` \| `"fail"` \| null | Overall gate outcome, null when no gates ran |

## Only with `--verbose`

| Field | Type | Meaning |
|---|---|---|
| `files` | array | Per-file outcomes, capped by `--limit` |
| `files_truncated` | boolean | True when `--limit` cut the array short |

## Only with `--explain-why`

| Field | Type | Meaning |
|---|---|---|
| `explain` | array | Diagnoses of why the predicate pruned as it did: `{code, severity, message, suggestion}`. Stable codes: `NO_PARTITION_FILTER`, `WEAK_DATA_SKIPPING`, `STATS_ABSENT`, `UNSUPPORTED_FRAGMENT` |

Per file: `path`, `size_bytes`, `partition_values` (string map),
`num_records` (nullable; includes soft-deleted rows under deletion
vectors), `has_stats`, **`kept`**, **`pruned_by`**.

- `kept: true` means the file survives every phase. The set of kept files
  is the survivor set: guaranteed to be a superset of the files any engine
  needs for this predicate (see [semantics](semantics.md)).
- `pruned_by` names the first phase that eliminated the file (one of the
  `phases[].name` values), or null when kept. Phases chain, so "first
  phase whose survivor set misses the file" is exactly the phase that
  eliminated it.

## `analysis.stats_coverage`

One entry per column a `stats_safe` fragment references and per family of
statistics it needs:

```json
{"column": "age", "requirement": "min_max",
 "candidate_files": 2, "covered_files": 2, "coverage_pct": 100.0}
```

- `column`: the logical column name, dotted for nested leaves
  (`profile.age`). Under column mapping the coverage is checked against
  the physical name, but the reported name is always the logical one.
- `requirement`: which statistics the fragment needs to skip a file:
  `min_max` (comparisons, `IN`, `BETWEEN`), `null_count` (`IS NULL`),
  `null_count_and_num_records` (`IS NOT NULL`).
- `candidate_files`: the files entering the data-skipping phase (the
  partition survivors; all files when nothing pruned before).
- `covered_files` / `coverage_pct`: how many of those candidates carry
  the required statistics. A file without them can never be skipped by
  that fragment, so coverage bounds what data skipping can achieve
  before any ranges are compared.

The array is empty when no `stats_safe` fragment needs statistics, and
the whole field is `null` when coverage could not be computed reliably
(a predicate column that does not resolve against the table schema).
Coverage is diagnostic only: it never changes the survivor set.

## `confidence`

Appears globally (`analysis.confidence`) and per phase. It labels how
precisely the elimination can be explained, never whether it is correct:

- `exact`: provably precise elimination (partition values compared
  directly);
- `conservative`: sound but possibly loose (min/max ranges can overlap a
  bound without the file containing a match);
- `incomplete`: part of the predicate could not be attributed (mixed OR,
  unsupported construct); totals remain sound, a note says why.

## Stable note codes

Notes are `{code, message}` pairs. The `message` text may be reworded at
any time; the `code` values are stable identifiers, and new codes are
additive:

| Code | Where | Meaning |
|---|---|---|
| `UNSPLITTABLE_OR` | `analysis.notes` | An OR mixes partition and non-partition columns; routed conservatively |
| `UNSUPPORTED_EXPRESSION` | `analysis.notes` | A fragment is outside the pruning language; it keeps all files, siblings still prune |
| `PARTITION_EVAL_GAP` | `analysis.notes` | Some partition values could not be evaluated against the `partition_exact` fragment; the affected files are kept |
| `DELETION_VECTORS` | `table_features.notes` | Files carry deletion vectors; record counts include soft-deleted rows |
| `COLUMN_MAPPING` | `table_features.notes` | The log stores physical column names; verbose stats may display them |
| `LIQUID_CLUSTERING` | `table_features.notes` | The table is liquid-clustered; layout is not directory partitions, data skipping still applies |
| `UNRECOGNIZED_TABLE_FEATURE` | `table_features.notes` | The table carries writer features this tool does not know; the numbers do not account for whatever they imply |

## `assertions[]`

Discriminated by `name`:

- `{"name": "min_pruning", "threshold": N, "actual": N, "result": "pass"|"fail"}`
- `{"name": "stats_complete", "missing_count": N, "result": "pass"|"fail"}`

`result` at the top level aggregates them; a `fail` also makes the process
exit 1.

## Error contract

On any runtime error stdout is empty and the message goes to stderr, so
`delta-explain ... --format json | jq ...` never parses a partial
document. The full exit-code table is in [semantics](semantics.md).

## Consumer patterns

```bash
# gate on the overall result
delta-explain ./t -w "..." --min-pruning 80 --format json | jq -e '.result == "pass"'

# survivor set as a file list
delta-explain ./t -w "..." --format json --verbose | jq -r '.files[] | select(.kept) | .path'

# fail a pipeline when deletion vectors appear
delta-explain ./t --format json | jq -e '.table_features.deletion_vectors.files_with_deletion_vectors == 0'
```
