# Predicate syntax

`delta-explain` accepts standard SQL WHERE-clause syntax, parsed via
[sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs).

```sql
-- Comparisons
age > 30
country = 'DE'
score >= 90.5

-- Logical operators
age > 30 AND country = 'DE'
country = 'DE' OR country = 'IT'
NOT country = 'US'

-- IN lists
country IN ('DE', 'IT', 'US')
country NOT IN ('US')

-- BETWEEN
age BETWEEN 20 AND 40

-- NULL checks
name IS NOT NULL
age IS NULL

-- Parentheses
(country = 'DE' OR country = 'IT') AND age > 30

-- Nested columns
payload.age > 30
```

Also supported: `IS [NOT] DISTINCT FROM`, `DATE '...'` / `TIMESTAMP '...'`
literal forms, schema-driven coercion (a quoted `'2026-07-01'` against a
`DATE` column just works, including `DECIMAL` and narrow integers), and
`LIKE`: prefix patterns (`country LIKE 'D%'`) prune on partition values and
on string min/max statistics, and on partition columns every other shape
(`'%son'`, `_`, `NOT LIKE`) prunes exactly too.

Subqueries, functions, and non-prefix `LIKE` on data columns are outside the
pruning language: they warn and keep files instead of failing. The exact
rules are the [degradation rules](semantics.md#degradation-rules) in the
semantics contract.

## What the analysis shows

The analysis block always displays the predicate *as analyzed*, so
normalization is visible rather than silent. A prefix `LIKE` appears as the
lexicographic range it was rewritten to, and prunes through the ordinary
partition rules:

```
$ delta-explain ./table -w "country LIKE 'D%' AND age > 40"

Predicate Analysis:
  partition-safe: country >= 'D' AND country < 'E'
  stats-safe:     age > 40
  stats coverage:
    age [min_max]: 2/2 candidate files (100%)
  unsplittable:   -
  confidence:     conservative

Files in snapshot: 6

Phase 1: Partition pruning [exact]
  predicate:       country >= 'D' AND country < 'E'
  files remaining: 2  (-4, 67% pruned)

Phase 2: Data skipping (min/max statistics) [conservative]
  predicate:       age > 40
  files remaining: 1  (-1, 50% pruned)

Total reduction: 6 -> 1 files (83% pruned)
```

A construct outside the pruning language degrades loudly instead of erroring:
the fragment routes to `unsplittable`, confidence drops to `incomplete`, a
warning names the offending expression, and the sibling conjunct still prunes:

```
$ delta-explain ./table -w "UPPER(country) = 'DE' AND age > 40"

Predicate Analysis:
  partition-safe: -
  stats-safe:     age > 40
  stats coverage:
    age [min_max]: 6/6 candidate files (100%)
  unsplittable:   UPPER(country) = 'DE'
  confidence:     incomplete

Files in snapshot: 6

Phase 1: Data skipping (min/max statistics) [incomplete]
  predicate:       age > 40  (+1 unsupported fragment, keeps all files)
  files remaining: 3  (-3, 50% pruned)

Warnings!
[UNSUPPORTED_EXPRESSION]: Unsupported expression: UPPER(country); the fragment 'UPPER(country) = 'DE'' cannot contribute to pruning and is applied conservatively (keeps all files)
```
