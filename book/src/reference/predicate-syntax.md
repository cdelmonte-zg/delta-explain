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
