# 0007. `--explain-why` is a deterministic diagnostic engine, not an ML model

Date: 2026-07-05
Status: accepted

## Context

The report answers "how many files does this predicate eliminate, and in
which phase". The next question a user asks is "*why* so few, and what do I
change" - the shift from a counter to an advisor (the VISION's v0.5
diagnostic layer). The obvious-but-wrong way to build that is a bundled ML
model that predicts advice from the report.

## Decision

`--explain-why` is a **deterministic rules engine** over data the tool
already computes (the classification, the stats coverage, the partition
columns, the per-phase pruning, the per-file min/max). Each diagnosis has a
stable code, a severity, a human message, and - where one exists - an
actionable suggestion. It emits a text section under `--explain-why` and,
in JSON, an additive `explain` array (schema minor bump, present only with
the flag, like `files[]` under `--verbose`).

No machine-learning model, bundled or otherwise. Three reasons, each tied
to what the tool *is*:

1. **It would forfeit the guarantee that is the whole value.** The contract
   (0002, docs/semantics.md) is "never a silent wrong number". A
   probabilistic, unexplainable diagnosis is the opposite; the *why* must be
   as trustworthy as the numbers it explains, which means deterministic and
   auditable.
2. **There is nothing to learn.** Every diagnosis is a function of data
   already in hand: "no partition pruning because the predicate references
   no partition column" is a lookup; "data skipping weak because the min/max
   ranges are wide" is computed from the stats we read. The rules are known,
   finite, and testable - exactly the shape the differential harness and
   unit tests can prove.
3. **It would break distribution and the CI-gate use.** A local model means
   an inference runtime and tens-to-hundreds of MB on a lean static binary,
   and non-deterministic output a CI gate cannot assert. Distribution is
   already the binding constraint; this would deepen it for no soundness.

Where an LLM legitimately fits is *outside* the tool, as a client of the
JSON contract (the 0004 boundary): the tool emits structured diagnoses,
and a user who wants prose pipes them to their own LLM
(`delta-explain ... --format json | my-llm`). The natural-language layer is
optional sugar and someone else's dependency, never bundled.

## Diagnosis taxonomy (v1)

Each is deterministic and computable from the existing report. Codes are
stable once shipped; new codes are additive.

- `NO_PARTITION_FILTER` - the table is partitioned but the predicate filters
  on no partition column; partition pruning is unavailable. Suggest filtering
  on a partition column.
- `WEAK_DATA_SKIPPING` - a stats-safe fragment reached data skipping but
  eliminated little or nothing because the per-file min/max ranges overlap
  the bound. The tool reports the observed overlap; the likely cause (the
  column is not sorted/clustered) is a recommendation, not a proven layout
  claim - the wording stays "may enable skipping". (The taxi `PULocationID`
  case.)
- `STATS_ABSENT` - a stats-safe fragment but the table carries no statistics
  for its column (none written, or past `dataSkippingNumIndexedCols`), so
  data skipping cannot act.
- `UNSUPPORTED_FRAGMENT` - reframes the existing `UNSUPPORTED_EXPRESSION` /
  `UNSPLITTABLE_OR` notes as advice (rewrite the mixed OR; the construct is
  outside the pruning language).

Deferred to later versions, each needing a threshold policy: `SMALL_FILES` /
`OVER_PARTITIONED` (file-count and partition-cardinality heuristics), and
engine-profile-aware advice.

## Alternatives considered

- **A bundled local ML model.** Rejected above: forfeits soundness,
  learns nothing that isn't already computable, wrecks distribution.
- **An LLM called by the tool at runtime.** Same non-determinism and
  dependency problems, plus a network/credentials surface; belongs outside
  the tool as a JSON consumer.
- **No `explain-why`, leave it to the existing notes.** The notes explain
  *classification*; they do not diagnose *why pruning was weak* (wide ranges,
  wrong partition column) or suggest a fix. The advisor value is exactly the
  gap the notes leave.

## Consequences

- A new pure module (diagnostics), fed the computed report, unit-testable in
  isolation and coverable by the differential harness's real tables (the
  taxi `WEAK_DATA_SKIPPING` case is a live example).
- The JSON contract grows by one additive, flag-gated array; the compact
  default document is unchanged.
- The advice is only as good as the taxonomy, which grows deliberately -
  every new diagnosis passes the same three-question gate as a new predicate
  construct (representable from data in hand, engine-neutral semantics,
  provable). No diagnosis ships that the tool cannot stand behind.
