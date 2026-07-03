# What delta-explain is validated against

The claim "works on real Delta tables" is only as good as the tables it is
exercised on. This page lists them, and just as deliberately, what is *not*
covered yet.

## Real writers

Two independent Delta implementations write the tables in the test suite:

- **delta-rs** (the `deltalake` Python package) generates the canonical
  fixtures: partitioned, non-partitioned, empty, partial statistics, nested
  struct columns, statistics budgets, temporal/decimal/narrow-int layouts
  (`fixtures/create_test_table.py`).
- **Apache Spark 4.1 + Delta 4.3** writes the exotic checkpoint fixtures:
  classic multi-part checkpoints and V2 UUID-named checkpoints with parquet
  sidecars (`fixtures/create_exotic_checkpoints.py`), plus the table behind
  the differential harness.

The checkpoint-only fixtures (JSON commits removed, the shape log-retention
cleanup produces) are derived from generated tables; one has its checkpoint
hand-rewritten to carry only structured `stats_parsed`, a layout deltalake
does not emit.

## Log shapes

The integration matrix covers: JSON-commit logs, checkpoint-only logs (both
stats layouts), multi-part checkpoints, V2 UUID-named checkpoints with
sidecars, log compaction (compacted files coexisting with their original
commits: no double counting, identical pruning, time travel into the
range), and multi-commit logs with time travel at every version.

## Protocol features

Synthesized logs (the suite's `LogBuilder`) exercise detect-and-declare on:
deletion vectors (present and enabled-but-unused), column mapping by name
with per-field physical names, liquid clustering domain metadata, in-commit
timestamps, unknown writer features, and the catalog-managed refusal path.

## The differential oracle

`examples/differential` runs Spark as ground truth over MinIO (S3 API): for
each of thirteen predicates, Spark computes which files actually contain
matching rows, and the harness asserts delta-explain's survivor set covers
them. The matrix includes normalized forms (De Morgan pushdown, factored
ORs) and null-safe comparisons. It is rerun on every change to predicate
semantics; results to date: sound on all, exact on that layout.

## Scale

Measured on synthetic logs at 200k files in three shapes: a single JSON
commit, 2000 JSON commits, and 2000 commits consolidated by a real
kernel-written parquet checkpoint. Numbers in the README's *Performance
notes*; anyone can reproduce them with
`cargo run --release --example gen_scale_log` (see its docstring for the
three invocations). The automated regression ceiling is a 1000-file smoke.

## Not covered yet, on purpose

- **Databricks/Unity-Catalog managed tables**: detected and refused with an
  explanation (their commits live in the catalog, so a filesystem-only
  analysis cannot be trusted). No support, by declaration.
- **Cloud auth chains in automated CI**: real AWS S3 (both `--profile`
  and `--env-creds`, which is how the env-creds no-op regression was
  caught), real GCS (service account), and real Azure (`az://` and
  `abfss://`/ADLS Gen2, account key and environment credentials; Azurite
  is how the `az://` log-reader regression was caught) are all manually
  verified, but the automated validation still runs on local filesystem
  and MinIO/S3 only. Emulator CI legs are on the trust-track roadmap.
- **Tables written by engines other than delta-rs and delta-spark** (e.g.
  Trino, Flink): the protocol is the contract and the kernel does the
  reading, but no fixture in the suite comes from them.
- **Unknown writer features**: not silently absorbed; declared with an
  `UNRECOGNIZED_TABLE_FEATURE` warning.
