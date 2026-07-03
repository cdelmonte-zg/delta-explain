#!/usr/bin/env bash
# The three-minute tour: five beats, each one thing delta-explain does.
# Repeatable: runs against the committed fixtures plus one Delta log this
# script synthesizes itself (delta-explain never reads parquet data, so a
# log alone is a perfectly good table).
#
# Usage, from anywhere inside the repo:
#   examples/quickstart/quickstart.sh
# Uses the delta-explain on PATH, or DX_BIN, or falls back to cargo run.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
table="$repo_root/fixtures/users"

if [ -n "${DX_BIN:-}" ]; then
    dx() { "$DX_BIN" "$@"; }
elif command -v delta-explain >/dev/null 2>&1; then
    dx() { delta-explain "$@"; }
else
    dx() { cargo run --quiet --release --manifest-path "$repo_root/Cargo.toml" -- "$@"; }
fi

beat() { printf '\n\n===== %s =====\n\n' "$1"; }

beat "1. Partition pruning: the partition filter eliminates directories"
dx "$table" -w "country = 'DE'"

beat "2. Data skipping: min/max statistics eliminate files inside partitions"
dx "$table" -w "age > 60"

beat "3. Degradation: an unsupported fragment warns, the rest still prunes"
dx "$table" -w "country = 'DE' AND UPPER(name) = 'X'"

beat "4. CI gate: --min-pruning turns the report into an assertion"
echo '--- passing gate (threshold 60):'
dx "$table" -w "country = 'DE'" --min-pruning 60 --format json | grep -E '"result"|"total_pruning_pct"'
echo
echo '--- failing gate (threshold 95) exits 1, report says why:'
if dx "$table" -w "country = 'DE'" --min-pruning 95 --format json | grep -E '"result"|"actual"|"threshold"'; then
    echo "unexpected: the gate should have failed" >&2
    exit 1
else
    echo '(exit code was 1, as a pipeline would want)'
fi

beat "5. Table features: deletion vectors are declared, never silently absorbed"
# A Delta table is its log: synthesize one where a file carries a deletion
# vector, no parquet needed.
dv_table="$(mktemp -d)"
mkdir -p "$dv_table/_delta_log"
cat > "$dv_table/_delta_log/00000000000000000000.json" <<'EOF'
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}
{"metaData":{"id":"00000000-0000-0000-0000-000000000042","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true"},"createdTime":1750000000000}}
{"add":{"path":"f0.parquet","partitionValues":{},"size":1024,"modificationTime":1750000000000,"dataChange":true,"stats":"{\"numRecords\":100,\"minValues\":{\"age\":0},\"maxValues\":{\"age\":50},\"nullCount\":{\"age\":0}}"}}
{"add":{"path":"f1.parquet","partitionValues":{},"size":1024,"modificationTime":1750000000000,"dataChange":true,"stats":"{\"numRecords\":100,\"minValues\":{\"age\":51},\"maxValues\":{\"age\":99},\"nullCount\":{\"age\":0}}","deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9wq","offset":1,"sizeInBytes":36,"cardinality":2}}}
EOF
dx "$dv_table"
rm -rf "$dv_table"

printf '\n\nDone. The JSON contract behind beat 4 is documented in docs/json-schema.md;\nwhat each number guarantees is in docs/semantics.md.\n'
