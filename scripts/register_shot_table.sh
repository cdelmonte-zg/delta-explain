#!/usr/bin/env bash
# Prepare the Unity Catalog table behind the README's catalog shot
# (scripts/render_readme_shots.py): `sales.events.orders` over a second
# RustFS bucket of the local catalog stack, so the resolved location reads
# as a plain warehouse path and nothing in the shot names real
# infrastructure. Needs the stack from examples/catalogs up and
# bootstrapped (docker compose up -d, python3 run_catalogs.py once).
#
# What it does, idempotently: creates bucket `warehouse`, copies the
# external table there, extends the running server's configuration so it
# vends for that bucket too (same RustFS credentials; lost on the next
# bootstrap, as it should be), restarts the server, registers the name.
set -euo pipefail

UC_HOST="${UC_HOST:-http://localhost:8080}"
UC="$UC_HOST/api/2.1/unity-catalog"
RUSTFS_URL="${RUSTFS_URL:-http://localhost:9020}"
CONTAINER="${UC_CONTAINER:-dxcat-unitycatalog}"
SOURCE="s3://cat/uc-external/users"
TARGET="s3://warehouse/events/orders"

say() { printf '==> %s\n' "$*"; }
for t in aws docker curl; do command -v "$t" >/dev/null || { echo "missing tool: $t" >&2; exit 1; }; done
export AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true

say "bucket warehouse and the table copy"
aws --endpoint-url "$RUSTFS_URL" s3 mb s3://warehouse >/dev/null 2>&1 || true
aws --endpoint-url "$RUSTFS_URL" s3 cp --recursive --quiet "$SOURCE" "$TARGET"

say "server configuration for $TARGET"
tmp=$(mktemp)
docker exec "$CONTAINER" cat etc/conf/server.properties > "$tmp"
if ! grep -q '^s3.bucketPath.1=s3://warehouse$' "$tmp"; then
  {
    echo
    echo "# Added by scripts/register_shot_table.sh: the same credentials for s3://warehouse."
    echo "s3.bucketPath.1=s3://warehouse"
    for key in region accessKey secretKey sessionToken; do
      sed -n "s/^s3\.$key\.0=/s3.$key.1=/p" "$tmp"
    done
  } >> "$tmp"
  # The server reads its configuration once at start and its entrypoint
  # waits for a fresh file on the shared volume (see examples/catalogs):
  # restart it, then hand it the extended file.
  docker restart "$CONTAINER" >/dev/null
  for _ in $(seq 1 30); do
    docker exec "$CONTAINER" test -f /bootstrap/request 2>/dev/null && break; sleep 1
  done
  docker exec -i "$CONTAINER" sh -c 'cat > /bootstrap/sp.tmp && mv /bootstrap/sp.tmp /bootstrap/server.properties' < "$tmp"
  for _ in $(seq 1 90); do
    curl -sf -o /dev/null "$UC/catalogs" && break; sleep 2
  done
fi
rm -f "$tmp"

say "registering sales.events.orders"
# Accepted answers: the created object, or the server's ALREADY_EXISTS
# (an HTTP 400) on a second run.
post() {
  local body
  body=$(curl -s -X POST -H 'Content-Type: application/json' "$UC$1" -d "$2")
  case "$body" in
    *ALREADY_EXISTS*|*'"table_id"'*|*'"catalog_name"'*|*'"name"'*) ;;
    *) echo "POST $1 -> $body" >&2; exit 1 ;;
  esac
}
post /catalogs '{"name":"sales"}'
post /schemas '{"name":"events","catalog_name":"sales"}'
post /tables "$(cat <<EOF
{"name":"orders","catalog_name":"sales","schema_name":"events","table_type":"EXTERNAL",
 "data_source_format":"DELTA","storage_location":"$TARGET","columns":[
 {"name":"uid","type_name":"LONG","type_text":"bigint","type_json":"{\"name\":\"uid\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}","position":0,"nullable":false},
 {"name":"age","type_name":"INT","type_text":"int","type_json":"{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}","position":1,"nullable":true},
 {"name":"country","type_name":"STRING","type_text":"string","type_json":"{\"name\":\"country\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}","position":2,"nullable":false,"partition_index":0},
 {"name":"score","type_name":"DOUBLE","type_text":"double","type_json":"{\"name\":\"score\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}","position":3,"nullable":true}]}
EOF
)"
say "ready: sales.events.orders -> $TARGET"
