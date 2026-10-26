#!/bin/sh
# One-shot, run in the RustFS image (it ships curl with --aws-sigv4):
# creates the bucket, then writes the server.properties Unity Catalog
# starts from, with credentials for s3://cat that the server can vend.
#
# Unity Catalog OSS vends S3 credentials through STS AssumeRole, which has
# no endpoint setting for an S3-compatible store. Its per-bucket config has
# one exception: when s3.sessionToken.N is set, the server hands out the
# configured key, secret and token as they are. RustFS rejects any session
# token it did not issue, so the ones to configure are RustFS's own: an
# AssumeRole against RustFS returns a key, a secret and a JWT token valid
# for at most 12 hours whatever DurationSeconds asks (`docker compose up`
# runs this again and renews them).
#
# Handshake with the unitycatalog service: it removes any server.properties
# left on the shared volume, touches `request`, and waits for a fresh file;
# this script answers one request per run. Neither side can read a stale file.
set -eu

RUSTFS=http://rustfs:9000
ROOT=rustfsadmin:rustfsadmin
OUT=/bootstrap/server.properties
GCS_KEY=/bootstrap/gcs-key.json

# The volume is created root-owned; the unitycatalog service (uid 100)
# must be able to write its request and remove a stale file.
chmod 0777 /bootstrap
# A request or file left by a previous run would be answered before the
# current unitycatalog container asks (it keeps asking until served).
rm -f /bootstrap/request /bootstrap/server.properties

until curl -sf -o /dev/null "$RUSTFS/health"; do
  echo 'waiting for rustfs...'; sleep 1
done
# /health answers before the S3 API does: retry the bucket creation.
until curl -sf -o /dev/null -X PUT --user "$ROOT" --aws-sigv4 'aws:amz:us-east-1:s3' "$RUSTFS/cat"; do
  echo 'waiting for the rustfs s3 api...'; sleep 1
done
echo 'bucket ready: cat'

until [ -f /bootstrap/request ]; do
  echo 'waiting for unity catalog...'; sleep 1
done
rm -f /bootstrap/request

STS=$(curl -sf --user "$ROOT" --aws-sigv4 'aws:amz:us-east-1:sts' -X POST "$RUSTFS/" \
  -d 'Action=AssumeRole&Version=2011-06-15&DurationSeconds=43200&RoleArn=arn:aws:iam::000000000000:role/dxcat&RoleSessionName=unitycatalog')
field() { printf '%s' "$STS" | sed -n "s|.*<$1>\([^<]*\)</$1>.*|\1|p"; }
KEY=$(field AccessKeyId)
SECRET=$(field SecretAccessKey)
TOKEN=$(field SessionToken)
EXPIRES=$(field Expiration)
if [ -z "$KEY" ] || [ -z "$SECRET" ] || [ -z "$TOKEN" ]; then
  echo "unexpected AssumeRole response: $STS" >&2
  exit 1
fi

if [ -n "${GCS_TABLE_PATH:-}" ]; then
  # The key is private on the host and the server runs as uid 100: it
  # gets a readable copy on the shared volume, next to server.properties.
  cp /gcs-key.json "$GCS_KEY"
  chmod 0644 "$GCS_KEY"
  GCS_BUCKET=$(printf '%s' "$GCS_TABLE_PATH" | sed 's|^\(gs://[^/]*\).*|\1|')
fi

{
  cat /uc/server.properties
  printf '\n# Appended by uc/bootstrap.sh: what the server vends for s3://cat.\n'
  printf 's3.bucketPath.0=s3://cat\n'
  printf 's3.region.0=us-east-1\n'
  printf 's3.accessKey.0=%s\n' "$KEY"
  printf 's3.secretKey.0=%s\n' "$SECRET"
  printf 's3.sessionToken.0=%s\n' "$TOKEN"
  if [ -n "${AZURE_STORAGE_ACCOUNT:-}" ]; then
    # Real ADLS Gen2 account: the server signs user-delegation SAS tokens
    # with this service principal (needs Storage Blob Delegator and
    # Storage Blob Data Reader on the account).
    printf '\n# Appended by uc/bootstrap.sh: what the server vends for abfss://...@%s.dfs.core.windows.net\n' "$AZURE_STORAGE_ACCOUNT"
    printf 'adls.storageAccountName.0=%s\n' "$AZURE_STORAGE_ACCOUNT"
    printf 'adls.tenantId.0=%s\n' "$AZURE_TENANT_ID"
    printf 'adls.clientId.0=%s\n' "$AZURE_CLIENT_ID"
    printf 'adls.clientSecret.0=%s\n' "$AZURE_CLIENT_SECRET"
  fi
  if [ -n "${GCS_TABLE_PATH:-}" ]; then
    # Real GCS bucket: the server exchanges this service account's token
    # for one downscoped to the table path (objectViewer for READ).
    printf '\n# Appended by uc/bootstrap.sh: what the server vends for %s\n' "$GCS_BUCKET"
    printf 'gcs.bucketPath.0=%s\n' "$GCS_BUCKET"
    printf 'gcs.jsonKeyFilePath.0=%s\n' "$GCS_KEY"
  fi
} > "$OUT.tmp"
mv "$OUT.tmp" "$OUT"
echo "unity catalog config written; vended credentials expire at $EXPIRES"
# No account or bucket names here: these lines end up in CI logs.
[ -n "${AZURE_STORAGE_ACCOUNT:-}" ] && echo "azure vending configured"
[ -n "${GCS_TABLE_PATH:-}" ] && echo "gcs vending configured"
exit 0
