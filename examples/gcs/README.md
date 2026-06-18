# delta-explain against a real Google Cloud Storage bucket

Run `delta-explain` against `gs://` tables on an actual GCS bucket — a real
remote example, no emulator. Like the [MinIO example](../minio-s3), it also
shows that the same data prunes very differently depending on physical layout.

> ⚠️ This example talks to real GCS. It needs a GCP project, a bucket, and a
> service-account key, and it will incur (tiny) storage/operation costs.

## 1. One-time GCP setup

```bash
PROJECT=your-project
BUCKET=my-delta-demo                      # must be globally unique

gcloud config set project "$PROJECT"
gcloud storage buckets create "gs://$BUCKET" --location=EU

# a service account that can read/write the bucket
gcloud iam service-accounts create delta-explain-demo
SA="delta-explain-demo@${PROJECT}.iam.gserviceaccount.com"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$SA" --role=roles/storage.objectAdmin

# download a key (keep it out of git — this dir's .gitignore already excludes *.json)
gcloud iam service-accounts keys create key.json --iam-account="$SA"
```

## 2. Point the tools at it

```bash
export GCS_BUCKET=my-delta-demo
export GOOGLE_SERVICE_ACCOUNT="$PWD/key.json"
```

## 3. Write two Delta tables

```bash
pip install -r requirements.txt
python write_tables.py
```

- `gs://$GCS_BUCKET/lake/users` — partitioned by `country`, files sorted into age bands
- `gs://$GCS_BUCKET/lake/users-flat` — no partitioning, rows shuffled across files

## 4. Explain the pruning

Credentials are passed with `--option service_account=<key path>`:

```bash
delta-explain "gs://$GCS_BUCKET/lake/users" \
  --option service_account="$GOOGLE_SERVICE_ACCOUNT" \
  -w "country = 'DE' AND age > 55"

delta-explain "gs://$GCS_BUCKET/lake/users-flat" \
  --option service_account="$GOOGLE_SERVICE_ACCOUNT" \
  -w "country = 'DE' AND age > 55"
```

Expect strong pruning on the partitioned table and ~none on the flat one
(same shape as the MinIO example: roughly 79% vs 0%; exact counts vary).

## 5. CI gate

```bash
delta-explain "gs://$GCS_BUCKET/lake/users" \
  --option service_account="$GOOGLE_SERVICE_ACCOUNT" \
  -w "country = 'DE' AND age > 55" --min-pruning 50          # exit 0

delta-explain "gs://$GCS_BUCKET/lake/users-flat" \
  --option service_account="$GOOGLE_SERVICE_ACCOUNT" \
  -w "country = 'DE' AND age > 55" --min-pruning 50          # exit 1
```

## Alternative: write the tables from Jupyter / Spark

If you'd rather create the tables with Spark (e.g. to match a Databricks-style
workflow), use the bundled notebook instead of `write_tables.py`:

```bash
cp .env.example .env          # set GCS_BUCKET and GOOGLE_SERVICE_ACCOUNT (host path to key.json)
docker compose up -d          # or: docker-compose up -d
# open http://localhost:8888/?token=delta  and run gcs-spark.ipynb
```

The only GCS-specific bit is the Spark config: the **GCS connector**
(`com.google.cloud.bigdataoss:gcs-connector`) replaces `hadoop-aws`, and auth is
a service-account keyfile — see the first cell of `gcs-spark.ipynb`. The key is
mounted into the container at `/home/jovyan/key.json` by `docker-compose.yml`.
delta-explain still reads from the host exactly as in step 4.

## Notes

- Credential `--option` aliases accepted: `service_account` / `service_account_path`
  (key file path), `service_account_key` (inline JSON), or the `google_*` spellings.
  On GCE/GKE with a default service account you can rely on the metadata server and
  omit the option entirely.
- Tables live under a bucket sub-prefix (`lake/users`) — supported.

## Teardown

```bash
gcloud storage rm -r "gs://$GCS_BUCKET/lake"
# or remove the whole bucket:
# gcloud storage buckets delete "gs://$GCS_BUCKET"
```
