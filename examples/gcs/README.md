# delta-explain against a real Google Cloud Storage bucket

Run `delta-explain` against `gs://` tables on an actual GCS bucket — a real
remote example, no emulator. Like the [MinIO example](../minio-s3), it also
shows that the same data prunes very differently depending on physical layout.

> ⚠️ This example talks to real GCS. It needs a GCP project, a bucket, and a
> service-account key, and it will incur (tiny) storage/operation costs.

## 1. One-time GCP setup

> Full step-by-step (install gcloud, billing, teardown): see [GCP_SETUP.md](GCP_SETUP.md).
> Quick version:

```bash
PROJECT=your-project
BUCKET=your-bucket                        # must be globally unique

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
export GCS_BUCKET=your-bucket
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
cp .env.example .env          # then edit it (see below)

# download the shaded GCS connector jar (compose mounts it onto Spark's classpath).
# do this BEFORE `up`: it is bind-mounted as a file, so if it is missing Docker
# would create an empty directory at that path instead and Spark wouldn't find it.
mkdir -p jars
curl -L -o jars/gcs-connector-4.0.4-shaded.jar \
  https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/4.0.4/gcs-connector-4.0.4-shaded.jar

docker compose up -d          # or: docker-compose up -d
# open http://localhost:8888/?token=delta  and run gcs-spark.ipynb
```

The `.env` file is **required** — both variables must be set to real values:

- `GCS_BUCKET` — your bucket name
- `GOOGLE_SERVICE_ACCOUNT` — absolute **host** path to `key.json` (mounted
  read-only into the container at `/home/jovyan/key.json`)

If either is missing or blank, `docker compose up` aborts immediately with a
message telling you which one to set (rather than a cryptic Docker volume
error). Exporting them in your shell instead of using `.env` works too.

The only GCS-specific bit is the Spark config (first cell of `gcs-spark.ipynb`):
the **GCS connector** replaces `hadoop-aws`, and auth is a service-account
keyfile mounted at `/home/jovyan/key.json`. delta-explain still reads from the
host exactly as in step 4.

**Version matrix — the connector must match Spark's bundled Hadoop:**

| Spark image | bundled Hadoop | delta-spark | gcs-connector | auth keys |
|---|---|---|---|---|
| `spark-4.1.x` (this example) | 3.4.x | `_2.13:4.3.0` | `4.0.x` | `fs.gs.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE` + `fs.gs.auth.service.account.json.keyfile` |
| `spark-3.5.x` | 3.3.4 | `_2.12:3.2.0` | `hadoop3-2.2.x` | `google.cloud.auth.service.account.enable` + `…json.keyfile` |

Spark 4 is Scala **2.13** (hence `delta-spark_2.13`). Always use the **`-shaded`**
connector jar (it relocates Guava); the thin jar pulled via `--packages` clashes
with Spark's bundled Guava. The compose file mounts the shaded jar onto Spark's
base classpath (`/usr/local/spark/jars/`) rather than passing it via `spark.jars`:
the filesystem class is then loaded at JVM boot, immune to the session/kernel
staleness that can make a `spark.jars` entry silently ignored in a notebook.

> Validated end-to-end against a real GCS bucket with this setup (Spark 4.1.2):
> healthy layout ~88% pruned (gate exit 0), flat rewrite 0% (gate exit 1).

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
