# GCP setup for the GCS example

Step-by-step to go from "I have a GCP account" to running this example against a
real bucket. Plain `gcloud` — no Terraform needed for a throwaway demo.

Interactive steps (browser) must run in **your own terminal**; the rest can run
anywhere `gcloud` is authenticated.

## 1. Install the gcloud CLI

```bash
sudo snap install google-cloud-cli --classic
# (alternative: the official Google apt repo)
```

## 2. Log in and pick a project

```bash
gcloud auth login                          # interactive (browser)
gcloud config set project YOUR_PROJECT     # gcloud projects list  to see them
```

No project yet?

```bash
gcloud projects create delta-explain-demo --name="delta-explain demo"
gcloud config set project delta-explain-demo
```

> ⚠️ **Billing must be enabled** — GCS needs it (even on the free trial a billing
> account must be linked). Check:
> ```bash
> gcloud billing projects describe "$(gcloud config get-value project)"
> ```
> If `billingEnabled: false`, link a billing account in the Console (Billing →
> Link a billing account).

## 3. Enable the Storage API

```bash
gcloud services enable storage.googleapis.com
```

## 4. Create the bucket (name must be globally unique)

```bash
BUCKET=delta-explain-demo-$RANDOM
gcloud storage buckets create "gs://$BUCKET" --location=EU
```

## 5. Service account + permissions + key

```bash
PROJECT=$(gcloud config get-value project)
gcloud iam service-accounts create delta-explain-demo

SA="delta-explain-demo@${PROJECT}.iam.gserviceaccount.com"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$SA" --role=roles/storage.objectAdmin

# download the key — keep it out of git (this dir's .gitignore already excludes *.json)
gcloud iam service-accounts keys create key.json --iam-account="$SA"
```

## 6. Point the example at it

```bash
export GCS_BUCKET=$BUCKET
export GOOGLE_SERVICE_ACCOUNT="$PWD/key.json"
cd examples/gcs
```

## 7. Create the tables and explain

**Lightweight (no Spark):**

```bash
pip install -r requirements.txt
python write_tables.py

delta-explain "gs://$GCS_BUCKET/lake/users" \
  --option service_account="$GOOGLE_SERVICE_ACCOUNT" \
  -w "country = 'DE' AND age > 55" --min-pruning 50          # exit 0

delta-explain "gs://$GCS_BUCKET/lake/users-flat" \
  --option service_account="$GOOGLE_SERVICE_ACCOUNT" \
  -w "country = 'DE' AND age > 55" --min-pruning 50          # exit 1
```

**From Jupyter/Spark instead:** `cp .env.example .env` (fill in the two values),
`docker compose up -d`, open `gcs-spark.ipynb`. See [README.md](README.md).

## 8. Teardown (so nothing keeps costing)

```bash
gcloud storage rm -r "gs://$GCS_BUCKET"
gcloud storage buckets delete "gs://$GCS_BUCKET"
gcloud iam service-accounts delete "$SA"
rm -f key.json
```

## Notes

- **Service-account key, not ADC.** `gcloud auth application-default login`
  produces *user* credentials that object_store / delta-explain may not accept;
  this example uses a service-account key. On GCE/GKE with a default service
  account you can rely on the metadata server and omit `--option` entirely.
- Non-interactive `gcloud` steps (project/bucket/SA/key) can also run in a
  Claude Code session with the `! ` prefix; `gcloud auth login` cannot (browser).
