# delta-explain against S3-compatible storage (MinIO)

A self-contained example that runs `delta-explain` against an `s3://` table on a
local [MinIO](https://min.io) server — no AWS account, no Spark. It also shows
the tool's core point: the same logical data prunes very differently depending
on physical layout.

## Prerequisites

- `delta-explain` on your PATH (`cargo install --path ../..` from the repo root, or a release binary)
- Docker + `docker compose` (v2) or `docker-compose` (v1)
- Python 3 with `pip`

## 1. Start MinIO and create the bucket

```bash
docker compose up -d            # or: docker-compose up -d
```

MinIO API is on `http://localhost:9000`; the web console is on
`http://localhost:9001` (`minioadmin` / `minioadmin`). The `lake` bucket is
created automatically.

## 2. Write two Delta tables

```bash
pip install -r requirements.txt
python write_tables.py
```

This writes the same rows under two layouts:

- `s3://lake/users` — partitioned by `country`, files sorted into age bands
- `s3://lake/users-flat` — no partitioning, rows shuffled across files

## 3. Explain the pruning

All `s3://` access needs the endpoint and credentials passed as `--option`s:

```bash
delta-explain s3://lake/users \
  --option endpoint=http://localhost:9000 \
  --option allow_http=true \
  --option access_key_id=minioadmin \
  --option secret_access_key=minioadmin \
  --option virtual_hosted_style_request=false \
  --region us-east-1 \
  -w "country = 'DE' AND age > 55"
```

The good layout prunes most files; the flat one prunes nothing:

| table | files | pruned |
|---|---|---|
| `s3://lake/users` | 14 → 3 | **79%** (partition pruning + data skipping) |
| `s3://lake/users-flat` | 6 → 6 | **0%** (every file spans every value) |

(Exact counts depend on data volume; the point is high-vs-zero.)

## Alternative: write the tables from Jupyter

`docker compose up -d` also starts a minimal (no-Spark) Jupyter on port **8889**,
with `deltalake` available, that writes the same two layouts from a notebook:

```bash
docker compose up -d          # or: docker-compose up -d
# open http://localhost:8889/?token=delta  and run minio-s3.ipynb
```

The notebook mirrors `write_tables.py` but runs inside the compose network, so it
reaches MinIO at `minio:9000` (vs `localhost:9000` from the host). It uses port
8889, so it can run **at the same time as the GCS example's notebook** (8888).
`delta-explain` still reads from the host exactly as in step 3.

## 4. Use it as a CI gate

`--min-pruning` turns the analysis into a pass/fail check:

```bash
delta-explain s3://lake/users      … -w "country = 'DE' AND age > 55" --min-pruning 50   # exit 0
delta-explain s3://lake/users-flat … -w "country = 'DE' AND age > 55" --min-pruning 50   # exit 1
```

A pipeline can fail when a writer regresses the layout, before the cost reaches
production.

## Notes

- The same `--option` keys work for any S3-compatible store; point `endpoint` at
  your own MinIO/Ceph/etc. For real AWS, drop `endpoint`/`allow_http` and use
  `--env-creds` or an instance profile.
- Tables live under a bucket sub-prefix (`lake/users`), which works as expected.
- `--option` keys also accept the `aws_`-prefixed spelling (e.g. `aws_endpoint`).

## Teardown

```bash
docker compose down -v
```
