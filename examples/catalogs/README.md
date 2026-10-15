# Catalog stack: Hive Metastore + Unity Catalog next to RustFS and Spark

`delta-explain` takes a path or an object-store URL. A table that lives in a
catalog has one too; the catalog knows it, the user often does not. This
stack exists to observe exactly what a catalog answers when asked for a
table, so that resolving `db.table` or `catalog.schema.table` to the
location `delta-explain` already reads can be designed against real
services rather than documentation:

- a **Hive Metastore** (3.1.3, Thrift on 9083, embedded Derby) holding a
  managed Delta table written by Spark with `saveAsTable`, location assigned
  by the metastore under its warehouse;
- a **Unity Catalog OSS server** (0.6.0, REST on 8080) holding the same
  table twice: registered `EXTERNAL` over a path Spark wrote, and created
  `MANAGED` through the catalog's staging flow, where the server picks the
  location under its storage root;
- **RustFS** as the object store both catalogs point into, and **Spark** as
  the writer.

Then `delta-explain` runs on each resolved location, with the same
predicate, and must report the same pruning: three names, three catalog
answers, one table.

Credential vending is exercised too: the driver asks Unity Catalog for
temporary `READ` credentials on each table and runs `delta-explain` with
them, first as explicit `--option` keys, then by name through
`--catalog unity`, where the resolver makes the same two calls itself. The
server never touches storage for the tables registered here, and its own
vending goes through AWS STS, which has no endpoint setting for an
S3-compatible store; what it hands out are temporary credentials RustFS
issued at startup (`uc/bootstrap.sh`), configured as static ones to vend.
Optionally, the same server vends for a real ADLS Gen2 account and a real
GCS bucket (see below).

Tables whose commits are coordinated by the catalog
(`delta.feature.catalogManaged`, Unity Catalog's own "managed tables" since
0.4) are a different problem and stay refused by `delta-explain`; the
`MANAGED` table here carries no such feature.

## Run it

```bash
docker compose up -d        # or docker-compose up -d; RustFS on :9020,
                            # metastore on :9083, Unity Catalog on :8080
python3 run_catalogs.py     # delta-explain on PATH, or DX_BIN=/path/to/bin
```

Unity Catalog's own web UI runs on http://localhost:3000: it lists the
catalogs, schemas and tables with their storage locations, the same answers
the driver prints below. The RustFS console on http://localhost:9021/rustfs/console/
(`rustfsadmin` / `rustfsadmin`) shows the bucket; Jupyter on :8891 (token
`catalogs`) reaches the Spark session. The metastore has no UI.

The first Spark run downloads the Delta and hadoop-aws jars (about a
minute). The driver runs three phases, each skippable (`--skip-write`,
`--skip-register`):

1. **Write.** Asks Unity Catalog for a staging location for the managed
   table, then runs Spark inside the container: `lake.users` via
   `saveAsTable` against the metastore, and two path writes. Spark records
   in `work/locations.json` what its Hive client learned from the metastore.
2. **Register.** Creates catalog `lake` and schema `default` in Unity
   Catalog, the `EXTERNAL` table over `s3://cat/uc-external/users`, and the
   `MANAGED` table over its staging location (the server refuses any other
   path, and requires the staging table's id in the table properties).
3. **Resolve.** Prints what each catalog returns for the table, then runs
   `delta-explain` with `-w "country = 'DE'"` on every location.

Tables are written once and reused; `DX_CAT_FRESH=1` forces a rewrite.
`docker compose stop` / `start` keeps the catalogs' state (both live inside
their containers) and renews the vended credentials, which RustFS limits
to 12 hours; restart the whole stack, not `unitycatalog` alone, which
would wait for a bootstrap that is not running. `docker compose down -v`
removes everything: the next `up` starts from empty catalogs and an empty
bucket.

## What the catalogs answer

```
Hive Metastore: lake.users
  location (via Spark's Hive client): s3a://cat/warehouse/lake.db/users
  database location: s3a://cat/warehouse/lake.db
  format: delta, files: 17, partitioned by ['country']
Unity Catalog: lake.default.users_ext
  table_type: EXTERNAL
  data_source_format: DELTA
  storage_location: s3://cat/uc-external/users
  table_id: <uuid>
  vended: aws_temp_credentials, access key <key>, expiration unreported
Unity Catalog: lake.default.users_mgd
  table_type: MANAGED
  data_source_format: DELTA
  storage_location: s3://cat/uc-managed/__unitystorage/tables/<uuid>
  table_id: <uuid>
  vended: aws_temp_credentials, access key <key>, expiration unreported

delta-explain on the resolved locations, -w "country = 'DE'"
  lake.users                 s3://cat/warehouse/lake.db/users
                             17 files, 5 kept, 70.6% pruned
  lake.default.users_ext     s3://cat/uc-external/users
                             17 files, 5 kept, 70.6% pruned
  lake.default.users_mgd     s3://cat/uc-managed/__unitystorage/tables/<uuid>
                             17 files, 5 kept, 70.6% pruned

same, with the credentials Unity Catalog vended, as --option
  lake.default.users_ext     17 files, 5 kept, 70.6% pruned
  lake.default.users_mgd     17 files, 5 kept, 70.6% pruned

same, by name through --catalog unity
  lake.default.users_ext     17 files, 5 kept, 70.6% pruned
  lake.default.users_mgd     17 files, 5 kept, 70.6% pruned
```

The last block is what a user runs; the driver only sets the two variables
the resolver reads and keeps the RustFS transport options, which stay
user-controlled in catalog mode:

```bash
DATABRICKS_HOST=http://localhost:8080 DATABRICKS_TOKEN=unused \
  delta-explain --catalog unity lake.default.users_ext -w "country = 'DE'" \
  --option aws_endpoint=http://localhost:9020 --option aws_allow_http=true \
  --option aws_virtual_hosted_style_request=false --option aws_region=us-east-1
```

Facts worth keeping for the resolver design:

- **Schemes differ by writer.** The metastore stores what Spark wrote,
  `s3a://`; Unity Catalog stores `s3://`. `delta-explain` speaks
  object-store URLs, so a Hive location needs its scheme normalized.
- **Unity Catalog** is one HTTP call: `GET /api/2.1/unity-catalog/tables/{full_name}`
  returns `storage_location`, `table_type`, `data_source_format` and
  `table_id`. Managed and external tables resolve the same way.
- **Vending is one more call**: `POST /api/2.1/unity-catalog/temporary-table-credentials`
  with `{table_id, operation: "READ"}` returns `aws_temp_credentials`
  (`access_key_id`, `secret_access_key`, `session_token`), `url` (the
  table's location) and `expiration_time`, which the server leaves `null`
  when it vends statically configured credentials; the Azure and GCP
  fields come back `null`. An error is `{error_code, message}` with the
  HTTP status (a bucket the server has no configuration for is a 400
  `FAILED_PRECONDITION`). RustFS accepts only session tokens it issued
  itself, which is why the bootstrap obtains them from its STS endpoint
  (`AssumeRole`, 12 hours at most).
- **Hive Metastore** is Thrift only. Here the location is read through
  Spark's Hive client (`DESCRIBE DETAIL`); a direct `get_table` from the
  host is the next step once the Rust client is chosen.
- **Version coupling on the Hive side.** Spark's built-in Hive client
  (2.3.x) talks to a 3.1 metastore; a 4.x metastore removes the Thrift
  methods it calls (`Invalid method name: 'get_table'`) and would force
  Spark to download a matching client at every session. A 4.x metastore
  is fine for a client that speaks its API; for this stack the writer
  decides.

## Azure

The same vending path against a real ADLS Gen2 account, optional: Unity
Catalog signs a user-delegation SAS for the table path, which needs an
account with the hierarchical namespace enabled and a service principal
the server authenticates as. The stack registers the table as
`lake.default.users_azure` and reads it only with what the catalog vends;
the host itself holds no Azure credentials.

```bash
SCOPE=$(az storage account show -n <account> --query id -o tsv)
az ad sp create-for-rbac --name delta-explain-catalogs \
  --role "Storage Blob Data Reader" --scopes "$SCOPE"   # tenant, appId, password
az role assignment create --assignee <appId> --role "Storage Blob Delegator" --scopes "$SCOPE"
```

Copy `.env.example` to `.env`, fill in the four `AZURE_*` values and the
table's `abfss://container@account.dfs.core.windows.net/path`, then
recreate the two containers that read them, so the bootstrap writes the
`adls.*` configuration and the server starts on it:

```bash
docker compose up -d                       # recreates bootstrap and unitycatalog
# docker-compose v1 cannot recreate in place:
docker-compose rm -sf bootstrap unitycatalog && docker-compose up -d
python3 run_catalogs.py                    # re-stages the MANAGED table too
```

The driver registers the table and prints:

```
Unity Catalog: lake.default.users_azure
  table_type: EXTERNAL
  data_source_format: DELTA
  storage_location: abfss://lake@<account>.dfs.core.windows.net/users
  table_id: <uuid>
  vended: azure_user_delegation_sas, expiration <timestamp>
...
same, with the credentials Unity Catalog vended, as --option
  lake.default.users_azure   15 files, 5 kept, 66.7% pruned

same, by name through --catalog unity
  lake.default.users_azure   15 files, 5 kept, 66.7% pruned
```

The SAS carries read and list on the table directory and lasts one hour;
`abfss` is the only Azure scheme the server dispatches (`az://` and
`https://` locations are not vended).

## GCS

The same again for a real GCS bucket: the server holds a service-account
key and exchanges its token for one downscoped to the table path (Google's
credential access boundary, `roles/storage.objectViewer` for `READ`, one
hour). The key needs read access on the bucket, nothing more; the
`examples/gcs` service account qualifies. The stack registers the table as
`lake.default.users_gcs` and, as for Azure, the host holds no GCS
credentials of its own.

Add to `.env` the host path of the key and the table location, then
recreate the two containers as above:

```bash
GOOGLE_SERVICE_ACCOUNT=/path/to/key.json
GCS_TABLE_PATH=gs://bucket/lake/users
```

The driver prints:

```
Unity Catalog: lake.default.users_gcs
  table_type: EXTERNAL
  data_source_format: DELTA
  storage_location: gs://<bucket>/lake/users
  table_id: <uuid>
  vended: gcp_oauth_token, expiration <timestamp>
...
same, with the credentials Unity Catalog vended, as --option
  lake.default.users_gcs     no --option key for a GCS OAuth token

same, by name through --catalog unity
  lake.default.users_gcs     14 files, 5 kept, 64.3% pruned
```

The `--option` row is empty by construction: the `object_store` release the
kernel pins has no key for a bearer token, so a vended GCS token only
travels through `--catalog unity`, which installs it on the store as a
credential provider.

## Versions

| component | version | notes |
|---|---|---|
| RustFS | `rustfs/rustfs:1.0.0` | bucket `cat`, ports 9020/9021; the same image runs `uc/bootstrap.sh` with `curl`: bucket creation, STS credentials, Unity Catalog config |
| Hive Metastore | `apache/hive:3.1.3` | Hadoop 3.1.0 inside; `HADOOP_OPTIONAL_TOOLS=hadoop-aws` puts the bundled `hadoop-aws` and AWS SDK on the classpath; conf in `hive-metastore/conf`; the entrypoint is wrapped so a restarted container skips the Derby schema init it would otherwise fail on |
| Unity Catalog | `unitycatalog/unitycatalog:v0.6.0` | only `uc/server.properties` is mounted; the image's conf directory also holds dev signing keys |
| Unity Catalog UI | `unitycatalog/unitycatalog-ui:main` | no versioned tag is published; the image reaches the server at the host name `server`, an alias of the catalog service |
| Spark | `quay.io/jupyter/pyspark-notebook:spark-4.1.2` | `io.delta:delta-spark_2.13:4.3.0`, `org.apache.hadoop:hadoop-aws:3.4.2`, same as the differential harness |

Ports are offset from the other example stacks (`s3-compatible` on 9000/9001,
`differential` on 9010/9011) so all three can run side by side.
