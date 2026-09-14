# Catalog stack: Hive Metastore + Unity Catalog next to MinIO and Spark

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
- **MinIO** as the object store both catalogs point into, and **Spark** as
  the writer.

Then `delta-explain` runs on each resolved location, with the same
predicate, and must report the same pruning: three names, three catalog
answers, one table.

This is name resolution only. Tables whose commits are coordinated by the
catalog (`delta.feature.catalogManaged`, Unity Catalog's own "managed
tables" since 0.4) are a different problem and stay refused by
`delta-explain`; the `MANAGED` table here carries no such feature. Credential
vending is not exercised either: the Unity Catalog server has no endpoint
setting for S3-compatible stores, and it never touches storage for the
tables registered here.

## Run it

```bash
docker compose up -d        # or docker-compose up -d; MinIO on :9020,
                            # metastore on :9083, Unity Catalog on :8080
python3 run_catalogs.py     # delta-explain on PATH, or DX_BIN=/path/to/bin
```

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
`docker compose down` removes everything (no volumes): the next `up` starts
from empty catalogs and an empty bucket.

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
Unity Catalog: lake.default.users_mgd
  table_type: MANAGED
  data_source_format: DELTA
  storage_location: s3://cat/uc-managed/__unitystorage/tables/<uuid>
  table_id: <uuid>

delta-explain on the resolved locations, -w "country = 'DE'"
  lake.users                 s3://cat/warehouse/lake.db/users
                             17 files, 5 kept, 70.6% pruned
  lake.default.users_ext     s3://cat/uc-external/users
                             17 files, 5 kept, 70.6% pruned
  lake.default.users_mgd     s3://cat/uc-managed/__unitystorage/tables/<uuid>
                             17 files, 5 kept, 70.6% pruned
```

Facts worth keeping for the resolver design:

- **Schemes differ by writer.** The metastore stores what Spark wrote,
  `s3a://`; Unity Catalog stores `s3://`. `delta-explain` speaks
  object-store URLs, so a Hive location needs its scheme normalized.
- **Unity Catalog** is one HTTP call: `GET /api/2.1/unity-catalog/tables/{full_name}`
  returns `storage_location`, `table_type`, `data_source_format` and
  `table_id`. Managed and external tables resolve the same way.
- **Hive Metastore** is Thrift only. Here the location is read through
  Spark's Hive client (`DESCRIBE DETAIL`); a direct `get_table` from the
  host is the next step once the Rust client is chosen.
- **Version coupling on the Hive side.** Spark's built-in Hive client
  (2.3.x) talks to a 3.1 metastore; a 4.x metastore removes the Thrift
  methods it calls (`Invalid method name: 'get_table'`) and would force
  Spark to download a matching client at every session. A 4.x metastore
  is fine for a client that speaks its API; for this stack the writer
  decides.

## Versions

| component | version | notes |
|---|---|---|
| MinIO | `quay.io/minio/minio:latest` | bucket `cat`, ports 9020/9021 |
| Hive Metastore | `apache/hive:3.1.3` | Hadoop 3.1.0 inside; `HADOOP_OPTIONAL_TOOLS=hadoop-aws` puts the bundled `hadoop-aws` and AWS SDK on the classpath; conf in `hive-metastore/conf` |
| Unity Catalog | `unitycatalog/unitycatalog:v0.6.0` | only `uc/server.properties` is mounted; the image's conf directory also holds dev signing keys |
| Spark | `quay.io/jupyter/pyspark-notebook:spark-4.1.2` | `io.delta:delta-spark_2.13:4.3.0`, `org.apache.hadoop:hadoop-aws:3.4.2`, same as the differential harness |

Ports are offset from the other example stacks (`minio-s3` on 9000/9001,
`differential` on 9010/9011) so all three can run side by side.
