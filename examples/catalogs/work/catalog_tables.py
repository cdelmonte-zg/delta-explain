"""Tables for the catalog stack, written by Spark.

Three copies of the differential harness's `users` table (60k rows,
partitioned by country, age-banded files -> 17 files), one per resolution
path we want to observe:

- `lake.users` in the Hive Metastore: managed, no path given, the metastore
  assigns the location under its warehouse.
- `s3a://cat/uc-external/users`: path write, registered EXTERNAL in Unity
  Catalog by the host driver.
- the Unity Catalog MANAGED table: path write to the staging location the
  catalog issued (read from uc_staging.json, written by the host driver),
  registered MANAGED by the host driver.

Emits locations.json with what Spark's Hive client learned from the
metastore and the schema the Unity Catalog registration needs.
"""
import json
import os

from pyspark.sql import SparkSession, functions as F

AK = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
SK = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
METASTORE = os.environ.get("HIVE_METASTORE_URI", "thrift://metastore:9083")
FRESH = os.environ.get("DX_CAT_FRESH", "") not in ("", "0")
WORK = "/home/jovyan/work"
OUT = f"{WORK}/locations.json"

HIVE_TABLE = "lake.users"
with open(f"{WORK}/uc_staging.json") as f:
    UC_MANAGED = json.load(f)["uc_managed_location"].replace("s3://", "s3a://", 1)
PATH_TABLES = {
    "uc_external": "s3a://cat/uc-external/users",
    "uc_managed": UC_MANAGED,
}

spark = (
    SparkSession.builder.appName("dx-catalogs")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.13:4.3.0,org.apache.hadoop:hadoop-aws:3.4.2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.hive.metastore.uris", METASTORE)
    .config("spark.sql.warehouse.dir", "s3a://cat/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", AK)
    .config("spark.hadoop.fs.s3a.secret.key", SK)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .enableHiveSupport()
    .getOrCreate()
)


def users_df():
    N = 60000
    return (
        spark.range(N)
        .withColumn("age", (F.rand(seed=42) * 52 + 18).cast("int"))
        .withColumn("_c", (F.rand(seed=7) * 3).cast("int"))
        .withColumn("country",
                    F.when(F.col("_c") == 0, "DE")
                     .when(F.col("_c") == 1, "US").otherwise("IT"))
        .withColumn("score", F.round(F.rand(seed=2) * 39 + 60, 1))
        .withColumnRenamed("id", "uid")
        .drop("_c")
        .orderBy("country", "age")
    )


def writer(df):
    return (df.write.format("delta").partitionBy("country")
              .option("maxRecordsPerFile", 4000).mode("overwrite"))


def path_exists(uri):
    if FRESH:
        return False
    try:
        spark.read.format("delta").load(uri).limit(1).collect()
        return True
    except Exception:
        return False


def build_hive_table():
    db = HIVE_TABLE.split(".")[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    if not FRESH and spark.catalog.tableExists(HIVE_TABLE):
        print(f"{HIVE_TABLE} exists in the metastore, reusing")
    else:
        writer(users_df()).saveAsTable(HIVE_TABLE)
        print("wrote", HIVE_TABLE)
    detail = spark.sql(f"DESCRIBE DETAIL {HIVE_TABLE}").collect()[0].asDict()
    return {
        "reference": HIVE_TABLE,
        "location": detail["location"],
        "format": detail["format"],
        "num_files": detail["numFiles"],
        "partition_columns": detail["partitionColumns"],
        "database_location": spark.sql(f"DESCRIBE DATABASE {db}")
            .filter(F.col("info_name") == "Location").collect()[0]["info_value"],
    }


def build_path_table(uri):
    if path_exists(uri):
        print(f"{uri} exists, reusing")
    else:
        writer(users_df()).save(uri)
        print("wrote", uri)
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{uri}`").collect()[0].asDict()
    return {"location": uri, "num_files": detail["numFiles"]}


if __name__ == "__main__":
    out = {
        "schema": users_df().schema.jsonValue()["fields"],
        "hive": build_hive_table(),
    }
    for key, uri in PATH_TABLES.items():
        out[key] = build_path_table(uri)
    with open(OUT, "w") as f:
        json.dump(out, f, indent=2)
    print("locations written to", OUT)
    spark.stop()
