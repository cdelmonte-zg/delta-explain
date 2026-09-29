"""Ground truth for the differential harness, computed by Spark.

Writes each table in /home/jovyan/work/predicates.json once, then for every
predicate evaluates which files actually contain matching rows, via
input_file_name(). Emits ground_truth.json keyed by table name.

Two tables:
- `users`: synthetic (partitioned by country, age-banded files).
- `taxi`: real NYC TLC yellow-taxi data read from work/taxi-src.parquet,
  partitioned by pickup date - a real writer's layout and statistics.

The soundness contract this feeds: every file that contains a matching row
must be in delta-explain's survivor set. delta-explain may keep more
(conservative min/max ranges), never fewer.
"""
import json
import os

from pyspark.sql import SparkSession, functions as F

AK = os.environ.get("AWS_ACCESS_KEY_ID", "rustfsadmin")
SK = os.environ.get("AWS_SECRET_ACCESS_KEY", "rustfsadmin")
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
REAL_S3 = os.environ.get("DX_DIFF_REAL_S3") == "1"
REGION = os.environ.get("AWS_REGION", "")
# Spark speaks s3a://; the harness hands the prefix over in the s3:// form
# delta-explain reads.
S3_PREFIX = os.environ.get("DX_DIFF_S3_PREFIX", "s3://diff").rstrip("/")
S3A_PREFIX = "s3a://" + S3_PREFIX.split("://", 1)[1]
OUT = "/home/jovyan/work/ground_truth.json"
TAXI_SRC = "/home/jovyan/work/taxi-src.parquet"

builder = (
    SparkSession.builder.appName("dx-differential")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.13:4.3.0,org.apache.hadoop:hadoop-aws:3.4.2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.access.key", AK)
    .config("spark.hadoop.fs.s3a.secret.key", SK)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
)
if REAL_S3:
    # AWS proper: default endpoint, TLS, virtual-hosted style; the region
    # spares the SDK a probe that the demo IAM user is not allowed to make.
    builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", REGION)
else:
    builder = (
        builder.config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

with open("/home/jovyan/work/predicates.json") as f:
    config = json.load(f)

# DX_DIFF_FRESH=1 forces a rewrite: an old data volume would otherwise keep
# serving a table whose layout no longer matches what this script writes.
FRESH = os.environ.get("DX_DIFF_FRESH") == "1"


def table_exists(uri):
    if FRESH:
        return False
    try:
        spark.read.format("delta").load(uri).limit(1).collect()
        return True
    except Exception:
        return False


def build_users(uri):
    if table_exists(uri):
        print(f"{uri} exists, reusing")
        return
    N = 60000
    df = (
        spark.range(N)
        .withColumn("age", (F.rand(seed=42) * 52 + 18).cast("int"))
        .withColumn("_c", (F.rand(seed=7) * 3).cast("int"))
        .withColumn("country",
                    F.when(F.col("_c") == 0, "DE")
                     .when(F.col("_c") == 1, "US").otherwise("IT"))
        .withColumn("score", F.round(F.rand(seed=2) * 39 + 60, 1))
        .withColumnRenamed("id", "uid")
        .drop("_c")
    )
    (df.orderBy("country", "age").write.format("delta")
       .partitionBy("country").option("maxRecordsPerFile", 4000)
       .mode("overwrite").save(uri))
    print("wrote", uri)


def build_taxi(uri):
    if table_exists(uri):
        print(f"{uri} exists, reusing")
        return
    cols = ["tpep_pickup_datetime", "trip_distance", "PULocationID",
            "DOLocationID", "payment_type", "fare_amount", "tip_amount", "total_amount"]
    src = spark.read.parquet(TAXI_SRC).select(*cols)
    df = src.withColumn("pickup_date", F.date_format("tpep_pickup_datetime", "yyyy-MM-dd"))
    # Filter to the first week FIRST, then cap: a plain .limit() before the
    # filter takes the first N rows in file order, which would silently yield
    # an empty table if the source is not time-ordered.
    week = [f"2024-01-0{d}" for d in range(1, 8)]
    df = df.filter(F.col("pickup_date").isin(week)).limit(900_000)
    # sort by (date, fare) and cap file size so each day splits into several
    # files with tight fare ranges - the layout that makes data skipping work,
    # and the interesting case to test against Spark.
    (df.orderBy("pickup_date", "fare_amount").write.format("delta")
       .partitionBy("pickup_date").option("maxRecordsPerFile", 20000)
       .mode("overwrite").save(uri))
    print("wrote", uri)


BUILDERS = {"users": build_users, "taxi": build_taxi}
URIS = {"users": f"{S3A_PREFIX}/users", "taxi": f"{S3A_PREFIX}/taxi"}

out = {}
for name, predicates in config.items():
    if name not in BUILDERS:
        raise SystemExit(
            f"unknown table {name!r} in predicates.json; add a builder and URI "
            f"here (known: {sorted(BUILDERS)})"
        )
    uri = URIS[name]
    BUILDERS[name](uri)
    full = spark.read.format("delta").load(uri)
    total_files = full.select(F.input_file_name()).distinct().count()

    results = []
    for pred in predicates:
        matched = (full.filter(pred).select(F.input_file_name().alias("f"))
                   .distinct().collect())
        files = sorted(os.path.basename(r["f"]) for r in matched)
        rows = full.filter(pred).count()
        results.append({"predicate": pred, "match_files": files, "match_rows": rows})
        print(f"  [{name}] {pred!r}: {len(files)} files, {rows} rows")
    out[name] = {"total_files": total_files, "results": results}

with open(OUT, "w") as f:
    json.dump(out, f, indent=2)
print("ground truth written:", OUT)

spark.stop()
