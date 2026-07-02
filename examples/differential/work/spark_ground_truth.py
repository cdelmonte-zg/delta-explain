"""Ground truth for the differential harness, computed by Spark.

Writes the test table once (partitioned by country, age-banded files), then
for every predicate in /home/jovyan/work/predicates.json evaluates which
files actually contain matching rows, via input_file_name(). Emits one JSON
document to /home/jovyan/work/ground_truth.json. The predicate list travels
as a file on the shared volume: argv through docker exec + bash -lc mangles
the quoting of SQL string literals.

The soundness contract this feeds: every file that contains a matching row
must be in delta-explain's survivor set. delta-explain may keep more
(conservative min/max ranges), never fewer.
"""
import json
import os

from pyspark.sql import SparkSession, functions as F

AK = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
SK = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
TABLE = "s3a://diff/users"
OUT = "/home/jovyan/work/ground_truth.json"

spark = (
    SparkSession.builder.appName("dx-differential")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.13:4.3.0,org.apache.hadoop:hadoop-aws:3.4.2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", AK)
    .config("spark.hadoop.fs.s3a.secret.key", SK)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

with open("/home/jovyan/work/predicates.json") as f:
    predicates = json.load(f)

# ── Table: written once, layout with real pruning structure ─────────
# DX_DIFF_FRESH=1 forces a rewrite: an old MinIO volume would otherwise keep
# serving a table whose layout no longer matches what this script writes.
fresh = os.environ.get("DX_DIFF_FRESH") == "1"
exists = False
if not fresh:
    try:
        spark.read.format("delta").load(TABLE).limit(1).collect()
        exists = True
        print("table exists, reusing (DX_DIFF_FRESH=1 to rewrite)")
    except Exception:
        pass
if not exists:
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
       .mode("overwrite").save(TABLE))
    print("table written:", TABLE)

full = spark.read.format("delta").load(TABLE)
total_files = full.select(F.input_file_name()).distinct().count()

# ── Ground truth per predicate ──────────────────────────────────────
results = []
for pred in predicates:
    matched = (
        full.filter(pred)
        .select(F.input_file_name().alias("f"))
        .distinct()
        .collect()
    )
    files = sorted(os.path.basename(r["f"]) for r in matched)
    rows = full.filter(pred).count()
    results.append({"predicate": pred, "match_files": files, "match_rows": rows})
    print(f"  {pred!r}: {len(files)} files contain matches, {rows} rows")

with open(OUT, "w") as f:
    json.dump({"total_files": total_files, "results": results}, f, indent=2)
print("ground truth written:", OUT)

spark.stop()
