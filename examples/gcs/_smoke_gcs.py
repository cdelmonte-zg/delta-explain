import os
import sys
from pyspark.sql import SparkSession, functions as F

mode = sys.argv[1] if len(sys.argv) > 1 else "good"
BUCKET = os.environ["GCS_BUCKET"]
KEYFILE = "/home/jovyan/key.json"

spark = (
    SparkSession.builder.appName("dx-gcs-smoke")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEYFILE)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

N = 60000
df = (
    spark.range(N)
    .withColumn("age", (F.rand(seed=42) * 52 + 18).cast("int"))
    .withColumn("_c", (F.rand(seed=7) * 3).cast("int"))
    .withColumn("country",
                F.when(F.col("_c") == 0, "DE").when(F.col("_c") == 1, "US").otherwise("IT"))
    .withColumn("score", F.round(F.rand(seed=2) * 39 + 60, 1))
    .withColumnRenamed("id", "uid").drop("_c")
)
TABLE = f"gs://{BUCKET}/lake/users"

if mode == "good":
    (df.orderBy("country", "age").write.format("delta")
       .partitionBy("country").option("maxRecordsPerFile", 4000)
       .option("overwriteSchema", "true").mode("overwrite").save(TABLE))
    print("SMOKE good ->", TABLE)
else:
    (df.repartition(6).write.format("delta")
       .option("overwriteSchema", "true").mode("overwrite").save(TABLE))
    print("SMOKE bad ->", TABLE)
spark.stop()
