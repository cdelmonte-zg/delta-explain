"""Generate the exotic-checkpoint fixtures: a classic multi-part checkpoint
and a V2 (UUID-named) checkpoint. Run inside the dxdiff-spark container:

    $SPARK_HOME/bin/spark-submit --packages io.delta:delta-spark_2.13:4.3.0 \
        create_exotic_checkpoints.py

Copy this script into the harness work dir (examples/differential/work), run
it in the container, then move ./exotic/* into fixtures/ and delete the empty
_staged_commits dirs. Small on purpose: 3 commits of
2 rows each, age ranges disjoint per commit so pruning is predictable.
"""
import os

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("gen-exotic-checkpoints")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # classic checkpoints: split into parts of at most 2 actions
    .config("spark.databricks.delta.checkpoint.partSize", "2")
    .getOrCreate()
)


def write_commits(path, extra_props=""):
    for c in range(3):
        lo = c * 20
        df = spark.createDataFrame(
            [(lo, f"row-{c}-0"), (lo + 9, f"row-{c}-1")], ["age", "name"]
        )
        (
            df.repartition(1)
            .write.format("delta")
            .mode("append" if c else "overwrite")
            .save(path)
        )
    if extra_props:
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ({extra_props})")
        # one more commit after the property change so the checkpoint
        # covers it
        df = spark.createDataFrame([(60, "row-3-0"), (69, "row-3-1")], ["age", "name"])
        df.repartition(1).write.format("delta").mode("append").save(path)


def checkpoint(path):
    jlog = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, path)
    jlog.checkpoint()


# 1. classic multi-part checkpoint (partSize=2 splits it)
multi = os.path.abspath("./exotic/test-table-checkpoint-multipart")
write_commits(multi)
checkpoint(multi)

# 2. V2 checkpoint, UUID-named
v2 = os.path.abspath("./exotic/test-table-checkpoint-v2")
write_commits(v2, extra_props="'delta.checkpointPolicy' = 'v2'")
checkpoint(v2)

print("done")
