"""Create a test Delta table with partition on country and age column with varied stats."""
import pyarrow as pa
from deltalake import write_deltalake

TABLE_PATH = "./test-table"

# We write multiple batches to get separate parquet files with different min/max stats.
# Partition by country => 3 partitions: DE, US, IT

batches = [
    # DE partition - file 1: age 20-35
    pa.table({
        "name": pa.array(["Hans", "Greta", "Klaus", "Liesel", "Fritz"]),
        "age": pa.array([25, 30, 35, 20, 28], type=pa.int32()),
        "country": pa.array(["DE", "DE", "DE", "DE", "DE"]),
        "score": pa.array([88.5, 92.0, 75.3, 91.2, 83.1], type=pa.float64()),
    }),
    # DE partition - file 2: age 40-60
    pa.table({
        "name": pa.array(["Dieter", "Helga", "Wolfgang", "Ursula"]),
        "age": pa.array([45, 52, 60, 40], type=pa.int32()),
        "country": pa.array(["DE", "DE", "DE", "DE"]),
        "score": pa.array([70.0, 65.5, 88.9, 77.3], type=pa.float64()),
    }),
    # US partition - file 1: age 18-29
    pa.table({
        "name": pa.array(["Alice", "Bob", "Charlie", "Diana"]),
        "age": pa.array([22, 18, 29, 25], type=pa.int32()),
        "country": pa.array(["US", "US", "US", "US"]),
        "score": pa.array([95.0, 78.2, 85.6, 90.1], type=pa.float64()),
    }),
    # US partition - file 2: age 31-55
    pa.table({
        "name": pa.array(["Eve", "Frank", "Grace"]),
        "age": pa.array([31, 45, 55], type=pa.int32()),
        "country": pa.array(["US", "US", "US"]),
        "score": pa.array([82.0, 71.5, 93.4], type=pa.float64()),
    }),
    # IT partition - file 1: age 22-38
    pa.table({
        "name": pa.array(["Marco", "Giulia", "Luca", "Sofia", "Alessandro"]),
        "age": pa.array([22, 35, 28, 38, 30], type=pa.int32()),
        "country": pa.array(["IT", "IT", "IT", "IT", "IT"]),
        "score": pa.array([87.0, 91.5, 76.8, 84.2, 89.3], type=pa.float64()),
    }),
    # IT partition - file 2: age 41-65
    pa.table({
        "name": pa.array(["Giovanni", "Maria", "Roberto"]),
        "age": pa.array([41, 58, 65], type=pa.int32()),
        "country": pa.array(["IT", "IT", "IT"]),
        "score": pa.array([68.0, 72.5, 80.1], type=pa.float64()),
    }),
]

# Write first batch to create the table
write_deltalake(
    TABLE_PATH,
    batches[0],
    partition_by=["country"],
    mode="overwrite",
)

# Append remaining batches as separate files
for batch in batches[1:]:
    write_deltalake(
        TABLE_PATH,
        batch,
        partition_by=["country"],
        mode="append",
    )

print(f"Test table created at {TABLE_PATH}")
print(f"  Partitions: DE, US, IT")
print(f"  Total batches written: {len(batches)} (each as separate file)")
print(f"  Age ranges per file vary to enable data skipping")
