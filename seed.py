"""Seed the local Iceberg REST catalog with a test namespace and table.

Usage:
    docker compose up -d
    pixi run python seed.py
"""

import pyarrow as pa
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "rest",
    **{
        "type": "rest",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    },
)

# Create namespace
try:
    catalog.create_namespace("default")
    print("Created namespace: default")
except Exception as e:
    print(f"Namespace: {e}")

# Create table with some data
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType,
    NestedField,
    StringType,
    DoubleType,
)

schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "value", DoubleType(), required=False),
)

try:
    table = catalog.create_table("default.test_table", schema=schema)
    print("Created table: default.test_table")
except Exception as e:
    print(f"Table: {e}")
    table = catalog.load_table("default.test_table")

# Append some data
arrow_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=True),
    pa.field("value", pa.float64(), nullable=True),
])
batch = pa.record_batch(
    [
        pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        pa.array(["alice", "bob", "charlie", "diana", "eve"], type=pa.string()),
        pa.array([10.5, 20.3, 30.1, 40.7, 50.2], type=pa.float64()),
    ],
    schema=arrow_schema,
)
table.append(pa.Table.from_batches([batch]))
print(f"Inserted {batch.num_rows} rows into default.test_table")

# Verify
scan = table.scan()
result = scan.to_arrow()
print(f"\nTable contents ({len(result)} rows):")
print(result)
