"""Seed a single-file 20M row table for throughput benchmarking.

Usage:
    pixi run python seed_xlarge.py
"""

import pyarrow as pa
import numpy as np
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, DoubleType

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

TABLE = "default.xlarge_table"
N = 20_000_000

try:
    catalog.create_namespace("default")
except Exception:
    pass

try:
    catalog.drop_table(TABLE)
    print(f"Dropped existing {TABLE}")
except Exception:
    pass

schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "value", DoubleType(), required=False),
)

table = catalog.create_table(TABLE, schema=schema)
print(f"Created {TABLE}")

# Write all 20M rows in one append → single Parquet file
names = ["alice", "bob", "charlie", "diana", "eve"]
arrow_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=True),
    pa.field("value", pa.float64(), nullable=True),
])

rng = np.random.default_rng(seed=42)
batch = pa.record_batch(
    [
        pa.array(np.arange(0, N, dtype=np.int64)),
        pa.array([names[i % len(names)] for i in range(N)]),
        pa.array(rng.uniform(0, 1000, size=N)),
    ],
    schema=arrow_schema,
)
table.append(pa.Table.from_batches([batch]))
print(f"Inserted {N:,} rows into {TABLE} (single file)")
