"""Seed the local Iceberg catalog with a 5M row table.

Usage:
    pixi run python seed_large.py
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

TABLE = "default.large_table"
N = 5_000_000

# Create namespace if needed
try:
    catalog.create_namespace("default")
except Exception:
    pass

# Drop and recreate
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

# Generate data in chunks to avoid memory issues
CHUNK = 1_000_000
names = ["alice", "bob", "charlie", "diana", "eve"]
arrow_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=True),
    pa.field("value", pa.float64(), nullable=True),
])

written = 0
for start in range(0, N, CHUNK):
    size = min(CHUNK, N - start)
    rng = np.random.default_rng(seed=start)

    batch = pa.record_batch(
        [
            pa.array(np.arange(start, start + size, dtype=np.int64)),
            pa.array([names[i % len(names)] for i in range(size)]),
            pa.array(rng.uniform(0, 1000, size=size)),
        ],
        schema=arrow_schema,
    )
    table.append(pa.Table.from_batches([batch]))
    written += size
    print(f"  Written {written:,}/{N:,} rows")

print(f"\nDone. {TABLE} has {written:,} rows in {written // CHUNK} file(s)")
