# ADBC Iceberg Driver

An [ADBC](https://arrow.apache.org/adbc/) driver for [Apache Iceberg](https://iceberg.apache.org/) tables, implemented in Rust. It enables any ADBC-compatible query engine to read Iceberg tables through a standard Arrow-native interface.

The idea: instead of each engine (DuckDB, DataFusion, Spark, etc.) implementing its own Iceberg connector, they all speak ADBC and this driver handles the Iceberg complexity once.

## Implementation

| | |
|---|---|
| Iceberg library | [iceberg-rust](https://github.com/apache/iceberg-rust) (pinned `main` commit) |
| Parquet reader | parquet-rs |
| S3 client | opendal |
| ADBC framework | [adbc_core](https://crates.io/crates/adbc_core) + [adbc_ffi](https://crates.io/crates/adbc_ffi) |
| Catalog support | REST (+ Glue, HMS, SQL, S3 Tables available via iceberg-rust) |
| Query input | Restricted SQL or a Substrait plan |
| Column projection | Yes |
| Predicate pushdown | Row group stats + page index + row filter |
| Partitioned execution | One partition per data file; each reads only its assigned file |
| Catalog enumeration | GetObjects, GetTableSchema, GetTableTypes, GetInfo |
| Statement options | Snapshot ID, time travel, branch/tag, batch size |

The driver is loadable from Python, DuckDB, or any ADBC consumer. An earlier Go implementation
was removed; it is preserved at the `go-driver-final` tag.

## Install with dbc

Releases double as a [dbc](https://github.com/columnar-tech/dbc) driver registry: every
release carries an `index.yaml` describing all published versions, so dbc can install the
driver without the package being mirrored into a central registry.

Add the repository as a registry in `dbc.toml` (or the global `config.toml`):

```toml
[[registries]]
name = 'adbc-iceberg'
url  = 'https://github.com/tokoko/adbc-iceberg/releases/latest/download'
```

Then install as usual:

```bash
dbc install iceberg --no-verify
dbc install 'iceberg>=0.1' --no-verify
```

```python
from adbc_driver_manager import dbapi

with dbapi.connect(driver="iceberg", db_kwargs={...}) as conn:
    ...
```

`--no-verify` is required because these artifacts are not signed by Columnar. dbc only
ships the Columnar public key, so it cannot verify a third-party publisher; the flag tells
it to skip the signature check. Install only if you trust this repository.

Currently published for `linux_amd64` only. On other platforms dbc reports
`no package found for platform '<tuple>'`; build from source instead.

## Quick Start

```bash
# Install dependencies
pixi install

# Start local Iceberg catalog (REST + MinIO)
docker compose up -d

# Build the drivers
pixi run build

# Seed test data (5M rows)
pixi run python seed_large.py

# Run the example
pixi run python example.py

# Run benchmarks
pixi run python benchmark.py
```

## Usage from Python

```python
import adbc_driver_manager.dbapi

conn = adbc_driver_manager.dbapi.connect(
    driver="./build/libadbc_driver_iceberg_rust.so",
    entrypoint="AdbcDriverIcebergInit",
    db_kwargs={
        "uri": "http://localhost:8181",
        "adbc.iceberg.catalog.name": "rest",
        "adbc.iceberg.s3.endpoint": "http://localhost:9000",
        "adbc.iceberg.s3.access_key": "admin",
        "adbc.iceberg.s3.secret_key": "password",
        # MinIO needs path-style URLs
        "adbc.iceberg.s3.path_style_access": "true",
    },
)

with conn.cursor() as cur:
    cur.execute("SELECT * FROM default.my_table WHERE id < 100")
    table = cur.fetch_arrow_table()
    print(table)
```

## Usage from DuckDB (via adbc_scanner)

```sql
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;

SELECT adbc_connect({
    'driver': './build/libadbc_driver_iceberg_rust.so',
    'entrypoint': 'AdbcDriverIcebergInit',
    'uri': 'http://localhost:8181',
    'adbc.iceberg.catalog.name': 'rest',
    'adbc.iceberg.s3.endpoint': 'http://localhost:9000',
    'adbc.iceberg.s3.access_key': 'admin',
    'adbc.iceberg.s3.secret_key': 'password',
    'adbc.iceberg.s3.path_style_access': 'true'
});

SELECT * FROM adbc_scan(0, 'SELECT * FROM default.my_table');
```

## Configuration

### Database Options

| Option | Description |
|---|---|
| `uri` | REST catalog URI |
| `adbc.iceberg.catalog.name` | Catalog name (default: `"rest"`) |
| `adbc.iceberg.auth.token` | Static bearer token |
| `adbc.iceberg.auth.credential` | OAuth2 client credentials |
| `adbc.iceberg.auth.scope` | OAuth2 scope |
| `adbc.iceberg.auth.uri` | Custom token endpoint URL |
| `adbc.iceberg.s3.endpoint` | S3-compatible endpoint URL |
| `adbc.iceberg.s3.region` | AWS region |
| `adbc.iceberg.s3.access_key` | S3 access key ID |
| `adbc.iceberg.s3.secret_key` | S3 secret access key |
| `adbc.iceberg.s3.path_style_access` | Use path-style S3 URLs instead of virtual-hosted-style (default: `false`); set to `true` for MinIO |

### Statement Options

| Option | Description |
|---|---|
| `adbc.iceberg.snapshot_id` | Scan a specific snapshot (time travel) |
| `adbc.iceberg.as_of_timestamp` | Scan as of timestamp (ms since epoch) |
| `adbc.iceberg.branch` | Scan the head of a named branch or tag |
| `adbc.iceberg.batch_size` | Arrow batch size |

`snapshot_id`, `as_of_timestamp` and `branch` are mutually exclusive, and
setting an option to an empty string clears it. Partitions from `ExecutePartitions` are pinned
to the snapshot they were planned against.

### Connection Options

The standard `adbc.connection.catalog` and `adbc.connection.db_schema` options are supported.
The current db_schema is the namespace used for unqualified table names (default: `default`).

## SQL Support

The driver accepts a restricted SQL subset:

```sql
SELECT <columns | *> FROM [schema.]<table> [WHERE <filter>]
```

Supported WHERE operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`, parentheses.

## Substrait Support

A statement also accepts a serialized [Substrait](https://substrait.io/) `Plan` in place of SQL
(`AdbcStatementSetSubstraitPlan`; from Python, pass `bytes` to `cursor.execute`). The plan must
reduce to a single table scan:

- **Relations**: a `ReadRel` on a named table, optionally under `FilterRel` and `ProjectRel` nodes.
  A projection may only pick columns. Anything else (fetch, sort, aggregate, join) is rejected.
- **Table name**: the last name is the table and the names before it are the namespace; a single
  name uses the connection's current db_schema.
- **Filters**: `and`, `or`, `not`, `equal`, `not_equal`, `lt`, `lte`, `gt`, `gte`, `is_null`,
  `is_not_null`, `is_nan`, `between`, `starts_with`, `like` with a prefix pattern (`'abc%'`), and
  `IN` lists. Comparisons are between a top-level column and a literal.
- **Literals**: boolean, integers, floats, strings, binary, date, time, timestamp (with or without
  time zone), decimal and UUID.

A plan the driver cannot translate exactly is rejected with an error instead of being run with a
weaker filter. The one exception is `ReadRel.best_effort_filter`, which is dropped if it cannot be
translated. Column renames in the plan root are not supported.

```python
from datafusion import SessionContext, substrait

plan = substrait.Serde.serialize_bytes("SELECT id, name FROM my_table WHERE id < 100", ctx)
with conn.cursor() as cur:
    cur.execute(plan)
    table = cur.fetch_arrow_table()
```

## Benchmarks

5,000,000 rows across 5 Parquet files, unpartitioned table, reading from a local REST catalog + MinIO. Median of five runs. PyIceberg is included as a baseline; it reads through PyArrow's C++ Parquet reader and S3 client, not iceberg-rust, so it shares no reader code with the driver.

```
Engine                         full scan    projection    id < 100    name=alice    proj+pred
─────────────────────────────  ─────────    ──────────    ────────    ──────────    ─────────
DuckDB Iceberg (built-in)        0.36s        0.22s        0.07s        0.26s        0.07s
ADBC Iceberg                     0.37s        0.22s        0.07s        0.38s        0.07s
PyIceberg (baseline)             0.16s        0.09s        0.06s        0.16s        0.04s
DuckDB adbc_scan()               0.57s        0.37s        0.13s        0.60s        0.12s
DataFusion + ADBC                0.53s        0.34s        0.07s        0.40s        0.07s
Partitioned (5x)                 0.33s           —            —            —            —
```

### Key Findings

**Full scan**: the driver (0.37s) is on par with DuckDB's built-in Iceberg reader (0.36s) but more than 2x slower than PyIceberg (0.16s). Raw benchmarking showed opendal reading bytes 2.8x slower than `object_store` — there is an [active PR](https://github.com/apache/iceberg-rust/pull/3165) to add `object_store` as an alternative storage backend for iceberg-rust.

**Predicate pushdown (`name='alice'`)**: parquet-rs supports page index filtering and dictionary filtering, skipping pages that can't match.

**Highly selective (`id < 100`)**: All engines are comparable (~0.06-0.07s) because row group statistics pruning skips most data regardless of reader implementation.

**Partitioned execution**: reads exactly 5M rows (1M per partition) in about the same time as a single full scan.

## Architecture

```
Query Engine (DuckDB / DataFusion / Python / ...)
    │
    ▼
ADBC Interface (C Data Interface)
    │
    ▼
┌──────────────┐
│ Rust Driver  │
│ iceberg-rust │
│  opendal S3  │
│  adbc_ffi    │
└──────────────┘
    │
    ▼
Iceberg REST Catalog ──► S3 / GCS / Azure
```

## Known Limitations

- **opendal S3 throughput** — 2.8x slower than `object_store` for raw reads; pending [upstream fix](https://github.com/apache/iceberg-rust/pull/3165)
- **Unreleased iceberg-rust** — pinned to a `main` commit for serializable `FileScanTask` ([#3091](https://github.com/apache/iceberg-rust/pull/3091)), which lets `ReadPartition` read its file without re-planning; move back to a crates.io release once one includes it
- **Read-only** — no write/append support
- **REST catalog only** (Glue, HMS, SQL could be added via iceberg-rust crates)
- **Parquet only** — no ORC or Avro

## Project Structure

```
├── rust/                   # Rust driver
│   ├── src/lib.rs          # Driver, Database, Connection, Statement
│   ├── src/sqlparser.rs    # Restricted SQL parser
│   ├── src/substrait.rs    # Substrait plan translation
│   ├── examples/           # Direct benchmarks (no ADBC)
│   └── Cargo.toml
├── docker-compose.yml      # Local REST catalog + MinIO
├── pixi.toml               # Dependencies and build tasks
├── benchmark.py            # Multi-engine benchmark suite
├── example.py              # Python usage example
├── seed.py                 # Seed small test table
└── seed_large.py           # Seed 5M row benchmark table
```
