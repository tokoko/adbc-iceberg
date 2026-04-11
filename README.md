# ADBC Iceberg Driver

An [ADBC](https://arrow.apache.org/adbc/) driver for [Apache Iceberg](https://iceberg.apache.org/) tables, implemented in both Go and Rust. It enables any ADBC-compatible query engine to read Iceberg tables through a standard Arrow-native interface.

The idea: instead of each engine (DuckDB, DataFusion, Spark, etc.) implementing its own Iceberg connector, they all speak ADBC and this driver handles the Iceberg complexity once.

## Two Implementations

| | Go | Rust |
|---|---|---|
| Iceberg library | [iceberg-go](https://github.com/apache/iceberg-go) | [iceberg-rust](https://github.com/apache/iceberg-rust) 0.9 |
| Parquet reader | Arrow Go | parquet-rs |
| S3 client | gocloud (AWS Go SDK) | opendal |
| ADBC framework | [driverbase-go](https://github.com/adbc-drivers/driverbase-go) | [adbc_core](https://crates.io/crates/adbc_core) + [adbc_ffi](https://crates.io/crates/adbc_ffi) |
| Catalog support | REST only | REST (+ Glue, HMS, SQL, S3 Tables available via iceberg-rust) |
| Column projection | Yes | Yes |
| Predicate pushdown | Row group stats + vectorized filter | Row group stats + page index + row filter |
| Partitioned execution | Broken (re-reads all files per partition) | Correct (reads only assigned file) |
| Catalog enumeration | GetObjects, GetTableSchema, GetTableTypes | Not yet implemented |
| Statement options | Snapshot ID, time travel, branch, batch size | Not yet implemented |

Both produce identical Arrow output and are loadable from Python, DuckDB, or any ADBC consumer.

## Quick Start

```bash
# Install dependencies
pixi install

# Start local Iceberg catalog (REST + MinIO)
docker compose up -d

# Build both drivers
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

# Either driver works — same interface, same options
conn = adbc_driver_manager.dbapi.connect(
    driver="./build/libadbc_driver_iceberg_rust.so",  # or _go.so
    entrypoint="AdbcDriverIcebergInit",
    db_kwargs={
        "uri": "http://localhost:8181",
        "adbc.iceberg.catalog.name": "rest",
        "adbc.iceberg.s3.endpoint": "http://localhost:9000",
        "adbc.iceberg.s3.access_key": "admin",
        "adbc.iceberg.s3.secret_key": "password",
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
    'adbc.iceberg.s3.secret_key': 'password'
});

SELECT * FROM adbc_scan(0, 'SELECT * FROM default.my_table');
```

## Configuration

### Database Options

| Option | Description | Go | Rust |
|---|---|---|---|
| `uri` | REST catalog URI | Yes | Yes |
| `adbc.iceberg.catalog.name` | Catalog name (default: `"default"`) | Yes | Yes |
| `adbc.iceberg.auth.token` | Static bearer token | Yes | — |
| `adbc.iceberg.auth.credential` | OAuth2 client credentials | Yes | — |
| `adbc.iceberg.auth.scope` | OAuth2 scope | Yes | — |
| `adbc.iceberg.auth.uri` | Custom token endpoint URL | Yes | — |
| `adbc.iceberg.s3.endpoint` | S3-compatible endpoint URL | Yes | Yes |
| `adbc.iceberg.s3.region` | AWS region | Yes | Yes |
| `adbc.iceberg.s3.access_key` | S3 access key ID | Yes | Yes |
| `adbc.iceberg.s3.secret_key` | S3 secret access key | Yes | Yes |

### Statement Options (Go driver only)

| Option | Description |
|---|---|
| `adbc.iceberg.snapshot_id` | Scan a specific snapshot (time travel) |
| `adbc.iceberg.as_of_timestamp` | Scan as of timestamp (ms since epoch) |
| `adbc.iceberg.branch` | Scan a named branch |
| `adbc.iceberg.start_snapshot_id` | Incremental scan start snapshot |
| `adbc.iceberg.batch_size` | Arrow batch size (default: 131072) |

## SQL Support

Both drivers accept a restricted SQL subset:

```sql
SELECT <columns | *> FROM [schema.]<table> [WHERE <filter>]
```

Supported WHERE operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`, parentheses. The Go driver additionally supports `IN`, `NOT IN`, `BETWEEN`.

## Benchmarks

5,000,000 rows across 5 Parquet files, unpartitioned table, reading from a local REST catalog + MinIO. PyIceberg included as a baseline (same iceberg-rust/parquet-rs stack, no ADBC overhead).

```
Engine                         full scan    projection    id < 100    name=alice    proj+pred
─────────────────────────────  ─────────    ──────────    ────────    ──────────    ─────────
DuckDB Iceberg (built-in)        0.57s        0.24s        0.08s        0.25s        0.07s
ADBC Iceberg (Go)                0.21s        0.14s        0.09s        0.79s        0.07s
ADBC Iceberg (Rust)              0.41s        0.26s        0.08s        0.44s        0.07s
PyIceberg (baseline)             0.17s        0.11s        0.08s        0.18s        0.06s
DuckDB adbc_scan() Go            0.78s        0.54s        0.17s        1.65s        0.17s
DuckDB adbc_scan() Rust          0.62s        0.42s        0.12s        0.69s        0.13s
DataFusion + ADBC Go             0.26s        0.17s        0.10s        0.86s        0.08s
Partitioned Go (5x)              1.31s           —            —            —            —
Partitioned Rust (5x)            0.44s           —            —            —            —
```

### Key Findings

**Full scan**: Go (0.21s) beats Rust (0.41s). Both use Arrow-native streaming, but gocloud's S3 client (AWS Go SDK) is faster than opendal at bulk reads. Raw benchmarking confirms opendal reads bytes 2.8x slower than `object_store` — there is an [active PR](https://github.com/apache/iceberg-rust/pull/2257) to add `object_store` as an alternative storage backend for iceberg-rust.

**Predicate pushdown (`name='alice'`)**: Rust (0.44s) beats Go (0.79s) by nearly 2x. parquet-rs supports page index filtering and dictionary filtering, skipping pages that can't match. Arrow Go's Parquet reader decodes all pages then filters with `compute.Filter`.

**Highly selective (`id < 100`)**: All engines are comparable (~0.07-0.09s) because row group statistics pruning skips most data regardless of reader implementation.

**Partitioned execution**: Rust reads exactly 5M rows (correct — 1M per partition). Go reads 25M rows (broken — each `ReadPartition` re-reads all files due to iceberg-go's unexported `arrowScan.GetRecords`).

**DuckDB adbc_scan()**: Rust variant is consistently faster than Go, especially on filtered queries (0.69s vs 1.65s for `name='alice'`).

## Architecture

```
Query Engine (DuckDB / DataFusion / Python / ...)
    │
    ▼
ADBC Interface (C Data Interface)
    │
    ├──────────────────────┐
    ▼                      ▼
┌──────────────┐   ┌──────────────┐
│  Go Driver   │   │ Rust Driver  │
│  iceberg-go  │   │ iceberg-rust │
│  gocloud S3  │   │  opendal S3  │
│  driverbase  │   │  adbc_ffi    │
└──────────────┘   └──────────────┘
    │                      │
    ▼                      ▼
Iceberg REST Catalog ──► S3 / GCS / Azure
```

## Known Limitations

### Go Driver
- **ReadPartition re-reads all files** — `ExecutePartitions` correctly plans per-file partitions, but `ReadPartition` re-reads the entire table because iceberg-go doesn't expose a public API to read specific `FileScanTask`s
- **No page-level predicate pushdown** — Arrow Go's Parquet reader lacks page index and dictionary filtering

### Rust Driver
- **No catalog enumeration** — `GetObjects`, `GetTableSchema`, `GetTableTypes` not yet implemented
- **No statement options** — snapshot ID, time travel, branch not yet wired up
- **opendal S3 throughput** — 2.8x slower than `object_store` for raw reads; pending [upstream fix](https://github.com/apache/iceberg-rust/pull/2257)
- **Partitioned execution re-plans** — `ReadPartition` re-loads the table and re-plans files (filtering by path) because `FileScanTask` [cannot be fully serialized](https://github.com/apache/iceberg-rust/issues/2220)

### Both
- **Read-only** — no write/append support
- **REST catalog only** (Rust could support Glue, HMS, SQL via iceberg-rust crates)
- **Parquet only** — no ORC or Avro

## Project Structure

```
├── go/                     # Go driver
│   ├── driver.go           # Driver — creates Database instances
│   ├── database.go         # Database — REST catalog client, auth, S3 config
│   ├── connection.go       # Connection — catalog enumeration, schema, ReadPartition
│   ├── statement.go        # Statement — SQL parsing, scan, predicate conversion
│   ├── partition.go        # Partition descriptors, ExecutePartitions/Query
│   ├── reader.go           # RecordBatch iterator → RecordReader adapter
│   ├── options.go          # Option key constants
│   ├── sqlparser/          # Restricted SQL parser (hand-rolled recursive descent)
│   └── pkg/iceberg/        # Generated C shared library export
├── rust/                   # Rust driver
│   ├── src/lib.rs          # Driver, Database, Connection, Statement
│   ├── src/sqlparser.rs    # Restricted SQL parser
│   ├── examples/           # Direct benchmarks (no ADBC)
│   └── Cargo.toml
├── docker-compose.yml      # Local REST catalog + MinIO
├── pixi.toml               # Dependencies and build tasks
├── benchmark.py            # Multi-engine benchmark suite
├── example.py              # Python usage example
├── seed.py                 # Seed small test table
└── seed_large.py           # Seed 5M row benchmark table
```
