# ADBC Iceberg Driver

An [ADBC](https://arrow.apache.org/adbc/) driver for [Apache Iceberg](https://iceberg.apache.org/) tables, written in Go. It enables any ADBC-compatible query engine to read Iceberg tables through a standard Arrow-native interface.

The idea: instead of each engine (DuckDB, DataFusion, Spark, etc.) implementing its own Iceberg connector, they all speak ADBC and this driver handles the Iceberg complexity once.

## Features

- **REST catalog** support with token/OAuth2 authentication
- **Parquet** data file reading via [iceberg-go](https://github.com/apache/iceberg-go)
- **Column projection** pushed down to Parquet column pruning
- **Predicate pushdown** — WHERE clauses converted to Iceberg expressions for file/row-group skipping and vectorized row filtering
- **Partitioned execution** via `ExecutePartitions` for distributed scan planning
- **S3/GCS/Azure** storage via gocloud
- **C shared library** (.so) loadable from Python, DuckDB, or any ADBC consumer

## Quick Start

```bash
# Install dependencies
pixi install

# Start local Iceberg catalog (REST + MinIO)
docker compose up -d

# Build the driver
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
    driver="./libadbc_driver_iceberg.so",
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
    'driver': './libadbc_driver_iceberg.so',
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

| Option | Description |
|---|---|
| `uri` | REST catalog URI |
| `adbc.iceberg.catalog.name` | Catalog name (default: `"default"`) |
| `adbc.iceberg.auth.token` | Static bearer token |
| `adbc.iceberg.auth.credential` | OAuth2 client credentials (`clientID:clientSecret`) |
| `adbc.iceberg.auth.scope` | OAuth2 scope |
| `adbc.iceberg.auth.uri` | Custom token endpoint URL |
| `adbc.iceberg.s3.endpoint` | S3-compatible endpoint URL |
| `adbc.iceberg.s3.region` | AWS region |
| `adbc.iceberg.s3.access_key` | S3 access key ID |
| `adbc.iceberg.s3.secret_key` | S3 secret access key |

### Statement Options

| Option | Description |
|---|---|
| `adbc.iceberg.snapshot_id` | Scan a specific snapshot (time travel) |
| `adbc.iceberg.as_of_timestamp` | Scan as of timestamp (ms since epoch) |
| `adbc.iceberg.branch` | Scan a named branch |
| `adbc.iceberg.start_snapshot_id` | Incremental scan start snapshot |
| `adbc.iceberg.batch_size` | Arrow batch size (default: 131072) |

## SQL Support

The driver accepts a restricted SQL subset:

```sql
SELECT <columns | *> FROM [schema.]<table> [WHERE <filter>]
```

Supported WHERE operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`, `BETWEEN`, `AND`, `OR`, `NOT`, parentheses.

## Benchmarks

5,000,000 rows across 5 Parquet files, unpartitioned table, reading from a local REST catalog + MinIO.

### Full Table Scan

| Engine | Time | Rows |
|---|---|---|
| DataFusion + ADBC Iceberg | 0.20s | 5,000,000 |
| **ADBC Iceberg** | **0.23s** | 5,000,000 |
| DuckDB Iceberg (built-in) | 0.35s | 5,000,000 |
| DuckDB adbc_scan() + ADBC Iceberg | 0.59s | 5,000,000 |

The ADBC driver and DataFusion are fastest because Arrow batches flow through with no format conversion. DuckDB's built-in Iceberg extension must convert between DuckDB vectors and Arrow at the Python boundary. DuckDB's `adbc_scan()` pays for two conversions (Arrow → DuckDB → Arrow).

### Column Projection (`SELECT id, name`)

| Engine | Time | Rows |
|---|---|---|
| DataFusion + ADBC Iceberg | 0.15s | 5,000,000 |
| DuckDB Iceberg (built-in) | 0.20s | 5,000,000 |
| ADBC Iceberg | 0.21s | 5,000,000 |
| DuckDB adbc_scan() | 0.39s | 5,000,000 |

Projection is pushed down to Parquet column pruning in all engines.

### Predicate Pushdown — Highly Selective (`WHERE id < 100`)

| Engine | Time | Rows |
|---|---|---|
| DuckDB Iceberg (built-in) | 0.06s | 100 |
| ADBC Iceberg | 0.07s | 100 |
| DataFusion + ADBC Iceberg | 0.10s | 100 |
| DuckDB adbc_scan() | 0.20s | 100 |

All engines benefit from row group statistics pruning. Performance is comparable when most data is skipped.

### Predicate Pushdown — Moderate Selectivity (`WHERE name = 'alice'`)

| Engine | Time | Rows |
|---|---|---|
| DuckDB Iceberg (built-in) | 0.25s | 1,000,000 |
| ADBC Iceberg | 0.71s | 1,000,000 |
| DataFusion + ADBC Iceberg | 0.73s | 1,000,000 |
| DuckDB adbc_scan() | 1.54s | 1,000,000 |

DuckDB is ~3x faster here. The table is unpartitioned and every row group contains all 5 name values, so no row groups can be skipped. DuckDB pushes string equality into its Parquet reader at the page level (page index filtering, dictionary filtering), avoiding full decode of non-matching pages. iceberg-go's Arrow Parquet reader decodes all pages first, then applies a vectorized Arrow `compute.Filter` — correct, but slower. This is an Arrow Go Parquet reader limitation, not a driver issue. iceberg-rust does not have this limitation as it uses parquet-rs which supports page-level predicate pushdown.

### Projection + Predicate (`SELECT id, value WHERE id < 1000`)

| Engine | Time | Rows |
|---|---|---|
| DataFusion + ADBC Iceberg | 0.08s | 1,000 |
| ADBC Iceberg | 0.09s | 1,000 |
| DuckDB Iceberg (built-in) | 0.13s | 1,000 |
| DuckDB adbc_scan() | 0.17s | 1,000 |

Combined projection and predicate pushdown. All engines perform well when the result set is small.

## Architecture

```
Query Engine (DuckDB / DataFusion / Python / ...)
    │
    ▼
ADBC Interface (C Data Interface)
    │
    ▼
┌─────────────────────────────────────────┐
│  ADBC Iceberg Driver (this project)     │
│                                         │
│  SQL Parser ─► Iceberg Expressions      │
│  REST Catalog Client                    │
│  Scan Planning (snapshot, partitions)   │
│  Parquet → Arrow RecordBatch streaming  │
└─────────────────────────────────────────┘
    │
    ▼
Iceberg REST Catalog ──► S3 / GCS / Azure
```

## Known Limitations

- **Read-only** — no write/append support yet
- **REST catalog only** — no Hive, Glue, or JDBC catalog
- **Parquet only** — no ORC or Avro data files
- **ReadPartition re-reads all files** — `ExecutePartitions` correctly plans per-file partitions, but `ReadPartition` currently re-reads the entire table for each partition because iceberg-go doesn't expose a public API to read specific `FileScanTask`s ([upstream issue needed](https://github.com/apache/iceberg-go))
- **No page-level predicate pushdown** — Arrow Go's Parquet reader lacks page index and dictionary filtering, making moderate-selectivity string predicates slower than engines with page-level pushdown (DuckDB, parquet-rs)

## Project Structure

```
├── driver.go           # Driver — creates Database instances
├── database.go         # Database — REST catalog client, auth, S3 config
├── connection.go       # Connection — catalog enumeration, schema, ReadPartition
├── statement.go        # Statement — SQL parsing, scan, predicate conversion
├── partition.go        # Partition descriptors (JSON), ExecutePartitions/Query
├── reader.go           # iter.Seq2[RecordBatch] → array.RecordReader adapter
├── options.go          # Option key constants
├── sqlparser/          # Restricted SQL parser (hand-rolled recursive descent)
│   ├── ast.go          # AST types
│   ├── lexer.go        # Tokenizer
│   ├── parser.go       # Parser
│   └── parser_test.go  # 22 tests
├── pkg/iceberg/        # Generated C shared library export (via ADBC pkg/gen)
├── docker-compose.yml  # Local REST catalog + MinIO
├── pixi.toml           # Dependencies and build tasks
├── benchmark.py        # Multi-engine benchmark suite
├── example.py          # Basic Python usage example
├── seed.py             # Seed small test table
└── seed_large.py       # Seed 5M row benchmark table
```
