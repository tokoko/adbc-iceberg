# ADBC Polaris Driver

An [ADBC](https://arrow.apache.org/adbc/) driver for [Apache Polaris](https://polaris.apache.org/) that supports reading both Iceberg and Delta tables through a single interface.

## Features

- **Auto-format detection** — queries the Polaris Generic Tables API to determine whether a table is Iceberg or Delta, then dispatches to the appropriate reader
- **Iceberg tables** — read via the standard Iceberg REST catalog API with predicate pushdown and column projection
- **Delta tables** — read via [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) with column projection
- **OAuth2 authentication** — client credentials flow (`client_id:client_secret`)
- **S3-compatible storage** — works with MinIO and other S3-compatible backends

## Building

```bash
# Debug build
pixi run polaris-build

# Release build
pixi run polaris-build-release
```

The shared library is output to `build/libadbc_driver_polaris.so`.

## Usage

### Python (via adbc-driver-manager)

```python
import adbc_driver_manager.dbapi

conn = adbc_driver_manager.dbapi.connect(
    driver="build/libadbc_driver_polaris.so",
    entrypoint="AdbcDriverPolarisInit",
    db_kwargs={
        "uri": "http://localhost:8181",
        "adbc.polaris.credential": "root:s3cr3t",
        "adbc.polaris.warehouse": "rest",
        "adbc.polaris.scope": "PRINCIPAL_ROLE:ALL",
        "adbc.polaris.s3.endpoint": "http://localhost:9000",
        "adbc.polaris.s3.region": "us-east-1",
        "adbc.polaris.s3.access_key": "admin",
        "adbc.polaris.s3.secret_key": "password",
    },
)

with conn.cursor() as cur:
    # Iceberg table
    cur.execute("SELECT * FROM default.test_table")
    print(cur.fetch_arrow_table())

    # Delta table (auto-detected)
    cur.execute("SELECT * FROM default.cities_delta")
    print(cur.fetch_arrow_table())
```

## Database Options

| Option | Description | Default |
|---|---|---|
| `uri` | Polaris server base URL | *required* |
| `adbc.polaris.credential` | OAuth2 credentials (`client_id:client_secret`) | — |
| `adbc.polaris.warehouse` | Catalog/warehouse name | `rest` |
| `adbc.polaris.scope` | OAuth2 scope | `PRINCIPAL_ROLE:ALL` |
| `adbc.polaris.s3.endpoint` | S3-compatible endpoint URL | — |
| `adbc.polaris.s3.region` | AWS region | — |
| `adbc.polaris.s3.access_key` | S3 access key ID | — |
| `adbc.polaris.s3.secret_key` | S3 secret access key | — |

## How Format Detection Works

When a query is executed, the driver:

1. Queries the Polaris Generic Tables API (`/api/catalog/polaris/v1/{warehouse}/namespaces/{ns}/generic-tables/{table}`)
2. If the table is found there, uses its declared format (Delta, etc.) and `base-location` to read the data
3. If the table is not found in the Generic Tables API, assumes Iceberg and reads via the standard REST catalog

## Local Development

```bash
# Start Polaris + MinIO
docker compose -f docker-compose.polaris.yml up -d

# Seed an Iceberg table
pixi run python seed_polaris.py

# Build and test
pixi run polaris-build
pixi run polaris-test
```
