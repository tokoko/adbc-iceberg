"""Benchmark: Compare reading Iceberg tables across engines and access methods.

Tests full table scan, predicate pushdown, column projection, and partitioned execution.

Prerequisites:
    docker compose up -d
    pixi run build
    pixi run python seed_large.py

Usage:
    pixi run python benchmark.py
"""

import os
import time

import adbc_driver_manager
import adbc_driver_manager.dbapi
import datafusion
import duckdb
import pyarrow as pa

DRIVER_PATH = os.path.join(os.path.dirname(__file__), "libadbc_driver_iceberg.so")

CATALOG_URI = "http://localhost:8181"
S3_ENDPOINT = "http://localhost:9000"
S3_REGION = "us-east-1"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"
TABLE = os.environ.get("ICEBERG_TABLE", "default.large_table")

ADBC_DB_KWARGS = {
    "uri": CATALOG_URI,
    "adbc.iceberg.catalog.name": "rest",
    "adbc.iceberg.s3.endpoint": S3_ENDPOINT,
    "adbc.iceberg.s3.region": S3_REGION,
    "adbc.iceberg.s3.access_key": S3_ACCESS_KEY,
    "adbc.iceberg.s3.secret_key": S3_SECRET_KEY,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - t0


def adbc_conn():
    return adbc_driver_manager.dbapi.connect(
        driver=DRIVER_PATH,
        entrypoint="AdbcDriverIcebergInit",
        db_kwargs=ADBC_DB_KWARGS,
    )


def duckdb_conn():
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; LOAD adbc_scanner")
    con.execute(f"""
        CREATE SECRET (TYPE S3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}',
            REGION '{S3_REGION}', ENDPOINT '{S3_ENDPOINT.replace("http://", "")}',
            USE_SSL false, URL_STYLE 'path')
    """)
    con.execute(f"""
        ATTACH '' AS cat (TYPE ICEBERG, ENDPOINT '{CATALOG_URI}', AUTHORIZATION_TYPE 'none')
    """)
    return con


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------


def duckdb_iceberg(query: str) -> tuple[pa.Table, float]:
    """DuckDB built-in Iceberg extension via REST catalog."""
    con = duckdb_conn()
    # Rewrite table reference for DuckDB's attached catalog
    duckdb_query = query.replace(TABLE, f"cat.{TABLE}")
    result, elapsed = timed(lambda: con.execute(duckdb_query).arrow().read_all())
    con.close()
    return result, elapsed


def adbc_iceberg(query: str) -> tuple[pa.Table, float]:
    """ADBC Iceberg driver directly."""
    conn = adbc_conn()
    try:
        with conn.cursor() as cur:
            result, elapsed = timed(lambda: (cur.execute(query), cur.fetch_arrow_table())[1])
    finally:
        conn.close()
    return result, elapsed


def duckdb_adbc_scan(query: str) -> tuple[pa.Table, float]:
    """DuckDB consuming ADBC Iceberg driver via adbc_scan()."""
    con = duckdb.connect()
    con.execute("LOAD adbc_scanner")
    handle = con.execute(
        "SELECT adbc_connect($1)",
        [{"driver": DRIVER_PATH, "entrypoint": "AdbcDriverIcebergInit", **ADBC_DB_KWARGS}],
    ).fetchone()[0]
    escaped = query.replace("'", "''")
    result, elapsed = timed(
        lambda: con.execute(f"SELECT * FROM adbc_scan($1, '{escaped}')", [handle])
        .arrow()
        .read_all()
    )
    con.execute("SELECT adbc_disconnect($1)", [handle])
    con.close()
    return result, elapsed


def datafusion_adbc(query: str) -> tuple[pa.Table, float]:
    """DataFusion consuming ADBC Iceberg driver's Arrow batches (zero-copy)."""
    conn = adbc_conn()
    try:

        def run():
            with conn.cursor() as cur:
                cur.execute(query)
                arrow_table = cur.fetch_arrow_table()
            ctx = datafusion.SessionContext()
            ctx.register_record_batches("t", [arrow_table.to_batches()])
            return ctx.sql("SELECT * FROM t").to_arrow_table()

        result, elapsed = timed(run)
    finally:
        conn.close()
    return result, elapsed


def adbc_partitioned(query: str) -> tuple[pa.Table, float, float, int]:
    """ADBC ExecutePartitions + ReadPartition (simulated distributed scan)."""
    db = adbc_driver_manager.AdbcDatabase(
        driver=DRIVER_PATH, entrypoint="AdbcDriverIcebergInit", **ADBC_DB_KWARGS,
    )
    conn = adbc_driver_manager.AdbcConnection(db)
    stmt = adbc_driver_manager.AdbcStatement(conn)
    stmt.set_sql_query(query)

    t0 = time.perf_counter()
    partitions, schema, _ = stmt.execute_partitions()
    plan_time = time.perf_counter() - t0

    batches = []
    for partition in partitions:
        handle = conn.read_partition(partition)
        reader = pa.RecordBatchReader._import_from_c(handle.address)
        for batch in reader:
            batches.append(batch)
    elapsed = time.perf_counter() - t0

    result = pa.Table.from_batches(batches, schema=batches[0].schema) if batches else pa.table({})
    stmt.close()
    conn.close()
    db.close()
    return result, elapsed, plan_time, len(partitions)


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

ENGINES = [
    ("DuckDB Iceberg", duckdb_iceberg),
    ("ADBC Iceberg", adbc_iceberg),
    ("DuckDB adbc_scan()", duckdb_adbc_scan),
    ("DataFusion + ADBC", datafusion_adbc),
]


def run_benchmark(label: str, query: str, include_partitioned: bool = False):
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print(f"  {query}")
    print(f"{'─' * 60}")

    results = {}
    for name, fn in ENGINES:
        table, elapsed = fn(query)
        results[name] = (table.num_rows, elapsed)
        print(f"  {elapsed:.4f}s  {table.num_rows:>12,} rows  {name}")

    if include_partitioned:
        table, elapsed, plan_time, num_parts = adbc_partitioned(query)
        name = f"ADBC Partitioned ({num_parts}x)"
        results[name] = (table.num_rows, elapsed)
        print(f"  {elapsed:.4f}s  {table.num_rows:>12,} rows  {name}  (plan: {plan_time:.4f}s)")

    return results


def print_summary(all_results: dict[str, dict]):
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")

    # Collect all engine names
    engines = []
    for results in all_results.values():
        for name in results:
            if name not in engines:
                engines.append(name)

    # Header
    labels = list(all_results.keys())
    header = f"  {'Engine':<28s}"
    for label in labels:
        header += f"  {label:>12s}"
    print(header)
    print(f"  {'─' * 28}" + f"  {'─' * 12}" * len(labels))

    # Rows
    for engine in engines:
        row = f"  {engine:<28s}"
        for label in labels:
            if engine in all_results[label]:
                _, elapsed = all_results[label][engine]
                row += f"  {elapsed:>11.4f}s"
            else:
                row += f"  {'—':>12s}"
        print(row)


def main():
    print(f"Table: {TABLE}")
    print(f"Engines: {', '.join(name for name, _ in ENGINES)}")

    all_results = {}

    # 1. Full table scan
    all_results["full scan"] = run_benchmark(
        "Full table scan",
        f"SELECT * FROM {TABLE}",
        include_partitioned=True,
    )

    # 2. Column projection
    all_results["projection"] = run_benchmark(
        "Column projection",
        f"SELECT id, name FROM {TABLE}",
    )

    # 3. Predicate pushdown — selective (should skip most data)
    all_results["id < 100"] = run_benchmark(
        "Predicate pushdown — selective",
        f"SELECT * FROM {TABLE} WHERE id < 100",
    )

    # 4. Predicate pushdown — moderate (20% of rows)
    all_results["name=alice"] = run_benchmark(
        "Predicate pushdown — moderate selectivity",
        f"SELECT * FROM {TABLE} WHERE name = 'alice'",
    )

    # 5. Combined projection + predicate
    all_results["proj+pred"] = run_benchmark(
        "Projection + predicate",
        f"SELECT id, value FROM {TABLE} WHERE id < 1000",
    )

    print_summary(all_results)


if __name__ == "__main__":
    main()
