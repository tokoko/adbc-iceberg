"""Example: Using the ADBC Iceberg driver from Python.

Prerequisites:
    docker compose up -d
    pixi run build
    pixi run python seed.py

Usage:
    pixi run python example.py
"""

import os
import adbc_driver_manager.dbapi

DRIVER_PATH = os.path.join(os.path.dirname(__file__), "libadbc_driver_iceberg.so")

db_kwargs = {
    "uri": os.environ.get("ICEBERG_CATALOG_URI", "http://localhost:8181"),
    "adbc.iceberg.catalog.name": "rest",
    "adbc.iceberg.s3.endpoint": os.environ.get("ICEBERG_S3_ENDPOINT", "http://localhost:9000"),
    "adbc.iceberg.s3.region": "us-east-1",
    "adbc.iceberg.s3.access_key": "admin",
    "adbc.iceberg.s3.secret_key": "password",
}


def main():
    conn = adbc_driver_manager.dbapi.connect(
        driver=DRIVER_PATH,
        entrypoint="AdbcDriverIcebergInit",
        db_kwargs=db_kwargs,
    )

    try:
        with conn.cursor() as cur:
            # List catalog objects
            objects = conn.adbc_get_objects(depth="tables").read_all()
            print("=== Catalog Objects ===")
            print(objects.to_pydict())
            print()

            # Query the test table
            table_name = os.environ.get("ICEBERG_TABLE", "default.test_table")
            print(f"=== SELECT * FROM {table_name} ===")
            cur.execute(f"SELECT * FROM {table_name}")
            table = cur.fetch_arrow_table()
            print(table.to_pydict())
            print()

            # Query with projection
            print(f"=== SELECT name, value FROM {table_name} ===")
            cur.execute(f"SELECT name, value FROM {table_name}")
            table = cur.fetch_arrow_table()
            print(table.to_pydict())
    finally:
        conn.close()


if __name__ == "__main__":
    main()
