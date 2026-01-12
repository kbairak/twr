#!/usr/bin/env python3
"""
VACUUM all cache tables to update visibility maps.

This should be run after cache refresh operations to ensure efficient
index-only scans on watermark calculations.
"""

import psycopg2
import argparse
from pathlib import Path
import json

# Import granularities configuration
migrations_dir = Path(__file__).parent.parent.parent / "migrations"
granularities_file = migrations_dir / "granularities.json"

try:
    with open(granularities_file) as f:
        GRANULARITIES = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    GRANULARITIES = []


def vacuum_all_caches(
    db_host="127.0.0.1",
    db_port=5432,
    db_name="twr",
    db_user="twr_user",
    db_password="twr_password",
):
    """VACUUM ANALYZE all cache tables."""

    # Connect with autocommit enabled (required for VACUUM)
    conn = psycopg2.connect(
        host=db_host, port=db_port, database=db_name, user=db_user, password=db_password
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("Vacuuming cache tables...")

    # VACUUM cumulative_cashflow_cache
    print("  - cumulative_cashflow_cache")
    cur.execute("VACUUM ANALYZE cumulative_cashflow_cache")

    # VACUUM all timeline caches
    for g in GRANULARITIES:
        suffix = g["suffix"]

        print(f"  - user_product_timeline_cache_{suffix}")
        cur.execute(f"VACUUM ANALYZE user_product_timeline_cache_{suffix}")

        print(f"  - user_timeline_cache_{suffix}")
        cur.execute(f"VACUUM ANALYZE user_timeline_cache_{suffix}")

    conn.close()
    print("\n✓ All cache tables vacuumed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="VACUUM all cache tables to update visibility maps"
    )
    parser.add_argument("--db-host", default="127.0.0.1", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", default="twr", help="Database name")
    parser.add_argument("--db-user", default="twr_user", help="Database user")
    parser.add_argument("--db-password", default="twr_password", help="Database password")

    args = parser.parse_args()

    vacuum_all_caches(
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password,
    )
