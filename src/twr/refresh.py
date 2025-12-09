#!/usr/bin/env python3
"""
Refresh cache tables with optional partial retention.

Usage:
    refresh.py [PERCENTAGE]

Examples:
    refresh.py          # Refresh and keep 100% (default)
    refresh.py 100%     # Refresh and keep 100%
    refresh.py 50%      # Refresh and keep oldest 50%
    refresh.py 0%       # Delete all cache (no refresh)
"""

import psycopg2
import argparse
import sys
from pathlib import Path
import json
import time

# Import granularities configuration
migrations_dir = Path(__file__).parent.parent.parent / "migrations"
granularities_file = migrations_dir / "granularities.json"

try:
    with open(granularities_file) as f:
        GRANULARITIES = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    GRANULARITIES = []


def parse_percentage(percentage_str):
    """Parse percentage string like '50%' to float 0.5"""
    percentage_str = percentage_str.strip()
    if percentage_str.endswith('%'):
        percentage_str = percentage_str[:-1]

    try:
        value = float(percentage_str)
        if not 0 <= value <= 100:
            raise ValueError(f"Percentage must be between 0 and 100, got {value}")
        return value / 100.0
    except ValueError as e:
        raise ValueError(f"Invalid percentage format: {percentage_str}") from e


def refresh_and_retain(
    percentage=1.0,
    db_host="127.0.0.1",
    db_port=5432,
    db_name="twr",
    db_user="twr_user",
    db_password="twr_password",
):
    """
    Refresh caches and retain specified percentage.

    Args:
        percentage: Float between 0.0 and 1.0 (e.g., 0.5 for 50%)
    """
    # Connect with autocommit enabled (required for VACUUM)
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
    conn.autocommit = True
    cur = conn.cursor()

    if percentage == 0.0:
        # Just delete all cache, no refresh needed
        print("Deleting all caches (0% retention)...")

        cur.execute("DELETE FROM cumulative_cashflow_cache")
        print(f"  - cumulative_cashflow_cache: {cur.rowcount:,} rows deleted")

        for g in GRANULARITIES:
            suffix = g["suffix"]
            cur.execute(f"DELETE FROM user_product_timeline_cache_{suffix}")
            print(f"  - user_product_timeline_cache_{suffix}: {cur.rowcount:,} rows deleted")

            cur.execute(f"DELETE FROM user_timeline_cache_{suffix}")
            print(f"  - user_timeline_cache_{suffix}: {cur.rowcount:,} rows deleted")

        print("\n✓ All caches deleted")
        conn.close()
        return

    # Refresh all caches
    print(f"Refreshing all caches (target retention: {percentage*100:.0f}%)...")

    start = time.time()
    cur.execute("SELECT refresh_cumulative_cashflow()")
    print(f"  - cumulative_cashflow_cache refreshed in {time.time() - start:.1f}s")

    for g in GRANULARITIES:
        suffix = g["suffix"]

        start = time.time()
        cur.execute(f"SELECT refresh_user_product_timeline_{suffix}()")
        print(f"  - user_product_timeline_cache_{suffix} refreshed in {time.time() - start:.1f}s")

        start = time.time()
        cur.execute(f"SELECT refresh_user_timeline_{suffix}()")
        print(f"  - user_timeline_cache_{suffix} refreshed in {time.time() - start:.1f}s")

    if percentage < 1.0:
        # Calculate percentile threshold for deletion
        print(f"\nDeleting cache to retain {percentage*100:.0f}%...")

        # Get the percentile threshold from cumulative_cashflow_cache
        percentile_value = 1.0 - percentage  # If we want to keep 50%, delete from 50th percentile onwards
        cur.execute(f"""
            SELECT percentile_disc({percentile_value}) WITHIN GROUP (ORDER BY timestamp) AS threshold
            FROM cumulative_cashflow_cache
        """)
        result = cur.fetchone()

        if result and result[0]:
            threshold = result[0]
            print(f"  Threshold timestamp: {threshold}")

            # Delete from all cache tables
            cur.execute("DELETE FROM cumulative_cashflow_cache WHERE timestamp >= %s", (threshold,))
            deleted_cc = cur.rowcount
            print(f"  - cumulative_cashflow_cache: {deleted_cc:,} rows deleted")

            for g in GRANULARITIES:
                suffix = g["suffix"]

                cur.execute(f"DELETE FROM user_product_timeline_cache_{suffix} WHERE timestamp >= %s", (threshold,))
                deleted_upt = cur.rowcount
                print(f"  - user_product_timeline_cache_{suffix}: {deleted_upt:,} rows deleted")

                cur.execute(f"DELETE FROM user_timeline_cache_{suffix} WHERE timestamp >= %s", (threshold,))
                deleted_ut = cur.rowcount
                print(f"  - user_timeline_cache_{suffix}: {deleted_ut:,} rows deleted")
        else:
            print("  No data in cache to delete")

    # VACUUM all cache tables
    print("\nVacuuming cache tables...")

    start = time.time()
    cur.execute("VACUUM ANALYZE cumulative_cashflow_cache")

    for g in GRANULARITIES:
        suffix = g["suffix"]
        cur.execute(f"VACUUM ANALYZE user_product_timeline_cache_{suffix}")
        cur.execute(f"VACUUM ANALYZE user_timeline_cache_{suffix}")

    print(f"  All tables vacuumed in {time.time() - start:.1f}s")

    conn.close()
    print(f"\n✓ Cache refresh complete ({percentage*100:.0f}% retained)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Refresh cache tables with optional partial retention",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s          # Refresh and keep 100%% (default)
  %(prog)s 100%%     # Refresh and keep 100%%
  %(prog)s 50%%      # Refresh and keep oldest 50%%
  %(prog)s 0%%       # Delete all cache (no refresh)
        """
    )

    parser.add_argument(
        "percentage",
        nargs="?",
        default="100%",
        help="Percentage of cache to retain (default: 100%%)"
    )
    parser.add_argument("--db-host", default="127.0.0.1", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", default="twr", help="Database name")
    parser.add_argument("--db-user", default="twr_user", help="Database user")
    parser.add_argument("--db-password", default="twr_password", help="Database password")

    args = parser.parse_args()

    try:
        percentage = parse_percentage(args.percentage)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    refresh_and_retain(
        percentage=percentage,
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password,
    )
