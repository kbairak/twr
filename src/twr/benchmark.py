#!/usr/bin/env python3
"""
Benchmark script for TWR database performance.

Measures:
1. Event generation and database insertion time
2. Continuous aggregate refresh time
3. Cache refresh time for all granularities
4. Query performance before and after cache refresh
"""

import argparse
import time
import psycopg2
from pathlib import Path
import json
import random
from generate import (
    EventGenerator,
    calculate_missing_parameter,
    parse_time_interval,
)
from drop import drop_and_recreate_schema
from migrate import run_all_migrations
from datetime import datetime, timezone

# Import granularities configuration
migrations_dir = Path(__file__).parent.parent.parent / "migrations"
granularities_file = migrations_dir / "granularities.json"

try:
    with open(granularities_file) as f:
        GRANULARITIES = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    GRANULARITIES = []


def format_time(seconds):
    """Format seconds into human readable string"""
    if seconds >= 60:
        return f"{int(seconds // 60)}m {seconds % 60:.1f}s"
    else:
        return f"{seconds:.2f}s"


class Benchmark:
    def __init__(
        self,
        db_name: str = "twr",
        db_user: str = "twr_user",
        db_password: str = "twr_password",
        db_host: str = "localhost",
        db_port: int = 5432,
    ):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

    def get_connection(self):
        return psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
        )

    def clear_data(self):
        """Clear all data from the database"""
        print("Clearing existing data...")
        conn = self.get_connection()
        cur = conn.cursor()

        # Truncate base tables
        cur.execute("TRUNCATE TABLE cashflow, price_update CASCADE")

        # Truncate cache tables
        cur.execute("TRUNCATE TABLE cumulative_cashflow_cache")
        for g in GRANULARITIES:
            suffix = g["suffix"]
            cur.execute(f"TRUNCATE TABLE user_product_timeline_cache_{suffix}")

        conn.commit()
        conn.close()
        print("  → Data cleared\n")

    def reset_database(self):
        """Reset database using functions from reset.py."""
        # First connection: drop schema
        conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password,
        )
        conn.autocommit = True
        drop_and_recreate_schema(conn)
        conn.close()

        # Second connection: run migrations (needed for TimescaleDB extension)
        conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password,
        )
        conn.autocommit = True
        run_all_migrations(conn)
        conn.close()

    def get_cache_percentile_thresholds(self, conn):
        """Get 25th, 50th, 75th percentile timestamps from cache.

        Returns:
            dict {'p25': timestamp, 'p50': timestamp, 'p75': timestamp} or None if cache is empty
        """
        cur = conn.cursor()
        # Use percentile_disc for discrete values (works with timestamps)
        cur.execute("""
            SELECT percentile_disc(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY "timestamp")
            FROM cumulative_cashflow_cache
        """)
        result = cur.fetchone()
        if result and result[0]:
            percentiles = result[0]
            return {"p25": percentiles[0], "p50": percentiles[1], "p75": percentiles[2]}
        return None

    def delete_cache_from_threshold(self, conn, threshold_timestamp):
        """Delete all cache entries >= threshold across all cache tables.

        Returns:
            int: Total rows deleted
        """
        cur = conn.cursor()
        total_deleted = 0

        # Delete from cumulative_cashflow_cache
        cur.execute(
            'DELETE FROM cumulative_cashflow_cache WHERE "timestamp" >= %s',
            (threshold_timestamp,),
        )
        total_deleted += cur.rowcount

        # Delete from user_product_timeline_cache_* for each granularity
        for g in GRANULARITIES:
            suffix = g["suffix"]
            cur.execute(
                f'DELETE FROM user_product_timeline_cache_{suffix} WHERE "timestamp" >= %s',
                (threshold_timestamp,),
            )
            total_deleted += cur.rowcount

        conn.commit()
        return total_deleted

    def measure_query_performance(
        self, cur, user_product_pairs, user_ids, max_time_per_granularity=5.0
    ):
        """Measure query performance for each view separately using time-based random sampling.

        For each granularity, randomly samples queries until max_time_per_granularity is exceeded,
        then calculates the average query duration.

        Args:
            cur: Database cursor
            user_product_pairs: List of (user_id, product_id) tuples
            user_ids: List of user IDs
            max_time_per_granularity: Maximum time in seconds to spend per granularity (default: 5.0)

        Returns:
            dict: {
                'upt_15min_ms': float, 'upt_1h_ms': float, 'upt_1d_ms': float,
                'ut_15min_ms': float, 'ut_1h_ms': float, 'ut_1d_ms': float,
            }
        """
        results = {}

        for g in GRANULARITIES:
            suffix = g["suffix"]

            # Measure user_product_timeline with random sampling
            elapsed = 0
            query_count = 0
            start_overall = time.time()

            while elapsed < max_time_per_granularity:
                # Pick random user-product pair
                user_id, product_id = random.choice(user_product_pairs)

                cur.execute(
                    f"SELECT * FROM user_product_timeline_business_{suffix}(%s, %s)",
                    (user_id, product_id),
                )
                cur.fetchall()
                query_count += 1
                elapsed = time.time() - start_overall

            avg_time_ms = (elapsed / query_count) * 1000 if query_count > 0 else 0
            results[f"upt_{suffix}_ms"] = avg_time_ms

            # Measure user_timeline with random sampling
            elapsed = 0
            query_count = 0
            start_overall = time.time()

            while elapsed < max_time_per_granularity:
                # Pick random user
                user_id = random.choice(user_ids)

                cur.execute(
                    f"SELECT * FROM user_timeline_business_{suffix}(%s)",
                    (user_id,),
                )
                cur.fetchall()
                query_count += 1
                elapsed = time.time() - start_overall

            avg_time_ms = (elapsed / query_count) * 1000 if query_count > 0 else 0
            results[f"ut_{suffix}_ms"] = avg_time_ms

        return results

    def get_sample_queries(self, cur, num_queries):
        """Get sample user-product pairs and user IDs for queries.

        Returns:
            Tuple of (user_product_pairs, user_ids)
        """
        cur.execute("SELECT DISTINCT user_id, product_id FROM cashflow LIMIT %s", (num_queries,))
        user_product_pairs = cur.fetchall()

        cur.execute("SELECT DISTINCT user_id FROM cashflow LIMIT %s", (num_queries,))
        user_ids = [row[0] for row in cur.fetchall()]

        return user_product_pairs, user_ids

    def refresh_all_caches(self, conn):
        """Refresh all caches and return total time.

        Returns:
            float: Total time in seconds
        """
        cur = conn.cursor()
        start = time.time()

        # Enable autocommit to allow VACUUM to run
        old_autocommit = conn.autocommit
        conn.autocommit = True

        try:
            # Refresh cumulative cashflow cache
            cur.execute("SELECT refresh_cumulative_cashflow()")
            # VACUUM to update visibility map for efficient watermark scans
            cur.execute("VACUUM ANALYZE cumulative_cashflow_cache")

            for g in GRANULARITIES:
                suffix = g["suffix"]
                # Refresh user_product_timeline cache
                cur.execute(f"SELECT refresh_user_product_timeline_{suffix}()")
                cur.execute(f"VACUUM ANALYZE user_product_timeline_cache_{suffix}")
        finally:
            # Restore original autocommit setting
            conn.autocommit = old_autocommit

        return time.time() - start

    def run(
        self,
        num_events: int,
        num_users: int,
        num_products: int,
        num_queries: int = 100,
        price_update_interval=None,
        end_date=None,
    ):
        """Run the complete benchmark suite"""
        print("\n=== TWR Database Benchmark ===")
        print(f"Events: {num_events:,}")
        print(f"Users: {num_users:,}")
        print(f"Products: {num_products:,}")
        print(f"Query samples: {num_queries:,}")
        print(f"Granularities: {', '.join([g['suffix'] for g in GRANULARITIES])}\n")

        # Clear existing data
        self.clear_data()

        results = {}

        # Step 1: Generate and insert events
        print("[1/6] Generating and inserting events...")
        start = time.time()
        gen = EventGenerator(
            db_name=self.db_name,
            db_user=self.db_user,
            db_password=self.db_password,
            db_host=self.db_host,
            db_port=self.db_port,
            num_users=num_users,
            num_products=num_products,
        )
        gen.generate_and_insert(
            num_events, price_update_interval=price_update_interval, end_date=end_date
        )
        insert_time = time.time() - start
        print(f"  → Inserted {num_events:,} events in {format_time(insert_time)}\n")

        # Step 2: Refresh continuous aggregates
        print("[2/6] Refreshing continuous aggregates...")
        start = time.time()
        gen.refresh_continuous_aggregate()
        gen.close()
        ca_refresh_time = time.time() - start
        print(f"  → Refreshed in {format_time(ca_refresh_time)}\n")
        results["ca_refresh_time"] = ca_refresh_time

        conn = self.get_connection()
        cur = conn.cursor()

        # Get sample data for queries
        cur.execute(
            "SELECT DISTINCT user_id, product_id FROM cashflow LIMIT %s",
            (num_queries,),
        )
        user_product_pairs = cur.fetchall()

        cur.execute("SELECT DISTINCT user_id FROM cashflow LIMIT %s", (num_queries,))
        user_ids = [row[0] for row in cur.fetchall()]

        # Step 3: Query performance with 0% cache (baseline)
        print("[3/6] Querying with 0% cache (baseline, 5s per granularity)...")
        query_0pct = self.measure_query_performance(cur, user_product_pairs, user_ids)

        for g in GRANULARITIES:
            granularity = g["suffix"]
            upt_time_ms = query_0pct[f"upt_{granularity}_ms"]
            ut_time_ms = query_0pct[f"ut_{granularity}_ms"]

            results[f"upt_0pct_{granularity}"] = upt_time_ms / 1000  # Convert to seconds
            results[f"ut_0pct_{granularity}"] = ut_time_ms / 1000
            print(f"  → {granularity}: user_product={upt_time_ms:.1f}ms, user={ut_time_ms:.1f}ms")

        print()

        # Commit any pending transaction before changing autocommit
        conn.commit()

        # Step 4: Cache refresh
        print("[4/6] Refreshing caches...")

        # Enable autocommit for VACUUM operations
        old_autocommit = conn.autocommit
        conn.autocommit = True

        try:
            # Refresh cumulative cashflow cache
            start = time.time()
            cur.execute("SELECT refresh_cumulative_cashflow()")
            # VACUUM to update visibility map for efficient watermark scans
            cur.execute("VACUUM ANALYZE cumulative_cashflow_cache")
            cumulative_refresh_time = time.time() - start
            print(f"  → Cumulative cashflow: {format_time(cumulative_refresh_time)}")

            for g in GRANULARITIES:
                granularity = g["suffix"]
                start = time.time()

                # Refresh user_product_timeline cache
                cur.execute(f"SELECT refresh_user_product_timeline_{granularity}()")
                cur.execute(f"VACUUM ANALYZE user_product_timeline_cache_{granularity}")

                cache_refresh_time = time.time() - start

                results[f"cache_refresh_{granularity}"] = cache_refresh_time
                print(f"  → {granularity}: {format_time(cache_refresh_time)}")
        finally:
            # Restore original autocommit setting
            conn.autocommit = old_autocommit

        print()

        # Get percentile thresholds for progressive cache deletion
        percentiles = self.get_cache_percentile_thresholds(conn)

        # Step 5: Query performance with 100% cache
        print("[5/6] Querying with 100% cache (5s per granularity)...")
        query_100pct = self.measure_query_performance(cur, user_product_pairs, user_ids)

        for g in GRANULARITIES:
            granularity = g["suffix"]
            upt_time_ms = query_100pct[f"upt_{granularity}_ms"]
            ut_time_ms = query_100pct[f"ut_{granularity}_ms"]

            results[f"upt_100pct_{granularity}"] = upt_time_ms / 1000  # Convert to seconds
            results[f"ut_100pct_{granularity}"] = ut_time_ms / 1000
            print(f"  → {granularity}: user_product={upt_time_ms:.1f}ms, user={ut_time_ms:.1f}ms")

        print()

        # Step 6: Progressive cache deletion and queries
        print("[6/6] Progressive cache deletion and querying...")

        # Store query results for different cache levels
        query_75pct = query_50pct = query_25pct = None

        if percentiles:
            # 75% cache (delete >= 75th percentile)
            print("  → Deleting cache >= 75th percentile...")
            self.delete_cache_from_threshold(conn, percentiles["p75"])

            # VACUUM ANALYZE after deletion
            conn.commit()  # Commit before changing autocommit
            conn.autocommit = True
            try:
                cur.execute("VACUUM ANALYZE cumulative_cashflow_cache")
                for g in GRANULARITIES:
                    cur.execute(f"VACUUM ANALYZE user_product_timeline_cache_{g['suffix']}")
            finally:
                conn.autocommit = old_autocommit

            print("  → Querying with 75% cache (5s per granularity)...")
            query_75pct = self.measure_query_performance(cur, user_product_pairs, user_ids)
            for g in GRANULARITIES:
                granularity = g["suffix"]
                upt_time_ms = query_75pct[f"upt_{granularity}_ms"]
                ut_time_ms = query_75pct[f"ut_{granularity}_ms"]
                results[f"upt_75pct_{granularity}"] = upt_time_ms / 1000
                results[f"ut_75pct_{granularity}"] = ut_time_ms / 1000
                print(f"     {granularity}: user_product={upt_time_ms:.1f}ms, user={ut_time_ms:.1f}ms")
            print()

            # 50% cache (delete >= 50th percentile)
            print("  → Deleting cache >= 50th percentile...")
            self.delete_cache_from_threshold(conn, percentiles["p50"])

            # VACUUM ANALYZE after deletion
            conn.commit()  # Commit before changing autocommit
            conn.autocommit = True
            try:
                cur.execute("VACUUM ANALYZE cumulative_cashflow_cache")
                for g in GRANULARITIES:
                    cur.execute(f"VACUUM ANALYZE user_product_timeline_cache_{g['suffix']}")
            finally:
                conn.autocommit = old_autocommit

            print("  → Querying with 50% cache (5s per granularity)...")
            query_50pct = self.measure_query_performance(cur, user_product_pairs, user_ids)
            for g in GRANULARITIES:
                granularity = g["suffix"]
                upt_time_ms = query_50pct[f"upt_{granularity}_ms"]
                ut_time_ms = query_50pct[f"ut_{granularity}_ms"]
                results[f"upt_50pct_{granularity}"] = upt_time_ms / 1000
                results[f"ut_50pct_{granularity}"] = ut_time_ms / 1000
                print(f"     {granularity}: user_product={upt_time_ms:.1f}ms, user={ut_time_ms:.1f}ms")
            print()

            # 25% cache (delete >= 25th percentile)
            print("  → Deleting cache >= 25th percentile...")
            self.delete_cache_from_threshold(conn, percentiles["p25"])

            # VACUUM ANALYZE after deletion
            conn.commit()  # Commit before changing autocommit
            conn.autocommit = True
            try:
                cur.execute("VACUUM ANALYZE cumulative_cashflow_cache")
                for g in GRANULARITIES:
                    cur.execute(f"VACUUM ANALYZE user_product_timeline_cache_{g['suffix']}")
            finally:
                conn.autocommit = old_autocommit

            print("  → Querying with 25% cache (5s per granularity)...")
            query_25pct = self.measure_query_performance(cur, user_product_pairs, user_ids)
            for g in GRANULARITIES:
                granularity = g["suffix"]
                upt_time_ms = query_25pct[f"upt_{granularity}_ms"]
                ut_time_ms = query_25pct[f"ut_{granularity}_ms"]
                results[f"upt_25pct_{granularity}"] = upt_time_ms / 1000
                results[f"ut_25pct_{granularity}"] = ut_time_ms / 1000
                print(f"     {granularity}: user_product={upt_time_ms:.1f}ms, user={ut_time_ms:.1f}ms")
            print()

        conn.close()

        # Display summary
        print("=== Query Performance Summary ===")
        print(f"Continuous aggregate refresh: {format_time(results['ca_refresh_time'])}\n")

        # Show cache refresh times
        print("Cache refresh times:")
        print(f"  Cumulative cashflow: {format_time(cumulative_refresh_time)}")
        for g in GRANULARITIES:
            granularity = g["suffix"]
            cache_time = results.get(f"cache_refresh_{granularity}", 0)
            print(f"  {granularity}: {format_time(cache_time)}")
        print()

        # Show query performance at each cache level
        for cache_level, cache_pct in [("100%", "100pct"), ("75%", "75pct"), ("50%", "50pct"), ("25%", "25pct"), ("0%", "0pct")]:
            # Check if we have results for this cache level
            has_results = any(f"upt_{cache_pct}_{g['suffix']}" in results for g in GRANULARITIES)
            if has_results:
                print(f"Queries with {cache_level} cache:")
                for g in GRANULARITIES:
                    granularity = g["suffix"]
                    upt_key = f"upt_{cache_pct}_{granularity}"
                    ut_key = f"ut_{cache_pct}_{granularity}"
                    if upt_key in results and ut_key in results:
                        upt_ms = results[upt_key] * 1000
                        ut_ms = results[ut_key] * 1000
                        print(f"  user-product-timeline-{granularity}: {upt_ms:.0f}ms")
                        print(f"  user-timeline-{granularity}:         {ut_ms:.0f}ms")
                print()

        return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark TWR database performance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 10 days of data with default 2min updates
  %(prog)s --days 10 --price-update-frequency 2min

  # Generate 100k events over 5 trading days
  %(prog)s --days 5 --num-events 100000

  # Generate 50k events with 5min price updates
  %(prog)s --num-events 50000 --price-update-frequency 5min

Note: Exactly 2 of the 3 parameters (days, num-events, price-update-frequency) must be provided.
        """,
    )

    # 2-of-3 parameter model
    parser.add_argument("--days", type=float, help="Number of trading days to simulate")
    parser.add_argument("--num-events", type=int, help="Total number of events to generate")
    parser.add_argument(
        "--price-update-frequency",
        type=str,
        help="Price update interval (e.g., '2min', '5min', '1h')",
    )

    # Standard parameters
    parser.add_argument("--num-users", type=int, default=50, help="Number of users (default: 50)")
    parser.add_argument(
        "--num-products",
        type=int,
        default=100,
        help="Number of products (default: 100)",
    )
    parser.add_argument(
        "--num-queries",
        type=int,
        default=100,
        help="Number of queries to sample (default: 100)",
    )

    args = parser.parse_args()

    benchmark = Benchmark()

    # Calculate missing parameter
    try:
        days, num_events, frequency = calculate_missing_parameter(
            days=args.days,
            num_events=args.num_events,
            price_update_frequency=args.price_update_frequency,
            num_products=args.num_products,
        )
    except ValueError as e:
        parser.error(str(e))

    # Calculate end_date as today at market close
    end_date = datetime.now(timezone.utc).replace(hour=16, minute=0, second=0, microsecond=0)

    # Display calculated parameters
    print("\n=== Calculated Parameters ===")
    print(f"Trading days: {days:.2f}")
    print(f"Total events: {num_events:,}")
    print(f"Price update frequency: {frequency}")
    print(f"Number of users: {args.num_users}")
    print(f"Number of products: {args.num_products}")
    print(f"End date: {end_date.date()}")
    print()

    benchmark.run(
        num_events=num_events,
        num_users=args.num_users,
        num_products=args.num_products,
        num_queries=args.num_queries,
        price_update_interval=frequency,
        end_date=end_date,
    )
