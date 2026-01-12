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


def calculate_scenario_parameters(num_scenarios=7, max_events=5_000_000, num_products=500):
    """Generate scenario list with linear spacing up to max_events.

    Returns:
        List of (days, num_events) tuples
    """
    event_counts = [int(max_events * (i + 1) / num_scenarios) for i in range(num_scenarios)]
    scenarios = []
    for num_events in event_counts:
        days, num_events, freq = calculate_missing_parameter(
            num_events=num_events,
            price_update_frequency="2min",
            num_products=num_products,
        )
        scenarios.append((days, num_events))
    return scenarios


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
                    f"""
                    SELECT * FROM user_product_timeline_business_{suffix}
                    WHERE user_id = %s AND product_id = %s ORDER BY timestamp
                """,
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
                    f"SELECT * FROM user_timeline_business_{suffix} WHERE user_id = %s ORDER BY timestamp",
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
        print("[1/5] Generating and inserting events...")
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
        print("[2/5] Refreshing continuous aggregates...")
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

        # Step 3: Query performance (before cache refresh)
        print("[3/5] Querying before cache refresh (5s per granularity)...")
        before_results = self.measure_query_performance(cur, user_product_pairs, user_ids)

        for g in GRANULARITIES:
            granularity = g["suffix"]
            upt_time_ms = before_results[f"upt_{granularity}_ms"]
            ut_time_ms = before_results[f"ut_{granularity}_ms"]

            results[f"upt_before_{granularity}"] = upt_time_ms / 1000  # Convert to seconds
            results[f"ut_before_{granularity}"] = ut_time_ms / 1000
            print(f"  → {granularity}: user_product={upt_time_ms:.1f}ms, user={ut_time_ms:.1f}ms")

        print()

        # Step 4: Cache refresh
        print("[4/5] Refreshing caches...")

        # Refresh cumulative cashflow cache
        start = time.time()
        cur.execute("SELECT refresh_cumulative_cashflow()")
        conn.commit()
        cumulative_refresh_time = time.time() - start
        print(f"  → Cumulative cashflow: {format_time(cumulative_refresh_time)}")

        for g in GRANULARITIES:
            granularity = g["suffix"]
            start = time.time()

            # Refresh user_product_timeline cache
            cur.execute(f"SELECT refresh_user_product_timeline_{granularity}()")
            conn.commit()
            cache_refresh_time = time.time() - start

            results[f"cache_refresh_{granularity}"] = cache_refresh_time
            print(f"  → {granularity}: {format_time(cache_refresh_time)}")

        print()

        # Step 5: Query performance after cache refresh
        print("[5/5] Querying after cache refresh (5s per granularity)...")
        after_results = self.measure_query_performance(cur, user_product_pairs, user_ids)

        for g in GRANULARITIES:
            granularity = g["suffix"]
            upt_time_ms = after_results[f"upt_{granularity}_ms"]
            ut_time_ms = after_results[f"ut_{granularity}_ms"]

            results[f"upt_after_{granularity}"] = upt_time_ms / 1000  # Convert to seconds
            results[f"ut_after_{granularity}"] = ut_time_ms / 1000

            # Calculate speedup
            upt_before = results[f"upt_before_{granularity}"]
            ut_before = results[f"ut_before_{granularity}"]
            upt_speedup = upt_before / (upt_time_ms / 1000) if upt_time_ms > 0 else 0
            ut_speedup = ut_before / (ut_time_ms / 1000) if ut_time_ms > 0 else 0

            print(
                f"  → {granularity}: user_product={upt_time_ms:.1f}ms ({upt_speedup:.1f}x), "
                f"user={ut_time_ms:.1f}ms ({ut_speedup:.1f}x)"
            )

        print()
        conn.close()

        # Display summary
        print("=== Summary ===")
        print(f"Continuous aggregate refresh: {format_time(results['ca_refresh_time'])}")
        for g in GRANULARITIES:
            granularity = g["suffix"]
            cache_time = results.get(f"cache_refresh_{granularity}", 0)
            print(f"Cache refresh ({granularity}): {format_time(cache_time)}")

        print()
        return results

    def run_single_scenario(
        self,
        scenario_num,
        days,
        num_events,
        num_users,
        num_products,
        num_queries,
        price_update_interval,
        end_date,
    ):
        """Run complete benchmark for one scenario.

        Returns:
            dict: Results with measurements for all cache levels
        """
        # 1. Reset database
        print(f"[{scenario_num}] Resetting database...")
        self.reset_database()

        # 2. Generate and insert events
        print(f"[{scenario_num}] Generating {num_events:,} events...")
        gen = EventGenerator(
            db_host=self.db_host,
            db_port=self.db_port,
            db_name=self.db_name,
            db_user=self.db_user,
            db_password=self.db_password,
            num_users=num_users,
            num_products=num_products,
        )
        # Parse interval string to timedelta if needed
        if isinstance(price_update_interval, str):
            price_update_interval = parse_time_interval(price_update_interval)
        gen.generate_and_insert(
            num_events, price_update_interval=price_update_interval, end_date=end_date
        )

        print(f"[{scenario_num}] Refreshing TimescaleDB continuous aggregates...")
        gen.refresh_continuous_aggregate()
        gen.close()

        # 3. Get sample queries
        conn = self.get_connection()
        cur = conn.cursor()
        user_product_pairs, user_ids = self.get_sample_queries(cur, num_queries)

        # 4. Query with 0% cache (baseline - ONLY TIME WE QUERY EMPTY CACHE)
        print(f"[{scenario_num}] Querying with 0% cache (baseline)...")
        query_0pct = self.measure_query_performance(cur, user_product_pairs, user_ids)

        # 5. Refresh all caches
        print(f"[{scenario_num}] Refreshing all caches...")
        cache_refresh_time = self.refresh_all_caches(conn)

        # 6. Get percentile thresholds
        percentiles = self.get_cache_percentile_thresholds(conn)

        # 7. Query with 100% cache
        print(f"[{scenario_num}] Querying with 100% cache...")
        query_100pct = self.measure_query_performance(cur, user_product_pairs, user_ids)

        # 8. Progressive cache reduction
        query_75pct = query_50pct = query_25pct = None
        if percentiles:
            print(f"[{scenario_num}] Deleting cache >= 75th percentile...")
            self.delete_cache_from_threshold(conn, percentiles["p75"])
            query_75pct = self.measure_query_performance(cur, user_product_pairs, user_ids)

            print(f"[{scenario_num}] Deleting cache >= 50th percentile...")
            self.delete_cache_from_threshold(conn, percentiles["p50"])
            query_50pct = self.measure_query_performance(cur, user_product_pairs, user_ids)

            print(f"[{scenario_num}] Deleting cache >= 25th percentile...")
            self.delete_cache_from_threshold(conn, percentiles["p25"])
            query_25pct = self.measure_query_performance(cur, user_product_pairs, user_ids)

        conn.close()

        return {
            "scenario": scenario_num,
            "days": days,
            "num_events": num_events,
            "cache_refresh_time": cache_refresh_time,
            "query_0pct": query_0pct,
            "query_25pct": query_25pct,
            "query_50pct": query_50pct,
            "query_75pct": query_75pct,
            "query_100pct": query_100pct,
        }

    def run_multi_scenario_benchmark(self, scenarios, num_queries=100):
        """Run benchmark across multiple scenarios.

        Args:
            scenarios: List of (days, num_events) tuples
            num_queries: Number of sample queries per scenario

        Returns:
            List of result dicts
        """
        all_results = []
        end_date = datetime.now(timezone.utc).replace(hour=16, minute=0, second=0, microsecond=0)

        for i, (days, num_events) in enumerate(scenarios, 1):
            print(f"\n{'=' * 80}")
            print(f"SCENARIO {i}/{len(scenarios)}: {days:.1f} days, {num_events:,} events")
            print(f"{'=' * 80}")

            results = self.run_single_scenario(
                scenario_num=i,
                days=days,
                num_events=num_events,
                num_users=10_000,
                num_products=500,
                num_queries=num_queries,
                price_update_interval="2min",
                end_date=end_date,
            )
            all_results.append(results)

        return all_results


def print_summary_table(all_results):
    """Print compact summary table matching README format."""
    for r in all_results:
        print(f"\n{'=' * 80}")
        print(f"Scenario {r['scenario']}: {r['days']:.1f} days, {r['num_events']:,} events")
        print(f"{'=' * 80}")
        print(f"Refresh all caches: {r['cache_refresh_time']:.1f}s")

        # Print each cache level
        for cache_label, cache_key in [
            ("100%", "query_100pct"),
            ("75%", "query_75pct"),
            ("50%", "query_50pct"),
            ("25%", "query_25pct"),
            ("0%", "query_0pct"),
        ]:
            if cache_key in r and r[cache_key]:
                data = r[cache_key]
                print(f"Queries with {cache_label} cache:")
                for g in GRANULARITIES:
                    suffix = g["suffix"]
                    upt_key = f"upt_{suffix}_ms"
                    ut_key = f"ut_{suffix}_ms"
                    if upt_key in data and ut_key in data:
                        print(f"    user-product-timeline-{suffix}:  {data[upt_key]:.0f}ms")
                        print(f"    user-timeline-{suffix}:          {data[ut_key]:.0f}ms")


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

    # Multi-scenario mode
    parser.add_argument(
        "--multi-scenario",
        action="store_true",
        help="Run multi-scenario benchmark (7 scenarios, 500 products, up to 2 months)",
    )

    # 2-of-3 parameter model (for single-scenario mode)
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

    if args.multi_scenario:
        # Run multi-scenario benchmark
        print("\n=== MULTI-SCENARIO BENCHMARK ===")
        print("Parameters:")
        print("  - 7 scenarios")
        print("  - 10,000 users")
        print("  - 500 products")
        print("  - 2min price updates")
        print("  - Up to ~5M events (~2 months)")
        print()

        scenarios = calculate_scenario_parameters()
        print("Scenario plan:")
        for i, (days, events) in enumerate(scenarios, 1):
            print(f"  {i}. {days:5.1f} days → {events:,} events")
        print()

        all_results = benchmark.run_multi_scenario_benchmark(scenarios, args.num_queries)
        print_summary_table(all_results)
    else:
        # Single-scenario mode (original behavior)
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
