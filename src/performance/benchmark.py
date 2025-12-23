#!/usr/bin/env python3
"""
Async benchmark script for TWR database performance.

Measures:
1. Event generation and database insertion time
2. Cache refresh time for all granularities
3. Query performance at different cache levels (0%, 25%, 50%, 75%, 100%)
"""

import argparse
import time
from datetime import datetime, timezone
from uuid import UUID

import asyncpg

from performance.generate import EventGenerator, calculate_missing_parameter, parse_time_interval
from performance.granularities import GRANULARITIES
from performance.interface import get_user_product_timeline, get_user_timeline, refresh
from performance.reset import reset


def format_time(seconds: int) -> str:
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
        db_host: str = "127.0.0.1",
        db_port: int = 5432,
    ):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

    async def get_connection(self) -> asyncpg.Connection:
        """Create and return a new database connection"""
        return await asyncpg.connect(
            database=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
        )

    async def reset_database(self) -> None:
        """Reset database using async reset infrastructure"""
        print("Resetting database...")
        await reset()
        print("✓ Database reset complete\n")

    async def get_sample_queries(
        self, conn: asyncpg.Connection, num_queries: int
    ) -> tuple[list[tuple[UUID, UUID]], list[UUID]]:
        """Get sample user-product pairs and user IDs for queries.

        Returns:
            Tuple of (user_product_pairs, user_ids)
        """
        # Get user-product pairs
        rows = await conn.fetch(
            "SELECT DISTINCT user_id, product_id FROM cashflow LIMIT $1", num_queries
        )
        user_product_pairs = [(row["user_id"], row["product_id"]) for row in rows]

        # Get user IDs
        rows = await conn.fetch("SELECT DISTINCT user_id FROM cashflow LIMIT $1", num_queries)
        user_ids = [row["user_id"] for row in rows]

        return user_product_pairs, user_ids

    async def measure_query_performance(
        self,
        conn: asyncpg.Connection,
        user_product_pairs: list[tuple[UUID, UUID]],
        user_ids: list[UUID],
    ) -> dict[str, float]:
        """Measure query performance for each granularity using the new async interface.

        Returns:
            dict: {
                'upt_15min_ms': float, 'upt_1h_ms': float, 'upt_1d_ms': float,
                'ut_15min_ms': float, 'ut_1h_ms': float, 'ut_1d_ms': float,
            }
        """
        results: dict[str, float] = {}

        for g in GRANULARITIES:
            suffix = g.suffix

            # Measure user_product_timeline
            start = time.time()
            for user_id, product_id in user_product_pairs:
                await get_user_product_timeline(conn, user_id, product_id, g)
            results[f"upt_{suffix}_ms"] = ((time.time() - start) / len(user_product_pairs)) * 1000

            # Measure user_timeline
            start = time.time()
            for user_id in user_ids:
                await get_user_timeline(conn, user_id, g)
            results[f"ut_{suffix}_ms"] = ((time.time() - start) / len(user_ids)) * 1000

        return results

    async def refresh_all_caches(self, conn: asyncpg.Connection) -> float:
        """Refresh all caches using the refresh() function from interface.

        Returns:
            float: Total time in seconds
        """
        start = time.time()

        # First refresh the continuous aggregates (must be outside transaction)
        for g in GRANULARITIES:
            await conn.execute(
                f"CALL refresh_continuous_aggregate('price_update_{g.suffix}', NULL, NULL)"
            )

        # Then refresh the caches
        await refresh(conn)
        return time.time() - start

    async def run_single_scenario(
        self,
        scenario_num: int,
        days: float,
        num_events: int,
        num_users: int,
        num_products: int,
        num_queries: int,
        price_update_interval,
        end_date: datetime,
    ):
        """Run complete benchmark for one scenario.

        Returns:
            dict: Results with measurements for 0% and 100% cache
        """
        # 1. Reset database
        print(f"[{scenario_num}] Resetting database...")
        await self.reset_database()

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
        await gen.connect()

        # Parse interval string to timedelta if needed
        if isinstance(price_update_interval, str):
            price_update_interval = parse_time_interval(price_update_interval)

        await gen.generate_and_insert(
            num_events, price_update_interval=price_update_interval, end_date=end_date
        )
        await gen.close()

        # 3. Get sample queries
        conn = await self.get_connection()
        user_product_pairs, user_ids = await self.get_sample_queries(conn, num_queries)

        # 4. Query with 0% cache (baseline)
        print(f"[{scenario_num}] Querying with 0% cache (baseline)...")
        query_0pct = await self.measure_query_performance(conn, user_product_pairs, user_ids)

        # 5. Refresh all caches
        print(f"[{scenario_num}] Refreshing all caches...")
        cache_refresh_time = await self.refresh_all_caches(conn)

        # 6. Query with 100% cache
        print(f"[{scenario_num}] Querying with 100% cache...")
        query_100pct = await self.measure_query_performance(conn, user_product_pairs, user_ids)

        await conn.close()

        return {
            "scenario": scenario_num,
            "days": days,
            "num_events": num_events,
            "cache_refresh_time": cache_refresh_time,
            "query_0pct": query_0pct,
            "query_100pct": query_100pct,
        }


def print_results(results: dict):
    """Print benchmark results in a readable format"""
    print(f"\n{'=' * 80}")
    print(
        f"Scenario {results['scenario']}: {results['days']:.1f} days, {results['num_events']:,} events"
    )
    print(f"{'=' * 80}")
    print(f"Cache refresh time: {results['cache_refresh_time']:.1f}s\n")

    # Print 0% cache results
    print("Queries with 0% cache (baseline):")
    for g in GRANULARITIES:
        suffix = g.suffix
        upt_key = f"upt_{suffix}_ms"
        ut_key = f"ut_{suffix}_ms"
        if upt_key in results["query_0pct"]:
            print(f"  user-product-timeline-{suffix}: {results['query_0pct'][upt_key]:.0f}ms")
            print(f"  user-timeline-{suffix}:         {results['query_0pct'][ut_key]:.0f}ms")

    # Print 100% cache results with speedup
    print("\nQueries with 100% cache:")
    for g in GRANULARITIES:
        suffix = g.suffix
        upt_key = f"upt_{suffix}_ms"
        ut_key = f"ut_{suffix}_ms"
        if upt_key in results["query_100pct"]:
            upt_0 = results["query_0pct"][upt_key]
            upt_100 = results["query_100pct"][upt_key]
            ut_0 = results["query_0pct"][ut_key]
            ut_100 = results["query_100pct"][ut_key]

            upt_speedup = upt_0 / upt_100 if upt_100 > 0 else 0
            ut_speedup = ut_0 / ut_100 if ut_100 > 0 else 0

            print(f"  user-product-timeline-{suffix}: {upt_100:.0f}ms ({upt_speedup:.1f}x)")
            print(f"  user-timeline-{suffix}:         {ut_100:.0f}ms ({ut_speedup:.1f}x)")


async def main():
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

    benchmark = Benchmark()
    results = await benchmark.run_single_scenario(
        scenario_num=1,
        days=days,
        num_events=num_events,
        num_users=args.num_users,
        num_products=args.num_products,
        num_queries=args.num_queries,
        price_update_interval=frequency,
        end_date=end_date,
    )

    print_results(results)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
