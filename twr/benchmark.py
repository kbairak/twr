"""Benchmark module for TWR database performance testing."""

import time
import psycopg2
from rich.console import Console
from rich.table import Table
from pathlib import Path
import sys

# Import granularities configuration
migrations_dir = Path(__file__).parent.parent / "migrations"
sys.path.insert(0, str(migrations_dir))
try:
    from granularities import GRANULARITIES
except ImportError:
    GRANULARITIES = []
finally:
    if str(migrations_dir) in sys.path:
        sys.path.remove(str(migrations_dir))


class Benchmark:
    """Benchmark suite for TWR database performance.

    Measures:
    1. Event generation and database insertion time
    2. View evaluation time (user_product_timeline and user_timeline)
    3. Query performance on specific user-products and users (before cache refresh)
    4. Cache refresh time
    5. Query performance after cache refresh
    """

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
        self.console = Console()

    def get_connection(self):
        return psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
        )

    def run(
        self,
        num_events: int,
        num_users: int,
        num_products: int,
        num_queries: int = 100,
        price_update_interval=None,
        end_date=None,
    ):
        """Run the complete benchmark suite

        Args:
            num_events: Total number of events to generate
            num_users: Number of users
            num_products: Number of products
            num_queries: Number of queries to sample for performance testing
            price_update_interval: Time between price updates
            end_date: End date for price updates (work backwards from this)
        """
        from .event_generator import EventGenerator

        self.console.print("\n[bold cyan]TWR Database Benchmark[/bold cyan]")
        self.console.print(f"Events: {num_events:,}")
        self.console.print(f"Users: {num_users:,}")
        self.console.print(f"Products: {num_products:,}")
        self.console.print(f"Query samples: {num_queries:,}")
        self.console.print(
            f"Testing all granularities: {', '.join([g['suffix'] for g in GRANULARITIES])}\n"
        )

        results = {}

        # Step 1: Generate and insert events
        self.console.print("[bold]Step 1: Generating and inserting events...[/bold]")
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
        gen.generate_and_insert(num_events, price_update_interval=price_update_interval, end_date=end_date)
        insert_time = time.time() - start
        self.console.print(f"✓ Inserted {num_events:,} events in {insert_time:.2f}s\n")

        # Step 1b: Refresh continuous aggregates for all granularities
        self.console.print(
            "[bold]Step 1b: Refreshing continuous aggregates for all granularities...[/bold]"
        )
        start = time.time()
        gen.refresh_continuous_aggregate()
        gen.close()
        ca_refresh_time = time.time() - start
        self.console.print(
            f"✓ Refreshed continuous aggregates in {ca_refresh_time:.2f}s\n"
        )
        results["ca_refresh_time"] = ca_refresh_time

        conn = self.get_connection()
        cur = conn.cursor()

        # Step 2: Query performance (before cache refresh)
        self.console.print(
            "[bold]Step 2: Measuring query performance (before cache refresh)...[/bold]"
        )

        # Get sample user-product pairs
        cur.execute(
            """
            SELECT DISTINCT user_id, product_id
            FROM user_cash_flow
            LIMIT %s
        """,
            (num_queries,),
        )
        user_product_pairs = cur.fetchall()

        # Get sample users
        cur.execute(
            "SELECT DISTINCT user_id FROM user_cash_flow LIMIT %s", (num_queries,)
        )
        user_ids = [row[0] for row in cur.fetchall()]

        # Query each granularity
        for g in GRANULARITIES:
            granularity = g["suffix"]

            # Query specific user-products
            upt_query_times = []
            for user_id, product_id in user_product_pairs:
                start = time.time()
                cur.execute(
                    f"""
                    SELECT * FROM user_product_timeline_{granularity}
                    WHERE user_id = %s AND product_id = %s
                    ORDER BY timestamp
                """,
                    (user_id, product_id),
                )
                rows = cur.fetchall()
                upt_query_times.append(time.time() - start)

            results[f"upt_query_avg_before_{granularity}"] = (
                sum(upt_query_times) / len(upt_query_times) if upt_query_times else 0
            )
            results[f"upt_query_min_before_{granularity}"] = (
                min(upt_query_times) if upt_query_times else 0
            )
            results[f"upt_query_max_before_{granularity}"] = (
                max(upt_query_times) if upt_query_times else 0
            )

            # Query specific users
            ut_query_times = []
            for user_id in user_ids:
                start = time.time()
                cur.execute(
                    f"SELECT * FROM user_timeline_{granularity} WHERE user_id = %s ORDER BY timestamp",
                    (user_id,),
                )
                rows = cur.fetchall()
                ut_query_times.append(time.time() - start)

            results[f"ut_query_avg_before_{granularity}"] = (
                sum(ut_query_times) / len(ut_query_times) if ut_query_times else 0
            )
            results[f"ut_query_min_before_{granularity}"] = (
                min(ut_query_times) if ut_query_times else 0
            )
            results[f"ut_query_max_before_{granularity}"] = (
                max(ut_query_times) if ut_query_times else 0
            )

            self.console.print(
                f"✓ {granularity}: user_product avg={results[f'upt_query_avg_before_{granularity}'] * 1000:.2f}ms, "
                f"user avg={results[f'ut_query_avg_before_{granularity}'] * 1000:.2f}ms"
            )

        self.console.print()

        # Step 3: Cache refresh for all granularities
        self.console.print(
            "[bold]Step 3: Refreshing cache for all granularities...[/bold]"
        )

        for g in GRANULARITIES:
            granularity = g["suffix"]
            start = time.time()
            cur.execute(f"SELECT refresh_timeline_cache_{granularity}()")
            conn.commit()
            cache_refresh_time = time.time() - start
            results[f"cache_refresh_time_{granularity}"] = cache_refresh_time
            self.console.print(
                f"✓ Cache for {granularity} refreshed in {cache_refresh_time:.2f}s"
            )

        self.console.print()

        # Step 4: Query performance after cache refresh
        self.console.print(
            "[bold]Step 4: Measuring query performance (after cache refresh)...[/bold]"
        )

        # Query each granularity
        for g in GRANULARITIES:
            granularity = g['suffix']

            # Query the same user-products again
            upt_query_times_after = []
            for user_id, product_id in user_product_pairs:
                start = time.time()
                cur.execute(
                    f"""
                    SELECT * FROM user_product_timeline_{granularity}
                    WHERE user_id = %s AND product_id = %s
                    ORDER BY timestamp
                """,
                    (user_id, product_id),
                )
                rows = cur.fetchall()
                upt_query_times_after.append(time.time() - start)

            results[f"upt_query_avg_after_{granularity}"] = (
                sum(upt_query_times_after) / len(upt_query_times_after)
                if upt_query_times_after
                else 0
            )
            results[f"upt_query_min_after_{granularity}"] = (
                min(upt_query_times_after) if upt_query_times_after else 0
            )
            results[f"upt_query_max_after_{granularity}"] = (
                max(upt_query_times_after) if upt_query_times_after else 0
            )

            # Query the same users again
            ut_query_times_after = []
            for user_id in user_ids:
                start = time.time()
                cur.execute(
                    f"SELECT * FROM user_timeline_{granularity} WHERE user_id = %s ORDER BY timestamp",
                    (user_id,),
                )
                rows = cur.fetchall()
                ut_query_times_after.append(time.time() - start)

            results[f"ut_query_avg_after_{granularity}"] = (
                sum(ut_query_times_after) / len(ut_query_times_after)
                if ut_query_times_after
                else 0
            )
            results[f"ut_query_min_after_{granularity}"] = (
                min(ut_query_times_after) if ut_query_times_after else 0
            )
            results[f"ut_query_max_after_{granularity}"] = (
                max(ut_query_times_after) if ut_query_times_after else 0
            )

            self.console.print(
                f"✓ {granularity}: user_product avg={results[f'upt_query_avg_after_{granularity}'] * 1000:.2f}ms, "
                f"user avg={results[f'ut_query_avg_after_{granularity}'] * 1000:.2f}ms"
            )

        self.console.print()

        conn.close()

        # Display summary
        self._display_summary(results)

        return results

    def _display_summary(self, results):
        """Display benchmark results in a nice table"""
        self.console.print("\n[bold cyan]Benchmark Results Summary[/bold cyan]\n")

        # Table 1: Query Performance Comparison by Granularity
        table1 = Table(title="Query Performance by Granularity")
        table1.add_column("Granularity", style="cyan")
        table1.add_column("Query Type", style="cyan")
        table1.add_column("Before Cache", style="yellow")
        table1.add_column("After Cache", style="green")
        table1.add_column("Speedup", style="magenta")

        for g in GRANULARITIES:
            granularity = g['suffix']

            # User-product queries
            upt_before = results.get(f"upt_query_avg_before_{granularity}", 0)
            upt_after = results.get(f"upt_query_avg_after_{granularity}", 0)
            speedup_upt = upt_before / upt_after if upt_after > 0 else 0

            table1.add_row(
                granularity,
                "user_product",
                f"{upt_before * 1000:.2f}ms",
                f"{upt_after * 1000:.2f}ms",
                f"{speedup_upt:.2f}x",
            )

            # User queries
            ut_before = results.get(f"ut_query_avg_before_{granularity}", 0)
            ut_after = results.get(f"ut_query_avg_after_{granularity}", 0)
            speedup_ut = ut_before / ut_after if ut_after > 0 else 0

            table1.add_row(
                "",
                "user",
                f"{ut_before * 1000:.2f}ms",
                f"{ut_after * 1000:.2f}ms",
                f"{speedup_ut:.2f}x",
            )

        self.console.print(table1)
        self.console.print()

        # Table 2: Refresh times (continuous aggregate + cache)
        table2 = Table(title="Refresh Performance")
        table2.add_column("Operation", style="cyan")
        table2.add_column("Time", style="yellow")

        # Continuous aggregate refresh time
        if "ca_refresh_time" in results:
            ca_time = results["ca_refresh_time"]
            if ca_time >= 60:
                ca_time_str = f"{int(ca_time // 60)}m {ca_time % 60:.1f}s"
            else:
                ca_time_str = f"{ca_time:.2f}s"
            table2.add_row("Continuous aggregate (all buckets)", ca_time_str)

        # Cache refresh times per granularity
        for g in GRANULARITIES:
            granularity = g['suffix']
            cache_key = f"cache_refresh_time_{granularity}"
            if cache_key in results:
                cache_time = results[cache_key]
                if cache_time >= 60:
                    cache_time_str = f"{int(cache_time // 60)}m {cache_time % 60:.1f}s"
                else:
                    cache_time_str = f"{cache_time:.2f}s"
                table2.add_row(f"Timeline cache ({granularity})", cache_time_str)

        self.console.print(table2)
        self.console.print()
