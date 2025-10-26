#!/usr/bin/env python3
"""
Benchmark script for TWR database performance.

Measures:
1. Event generation and database insertion time
2. View evaluation time (user_product_timeline and user_timeline)
3. Query performance on specific user-products and users (before cache refresh)
4. Cache refresh time
5. Query performance after cache refresh
"""

import argparse
import time
from datetime import datetime, timezone
from decimal import Decimal
import psycopg2
from rich.console import Console
from rich.table import Table
from event_generator import EventGenerator


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
    ):
        """Run the complete benchmark suite"""
        self.console.print(f"\n[bold cyan]TWR Database Benchmark[/bold cyan]")
        self.console.print(f"Events: {num_events:,}")
        self.console.print(f"Users: {num_users:,}")
        self.console.print(f"Products: {num_products:,}")
        self.console.print(f"Query samples: {num_queries:,}\n")

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
        gen.generate_and_insert(num_events)
        gen.close()
        insert_time = time.time() - start
        results["insert_time"] = insert_time
        results["events_per_sec"] = num_events / insert_time
        self.console.print(
            f"✓ Inserted {num_events:,} events in {insert_time:.2f}s "
            f"({results['events_per_sec']:.0f} events/sec)\n"
        )

        # Get event counts and view row counts
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM product_price")
        results["num_price_events"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM user_cash_flow")
        results["num_cashflow_events"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM user_product_timeline")
        results["upt_rows"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM user_timeline")
        results["ut_rows"] = cur.fetchone()[0]
        self.console.print(
            f"  Price events: {results['num_price_events']:,}, "
            f"Cashflow events: {results['num_cashflow_events']:,}\n"
            f"  Timeline rows: {results['upt_rows']:,}\n"
        )

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

        # Query specific user-products
        upt_query_times = []
        for user_id, product_id in user_product_pairs:
            start = time.time()
            cur.execute(
                """
                SELECT * FROM user_product_timeline
                WHERE user_id = %s AND product_id = %s
                ORDER BY timestamp
            """,
                (user_id, product_id),
            )
            rows = cur.fetchall()
            upt_query_times.append(time.time() - start)

        results["upt_query_avg_before"] = (
            sum(upt_query_times) / len(upt_query_times) if upt_query_times else 0
        )
        results["upt_query_min_before"] = min(upt_query_times) if upt_query_times else 0
        results["upt_query_max_before"] = max(upt_query_times) if upt_query_times else 0
        self.console.print(
            f"✓ user_product queries (n={len(upt_query_times)}): "
            f"avg={results['upt_query_avg_before']*1000:.2f}ms, "
            f"min={results['upt_query_min_before']*1000:.2f}ms, "
            f"max={results['upt_query_max_before']*1000:.2f}ms\n"
        )

        # Get sample users
        cur.execute("SELECT DISTINCT user_id FROM user_cash_flow LIMIT %s", (num_queries,))
        user_ids = [row[0] for row in cur.fetchall()]

        # Query specific users
        ut_query_times = []
        for user_id in user_ids:
            start = time.time()
            cur.execute(
                "SELECT * FROM user_timeline WHERE user_id = %s ORDER BY timestamp",
                (user_id,),
            )
            rows = cur.fetchall()
            ut_query_times.append(time.time() - start)

        results["ut_query_avg_before"] = (
            sum(ut_query_times) / len(ut_query_times) if ut_query_times else 0
        )
        results["ut_query_min_before"] = min(ut_query_times) if ut_query_times else 0
        results["ut_query_max_before"] = max(ut_query_times) if ut_query_times else 0
        self.console.print(
            f"✓ user queries (n={len(ut_query_times)}): "
            f"avg={results['ut_query_avg_before']*1000:.2f}ms, "
            f"min={results['ut_query_min_before']*1000:.2f}ms, "
            f"max={results['ut_query_max_before']*1000:.2f}ms\n"
        )

        # Step 3: Cache refresh
        self.console.print("[bold]Step 3: Refreshing cache...[/bold]")
        start = time.time()
        cur.execute("SELECT refresh_timeline_cache()")
        conn.commit()
        refresh_time = time.time() - start
        results["refresh_time"] = refresh_time
        self.console.print(f"✓ Cache refreshed in {refresh_time:.2f}s\n")

        # Check cache sizes
        cur.execute("SELECT COUNT(*) FROM user_product_timeline_cache")
        results["upt_cache_rows"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM user_timeline_cache")
        results["ut_cache_rows"] = cur.fetchone()[0]
        self.console.print(
            f"  Cached user_product_timeline: {results['upt_cache_rows']:,} rows\n"
            f"  Cached user_timeline: {results['ut_cache_rows']:,} rows\n"
        )

        # Step 4: Query performance (after cache refresh)
        self.console.print(
            "[bold]Step 4: Measuring query performance (after cache refresh)...[/bold]"
        )

        # Get NEW sample user-product pairs (to avoid disk cache effects)
        cur.execute(
            """
            SELECT DISTINCT user_id, product_id
            FROM user_cash_flow
            LIMIT %s
        """,
            (num_queries,),
        )
        user_product_pairs_after = cur.fetchall()

        # Query specific user-products
        upt_query_times_after = []
        for user_id, product_id in user_product_pairs_after:
            start = time.time()
            cur.execute(
                """
                SELECT * FROM user_product_timeline
                WHERE user_id = %s AND product_id = %s
                ORDER BY timestamp
            """,
                (user_id, product_id),
            )
            rows = cur.fetchall()
            upt_query_times_after.append(time.time() - start)

        results["upt_query_avg_after"] = (
            sum(upt_query_times_after) / len(upt_query_times_after)
            if upt_query_times_after
            else 0
        )
        results["upt_query_min_after"] = (
            min(upt_query_times_after) if upt_query_times_after else 0
        )
        results["upt_query_max_after"] = (
            max(upt_query_times_after) if upt_query_times_after else 0
        )
        self.console.print(
            f"✓ user_product queries (n={len(upt_query_times_after)}): "
            f"avg={results['upt_query_avg_after']*1000:.2f}ms, "
            f"min={results['upt_query_min_after']*1000:.2f}ms, "
            f"max={results['upt_query_max_after']*1000:.2f}ms\n"
        )

        # Get NEW sample users (to avoid disk cache effects)
        cur.execute("SELECT DISTINCT user_id FROM user_cash_flow LIMIT %s", (num_queries,))
        user_ids_after = [row[0] for row in cur.fetchall()]

        # Query specific users
        ut_query_times_after = []
        for user_id in user_ids_after:
            start = time.time()
            cur.execute(
                "SELECT * FROM user_timeline WHERE user_id = %s ORDER BY timestamp",
                (user_id,),
            )
            rows = cur.fetchall()
            ut_query_times_after.append(time.time() - start)

        results["ut_query_avg_after"] = (
            sum(ut_query_times_after) / len(ut_query_times_after)
            if ut_query_times_after
            else 0
        )
        results["ut_query_min_after"] = min(ut_query_times_after) if ut_query_times_after else 0
        results["ut_query_max_after"] = max(ut_query_times_after) if ut_query_times_after else 0
        self.console.print(
            f"✓ user queries (n={len(ut_query_times_after)}): "
            f"avg={results['ut_query_avg_after']*1000:.2f}ms, "
            f"min={results['ut_query_min_after']*1000:.2f}ms, "
            f"max={results['ut_query_max_after']*1000:.2f}ms\n"
        )

        conn.close()

        # Display summary
        self._display_summary(results)

        return results

    def _display_summary(self, results):
        """Display benchmark results in a nice table"""
        self.console.print("\n[bold cyan]Benchmark Results Summary[/bold cyan]\n")

        # Table 1: Data Generation
        table1 = Table(title="Data Generation Performance")
        table1.add_column("Metric", style="cyan")
        table1.add_column("Value", style="green", justify="right")

        table1.add_row("Total events", f"{results.get('num_price_events', 0) + results.get('num_cashflow_events', 0):,}")
        table1.add_row("  Price events", f"{results.get('num_price_events', 0):,}")
        table1.add_row("  Cashflow events", f"{results.get('num_cashflow_events', 0):,}")
        table1.add_row("Insert time", f"{results['insert_time']:.2f}s")
        table1.add_row("Throughput", f"{results['events_per_sec']:.0f} events/sec")

        self.console.print(table1)
        self.console.print()

        # Table 2: Query Performance Comparison
        table2 = Table(title="Query Performance (Before vs After Cache)")
        table2.add_column("Query Type", style="cyan")
        table2.add_column("Before Cache", style="yellow", justify="right")
        table2.add_column("After Cache", style="green", justify="right")
        table2.add_column("Speedup", style="magenta", justify="right")

        upt_speedup = (
            results["upt_query_avg_before"] / results["upt_query_avg_after"]
            if results["upt_query_avg_after"] > 0
            else 0
        )
        table2.add_row(
            "user_product (avg)",
            f"{results['upt_query_avg_before']*1000:.2f}ms",
            f"{results['upt_query_avg_after']*1000:.2f}ms",
            f"{upt_speedup:.1f}x",
        )

        ut_speedup = (
            results["ut_query_avg_before"] / results["ut_query_avg_after"]
            if results["ut_query_avg_after"] > 0
            else 0
        )
        table2.add_row(
            "user (avg)",
            f"{results['ut_query_avg_before']*1000:.2f}ms",
            f"{results['ut_query_avg_after']*1000:.2f}ms",
            f"{ut_speedup:.1f}x",
        )

        self.console.print(table2)
        self.console.print()

        # Table 3: Cache Statistics
        table3 = Table(title="Cache Statistics")
        table3.add_column("Metric", style="cyan")
        table3.add_column("Value", style="green", justify="right")

        table3.add_row("Refresh time", f"{results['refresh_time']:.2f}s")
        table3.add_row("user_product_timeline cached", f"{results['upt_cache_rows']:,} rows")
        table3.add_row("user_timeline cached", f"{results['ut_cache_rows']:,} rows")

        self.console.print(table3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark TWR database performance")
    parser.add_argument("--num-events", type=int, default=1000, help="Number of events to generate")
    parser.add_argument("--num-users", type=int, default=50, help="Number of users")
    parser.add_argument("--num-products", type=int, default=100, help="Number of products")
    parser.add_argument(
        "--num-queries", type=int, default=100, help="Number of queries to sample"
    )
    args = parser.parse_args()

    benchmark = Benchmark()
    benchmark.run(
        num_events=args.num_events,
        num_users=args.num_users,
        num_products=args.num_products,
        num_queries=args.num_queries,
    )
