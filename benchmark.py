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
        self.console.print("\n[bold cyan]TWR Database Benchmark[/bold cyan]")
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
        self.console.print(f"✓ Inserted {num_events:,} events in {insert_time:.2f}s\n")

        conn = self.get_connection()
        cur = conn.cursor()

        # Step 2: Query performance
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
            f"avg={results['upt_query_avg_before'] * 1000:.2f}ms, "
            f"min={results['upt_query_min_before'] * 1000:.2f}ms, "
            f"max={results['upt_query_max_before'] * 1000:.2f}ms\n"
        )

        # Get sample users
        cur.execute(
            "SELECT DISTINCT user_id FROM user_cash_flow LIMIT %s", (num_queries,)
        )
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
            f"avg={results['ut_query_avg_before'] * 1000:.2f}ms, "
            f"min={results['ut_query_min_before'] * 1000:.2f}ms, "
            f"max={results['ut_query_max_before'] * 1000:.2f}ms\n"
        )

        conn.close()

        # Display summary
        self._display_summary(results)

        return results

    def _display_summary(self, results):
        """Display benchmark results in a nice table"""
        self.console.print("\n[bold cyan]Benchmark Results Summary[/bold cyan]\n")

        # Table 1: Query Performance Comparison
        table1 = Table(title="Query Performance")
        table1.add_column("Query Type", style="cyan")

        table1.add_row(
            "user_product (avg)",
            f"{results['upt_query_avg_before'] * 1000:.2f}ms",
        )

        table1.add_row(
            "user (avg)",
            f"{results['ut_query_avg_before'] * 1000:.2f}ms",
        )

        self.console.print(table1)
        self.console.print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark TWR database performance")
    parser.add_argument(
        "--num-events", type=int, default=1000, help="Number of events to generate"
    )
    parser.add_argument("--num-users", type=int, default=50, help="Number of users")
    parser.add_argument(
        "--num-products", type=int, default=100, help="Number of products"
    )
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
