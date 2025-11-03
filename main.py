#!/usr/bin/env python3
"""
CLI tool for TWR (Time-Weighted Return) tracking system.
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
import psycopg2.extras
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import sys

# Import granularities configuration
migrations_dir = Path(__file__).parent / "migrations"
sys.path.insert(0, str(migrations_dir))
try:
    from granularities import GRANULARITIES
except ImportError:
    # Graceful fallback if granularities.py doesn't exist yet
    GRANULARITIES = []
finally:
    if str(migrations_dir) in sys.path:
        sys.path.remove(str(migrations_dir))

# Build list of valid granularity suffixes
VALID_GRANULARITIES = [g['suffix'] for g in GRANULARITIES]


class TWRDatabase:
    """Database operations for TWR tracking system."""

    def __init__(
        self,
        host="localhost",
        port=5432,
        dbname="twr",
        user="twr_user",
        password="twr_password",
    ):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.console = Console()

    def _get_connection(self):
        """Get a database connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )

    def _execute_query(self, query, params=None, fetch=False):
        """Execute a query with optional fetch."""
        conn = self._get_connection()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(query, params)

            if fetch:
                result = cur.fetchall()
                conn.commit()
                return result
            else:
                conn.commit()
                return None
        finally:
            conn.close()

    def drop_database(self):
        """Drop and recreate the database."""
        # Connect to postgres database to drop/create twr database
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname="postgres",
            user=self.user,
            password=self.password,
        )
        # Need to be outside transaction to drop/create database
        conn.autocommit = True

        try:
            cur = conn.cursor()

            # Terminate existing connections
            self.console.print("Terminating existing connections...")
            cur.execute(
                """
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = %s
                  AND pid <> pg_backend_pid()
            """,
                (self.dbname,),
            )

            # Drop database
            self.console.print(f"Dropping database {self.dbname}...")
            cur.execute(f'DROP DATABASE IF EXISTS "{self.dbname}"')

            # Create database
            self.console.print(f"Creating database {self.dbname}...")
            cur.execute(f'CREATE DATABASE "{self.dbname}" OWNER {self.user}')

            self.console.print(
                "[green]✓[/green] Database dropped and recreated successfully!"
            )

        finally:
            conn.close()

    def run_migrations(self):
        """Run database migrations."""
        import jinja2
        import sys

        migrations_dir = Path(__file__).parent / "migrations"

        # Add migrations to Python path to import granularities
        sys.path.insert(0, str(migrations_dir))
        try:
            from granularities import GRANULARITIES
        except ImportError:
            raise FileNotFoundError(f"Could not import granularities from {migrations_dir}/granularities.py")
        finally:
            sys.path.remove(str(migrations_dir))

        # Get all SQL files (regular) and templates (.j2)
        sql_files = sorted(migrations_dir.glob("*.sql"))
        template_files = sorted(migrations_dir.glob("*.sql.j2"))

        if not sql_files and not template_files:
            raise FileNotFoundError(f"No SQL migration files found in {migrations_dir}")

        # Render templates to temporary files
        compiled_files = []
        if template_files:
            self.console.print(f"Compiling {len(template_files)} Jinja2 templates...")
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(migrations_dir))

            for template_file in template_files:
                template = env.get_template(template_file.name)
                rendered = template.render(GRANULARITIES=GRANULARITIES)

                # Write to temporary file (same name without .j2 extension)
                compiled_file = migrations_dir / template_file.stem
                compiled_file.write_text(rendered)
                compiled_files.append(compiled_file)
                self.console.print(f"  Compiled {template_file.name} -> {compiled_file.name}")

        # Get all SQL files to execute (original + compiled)
        all_sql_files = sorted(sql_files + compiled_files)

        conn = self._get_connection()
        conn.autocommit = True  # Use autocommit to support TimescaleDB continuous aggregates

        try:
            cur = conn.cursor()

            for sql_file in all_sql_files:
                self.console.print(f"Executing {sql_file.name}...")

                # Read the SQL file
                sql_content = sql_file.read_text()

                # Execute the SQL
                try:
                    cur.execute(sql_content)
                    self.console.print(
                        f"[green]✓[/green] {sql_file.name} completed successfully"
                    )
                except Exception as e:
                    raise RuntimeError(f"Failed to execute {sql_file.name}: {e}")

            self.console.print(
                "[green]✓[/green] All migrations completed successfully!"
            )

        finally:
            conn.close()

            # Clean up compiled files
            for compiled_file in compiled_files:
                try:
                    compiled_file.unlink()
                    self.console.print(f"Cleaned up {compiled_file.name}")
                except Exception:
                    pass  # Ignore cleanup errors

    def clear(self):
        """Clear all data from tables while preserving schema."""
        # Truncate in reverse order of dependencies
        self._execute_query("TRUNCATE TABLE user_cash_flow CASCADE")
        self._execute_query("TRUNCATE TABLE product_price CASCADE")
        self._execute_query('TRUNCATE TABLE "user" CASCADE')
        self._execute_query("TRUNCATE TABLE product CASCADE")
        # Reset cache tables (no watermark table anymore - using MAX(timestamp) instead)
        for g in GRANULARITIES:
            self._execute_query(f"TRUNCATE TABLE user_product_timeline_cache_{g['suffix']}")
            self._execute_query(f"TRUNCATE TABLE user_timeline_cache_{g['suffix']}")

    def refresh_cache(self, granularity="all"):
        """Refresh the timeline cache with new data.

        Args:
            granularity: Which granularity to refresh ('all' or specific suffix like '15min', '1h', '1d')
        """
        granularities_to_refresh = VALID_GRANULARITIES if granularity == "all" else [granularity]

        for gran in granularities_to_refresh:
            self._execute_query(f"SELECT refresh_timeline_cache_{gran}()")
            self.console.print(f"[green]✓[/green] Cache for {gran} granularity refreshed successfully!")

    def refresh_buckets(self, granularity="all"):
        """Refresh continuous aggregates (buckets).

        Args:
            granularity: Which granularity to refresh ('all' or specific suffix like '15min', '1h', '1d')
        """
        granularities_to_refresh = VALID_GRANULARITIES if granularity == "all" else [granularity]

        for gran in granularities_to_refresh:
            self._execute_query(f"CALL refresh_continuous_aggregate('product_price_{gran}', NULL, NULL)")
            self.console.print(f"[green]✓[/green] Buckets for {gran} granularity refreshed successfully!")

    def add_price(self, product_name, price, timestamp=None):
        """Add a price record for a product."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        elif isinstance(timestamp, str):
            # Parse string timestamp for CLI compatibility
            timestamp = datetime.fromisoformat(timestamp)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = timestamp.astimezone(timezone.utc)
        else:
            # Accept datetime objects directly
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = timestamp.astimezone(timezone.utc)

        # Look up or create product
        query_lookup = """
            SELECT id FROM product WHERE name = %s
        """
        result = self._execute_query(query_lookup, (product_name,), fetch=True)

        if result:
            product_id = result[0]["id"]
        else:
            query_insert = """
                INSERT INTO product (name)
                VALUES (%s)
                RETURNING id
            """
            result = self._execute_query(query_insert, (product_name,), fetch=True)
            product_id = result[0]["id"]

        # Insert price
        query = """
            INSERT INTO product_price (product_id, price, timestamp)
            VALUES (%s, %s, %s)
        """
        self._execute_query(query, (product_id, price, timestamp))
        self.console.print(
            f"[green]✓[/green] Added price for [cyan]{product_name}[/cyan]: [yellow]${price}[/yellow] at {timestamp.isoformat()}"
        )

    def add_cashflow(
        self, user_name, product_name, units=None, money=None, fee=0, timestamp=None
    ):
        """Add a cash flow (buy or sell) for a user.

        User can provide:
        - Just money (trigger calculates units)
        - Just units (trigger calculates money)
        - Both (captures slippage/spread)
        - Optional fee (defaults to 0)
        """
        if units is None and money is None:
            raise ValueError("Must provide at least one of: --units or --money")

        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        elif isinstance(timestamp, str):
            # Parse string timestamp for CLI compatibility
            timestamp = datetime.fromisoformat(timestamp)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = timestamp.astimezone(timezone.utc)
        else:
            # Accept datetime objects directly
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = timestamp.astimezone(timezone.utc)

        # Look up or create user
        query_lookup_user = """
            SELECT id FROM "user" WHERE name = %s
        """
        result_user = self._execute_query(query_lookup_user, (user_name,), fetch=True)

        if result_user:
            user_id = result_user[0]["id"]
        else:
            query_insert_user = """
                INSERT INTO "user" (name)
                VALUES (%s)
                RETURNING id
            """
            result_user = self._execute_query(query_insert_user, (user_name,), fetch=True)
            user_id = result_user[0]["id"]

        # Look up or create product
        query_lookup_product = """
            SELECT id FROM product WHERE name = %s
        """
        result_product = self._execute_query(query_lookup_product, (product_name,), fetch=True)

        if result_product:
            product_id = result_product[0]["id"]
        else:
            query_insert_product = """
                INSERT INTO product (name)
                VALUES (%s)
                RETURNING id
            """
            result_product = self._execute_query(query_insert_product, (product_name,), fetch=True)
            product_id = result_product[0]["id"]

        # Insert cash flow - trigger will derive missing field and calculate all totals
        query = """
            INSERT INTO user_cash_flow (user_id, product_id, units, money, fee, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        self._execute_query(query, (user_id, product_id, units, money, fee, timestamp))

        # Display confirmation
        if units is not None and money is not None:
            # Both provided - show effective price
            effective_price = abs(money / units) if units != 0 else 0
            action = "bought" if units > 0 else "sold"
            color = "green" if units > 0 else "red"
            fee_str = f" (fee: ${fee:.2f})" if fee > 0 else ""
            self.console.print(
                f"[green]✓[/green] [cyan]{user_name}[/cyan] [{color}]{action}[/{color}] "
                f"{abs(units):.6f} units for ${abs(money):.2f} "
                f"(effective price: ${effective_price:.2f}){fee_str} at {timestamp.isoformat()}"
            )
        elif money is not None:
            # Money provided
            action_desc = "invested" if money > 0 else "withdrew"
            color = "green" if money > 0 else "red"
            fee_str = f" (fee: ${fee:.2f})" if fee > 0 else ""
            self.console.print(
                f"[green]✓[/green] [cyan]{user_name}[/cyan] [{color}]{action_desc}[/{color}] "
                f"${abs(money):.2f} in [cyan]{product_name}[/cyan]{fee_str} at {timestamp.isoformat()}"
            )
        else:
            # Units provided
            action = "bought" if units > 0 else "sold"
            color = "green" if units > 0 else "red"
            fee_str = f" (fee: ${fee:.2f})" if fee > 0 else ""
            self.console.print(
                f"[green]✓[/green] [cyan]{user_name}[/cyan] [{color}]{action}[/{color}] "
                f"{abs(units):.6f} units of [cyan]{product_name}[/cyan]{fee_str} at {timestamp.isoformat()}"
            )

    def show_all(self):
        """Display all tables and views."""
        self.console.print()

        # Show product prices
        self.console.print(Panel("[bold cyan]PRODUCT PRICES[/bold cyan]", expand=False))
        prices = self._execute_query(
            """
            SELECT p.name as product_name, pp.price, pp.timestamp
            FROM product_price pp
            JOIN product p ON pp.product_id = p.id
            ORDER BY pp.timestamp, p.name
        """,
            fetch=True,
        )
        if prices:
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("name")
            table.add_column("price", justify="right")
            table.add_column("timestamp")
            for row in prices:
                table.add_row(
                    str(row["product_name"]),
                    f"${row['price']:.2f}",
                    str(row["timestamp"]),
                )
            self.console.print(table)
        else:
            self.console.print("[dim]No prices found.[/dim]")

        self.console.print()

        # Show cash flows
        self.console.print(
            Panel("[bold cyan]USER CASH FLOWS[/bold cyan]", expand=False)
        )
        cash_flows = self._execute_query(
            """
            SELECT
                u.name as user_name,
                p.name as product_name,
                ucf.units,
                ucf.money,
                ucf.fee,
                ucf.bank_flow,
                ucf.timestamp,
                ucf.cumulative_units - ucf.units AS units_before_flow,
                ucf.cumulative_units AS units_after_flow,
                ucf.total_deposits,
                ucf.total_withdrawals,
                ucf.cumulative_fees,
                ucf.period_return,
                ucf.cumulative_twr_factor,
                (ucf.cumulative_twr_factor - 1) * 100 as cumulative_twr_pct
            FROM user_cash_flow ucf
            JOIN "user" u ON ucf.user_id = u.id
            JOIN product p ON ucf.product_id = p.id
            ORDER BY ucf.timestamp, u.name, p.name
        """,
            fetch=True,
        )
        if cash_flows:
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("user")
            table.add_column("product")
            table.add_column("units", justify="right")
            table.add_column("money", justify="right")
            table.add_column("fee", justify="right")
            table.add_column("bank_flow", justify="right")
            table.add_column("timestamp")
            table.add_column("cumulative_twr_pct", justify="right")

            for row in cash_flows:
                money_color = (
                    "green" if row["money"] and row["money"] >= 0 else "red"
                )
                money_sign = "+" if row["money"] and row["money"] >= 0 else ""

                bank_color = (
                    "red" if row["bank_flow"] and row["bank_flow"] < 0 else "green"
                )
                bank_sign = "+" if row["bank_flow"] and row["bank_flow"] >= 0 else ""

                table.add_row(
                    str(row["user_name"]),
                    str(row["product_name"]),
                    f"{row['units']:.2f}",
                    f"[{money_color}]{money_sign}${row['money']:.2f}[/{money_color}]"
                    if row["money"] is not None
                    else "N/A",
                    f"${row['fee']:.2f}" if row["fee"] is not None else "N/A",
                    f"[{bank_color}]{bank_sign}${row['bank_flow']:.2f}[/{bank_color}]"
                    if row["bank_flow"] is not None
                    else "N/A",
                    str(row["timestamp"]),
                    f"[green]{row['cumulative_twr_pct']:.2f}%[/green]"
                    if row["cumulative_twr_pct"] and row["cumulative_twr_pct"] >= 0
                    else f"[red]{row['cumulative_twr_pct']:.2f}%[/red]"
                    if row["cumulative_twr_pct"]
                    else "N/A",
                )
            self.console.print(table)
        else:
            self.console.print("[dim]No cash flows found.[/dim]")

        self.console.print()

        # Show user-product timeline
        self.console.print(
            Panel("[bold cyan]USER-PRODUCT TIMELINE[/bold cyan]", expand=False)
        )
        user_product_state = self._execute_query(
            """
            SELECT
                u.name as user_name,
                p.name as product_name,
                upt.timestamp,
                upt.holdings,
                upt.net_deposits,
                upt.current_price,
                upt.current_value,
                upt.current_twr * 100 as twr_pct,
                upt.is_cached
            FROM user_product_timeline_15min upt
            JOIN "user" u ON upt.user_id = u.id
            JOIN product p ON upt.product_id = p.id
            ORDER BY upt.timestamp, u.name, p.name
        """,
            fetch=True,
        )
        if user_product_state:
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("user")
            table.add_column("product")
            table.add_column("timestamp")
            table.add_column("holdings", justify="right")
            table.add_column("net_deposits", justify="right")
            table.add_column("price", justify="right")
            table.add_column("value", justify="right")
            table.add_column("twr_pct", justify="right")
            table.add_column("cached", justify="center")

            for row in user_product_state:
                twr_color = "green" if row["twr_pct"] and row["twr_pct"] >= 0 else "red"
                deposits_color = (
                    "green"
                    if row["net_deposits"] and row["net_deposits"] >= 0
                    else "red"
                )
                table.add_row(
                    str(row["user_name"]),
                    str(row["product_name"]),
                    str(row["timestamp"]),
                    f"{row['holdings']:.6f}" if row["holdings"] is not None else "N/A",
                    f"[{deposits_color}]${row['net_deposits']:.2f}[/{deposits_color}]"
                    if row["net_deposits"] is not None
                    else "N/A",
                    f"${row['current_price']:.2f}"
                    if row["current_price"] is not None
                    else "N/A",
                    f"${row['current_value']:.2f}"
                    if row["current_value"] is not None
                    else "N/A",
                    f"[{twr_color}]{row['twr_pct']:.2f}%[/{twr_color}]"
                    if row["twr_pct"] is not None
                    else "N/A",
                    "✓" if row["is_cached"] else "✗",
                )
            self.console.print(table)
        else:
            self.console.print("[dim]No user-product data found.[/dim]")

        self.console.print()

        # Show user timeline
        self.console.print(
            Panel(
                "[bold cyan]USER TIMELINE (Portfolio Over Time)[/bold cyan]",
                expand=False,
            )
        )
        timeline = self._execute_query(
            """
            SELECT
                u.name as user_name,
                ut.timestamp,
                ut.total_net_deposits,
                ut.total_value,
                ut.value_weighted_twr * 100 as twr_pct,
                ut.is_cached
            FROM user_timeline_15min ut
            JOIN "user" u ON ut.user_id = u.id
            ORDER BY ut.timestamp, u.name
        """,
            fetch=True,
        )
        if timeline:
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("user")
            table.add_column("timestamp")
            table.add_column("net_deposits", justify="right")
            table.add_column("total_value", justify="right")
            table.add_column("twr_pct", justify="right")
            table.add_column("cached", justify="center")

            for row in timeline:
                twr_color = "green" if row["twr_pct"] and row["twr_pct"] >= 0 else "red"
                deposits_color = (
                    "green"
                    if row["total_net_deposits"] and row["total_net_deposits"] >= 0
                    else "red"
                )
                table.add_row(
                    str(row["user_name"]),
                    str(row["timestamp"]),
                    f"[{deposits_color}]${row['total_net_deposits']:.2f}[/{deposits_color}]"
                    if row["total_net_deposits"] is not None
                    else "N/A",
                    f"${row['total_value']:.2f}"
                    if row["total_value"] is not None
                    else "N/A",
                    f"[{twr_color}]{row['twr_pct']:.2f}%[/{twr_color}]"
                    if row["twr_pct"] is not None
                    else "N/A",
                    "✓" if row["is_cached"] else "✗",
                )
            self.console.print(table)
        else:
            self.console.print("[dim]No timeline data found.[/dim]")

        self.console.print()


def main():
    parser = argparse.ArgumentParser(description="TWR Database CLI Tool")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # drop subcommand
    subparsers.add_parser("drop", help="Drop and recreate the database")

    # migrate subcommand
    subparsers.add_parser("migrate", help="Run database migrations")

    # refresh subcommand
    refresh_parser = subparsers.add_parser("refresh", help="Refresh the timeline cache")
    refresh_parser.add_argument(
        "--granularity",
        choices=VALID_GRANULARITIES + ["all"],
        default="all",
        help=f"Which granularity to refresh (default: all). Options: {', '.join(VALID_GRANULARITIES + ['all'])}"
    )

    # refresh-buckets subcommand
    refresh_buckets_parser = subparsers.add_parser("refresh-buckets", help="Refresh continuous aggregates (buckets)")
    refresh_buckets_parser.add_argument(
        "--granularity",
        choices=VALID_GRANULARITIES + ["all"],
        default="all",
        help=f"Which granularity to refresh (default: all). Options: {', '.join(VALID_GRANULARITIES + ['all'])}"
    )

    # add-price subcommand
    price_parser = subparsers.add_parser("add-price", help="Add a price record")
    price_parser.add_argument("--product", required=True, help="Product name")
    price_parser.add_argument(
        "--price", type=float, required=True, help="Price per unit"
    )
    price_parser.add_argument("--timestamp", help="ISO timestamp (default: now)")

    # add-cashflow subcommand
    cashflow_parser = subparsers.add_parser("add-cashflow", help="Add a cash flow")
    cashflow_parser.add_argument("--user", required=True, help="User name")
    cashflow_parser.add_argument("--product", required=True, help="Product name")

    # Allow either units, money, or both (at least one required)
    cashflow_parser.add_argument(
        "--units",
        type=float,
        help="Units (positive for buy, negative for sell)",
    )
    cashflow_parser.add_argument(
        "--money",
        type=float,
        help="Money amount (positive for buy, negative for sell)",
    )
    cashflow_parser.add_argument(
        "--fee",
        type=float,
        default=0,
        help="Transaction fee (default: 0)",
    )

    cashflow_parser.add_argument("--timestamp", help="ISO timestamp (default: now)")

    # show subcommand
    subparsers.add_parser("show", help="Display all tables and views")

    args = parser.parse_args()

    # Initialize database with default connection parameters
    db = TWRDatabase()

    if args.command == "drop":
        db.drop_database()
    elif args.command == "migrate":
        db.run_migrations()
    elif args.command == "refresh":
        db.refresh_cache(granularity=args.granularity)
    elif args.command == "refresh-buckets":
        db.refresh_buckets(granularity=args.granularity)
    elif args.command == "add-price":
        db.add_price(args.product, args.price, args.timestamp)
    elif args.command == "add-cashflow":
        db.add_cashflow(
            args.user,
            args.product,
            units=args.units if hasattr(args, "units") else None,
            money=args.money if hasattr(args, "money") else None,
            fee=args.fee if hasattr(args, "fee") else 0,
            timestamp=args.timestamp,
        )
    elif args.command == "show":
        db.show_all()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
