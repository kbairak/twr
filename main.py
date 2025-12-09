#!/usr/bin/env python3
"""
TWR (Time-Weighted Return) Calculator CLI
PostgreSQL-based portfolio tracking with incremental caching
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Import from twr package
from twr import TWRDatabase
from twr.generate import EventGenerator, calculate_missing_parameter, parse_time_interval
from twr.benchmark import Benchmark

# Import granularities for validation
migrations_dir = Path(__file__).parent / "migrations"
sys.path.insert(0, str(migrations_dir))
try:
    from granularities import GRANULARITIES
except ImportError:
    GRANULARITIES = []
finally:
    if str(migrations_dir) in sys.path:
        sys.path.remove(str(migrations_dir))

VALID_GRANULARITIES = [g['suffix'] for g in GRANULARITIES]


def create_parser():
    """Create the main argument parser with nested subcommands"""
    parser = argparse.ArgumentParser(
        description="""
╭─────────────────────────────────────────────────────────────────╮
│ TWR (Time-Weighted Return) Calculator                           │
│ PostgreSQL-based portfolio tracking with incremental caching    │
╰─────────────────────────────────────────────────────────────────╯
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Global database connection options
    parser.add_argument("--db-host", default="127.0.0.1", help="Database host (default: 127.0.0.1)")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port (default: 5432)")
    parser.add_argument("--db-name", default="twr", help="Database name (default: twr)")
    parser.add_argument("--db-user", default="twr_user", help="Database user (default: twr_user)")
    parser.add_argument("--db-password", default="twr_password", help="Database password (default: twr_password)")

    # Create subparsers for main command groups
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ===== DB COMMAND GROUP =====
    db_parser = subparsers.add_parser("db", help="Database management")
    db_subparsers = db_parser.add_subparsers(dest="db_command", help="Database operations")

    # db drop
    db_subparsers.add_parser("drop", help="Drop and recreate database")

    # db migrate
    db_subparsers.add_parser("migrate", help="Run database migrations")

    # db reset
    db_subparsers.add_parser("reset", help="Drop, recreate, and migrate database (drop + migrate)")

    # db refresh
    db_refresh_parser = db_subparsers.add_parser("refresh", help="Refresh timeline cache")
    db_refresh_parser.add_argument(
        "--granularity",
        choices=VALID_GRANULARITIES + ["all"],
        default="all",
        help=f"Which granularity to refresh (default: all)"
    )

    # db refresh-buckets
    db_refresh_buckets_parser = db_subparsers.add_parser("refresh-buckets", help="Refresh continuous aggregates (TimescaleDB buckets)")
    db_refresh_buckets_parser.add_argument(
        "--granularity",
        choices=VALID_GRANULARITIES + ["all"],
        default="all",
        help=f"Which granularity to refresh (default: all)"
    )

    # ===== ADD COMMAND GROUP =====
    add_parser = subparsers.add_parser("add", help="Add data (prices, cashflows)")
    add_subparsers = add_parser.add_subparsers(dest="add_command", help="Add data operations")

    # add price
    add_price_parser = add_subparsers.add_parser("price", help="Add a price record")
    add_price_parser.add_argument("--product", required=True, help="Product name")
    add_price_parser.add_argument("--price", type=float, required=True, help="Price per unit")
    add_price_parser.add_argument("--timestamp", help="ISO timestamp (default: now)")

    # add cashflow
    add_cashflow_parser = add_subparsers.add_parser("cashflow", help="Add a cash flow (buy/sell)")
    add_cashflow_parser.add_argument("--user", required=True, help="User name")
    add_cashflow_parser.add_argument("--product", required=True, help="Product name")
    add_cashflow_parser.add_argument(
        "--units",
        type=float,
        help="Units bought/sold (positive=buy, negative=sell)"
    )
    add_cashflow_parser.add_argument(
        "--money",
        type=float,
        help="Money amount (positive=buy, negative=sell)"
    )
    add_cashflow_parser.add_argument(
        "--fee",
        type=float,
        default=0,
        help="Transaction fee (default: 0)"
    )
    add_cashflow_parser.add_argument("--timestamp", help="ISO timestamp (default: now)")

    # ===== QUERY COMMAND GROUP =====
    query_parser = subparsers.add_parser("query", help="Query and display data")
    query_subparsers = query_parser.add_subparsers(dest="query_command", help="Query operations")

    # query all
    query_all_parser = query_subparsers.add_parser("all", help="Show all tables and views")
    query_all_parser.add_argument("--user", help="Filter by user name")
    query_all_parser.add_argument("--product", help="Filter by product name")
    query_all_parser.add_argument("--since", help="Filter results from this timestamp (ISO format)")
    query_all_parser.add_argument("--until", help="Filter results until this timestamp (ISO format)")

    # query prices
    query_prices_parser = query_subparsers.add_parser("prices", help="Show product prices")
    query_prices_parser.add_argument("--product", help="Filter by product name")
    query_prices_parser.add_argument("--since", help="Filter results from this timestamp (ISO format)")
    query_prices_parser.add_argument("--until", help="Filter results until this timestamp (ISO format)")

    # query cashflows
    query_cashflows_parser = query_subparsers.add_parser("cashflows", help="Show user cash flows")
    query_cashflows_parser.add_argument("--user", help="Filter by user name")
    query_cashflows_parser.add_argument("--product", help="Filter by product name")
    query_cashflows_parser.add_argument("--since", help="Filter results from this timestamp (ISO format)")
    query_cashflows_parser.add_argument("--until", help="Filter results until this timestamp (ISO format)")

    # query timeline
    query_timeline_parser = query_subparsers.add_parser("timeline", help="Show user-product timeline")
    query_timeline_parser.add_argument("--user", help="Filter by user name")
    query_timeline_parser.add_argument("--product", help="Filter by product name")
    query_timeline_parser.add_argument("--since", help="Filter results from this timestamp (ISO format)")
    query_timeline_parser.add_argument("--until", help="Filter results until this timestamp (ISO format)")

    # query portfolio
    query_portfolio_parser = query_subparsers.add_parser("portfolio", help="Show user portfolio timeline")
    query_portfolio_parser.add_argument("--user", help="Filter by user name")
    query_portfolio_parser.add_argument("--since", help="Filter results from this timestamp (ISO format)")
    query_portfolio_parser.add_argument("--until", help="Filter results until this timestamp (ISO format)")

    # ===== GENERATE COMMAND =====
    generate_parser = subparsers.add_parser("generate", help="Generate synthetic test data")

    # 2-of-3 parameter model
    generate_parser.add_argument("--days", type=float, help="Number of trading days to simulate")
    generate_parser.add_argument("--num-events", type=int, help="Total number of events to generate")
    generate_parser.add_argument("--price-update-frequency", type=str, help="Price update interval (e.g., '2min', '5min', '1h')")

    # Standard parameters
    generate_parser.add_argument("--num-users", type=int, default=5, help="Number of users (default: 5)")
    generate_parser.add_argument("--num-products", type=int, default=10, help="Number of products (default: 10)")
    generate_parser.add_argument("--price-delta-min", type=float, default=-0.02, help="Min price change %% (default: -0.02)")
    generate_parser.add_argument("--price-delta-max", type=float, default=0.025, help="Max price change %% (default: 0.025)")
    generate_parser.add_argument("--cashflow-min", type=float, default=50, help="Min cashflow amount (default: 50)")
    generate_parser.add_argument("--cashflow-max", type=float, default=500, help="Max cashflow amount (default: 500)")
    generate_parser.add_argument("--initial-price", type=float, default=100.0, help="Initial price for products (default: 100.0)")
    generate_parser.add_argument("--existing-product-prob", type=float, default=0.9, help="Probability of investing in existing product (default: 0.9)")

    # ===== BENCHMARK COMMAND =====
    benchmark_parser = subparsers.add_parser("benchmark", help="Run performance benchmarks")

    # 2-of-3 parameter model
    benchmark_parser.add_argument("--days", type=float, help="Number of trading days to simulate")
    benchmark_parser.add_argument("--num-events", type=int, help="Total number of events to generate")
    benchmark_parser.add_argument("--price-update-frequency", type=str, help="Price update interval (e.g., '2min', '5min', '1h')")

    # Standard parameters
    benchmark_parser.add_argument("--num-users", type=int, default=50, help="Number of users (default: 50)")
    benchmark_parser.add_argument("--num-products", type=int, default=100, help="Number of products (default: 100)")
    benchmark_parser.add_argument("--num-queries", type=int, default=100, help="Number of queries to sample (default: 100)")

    return parser


def handle_db_commands(args, db):
    """Handle database management commands"""
    if args.db_command == "drop":
        db.drop_database()
    elif args.db_command == "migrate":
        db.run_migrations()
    elif args.db_command == "reset":
        db.drop_database()
        db.run_migrations()
    elif args.db_command == "refresh":
        db.refresh_cache(granularity=args.granularity)
    elif args.db_command == "refresh-buckets":
        db.refresh_buckets(granularity=args.granularity)
    else:
        print("Error: No database command specified. Use 'uv run main.py db --help' for available commands.")
        sys.exit(1)


def handle_add_commands(args, db):
    """Handle add data commands"""
    if args.add_command == "price":
        db.add_price(args.product, args.price, args.timestamp)
    elif args.add_command == "cashflow":
        db.add_cashflow(
            args.user,
            args.product,
            units=args.units,
            money=args.money,
            fee=args.fee,
            timestamp=args.timestamp
        )
    else:
        print("Error: No add command specified. Use 'uv run main.py add --help' for available commands.")
        sys.exit(1)


def handle_query_commands(args, db):
    """Handle query commands"""
    # Extract filter arguments
    filters = {
        'user': getattr(args, 'user', None),
        'product': getattr(args, 'product', None),
        'since': getattr(args, 'since', None),
        'until': getattr(args, 'until', None),
    }

    if args.query_command == "all":
        db.show_all(**filters)
    elif args.query_command == "prices":
        db.show_prices(**filters)
    elif args.query_command == "cashflows":
        db.show_cashflows(**filters)
    elif args.query_command == "timeline":
        db.show_timeline(**filters)
    elif args.query_command == "portfolio":
        db.show_portfolio(**filters)
    else:
        print("Error: No query command specified. Use 'uv run main.py query --help' for available commands.")
        sys.exit(1)


def handle_generate_command(args):
    """Handle generate command"""
    # Calculate missing parameter
    try:
        days, num_events, frequency = calculate_missing_parameter(
            days=args.days,
            num_events=args.num_events,
            price_update_frequency=args.price_update_frequency,
            num_products=args.num_products
        )
    except ValueError as e:
        print(f"Error: {e}")
        print("\nExactly 2 of the 3 parameters (--days, --num-events, --price-update-frequency) must be provided.")
        sys.exit(1)

    # Calculate end_date as today at market close
    end_date = datetime.now(timezone.utc).replace(
        hour=16, minute=0, second=0, microsecond=0
    )

    # Display calculated parameters
    print("\n=== Calculated Parameters ===")
    print(f"Trading days: {days:.2f}")
    print(f"Total events: {num_events:,}")
    print(f"Price update frequency: {frequency}")
    print(f"Number of users: {args.num_users}")
    print(f"Number of products: {args.num_products}")
    print(f"End date: {end_date.date()}")
    print()

    gen = EventGenerator(
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password,
        num_users=args.num_users,
        num_products=args.num_products,
        price_delta_range=(args.price_delta_min, args.price_delta_max),
        cashflow_money_range=(args.cashflow_min, args.cashflow_max),
        initial_price=args.initial_price,
        existing_product_probability=args.existing_product_prob,
    )

    try:
        gen.generate_and_insert(num_events, price_update_interval=frequency, end_date=end_date)
        print("\nRefreshing continuous aggregates for all granularities...")
        gen.refresh_continuous_aggregate()
    finally:
        gen.close()

    print("\nDone! Run 'uv run main.py query all' to see results")


def handle_benchmark_command(args):
    """Handle benchmark command"""
    # Calculate missing parameter
    try:
        days, num_events, frequency = calculate_missing_parameter(
            days=args.days,
            num_events=args.num_events,
            price_update_frequency=args.price_update_frequency,
            num_products=args.num_products
        )
    except ValueError as e:
        print(f"Error: {e}")
        print("\nExactly 2 of the 3 parameters (--days, --num-events, --price-update-frequency) must be provided.")
        sys.exit(1)

    # Calculate end_date as today at market close
    end_date = datetime.now(timezone.utc).replace(
        hour=16, minute=0, second=0, microsecond=0
    )

    # Display calculated parameters
    print("\n=== Calculated Parameters ===")
    print(f"Trading days: {days:.2f}")
    print(f"Total events: {num_events:,}")
    print(f"Price update frequency: {frequency}")
    print(f"Number of users: {args.num_users}")
    print(f"Number of products: {args.num_products}")
    print(f"End date: {end_date.date()}")
    print()

    benchmark = Benchmark(
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password,
    )

    benchmark.run(
        num_events=num_events,
        num_users=args.num_users,
        num_products=args.num_products,
        num_queries=args.num_queries,
        price_update_interval=frequency,
        end_date=end_date,
    )


def main():
    parser = create_parser()
    args = parser.parse_args()

    # If no command provided, show help
    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Initialize database connection for db/add/query commands
    if args.command in ["db", "add", "query"]:
        db = TWRDatabase(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password,
        )

        if args.command == "db":
            handle_db_commands(args, db)
        elif args.command == "add":
            handle_add_commands(args, db)
        elif args.command == "query":
            handle_query_commands(args, db)

    elif args.command == "generate":
        handle_generate_command(args)

    elif args.command == "benchmark":
        handle_benchmark_command(args)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
