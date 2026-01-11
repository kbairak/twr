from faker import Faker
from decimal import Decimal
from datetime import datetime, timedelta, timezone, time
from typing import Iterator
import random
import math
import uuid
import psycopg2
from psycopg2.extras import execute_values

# Trading constants
MARKET_OPEN = time(9, 30)  # 9:30 AM
MARKET_CLOSE = time(16, 0)  # 4:00 PM
PRICE_UPDATE_INTERVAL = timedelta(minutes=2)
START_DATE = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)  # Tuesday Jan 2, 2024


def generate_trading_timestamps(
    num_ticks: int, interval: timedelta = None, end_date: datetime = None
) -> Iterator[datetime]:
    """
    Generator that yields trading timestamps during market hours.
    Automatically skips weekends.

    Args:
        num_ticks: Total number of price update ticks needed
        interval: Time between ticks (default: PRICE_UPDATE_INTERVAL)
        end_date: Optional end date to work backwards from (default: work forwards from START_DATE)

    Yields:
        datetime objects during trading hours only
    """
    if interval is None:
        interval = PRICE_UPDATE_INTERVAL

    if end_date is not None:
        # Work backwards from end_date
        timestamps = []
        current_dt = end_date
        ticks_generated = 0

        while ticks_generated < num_ticks:
            # Skip weekends
            while current_dt.weekday() >= 5:  # Saturday=5, Sunday=6
                current_dt -= timedelta(days=1)
                current_dt = current_dt.replace(
                    hour=MARKET_CLOSE.hour,
                    minute=MARKET_CLOSE.minute,
                    second=0,
                    microsecond=0,
                )

            # If before market open, jump to previous day's market close
            if current_dt.time() < MARKET_OPEN:
                current_dt -= timedelta(days=1)
                current_dt = current_dt.replace(
                    hour=MARKET_CLOSE.hour,
                    minute=MARKET_CLOSE.minute,
                    second=0,
                    microsecond=0,
                )

            # Store this tick
            timestamps.append(current_dt)
            ticks_generated += 1

            # Move backwards
            current_dt -= interval

        # Yield in chronological order
        for ts in reversed(timestamps):
            yield ts
    else:
        # Work forwards from START_DATE
        current_dt = START_DATE
        ticks_generated = 0

        while ticks_generated < num_ticks:
            # Skip weekends
            while current_dt.weekday() >= 5:  # Saturday=5, Sunday=6
                current_dt += timedelta(days=1)
                current_dt = current_dt.replace(
                    hour=MARKET_OPEN.hour,
                    minute=MARKET_OPEN.minute,
                    second=0,
                    microsecond=0,
                )

            # Yield this tick
            yield current_dt
            ticks_generated += 1

            # Advance to next tick
            current_dt += interval

            # If past market close, jump to next day's market open
            if current_dt.time() >= MARKET_CLOSE:
                current_dt += timedelta(days=1)
                current_dt = current_dt.replace(
                    hour=MARKET_OPEN.hour,
                    minute=MARKET_OPEN.minute,
                    second=0,
                    microsecond=0,
                )


class EventGenerator:
    def __init__(
        self,
        db_name: str = "twr",
        db_user: str = "twr_user",
        db_password: str = "twr_password",
        db_host: str = "localhost",
        db_port: int = 5432,
        num_users: int = 5,
        num_products: int = 10,
        price_delta_range: tuple = (-0.02, 0.025),
        cashflow_money_range: tuple = (50, 500),
        initial_price: float = 100.0,
        existing_product_probability: float = 0.9,
        max_price: float = 100000.0,  # Cap at 100k to avoid overflow
        min_price: float = 1.0,  # Floor at $1
    ):
        # Database connection
        self.conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )

        self.faker = Faker()
        Faker.seed(42)

        # Generate unique names
        self.users = []
        seen_users = set()
        while len(self.users) < num_users:
            name = self.faker.name()
            if name not in seen_users:
                self.users.append(name)
                seen_users.add(name)

        self.products = []
        seen_products = set()
        while len(self.products) < num_products:
            name = self.faker.company()
            if name not in seen_products:
                self.products.append(name)
                seen_products.add(name)

        # Generate UUIDs for users and products (no DB insertion)
        self.user_ids = {}  # name -> uuid
        self.product_ids = {}  # name -> uuid
        self._generate_entity_ids()

        # State tracking
        self.current_prices = {}  # product_name -> price
        self.holdings = {}  # (user_name, product_name) -> units
        self.user_products = {}  # user_name -> set of product_names they've invested in

        # Config
        self.price_delta_range = price_delta_range
        self.cashflow_money_range = cashflow_money_range
        self.initial_price = initial_price
        self.existing_product_probability = existing_product_probability
        self.max_price = Decimal(str(max_price))
        self.min_price = Decimal(str(min_price))

    def _generate_entity_ids(self):
        """Generate UUIDs for users and products"""
        for user in self.users:
            self.user_ids[user] = uuid.uuid4()
        for product in self.products:
            self.product_ids[product] = uuid.uuid4()

    def generate_and_insert(
        self,
        num_events: int,
        batch_size: int = 10000,
        price_update_interval: timedelta = None,
        end_date: datetime = None,
    ):
        """Generate events with realistic market timing and insert into DB

        Args:
            num_events: Total number of events to generate
            batch_size: Number of events to insert per batch
            price_update_interval: Time between price updates (default: PRICE_UPDATE_INTERVAL)
            end_date: Optional end date to work backwards from (default: work forwards from START_DATE)
        """
        if price_update_interval is None:
            price_update_interval = PRICE_UPDATE_INTERVAL

        # Calculate event splits (90% price, 10% cash flows)
        num_price_events = int(num_events * 0.9)
        num_cash_flow_events = int(num_events * 0.1)

        # Calculate number of ticks needed
        num_ticks = math.ceil(num_price_events / len(self.products))

        print("\n=== Event Generation Plan ===")
        print(f"Total events: {num_events:,}")
        print(f"Price events: {num_price_events:,} (90%)")
        print(f"  - Ticks: {num_ticks:,}")
        print(f"  - Products per tick: {len(self.products)}")
        print(f"Cash flow events: {num_cash_flow_events:,} (10%)")
        print(f"Price update interval: {price_update_interval}")
        if end_date:
            print(f"End date: {end_date.date()}")
        print()

        price_events = []

        # Generate synchronized price updates
        print(f"Generating {num_ticks:,} price ticks...")
        for i, tick_time in enumerate(generate_trading_timestamps(
            num_ticks, interval=price_update_interval, end_date=end_date
        )):
            # All products update at this tick (with millisecond jitter)
            for product in self.products:
                jitter_ms = random.randint(0, 100)
                timestamp = tick_time + timedelta(milliseconds=jitter_ms)

                # Update price
                if product not in self.current_prices:
                    price = Decimal(str(self.initial_price))
                else:
                    delta = random.uniform(*self.price_delta_range)
                    price = self.current_prices[product] * (1 + Decimal(str(delta)))
                    # Clamp price to prevent overflow
                    price = max(self.min_price, min(self.max_price, price))

                self.current_prices[product] = price
                price_events.append((str(self.product_ids[product]), timestamp, price))

        # Determine time range from price events
        start_time = min(e[1] for e in price_events)
        end_time = max(e[1] for e in price_events)
        time_range = end_time - start_time
        calendar_days = (end_time.date() - start_time.date()).days + 1

        print(f"\nTime range: {start_time.date()} to {end_time.date()}")
        print(f"Calendar days: {calendar_days}")
        print(f"Time span: {time_range}")

        # Generate cash flows randomly within time range
        print(f"\nGenerating {num_cash_flow_events:,} cash flows...")
        cashflow_events = []
        for i in range(num_cash_flow_events):
            # Random timestamp within [start_time, end_time]
            random_offset = time_range * random.random()
            timestamp = start_time + random_offset

            # 80% during market hours, 20% after-hours
            if random.random() > 0.8:
                # Force to after-hours if not already
                if MARKET_OPEN <= timestamp.time() <= MARKET_CLOSE:
                    # Shift to after market close
                    timestamp = timestamp.replace(
                        hour=16, minute=random.randint(0, 59)
                    )

            event = self._generate_cashflow_event(timestamp)
            if event:
                cashflow_events.append(event)

        # Sort and insert all events
        print()
        self._batch_insert_all_events(
            price_events, cashflow_events, batch_size
        )

    def _generate_cashflow_event(self, timestamp: datetime):
        """Generate a cashflow event at given timestamp"""
        if not self.current_prices:
            return None  # No products with prices yet

        user = random.choice(self.users)

        # Initialize user's product set if not exists
        if user not in self.user_products:
            self.user_products[user] = set()

        # Choose product based on probability
        user_existing_products = self.user_products[user] & set(
            self.current_prices.keys()
        )

        if (
            user_existing_products
            and random.random() < self.existing_product_probability
        ):
            # 90% chance: pick from products the user already has
            product = random.choice(list(user_existing_products))
        else:
            # 10% chance: pick a new product
            available_new_products = (
                set(self.current_prices.keys()) - self.user_products[user]
            )
            if available_new_products:
                product = random.choice(list(available_new_products))
            elif user_existing_products:
                product = random.choice(list(user_existing_products))
            else:
                product = random.choice(list(self.current_prices.keys()))

        current_price = self.current_prices[product]
        key = (user, product)
        current_holdings = self.holdings.get(key, Decimal("0"))

        # Generate money amount
        money = Decimal(str(random.uniform(*self.cashflow_money_range)))

        # 20% chance of sell
        if random.random() < 0.2 and current_holdings > 0:
            # Sell between 10% and 80% of holdings
            sell_fraction = Decimal(str(random.uniform(0.1, 0.8)))
            units = -(current_holdings * sell_fraction)
            # For sells, calculate money from units (50% of time)
            # Other 50% of time, provide both to simulate slippage
            if random.random() < 0.5:
                # Just units - trigger calculates money
                money = None
            else:
                # Both units and money - simulate slippage
                slippage = Decimal(str(random.uniform(0.999, 1.001)))
                money = units * current_price * slippage
        else:
            # Buy: 50% of time just money, 50% both (slippage)
            if random.random() < 0.5:
                # Just money - trigger calculates units
                units = None
            else:
                # Both units and money - simulate slippage
                slippage = Decimal(str(random.uniform(0.999, 1.001)))
                units = money / (current_price * slippage)

        # Calculate expected units for holdings tracking
        if units is None:
            # Money-only transaction
            calc_units = money / current_price
        else:
            calc_units = units

        # Update holdings
        new_holdings = current_holdings + calc_units
        if new_holdings < 0:
            return None

        self.holdings[key] = new_holdings
        self.user_products[user].add(product)

        # Return tuple for batch insertion
        # Provide units_delta, execution_price, user_money
        # user_money = execution_money + fees = units_delta * execution_price + fees
        # Since fees = 0, user_money = units_delta * execution_price
        final_units = units if units is not None else calc_units
        user_money = final_units * current_price
        return (
            str(self.user_ids[user]),
            str(self.product_ids[product]),
            timestamp,
            final_units,
            current_price,
            user_money,
        )

    def _batch_insert_all_events(
        self, price_events, cashflow_events, batch_size
    ):
        """Sort and batch insert all price and cashflow events"""
        cur = self.conn.cursor()

        # Insert price events
        if price_events:
            price_events.sort(key=lambda x: (x[0], x[1]))
            print(f"Inserting {len(price_events):,} price events...")

            num_batches = (len(price_events) + batch_size - 1) // batch_size
            for batch_idx, i in enumerate(range(0, len(price_events), batch_size)):
                batch = price_events[i : i + batch_size]
                execute_values(
                    cur,
                    "INSERT INTO price_update (product_id, timestamp, price) VALUES %s",
                    batch,
                    page_size=1000,
                )
                self.conn.commit()

        # Insert cashflow events
        if cashflow_events:
            cashflow_events.sort(
                key=lambda x: (x[0], x[1], x[2])
            )  # Sort by user, product, timestamp
            print(f"\nInserting {len(cashflow_events):,} cashflow events...")

            num_batches = (len(cashflow_events) + batch_size - 1) // batch_size
            for batch_idx, i in enumerate(range(0, len(cashflow_events), batch_size)):
                batch = cashflow_events[i : i + batch_size]
                execute_values(
                    cur,
                    "INSERT INTO cashflow (user_id, product_id, timestamp, units_delta, execution_price, user_money) VALUES %s",
                    batch,
                    page_size=100,
                )
                self.conn.commit()

    def refresh_continuous_aggregate(self):
        """Refresh continuous aggregates for all granularities"""
        # Import granularities configuration
        import json
        from pathlib import Path

        migrations_dir = Path(__file__).parent.parent.parent / "migrations"
        granularities_file = migrations_dir / "granularities.json"

        with open(granularities_file) as f:
            GRANULARITIES = json.load(f)

        # Set autocommit to avoid transaction block error
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = True

        cur = self.conn.cursor()
        for g in GRANULARITIES:
            print(f"  Refreshing {g['suffix']} buckets...")
            cur.execute(
                f"CALL refresh_continuous_aggregate('price_update_{g['suffix']}', NULL, NULL)"
            )

        # Restore autocommit setting
        self.conn.autocommit = old_autocommit

    def close(self):
        self.conn.close()


def parse_time_interval(interval_str: str) -> timedelta:
    """Parse time interval string like '2min', '5min', '1h' to timedelta"""
    import re

    match = re.match(r"^(\d+)(min|h|d)$", interval_str.lower())
    if not match:
        raise ValueError(
            f"Invalid interval format: {interval_str}. Use format like '2min', '1h', '1d'"
        )

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "min":
        return timedelta(minutes=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)


def calculate_missing_parameter(
    days=None, num_events=None, price_update_frequency=None, num_products=10
):
    """
    Calculate the missing parameter from the 2-of-3 model.

    Formulas:
    - num_events = (days × 6.5 hours × num_products / frequency) + (days × 6.5 hours × num_products / frequency × 0.1 / 0.9)
    - Simplified: num_events = (days × 6.5 hours × num_products / frequency) × (1 / 0.9)

    Args:
        days: Number of trading days
        num_events: Total number of events
        price_update_frequency: Interval between price updates (timedelta or string)
        num_products: Number of products

    Returns:
        Tuple of (days, num_events, price_update_frequency as timedelta)
    """
    HOURS_PER_TRADING_DAY = 6.5
    PRICE_EVENT_RATIO = 0.9  # 90% of events are price updates

    # Parse frequency if it's a string
    if isinstance(price_update_frequency, str):
        price_update_frequency = parse_time_interval(price_update_frequency)

    params_provided = sum(
        [days is not None, num_events is not None, price_update_frequency is not None]
    )

    if params_provided != 2:
        raise ValueError(
            "Exactly 2 of 3 parameters (days, num-events, price-update-frequency) must be provided"
        )

    if days is not None and num_events is not None:
        # Calculate frequency
        # num_price_events = num_events * 0.9
        # num_ticks = num_price_events / num_products
        # frequency = (days * 6.5 hours) / num_ticks
        num_price_events = num_events * PRICE_EVENT_RATIO
        num_ticks = num_price_events / num_products
        total_seconds = days * HOURS_PER_TRADING_DAY * 3600
        frequency = timedelta(seconds=total_seconds / num_ticks)
        return days, num_events, frequency

    elif days is not None and price_update_frequency is not None:
        # Calculate num_events
        # num_ticks = (days * 6.5 hours) / frequency
        # num_price_events = num_ticks * num_products
        # num_events = num_price_events / 0.9
        total_seconds = days * HOURS_PER_TRADING_DAY * 3600
        frequency_seconds = price_update_frequency.total_seconds()
        num_ticks = total_seconds / frequency_seconds
        num_price_events = num_ticks * num_products
        num_events = int(num_price_events / PRICE_EVENT_RATIO)
        return days, num_events, price_update_frequency

    elif num_events is not None and price_update_frequency is not None:
        # Calculate days
        # num_price_events = num_events * 0.9
        # num_ticks = num_price_events / num_products
        # days = (num_ticks * frequency) / 6.5 hours
        num_price_events = num_events * PRICE_EVENT_RATIO
        num_ticks = num_price_events / num_products
        frequency_seconds = price_update_frequency.total_seconds()
        total_seconds = num_ticks * frequency_seconds
        days = total_seconds / (HOURS_PER_TRADING_DAY * 3600)
        return days, num_events, price_update_frequency


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate realistic trading events with 2-of-3 parameter model",
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
The third will be calculated automatically.
        """,
    )

    # 2-of-3 parameter model
    parser.add_argument("--days", type=float, help="Number of trading days to simulate")
    parser.add_argument(
        "--num-events", type=int, help="Total number of events to generate"
    )
    parser.add_argument(
        "--price-update-frequency",
        type=str,
        help="Price update interval (e.g., '2min', '5min', '1h')",
    )

    # Standard parameters
    parser.add_argument(
        "--num-users", type=int, default=5, help="Number of users (default: 5)"
    )
    parser.add_argument(
        "--num-products", type=int, default=10, help="Number of products (default: 10)"
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

    gen = EventGenerator(num_users=args.num_users, num_products=args.num_products)
    try:
        gen.generate_and_insert(
            num_events, price_update_interval=frequency, end_date=end_date
        )
        print("\nRefreshing continuous aggregates for all granularities...")
        gen.refresh_continuous_aggregate()
    finally:
        gen.close()

    print("Done! Run './main.py show' to see results")
