from faker import Faker
from decimal import Decimal
from datetime import datetime, timedelta, timezone, time
from typing import Iterator
import random
import math
import psycopg2
from psycopg2.extras import execute_values
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn

# Trading constants
MARKET_OPEN = time(9, 30)  # 9:30 AM
MARKET_CLOSE = time(16, 0)  # 4:00 PM
PRICE_UPDATE_INTERVAL = timedelta(minutes=2)
START_DATE = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)  # Tuesday Jan 2, 2024


def generate_trading_timestamps(num_ticks: int) -> Iterator[datetime]:
    """
    Generator that yields trading timestamps (2-min intervals during market hours).
    Automatically skips weekends.

    Args:
        num_ticks: Total number of price update ticks needed

    Yields:
        datetime objects during trading hours only
    """
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
                microsecond=0
            )

        # Yield this tick
        yield current_dt
        ticks_generated += 1

        # Advance to next tick
        current_dt += PRICE_UPDATE_INTERVAL

        # If past market close, jump to next day's market open
        if current_dt.time() >= MARKET_CLOSE:
            current_dt += timedelta(days=1)
            current_dt = current_dt.replace(
                hour=MARKET_OPEN.hour,
                minute=MARKET_OPEN.minute,
                second=0,
                microsecond=0
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
    ):
        # Database connection
        self.conn = psycopg2.connect(
            dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
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

        # Create users and products in DB, store their UUIDs
        self.user_ids = {}  # name -> uuid
        self.product_ids = {}  # name -> uuid
        self._initialize_entities()

        # State tracking
        self.current_prices = {}  # product_name -> price
        self.holdings = {}  # (user_name, product_name) -> units
        self.user_products = {}  # user_name -> set of product_names they've invested in

        # Config
        self.price_delta_range = price_delta_range
        self.cashflow_money_range = cashflow_money_range
        self.initial_price = initial_price
        self.existing_product_probability = existing_product_probability

    def _initialize_entities(self):
        """Create users and products in DB"""
        cur = self.conn.cursor()
        for user in self.users:
            cur.execute('INSERT INTO "user" (name) VALUES (%s) RETURNING id', (user,))
            self.user_ids[user] = cur.fetchone()[0]
        for product in self.products:
            cur.execute("INSERT INTO product (name) VALUES (%s) RETURNING id", (product,))
            self.product_ids[product] = cur.fetchone()[0]
        self.conn.commit()

    def generate_and_insert(self, num_events: int, batch_size: int = 10000):
        """Generate events with realistic market timing and insert into DB"""
        # Calculate event splits (90% price, 10% cash flows)
        num_price_events = int(num_events * 0.9)
        num_cash_flow_events = int(num_events * 0.1)

        # Calculate number of ticks needed
        num_ticks = math.ceil(num_price_events / len(self.products))

        print(f"\n=== Event Generation Plan ===")
        print(f"Total events: {num_events:,}")
        print(f"Price events: {num_price_events:,} (90%)")
        print(f"  - Ticks: {num_ticks:,}")
        print(f"  - Products per tick: {len(self.products)}")
        print(f"Cash flow events: {num_cash_flow_events:,} (10%)")
        print()

        price_events = []

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
        ) as progress:
            # Generate synchronized price updates
            price_task = progress.add_task("Generating price ticks", total=num_ticks)

            for tick_time in generate_trading_timestamps(num_ticks):
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

                    self.current_prices[product] = price
                    price_events.append((self.product_ids[product], timestamp, price))

                progress.update(price_task, advance=1)

            # Determine time range from price events
            start_time = min(e[1] for e in price_events)
            end_time = max(e[1] for e in price_events)
            time_range = end_time - start_time

            print(f"\nTime range: {start_time.date()} to {end_time.date()}")
            print(f"Duration: {time_range}")

            # Generate cash flows randomly within time range
            progress.remove_task(price_task)
            cashflow_task = progress.add_task("Generating cash flows", total=num_cash_flow_events)

            cashflow_events = []
            for _ in range(num_cash_flow_events):
                # Random timestamp within [start_time, end_time]
                random_offset = time_range * random.random()
                timestamp = start_time + random_offset

                # 80% during market hours, 20% after-hours
                if random.random() > 0.8:
                    # Force to after-hours if not already
                    if MARKET_OPEN <= timestamp.time() <= MARKET_CLOSE:
                        # Shift to after market close
                        timestamp = timestamp.replace(hour=16, minute=random.randint(0, 59))

                event = self._generate_cashflow_event(timestamp)
                if event:
                    cashflow_events.append(event)

                progress.update(cashflow_task, advance=1)

            # Sort and insert all events
            progress.remove_task(cashflow_task)
            self._batch_insert_all_events(price_events, cashflow_events, batch_size, progress)

    def _generate_cashflow_event(self, timestamp: datetime):
        """Generate a cashflow event at given timestamp"""
        if not self.current_prices:
            return None  # No products with prices yet

        user = random.choice(self.users)

        # Initialize user's product set if not exists
        if user not in self.user_products:
            self.user_products[user] = set()

        # Choose product based on probability
        user_existing_products = self.user_products[user] & set(self.current_prices.keys())

        if user_existing_products and random.random() < self.existing_product_probability:
            # 90% chance: pick from products the user already has
            product = random.choice(list(user_existing_products))
        else:
            # 10% chance: pick a new product
            available_new_products = set(self.current_prices.keys()) - self.user_products[user]
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
            money = units * current_price
        else:
            # Buy: convert money to units
            units = money / current_price

        # Update holdings
        new_holdings = current_holdings + units
        if new_holdings < 0:
            return None

        self.holdings[key] = new_holdings
        self.user_products[user].add(product)

        # Return tuple for batch insertion
        return (self.user_ids[user], self.product_ids[product], units, timestamp)

    def _batch_insert_all_events(self, price_events, cashflow_events, batch_size, progress):
        """Sort and batch insert all price and cashflow events"""
        cur = self.conn.cursor()

        # Insert price events
        if price_events:
            price_events.sort(key=lambda x: (x[0], x[1]))
            insert_task = progress.add_task("Inserting price events", total=len(price_events))

            for i in range(0, len(price_events), batch_size):
                batch = price_events[i:i + batch_size]
                execute_values(
                    cur,
                    "INSERT INTO product_price (product_id, timestamp, price) VALUES %s",
                    batch,
                    page_size=1000,
                )
                self.conn.commit()
                progress.update(insert_task, advance=len(batch))

            progress.remove_task(insert_task)

        # Insert cashflow events
        if cashflow_events:
            cashflow_events.sort(key=lambda x: (x[0], x[1], x[3]))
            insert_task = progress.add_task("Inserting cashflow events", total=len(cashflow_events))

            for i in range(0, len(cashflow_events), batch_size):
                batch = cashflow_events[i:i + batch_size]
                execute_values(
                    cur,
                    "INSERT INTO user_cash_flow (user_id, product_id, units, timestamp) VALUES %s",
                    batch,
                    page_size=100,
                )
                self.conn.commit()
                progress.update(insert_task, advance=len(batch))

            progress.remove_task(insert_task)

    def refresh_continuous_aggregate(self):
        """Refresh the continuous aggregate to populate 15-minute price buckets"""
        # Set autocommit to avoid transaction block error
        old_autocommit = self.conn.autocommit
        self.conn.autocommit = True

        cur = self.conn.cursor()
        cur.execute("CALL refresh_continuous_aggregate('product_price_15min', NULL, NULL)")

        # Restore autocommit setting
        self.conn.autocommit = old_autocommit

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events", type=int, default=100)
    parser.add_argument("--num-users", type=int, default=5)
    parser.add_argument("--num-products", type=int, default=10)
    args = parser.parse_args()

    gen = EventGenerator(num_users=args.num_users, num_products=args.num_products)
    try:
        gen.generate_and_insert(args.num_events)
        print("\nRefreshing continuous aggregate...")
        gen.refresh_continuous_aggregate()
    finally:
        gen.close()

    print("Done! Run './main.py show' to see results")
