from faker import Faker
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import random
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn


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
        price_cashflow_ratio: float = 9.0,
        price_delta_range: tuple = (-0.02, 0.025),
        cashflow_money_range: tuple = (50, 500),
        initial_price: float = 100.0,
        start_timestamp: datetime = None,
        time_increment_seconds: int = 120,
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
        self.products_with_prices = set()  # product names that have prices
        self.holdings = {}  # (user_name, product_name) -> units
        self.current_timestamp = start_timestamp or datetime.now(timezone.utc)

        # Config
        self.price_cashflow_ratio = price_cashflow_ratio
        self.price_delta_range = price_delta_range
        self.cashflow_money_range = cashflow_money_range
        self.initial_price = initial_price
        self.time_increment_seconds = time_increment_seconds

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
        """Generate events and insert into DB using batch operations"""
        price_events = []
        cashflow_events = []

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
        ) as progress:
            gen_task = progress.add_task("Generating events", total=num_events)

            # Generate all events first
            for i in range(num_events):
                if random.random() < (
                    self.price_cashflow_ratio / (self.price_cashflow_ratio + 1)
                ):
                    event = self._generate_price_event()
                    if event:
                        price_events.append(event)
                else:
                    event = self._generate_cashflow_event()
                    if event:
                        cashflow_events.append(event)

                self.current_timestamp += timedelta(seconds=self.time_increment_seconds)
                progress.update(gen_task, advance=1)

            # Now insert all events in batches
            progress.remove_task(gen_task)
            self._batch_insert_all_events(price_events, cashflow_events, batch_size, progress)

    def _generate_price_event(self):
        """Generate a price event (returns tuple for batch insertion)"""
        product = random.choice(self.products)

        if product not in self.current_prices:
            # First price for this product
            price = Decimal(str(self.initial_price))
            self.products_with_prices.add(product)
        else:
            # Apply percentage delta
            delta = random.uniform(*self.price_delta_range)
            price = self.current_prices[product] * (1 + Decimal(str(delta)))

        self.current_prices[product] = price

        # Return tuple for batch insertion
        return (self.product_ids[product], self.current_timestamp, price)

    def _generate_cashflow_event(self):
        """Generate a cashflow event (returns tuple for batch insertion)"""
        # Only pick from products that have prices
        if not self.products_with_prices:
            return None  # No products with prices yet, skip

        user = random.choice(self.users)
        product = random.choice(list(self.products_with_prices))

        current_price = self.current_prices[product]
        key = (user, product)
        current_holdings = self.holdings.get(key, Decimal("0"))

        # Generate money amount
        money = Decimal(str(random.uniform(*self.cashflow_money_range)))

        # 20% chance of sell (negative money), but only sell up to 80% of holdings to be safe
        # This conservative approach ensures we never sell more than we have even with rounding errors
        if random.random() < 0.2 and current_holdings > 0:
            # Sell between 10% and 80% of current holdings
            sell_fraction = Decimal(str(random.uniform(0.1, 0.8)))
            units = -(current_holdings * sell_fraction)
            money = units * current_price
        else:
            # Convert money to units for buys
            units = money / current_price

        # Update holdings
        new_holdings = current_holdings + units

        # Sanity check - should never happen with our conservative sell logic
        if new_holdings < 0:
            return None

        self.holdings[key] = new_holdings

        # Return tuple for batch insertion
        return (self.user_ids[user], self.product_ids[product], units, self.current_timestamp)

    def _batch_insert_all_events(self, price_events, cashflow_events, batch_size, progress):
        """Sort and batch insert all price and cashflow events"""
        cur = self.conn.cursor()

        # Sort price events by (product_id, timestamp) to ensure chronological order
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

        # Sort cashflow events by (user_id, product_id, timestamp) to ensure chronological order
        # This is critical for the trigger to correctly calculate cumulative values
        if cashflow_events:
            cashflow_events.sort(key=lambda x: (x[0], x[1], x[3]))
            insert_task = progress.add_task("Inserting cashflow events", total=len(cashflow_events))

            for i in range(0, len(cashflow_events), batch_size):
                batch = cashflow_events[i:i + batch_size]
                execute_values(
                    cur,
                    "INSERT INTO user_cash_flow (user_id, product_id, units, timestamp) VALUES %s",
                    batch,
                    page_size=100,  # Smaller page size to ensure trigger sees previous rows
                )
                self.conn.commit()
                progress.update(insert_task, advance=len(batch))

            progress.remove_task(insert_task)

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
    finally:
        gen.close()

    print("\nDone! Run './main.py show' to see results")
