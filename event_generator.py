from faker import Faker
from decimal import Decimal
from datetime import datetime, timedelta, timezone
import random
import psycopg2
from psycopg2.extras import execute_batch


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

    def generate_and_insert(self, num_events: int):
        """Generate events and insert into DB"""
        for i in range(num_events):
            if random.random() < (
                self.price_cashflow_ratio / (self.price_cashflow_ratio + 1)
            ):
                self._generate_price_event()
            else:
                self._generate_cashflow_event()

            self.current_timestamp += timedelta(seconds=self.time_increment_seconds)

            if (i + 1) % 100 == 0:
                print(f"Generated {i + 1}/{num_events} events...")

        print(f"Done! Generated {num_events} events")

    def _generate_price_event(self):
        """Generate and insert a price event"""
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

        # Insert into DB
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO product_price (product_id, timestamp, price) VALUES (%s, %s, %s)",
            (self.product_ids[product], self.current_timestamp, price),
        )
        self.conn.commit()

    def _generate_cashflow_event(self):
        """Generate and insert a cashflow event"""
        # Only pick from products that have prices
        if not self.products_with_prices:
            return  # No products with prices yet, skip

        user = random.choice(self.users)
        product = random.choice(list(self.products_with_prices))

        current_price = self.current_prices[product]
        key = (user, product)
        current_holdings = self.holdings.get(key, Decimal("0"))

        # Generate money amount
        money = Decimal(str(random.uniform(*self.cashflow_money_range)))

        # 20% chance of sell (negative money)
        if random.random() < 0.2 and current_holdings > 0:
            money = -money

        # Convert to units
        units = money / current_price

        # If selling, cap at current holdings
        if units < 0 and abs(units) > current_holdings:
            units = -current_holdings
            money = units * current_price

        # Update holdings
        self.holdings[key] = current_holdings + units

        # Insert into DB
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO user_cash_flow (user_id, product_id, units, timestamp) VALUES (%s, %s, %s, %s)",
            (self.user_ids[user], self.product_ids[product], units, self.current_timestamp),
        )
        self.conn.commit()

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
