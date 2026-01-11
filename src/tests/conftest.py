"""Pytest configuration and fixtures for TWR tests."""

from typing import Callable
import pytest
import psycopg2
from testcontainers.postgres import PostgresContainer
from tests.utils import parse_time
from twr.migrate import run_all_migrations


@pytest.fixture(scope="session")
def postgres_container():
    """Start PostgreSQL container with TimescaleDB for tests."""
    with PostgresContainer("timescale/timescaledb:latest-pg16") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def db_connection(postgres_container):
    """Create database connection and run migrations."""
    connection = psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        database=postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password,
    )
    connection.autocommit = True

    # Run migrations
    run_all_migrations(connection=connection)

    yield connection
    connection.close()


@pytest.fixture
def query(db_connection):
    """Execute query and return results as list of dicts (or empty list for statements without RETURNING)."""

    with db_connection.cursor() as cursor:
        # Truncate tables in dependency order (CASCADE handles foreign keys)
        cursor.execute('TRUNCATE TABLE cashflow, price_update CASCADE')

    def fn(q, params=None):
        with db_connection.cursor() as cursor:
            cursor.execute(q, params or ())
            # cursor.description is None for statements without RETURNING
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    return fn


@pytest.fixture
def user(query) -> Callable[[str], str]:
    """Generate consistent UUIDs for user names (no user table anymore)."""
    import uuid
    users = {}

    def fn(seed: str) -> str:
        if seed not in users:
            # Generate deterministic UUID from seed for consistency
            users[seed] = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"user:{seed}"))
        return users[seed]

    return fn


@pytest.fixture
def product(query) -> Callable[[str], str]:
    """Generate consistent UUIDs for product names (no product table anymore)."""
    import uuid
    products = {}

    def fn(seed: str) -> str:
        if seed not in products:
            # Generate deterministic UUID from seed for consistency
            products[seed] = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"product:{seed}"))
        return products[seed]

    return fn


@pytest.fixture
def make_data(query, product, user):
    '''
    Usage:

        def test_foo(make_data):
            make_data("""
                            12:00, 12:10, 12:20, 12:30
                AAPL:       10   ,      ,    15
                Alice/AAPL:      ,     3,      ,    -8
            """)

    This will create two price updates for AAPL: one for 10$/unit at 12:00 and one for 15$/unit at
    12:20 and two cashflows for Alice/AAPL: one for 3 units at 12:10 and one for -8 units at 12:30
    (at market prices).
    '''

    def fn(text):
        price_updates = {}
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        timestamps = [parse_time(t.strip()) for t in lines[0].split(",")]
        for line in lines[1:]:
            identifier, values = [t.strip() for t in line.split(":") if t.strip()]
            try:
                user_name, product_name = [w.strip() for w in identifier.split("/")]
            except ValueError:
                product_name = identifier.strip()
                prices_str = [p.strip() for p in values.split(",")]
                for timestamp, price_str in zip(timestamps, prices_str):
                    try:
                        price = float(price_str)
                    except ValueError:
                        continue
                    price_updates.setdefault(product_name, {})[timestamp] = price
                    query(
                        "INSERT INTO price_update (product_id, timestamp, price) VALUES (%s, %s, %s)",
                        (product(product_name), timestamp, price),
                    )
            else:
                units_str = [u.strip() for u in values.split(",")]
                for timestamp, unit_str in zip(timestamps, units_str):
                    try:
                        units = float(unit_str)
                    except ValueError:
                        continue
                    price = sorted(
                        [
                            (t, p)
                            for t, p in price_updates[product_name].items()
                            if t <= timestamp
                        ],
                        key=lambda x: x[0],
                        reverse=True,
                    )[0][1]
                    query(
                        "INSERT INTO cashflow ("
                        "user_id, product_id, timestamp, units_delta, execution_price, "
                        "user_money"
                        ") VALUES (%s, %s, %s, %s, %s, %s)",
                        (
                            user(user_name),
                            product(product_name),
                            timestamp,
                            units,
                            price,
                            units * price,  # user_money = execution_money (fees=0 in this case)
                        ),
                    )

    return fn
