"""Pytest configuration and fixtures for TWR tests."""

import uuid
from typing import Any, Callable, Generator, Union

import psycopg2
import pytest
from psycopg2.extensions import connection as Connection
from testcontainers.postgres import PostgresContainer

from tests.utils import map_to_model, parse_time
from twr.migrate import run_all_migrations
from twr.models import (
    Cashflow,
    CumulativeCashflow,
    PriceUpdate,
    UserProductTimelineBusinessEvent,
    UserTimelineBusinessEvent,
)


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start PostgreSQL container with TimescaleDB for tests."""
    with PostgresContainer("timescale/timescaledb:latest-pg16") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def db_connection(postgres_container: PostgresContainer) -> Generator[Connection, None, None]:
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
def truncate(db_connection: Connection) -> None:
    """Truncate tables before each test."""

    with db_connection.cursor() as cursor:
        # Truncate tables in dependency order (CASCADE handles foreign keys)
        cursor.execute("TRUNCATE TABLE cashflow, price_update CASCADE")


@pytest.fixture
def query(
    db_connection: Connection, truncate: None
) -> Callable[
    [str, tuple[Any, ...] | dict[str, Any] | None],
    list[
        (
            PriceUpdate
            | Cashflow
            | CumulativeCashflow
            | UserProductTimelineBusinessEvent
            | UserTimelineBusinessEvent
            | dict[str, Any]
        )
    ],
]:
    """Execute query and return results as list of dicts (or empty list for statements without
    RETURNING)
    """

    def fn(
        q: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[
        (
            PriceUpdate
            | Cashflow
            | CumulativeCashflow
            | UserProductTimelineBusinessEvent
            | UserTimelineBusinessEvent
            | dict[str, Any]
        )
    ]:
        with db_connection.cursor() as cursor:
            cursor.execute(q, params or ())
            # cursor.description is None for statements without RETURNING
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [map_to_model(dict(zip(columns, row))) for row in cursor.fetchall()]

    return fn


@pytest.fixture
def user() -> Callable[[str], str]:
    """Generate consistent UUIDs for user names (no user table anymore)."""

    def fn(seed: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"user:{seed}"))

    return fn


@pytest.fixture
def product() -> Callable[[str], str]:
    """Generate consistent UUIDs for product names (no product table anymore)."""

    def fn(seed: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"product:{seed}"))

    return fn


@pytest.fixture
def make_data(
    product: Callable[[str], str],
    user: Callable[[str], str],
    insert: Callable[[Union[PriceUpdate, Cashflow]], None],
) -> Callable[[str], None]:
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

    def fn(text: str) -> None:
        price_updates = {}
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        timestamps = [parse_time(t.strip()) for t in lines[0].split(",")]
        for line in lines[1:]:
            identifier, values = [t.strip() for t in line.split(":") if t.strip()]
            match [w.strip() for w in identifier.split("/")]:
                case [user_name, product_name]:
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
                        insert(
                            Cashflow(
                                user(user_name),
                                product(product_name),
                                timestamp,
                                units,
                                price,
                                units * price,
                            )
                        )
                case [product_name]:
                    prices_str = [p.strip() for p in values.split(",")]
                    for timestamp, price_str in zip(timestamps, prices_str):
                        try:
                            price = float(price_str)
                        except ValueError:
                            continue
                        price_updates.setdefault(product_name, {})[timestamp] = price
                        insert(PriceUpdate(product(product_name), timestamp, price))

    return fn


@pytest.fixture
def insert(
    db_connection: Connection, truncate: None
) -> Callable[[Union[PriceUpdate, Cashflow]], None]:
    """Insert data into the database using raw SQL (for more complex scenarios than make_data)."""

    def fn(obj: Union[PriceUpdate, Cashflow]) -> None:
        if isinstance(obj, PriceUpdate):
            with db_connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO price_update (product_id, timestamp, price) VALUES (%s, %s, %s)",
                    (obj.product_id, obj.timestamp, obj.price),
                )
        elif isinstance(obj, Cashflow):
            with db_connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO cashflow (user_id, product_id, timestamp, units_delta,
                                             execution_price, user_money)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        obj.user_id,
                        obj.product_id,
                        obj.timestamp,
                        obj.units_delta,
                        obj.execution_price,
                        obj.user_money,
                    ),
                )
        else:
            raise ValueError(f"Unsupported object type: {type(obj)}")

    return fn
