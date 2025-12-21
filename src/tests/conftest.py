from typing import AsyncGenerator, Awaitable, Callable, cast
from uuid import UUID

import asyncpg
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from performance.granularities import GRANULARITIES
from performance.migrate import run_all_migrations
from tests.utils import parse_time


@pytest_asyncio.fixture(scope="session")
async def postgres() -> AsyncGenerator[PostgresContainer, None]:
    with PostgresContainer("timescale/timescaledb:latest-pg16", driver=None) as postgres:
        conn: asyncpg.Connection = await asyncpg.connect(postgres.get_connection_url())
        try:
            await run_all_migrations(conn)
        finally:
            await conn.close()
        yield postgres


@pytest_asyncio.fixture
async def connection(postgres) -> AsyncGenerator[asyncpg.Connection, None]:
    conn: asyncpg.Connection = await asyncpg.connect(postgres.get_connection_url())
    try:
        yield conn
        await conn.execute(f"""
            TRUNCATE TABLE
                "user", product, cashflow, price_update, cumulative_cashflow_cache,
                {", ".join(f"user_product_timeline_cache_{g.suffix}" for g in GRANULARITIES)},
                {", ".join(f"user_timeline_cache_{g.suffix}" for g in GRANULARITIES)}
            CASCADE
        """)
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def user(connection: asyncpg.Connection) -> Callable[[str], Awaitable[UUID]]:
    users: dict[str, UUID] = {}

    async def fn(seed: str) -> UUID:
        if seed not in users:
            user_id = await connection.fetchval(
                'INSERT INTO "user" (name) VALUES ($1) RETURNING id', seed
            )
            users[seed] = cast(UUID, user_id)
        return users[seed]

    return fn


@pytest_asyncio.fixture
async def alice(user: Callable[[str], Awaitable[UUID]]) -> UUID:
    return await user("Alice")


@pytest_asyncio.fixture
async def product(connection: asyncpg.Connection) -> Callable[[str], Awaitable[UUID]]:
    products: dict[str, UUID] = {}

    async def fn(seed: str) -> UUID:
        if seed not in products:
            product_id = await connection.fetchval(
                "INSERT INTO product (name) VALUES ($1) RETURNING id", seed
            )
            products[seed] = cast(UUID, product_id)
        return products[seed]

    return fn


@pytest_asyncio.fixture
async def aapl(product: Callable[[str], Awaitable[UUID]]) -> UUID:
    return await product("AAPL")


@pytest_asyncio.fixture
async def make_data(
    connection: asyncpg.Connection,
    product: Callable[[str], Awaitable[str]],
    user: Callable[[str], Awaitable[str]],
) -> Callable[[str], Awaitable[None]]:
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

    async def fn(text):
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
                    await connection.execute(
                        "INSERT INTO price_update (product_id, timestamp, price) VALUES ($1, $2, $3)",
                        await product(product_name),
                        timestamp,
                        price,
                    )
            else:
                units_str = [u.strip() for u in values.split(",")]
                for timestamp, unit_str in zip(timestamps, units_str):
                    try:
                        units = float(unit_str)
                    except ValueError:
                        continue
                    price = sorted(
                        [(t, p) for t, p in price_updates[product_name].items() if t <= timestamp],
                        key=lambda x: x[0],
                        reverse=True,
                    )[0][1]
                    await connection.execute(
                        "INSERT INTO cashflow ("
                        "user_id, product_id, timestamp, units_delta, execution_price, "
                        "execution_money, user_money, fees"
                        ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                        await user(user_name),
                        await product(product_name),
                        timestamp,
                        units,
                        price,
                        units * price,
                        units * price,
                        0,
                    )

    return fn
