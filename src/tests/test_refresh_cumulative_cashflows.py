from dataclasses import astuple, fields
from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID

import asyncpg
import pytest

from performance.iter_utils import cursor_to_async_iterator
from performance.models import Cashflow, CumulativeCashflow
from performance.refresh_utils import refresh_cumulative_cashflows
from tests.utils import parse_time


@pytest.mark.asyncio
async def test_refresh_cumulative_cashflows(
    make_data: Callable[[str], Awaitable[None]], connection: asyncpg.Connection
) -> None:
    # arrange
    await make_data("""
                    12:00, 12:10, 12:20
        AAPL:         100
        Alice/AAPL:      ,     1,     2
    """)

    # act
    async with connection.transaction():
        cashflow_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            ORDER BY "timestamp"
        """)
        cashflow_iter = cursor_to_async_iterator(cashflow_cursor, Cashflow)
        count = await refresh_cumulative_cashflows(connection, cashflow_iter)

    # assert
    assert count == 2
    cumulative_cashflow_rows: list[asyncpg.Record] = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        ORDER BY "timestamp"
    """)
    cumulative_cashflows = [CumulativeCashflow(*ccf) for ccf in cumulative_cashflow_rows]
    assert [ccf.units for ccf in cumulative_cashflows] == [
        Decimal("1.000000"),
        Decimal("3.000000"),
    ]


@pytest.mark.asyncio
async def test_refresh_only_a_few(
    make_data: Callable[[str], Awaitable[None]], connection: asyncpg.Connection
) -> None:
    # arrange
    await make_data("""
                    12:00, 12:10, 12:20
        AAPL:         100
        Alice/AAPL:      ,     1,     2
    """)

    # act
    async with connection.transaction():
        cashflow_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            ORDER BY "timestamp"
            LIMIT 1
        """)
        cashflow_iter = cursor_to_async_iterator(cashflow_cursor, Cashflow)
        count = await refresh_cumulative_cashflows(connection, cashflow_iter)

    # assert
    assert count == 1
    cumulative_cashflow_rows: list[asyncpg.Record] = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        ORDER BY "timestamp"
    """)
    cumulative_cashflows = [CumulativeCashflow(*ccf) for ccf in cumulative_cashflow_rows]
    assert [ccf.units for ccf in cumulative_cashflows] == [Decimal("1.000000")]


@pytest.mark.asyncio
async def test_with_seed_data(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
) -> None:
    # arrange
    await make_data("""
                    12:00, 12:10
        AAPL:         100
        Alice/AAPL:      ,     1
    """)
    async with connection.transaction():
        cashflow_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            ORDER BY "timestamp"
        """)
        cashflow_iter = cursor_to_async_iterator(cashflow_cursor, Cashflow)
        await refresh_cumulative_cashflows(connection, cashflow_iter)

    # Fetch the first cumulative cashflow for use as seed
    ccf_1_row = await connection.fetchrow(f"""
        SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        ORDER BY "timestamp"
        LIMIT 1
    """)
    assert ccf_1_row is not None
    ccf_1 = CumulativeCashflow(*ccf_1_row)

    cf = Cashflow(
        user_id=alice,
        product_id=aapl,
        timestamp=parse_time("12:20"),
        units_delta=Decimal("2.000000"),
        execution_price=Decimal("100.000000"),
        fees=Decimal("0.000000"),
    )
    await connection.execute(
        f"""
            INSERT INTO cashflow ({", ".join(f.name for f in fields(Cashflow))})
            VALUES ({", ".join(f"${i}" for i in range(1, len(fields(Cashflow)) + 1))})
        """,
        *astuple(cf),
    )

    # act
    async with connection.transaction():
        cashflow_cursor_2 = connection.cursor(
            f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            WHERE "timestamp" = $1
            ORDER BY "timestamp"
        """,
            parse_time("12:20"),
        )
        cashflow_iter_2 = cursor_to_async_iterator(cashflow_cursor_2, Cashflow)
        await refresh_cumulative_cashflows(
            connection, cashflow_iter_2, seed_cumulative_cashflows={alice: {aapl: ccf_1}}
        )

    # assert
    ccf_2_row = await connection.fetchrow(
        f"""
        SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        WHERE "timestamp" = $1
    """,
        parse_time("12:20"),
    )
    assert ccf_2_row is not None
    ccf_2 = CumulativeCashflow(*ccf_2_row)

    assert [ccf_1.units, ccf_2.units] == [Decimal("1.000000"), Decimal("3.000000")]
