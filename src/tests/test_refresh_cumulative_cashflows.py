from dataclasses import astuple, fields
from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID
import asyncpg
import pytest

from performance.models import Cashflow, CumulativeCashflow
from performance.utils import refresh_cumulative_cashflows
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
    cashflow_rows: list[asyncpg.Record] = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
    """)
    cashflows = [Cashflow(*cf) for cf in cashflow_rows]

    # act
    await refresh_cumulative_cashflows(connection, cashflows)

    # assert
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
    cashflow_rows: list[asyncpg.Record] = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
        LIMIT 1
    """)
    cashflows = [Cashflow(*cf) for cf in cashflow_rows]

    # act
    await refresh_cumulative_cashflows(connection, cashflows)

    # assert
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
    cashflow_rows: list[asyncpg.Record] = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
    """)
    cashflows = [Cashflow(*cf) for cf in cashflow_rows]
    (ccf_1,) = await refresh_cumulative_cashflows(connection, cashflows)

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
    (ccf_2,) = await refresh_cumulative_cashflows(
        connection,
        [cf],
        seed_cumulative_cashflows={alice: {aapl: ccf_1}},
    )

    assert [ccf_1.units, ccf_2.units] == [Decimal("1.000000"), Decimal("3.000000")]
