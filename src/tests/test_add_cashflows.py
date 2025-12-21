from dataclasses import fields
from decimal import Decimal
from typing import Awaitable, Callable, cast
from unittest import mock
from uuid import UUID
import asyncpg
import pytest

from performance.granularities import GRANULARITIES
from performance.interface import add_cashflows
from performance.models import (
    Cashflow,
    CumulativeCashflow,
    PriceUpdate,
    UserProductTimelineEntry,
    UserTimelineEntry,
)
from performance.utils import (
    refresh_cumulative_cashflows,
    refresh_user_product_timeline,
    refresh_user_timeline,
)
from tests.utils import parse_time


@pytest.mark.asyncio
async def test_add_cashflow(connection: asyncpg.Connection, alice: UUID, aapl: UUID) -> None:
    # act
    await add_cashflows(
        connection,
        Cashflow(
            user_id=alice,
            product_id=aapl,
            timestamp=parse_time("12:00"),
            units_delta=Decimal("10.000000"),
            execution_price=Decimal("1000.00000"),
            fees=Decimal("0.000000"),
        ),
    )

    # assert
    cf = cast(asyncpg.Record, await connection.fetchrow("SELECT * FROM cashflow"))
    assert dict(cf) == {
        "execution_money": Decimal("10000.000000"),
        "execution_price": Decimal("1000.000000"),
        "fees": Decimal("0.000000"),
        "id": mock.ANY,
        "product_id": aapl,
        "timestamp": parse_time("12:00"),
        "units_delta": Decimal("10.000000"),
        "user_id": alice,
        "user_money": Decimal("10000.000000"),
    }


@pytest.mark.asyncio
async def test_invalidate_and_reresh(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
) -> None:
    # arrange
    # Add a cashflow for Bob to maintain the watermark
    await make_data("""
                    11:59, 12:10, 12:20, 12:40, 12:50, 13:00
        AAPL:         100,      ,   110,   120
        GOOGL:        200,      ,      ,   210
        Alice/AAPL:      ,    10,      ,      ,     8
        Alice/GOOGL:     ,     5,      ,      ,
        Bob/AAPL:        ,      ,      ,      ,      ,     1
    """)
    cashflow_rows: list[asyncpg.Record] = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
    """)
    cashflows = [Cashflow(*cf) for cf in cashflow_rows]
    cumulative_cashflows = await refresh_cumulative_cashflows(connection, cashflows)

    # Populate user_product_timeline_cache
    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )
    price_update_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(PriceUpdate))}
        FROM price_update_{granularity.suffix}
        ORDER BY "timestamp"
    """)
    price_updates = [PriceUpdate(*pu) for pu in price_update_rows]
    sorted_events = sorted(cumulative_cashflows + price_updates, key=lambda e: e.timestamp)
    await refresh_user_product_timeline(connection, granularity, sorted_events)

    # Also populate user_timeline_cache
    upt_rows = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(UserProductTimelineEntry))}
            FROM user_product_timeline_cache_{granularity.suffix}
            ORDER BY "timestamp"
        """
    )
    upt_entries = [UserProductTimelineEntry(*upt) for upt in upt_rows]
    await refresh_user_timeline(connection, granularity, upt_entries, {})

    # act
    await add_cashflows(
        connection,
        Cashflow(
            alice,
            aapl,
            parse_time("12:16"),
            units_delta=Decimal("-4.000000"),
            execution_price=Decimal("100.000000"),
            fees=Decimal("0.000000"),
        ),
    )
    # Now the data looks like this:
    # await make_data("""
    #                 11:59, 12:10, 12:16, 12:20, 12:40, 12:50, 13:00
    #     AAPL:         100,      ,      ,   110,   120
    #     Alice/AAPL:      ,    10,    -4,      ,      ,     8
    #     Bob/AAPL:        ,      ,      ,      ,      ,      ,     1
    # """)

    # assert
    cumulative_cashflow_rows: list[asyncpg.Record] = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
            FROM cumulative_cashflow_cache
            WHERE user_id = $1 AND product_id = $2
            ORDER BY "timestamp"
        """,
        alice,
        aapl,
    )
    cumulative_cashflows = [CumulativeCashflow(*ccf) for ccf in cumulative_cashflow_rows]
    assert [(ccf.timestamp, ccf.units_held) for ccf in cumulative_cashflows] == [
        (parse_time("12:10"), Decimal("10.000000")),
        (parse_time("12:16"), Decimal("6.000000")),
        (parse_time("12:50"), Decimal("14.000000")),
    ]

    # assert user_product_timeline_cache was also invalidated and repaired
    user_product_timeline_rows: list[asyncpg.Record] = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(UserProductTimelineEntry))}
            FROM user_product_timeline_cache_{granularity.suffix}
            WHERE user_id = $1 AND product_id = $2
            ORDER BY "timestamp"
        """,
        alice,
        aapl,
    )
    user_product_timeline_entries = [
        UserProductTimelineEntry(*upt) for upt in user_product_timeline_rows
    ]
    assert [
        (upt.timestamp, upt.units_held, upt.market_value) for upt in user_product_timeline_entries
    ] == [
        (parse_time("12:10"), Decimal("10.000000"), Decimal("1000.000000")),
        (parse_time("12:16"), Decimal("6.000000"), Decimal("600.000000")),
        (parse_time("12:30"), Decimal("6.000000"), Decimal("660.000000")),
        (parse_time("12:45"), Decimal("6.000000"), Decimal("720.000000")),
        (parse_time("12:50"), Decimal("14.000000"), Decimal("1680.000000")),
    ]

    # assert user_timeline_cache was also invalidated and repaired
    user_timeline_rows: list[asyncpg.Record] = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(UserTimelineEntry))}
            FROM user_timeline_cache_{granularity.suffix}
            WHERE user_id = $1
            ORDER BY "timestamp"
        """,
        alice,
    )
    user_timeline_entries = [UserTimelineEntry(*ut) for ut in user_timeline_rows]

    # Verify aggregation across AAPL and GOOGL
    assert [
        (ut.timestamp, ut.net_investment, ut.market_value) for ut in user_timeline_entries
    ] == [
        (parse_time("12:10"), Decimal("2000.000000"), Decimal("2000.000000")),  # 10×100 + 5×200
        (parse_time("12:16"), Decimal("1600.000000"), Decimal("1600.000000")),  # Out-of-order insert
        (parse_time("12:30"), Decimal("1600.000000"), Decimal("1660.000000")),  # Price updates
        (parse_time("12:45"), Decimal("1600.000000"), Decimal("1770.000000")),  # AAPL:120, GOOGL:210
        (parse_time("12:50"), Decimal("2560.000000"), Decimal("2730.000000")),  # Alice buys more AAPL
    ]
