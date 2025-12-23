from dataclasses import fields
from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID

import asyncpg
import pytest

from performance.granularities import GRANULARITIES
from performance.models import Cashflow, CumulativeCashflow, PriceUpdate, UserProductTimelineEntry
from performance.refresh_utils import refresh_cumulative_cashflows, refresh_user_product_timeline
from tests.utils import parse_time


@pytest.mark.asyncio
async def test_inbetween_price_updates_create_timeline_events(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
) -> None:
    # arrange
    # We use 11:59 so that the bucketed timestamp goes to 12:00
    await make_data("""
                    11:59, 12:10, 12:20, 12:40, 12:50
        AAPL:         100,      ,   110,   120,
        Alice/AAPL:      ,    10,      ,      ,     8
    """)
    async with connection.transaction():
        cashflow_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            ORDER BY "timestamp"
        """)
        await refresh_cumulative_cashflows(connection, cashflow_cursor)

    # Query back the cumulative cashflows
    cumulative_cashflow_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        ORDER BY "timestamp"
    """)
    cumulative_cashflows = [CumulativeCashflow(*ccf) for ccf in cumulative_cashflow_rows]
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
    sorted_events: list[CumulativeCashflow | PriceUpdate] = sorted(
        cumulative_cashflows + price_updates,
        key=lambda e: (e.timestamp, isinstance(e, CumulativeCashflow)),
    )

    # act
    await refresh_user_product_timeline(connection, granularity, sorted_events)

    # assert
    user_product_timeline_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(UserProductTimelineEntry))}
        FROM user_product_timeline_cache_{granularity.suffix}
        ORDER BY "timestamp"
    """)
    user_product_timeline_entries = [
        UserProductTimelineEntry(*upt) for upt in user_product_timeline_rows
    ]
    assert [(upt.timestamp, upt.market_value) for upt in user_product_timeline_entries] == [
        (parse_time("12:10"), Decimal("1000.000000")),
        (parse_time("12:30"), Decimal("1100.000000")),
        (parse_time("12:45"), Decimal("1200.000000")),
        (parse_time("12:50"), Decimal("2160.000000")),  # 18 * 120
    ]


@pytest.mark.asyncio
async def test_refresh_only_a_few(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
) -> None:
    # arrange
    await make_data("""
                    11:59, 12:10, 12:20, 12:40, 12:50
        AAPL:         100,      ,   110,   120,
        Alice/AAPL:      ,    10,      ,      ,     8
    """)
    async with connection.transaction():
        cashflow_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            ORDER BY "timestamp"
        """)
        await refresh_cumulative_cashflows(connection, cashflow_cursor)

    # Query back the cumulative cashflows (filtered by timestamp)
    cumulative_cashflow_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        WHERE "timestamp" < $1
        ORDER BY "timestamp"
    """, parse_time("12:40"))
    cumulative_cashflows = [CumulativeCashflow(*ccf) for ccf in cumulative_cashflow_rows]
    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )
    price_update_rows = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            WHERE "timestamp" < $1
            ORDER BY "timestamp"
        """,
        parse_time("12:40"),
    )
    price_updates = [PriceUpdate(*pu) for pu in price_update_rows]
    sorted_events: list[CumulativeCashflow | PriceUpdate] = sorted(
        cumulative_cashflows + price_updates,
        key=lambda e: (e.timestamp, isinstance(e, CumulativeCashflow)),
    )

    # act
    await refresh_user_product_timeline(connection, granularity, sorted_events)

    # assert
    user_product_timeline_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(UserProductTimelineEntry))}
        FROM user_product_timeline_cache_{granularity.suffix}
        ORDER BY "timestamp"
    """)
    user_product_timeline_entries = [
        UserProductTimelineEntry(*upt) for upt in user_product_timeline_rows
    ]
    assert [(upt.timestamp, upt.market_value) for upt in user_product_timeline_entries] == [
        (parse_time("12:10"), Decimal("1000.000000")),
        (parse_time("12:30"), Decimal("1100.000000")),
        # (parse_time("12:45"), Decimal("1200.000000")),
        # (parse_time("12:50"), Decimal("2160.000000")),  # 18 * 120
    ]


@pytest.mark.asyncio
async def test_same_timestamp_price_update_before_cashflow(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
) -> None:
    # Test that when price update and cashflow have same timestamp,
    # price update is processed first
    await make_data("""
                    11:59, 12:10, 12:20
        AAPL:         100,      ,   120
        Alice/AAPL:      ,    10
    """)
    async with connection.transaction():
        cashflow_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            ORDER BY "timestamp"
        """)
        await refresh_cumulative_cashflows(connection, cashflow_cursor)

    # Query back the cumulative cashflows
    cumulative_cashflow_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        ORDER BY "timestamp"
    """)
    cumulative_cashflows = [CumulativeCashflow(*ccf) for ccf in cumulative_cashflow_rows]

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

    # Manually create a price update with the same timestamp as the cashflow
    # to test the sorting behavior
    ccf = cumulative_cashflows[0]
    pu_same_time = PriceUpdate(
        product_id=ccf.product_id,
        timestamp=ccf.timestamp,  # Same timestamp!
        price=Decimal("150.000000"),
    )

    # Mix them together - cashflow comes first in the list
    events: list[CumulativeCashflow | PriceUpdate] = [ccf, pu_same_time]
    sorted_events = sorted(
        events,
        key=lambda e: (e.timestamp, isinstance(e, CumulativeCashflow)),
    )

    # Verify that price update comes before cashflow in sorted order
    assert isinstance(sorted_events[0], PriceUpdate)
    assert isinstance(sorted_events[1], CumulativeCashflow)
    assert sorted_events[0].timestamp == sorted_events[1].timestamp

    # Now test the full refresh - the market value should use the new price (150)
    await refresh_user_product_timeline(connection, granularity, sorted_events)

    user_product_timeline_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(UserProductTimelineEntry))}
        FROM user_product_timeline_cache_{granularity.suffix}
        ORDER BY "timestamp"
    """)
    user_product_timeline_entries = [
        UserProductTimelineEntry(*upt) for upt in user_product_timeline_rows
    ]

    # Market value should be 10 units * 150 price = 1500
    # (not 10 * 100 = 1000, which would happen if cashflow was processed first)
    assert len(user_product_timeline_entries) == 1
    assert user_product_timeline_entries[0].timestamp == ccf.timestamp
    assert user_product_timeline_entries[0].units == Decimal("10.000000")
    assert user_product_timeline_entries[0].market_value == Decimal("1500.000000")


@pytest.mark.asyncio
async def test_with_seed_values(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
) -> None:
    # arrange
    await make_data("""
                    11:59, 12:10, 12:20, 12:40, 12:50
        AAPL:         100,      ,   110,   120,
        Alice/AAPL:      ,    10,      ,      ,     8
    """)
    async with connection.transaction():
        cashflow_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            ORDER BY "timestamp"
        """)
        await refresh_cumulative_cashflows(connection, cashflow_cursor)

    # Query back the cumulative cashflows
    cumulative_cashflow_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        ORDER BY "timestamp"
    """)
    (ccf1, ccf2) = [CumulativeCashflow(*ccf) for ccf in cumulative_cashflow_rows]
    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )
    price_update_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(PriceUpdate))}
        FROM price_update_{granularity.suffix}
        ORDER BY "timestamp"
    """)
    (pu1, pu2, pu3) = [PriceUpdate(*pu) for pu in price_update_rows]
    # pu1: AAPL at 12:00 for 100
    # ccf1: Alice/AAPL at 12:10 for 10 units
    # pu2: AAPL at 12:30 for 110
    # pu3: AAPL at 12:45 for 120
    # ccf2: Alice/AAPL at 12:50 for 18 units

    await refresh_user_product_timeline(connection, granularity, [pu1, ccf1, pu2])

    # act
    await refresh_user_product_timeline(
        connection, granularity, [pu3, ccf2], {aapl: {alice: ccf1}}, {aapl: pu2}
    )

    # assert
    user_product_timeline_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(UserProductTimelineEntry))}
        FROM user_product_timeline_cache_{granularity.suffix}
        ORDER BY "timestamp"
    """)
    user_product_timeline_entries = [
        UserProductTimelineEntry(*upt) for upt in user_product_timeline_rows
    ]
    assert [(upt.timestamp, upt.market_value) for upt in user_product_timeline_entries] == [
        (parse_time("12:10"), Decimal("1000.000000")),
        (parse_time("12:30"), Decimal("1100.000000")),
        (parse_time("12:45"), Decimal("1200.000000")),
        (parse_time("12:50"), Decimal("2160.000000")),  # 18 * 120
    ]
