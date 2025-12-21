from dataclasses import fields
from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID
import asyncpg
import pytest

from performance.granularities import GRANULARITIES
from performance.models import Cashflow, PriceUpdate, UserProductTimelineEntry
from performance.utils import refresh_cumulative_cashflows, refresh_user_product_timeline
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
    sorted_cashlow_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
    """)
    sorted_cashlows = [Cashflow(*cf) for cf in sorted_cashlow_rows]
    cumulative_cashflows = await refresh_cumulative_cashflows(connection, sorted_cashlows)
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
    sorted_cashlow_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
    """)
    sorted_cashlows = [Cashflow(*cf) for cf in sorted_cashlow_rows]
    cumulative_cashflows = await refresh_cumulative_cashflows(connection, sorted_cashlows)
    cumulative_cashflows = [
        ccf for ccf in cumulative_cashflows if ccf.timestamp < parse_time("12:40")
    ]
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
    sorted_events = sorted(cumulative_cashflows + price_updates, key=lambda e: e.timestamp)

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
    sorted_cashlow_rows = await connection.fetch(f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
    """)
    sorted_cashlows = [Cashflow(*cf) for cf in sorted_cashlow_rows]
    (ccf1, ccf2) = await refresh_cumulative_cashflows(connection, sorted_cashlows)
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
