from dataclasses import fields
from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID
import asyncpg
import pytest

from performance.granularities import GRANULARITIES
from performance.models import Cashflow, PriceUpdate, UserProductTimelineEntry, UserTimelineEntry
from performance.utils import (
    refresh_cumulative_cashflows,
    refresh_user_product_timeline,
    refresh_user_timeline,
)
from tests.utils import parse_time


@pytest.mark.asyncio
async def test_multi_product_creates_timeline_events(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
) -> None:
    # Create test data with multiple products
    await make_data("""
                11:59, 12:10, 12:20, 12:40, 12:50
    AAPL:         100,      ,   110,   120,
    GOOGL:        200,      ,      ,   210,
    Alice/AAPL:      ,    10,      ,      ,     8
    Alice/GOOGL:     ,     5,      ,      ,
    """)

    # Fetch cashflows
    sorted_cashflow_rows = await connection.fetch(
        f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
        """
    )
    sorted_cashflows = [Cashflow(*cf) for cf in sorted_cashflow_rows]

    # Refresh cumulative cashflows
    cumulative_cashflows = await refresh_cumulative_cashflows(connection, sorted_cashflows, None)

    # Refresh continuous aggregate for price updates
    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # Fetch price updates
    price_update_rows = await connection.fetch(
        f"""
        SELECT {", ".join(f.name for f in fields(PriceUpdate))}
        FROM price_update_{granularity.suffix}
        ORDER BY "timestamp"
        """
    )
    price_updates = [PriceUpdate(*pu) for pu in price_update_rows]

    # Sort events
    sorted_events = sorted(cumulative_cashflows + price_updates, key=lambda e: e.timestamp)

    # Refresh user_product_timeline
    user_product_timeline_entries = await refresh_user_product_timeline(
        connection, granularity, sorted_events, None, None
    )

    # Now refresh user_timeline
    user_timeline_entries = await refresh_user_timeline(
        connection, granularity, user_product_timeline_entries, {}
    )

    # Fetch results
    user_timeline_rows = await connection.fetch(
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
    # At 12:10: AAPL (10 units @ 100) + GOOGL (5 units @ 200)
    # net_investment = 1000 + 1000 = 2000
    # market_value = 1000 + 1000 = 2000
    # cost_basis = 10 * (1000/10) + 5 * (1000/5) = 1000 + 1000 = 2000
    #
    # At 12:30 (price update): AAPL @ 110, GOOGL @ 200
    # market_value = 10 * 110 + 5 * 200 = 1100 + 1000 = 2100
    # cost_basis unchanged = 2000
    #
    # At 12:45 (price update): AAPL @ 120, GOOGL @ 210
    # market_value = 10 * 120 + 5 * 210 = 1200 + 1050 = 2250
    #
    # At 12:50: Alice buys 8 more AAPL @ 120
    # net_investment = 2000 + 960 = 2960
    # market_value = 18 * 120 + 5 * 210 = 2160 + 1050 = 3210
    # cost_basis = 18 * ((1000 + 960)/18) + 5 * (1000/5) = 1960 + 1000 = 2960

    assert [
        (ut.timestamp, ut.net_investment, ut.market_value, ut.cost_basis)
        for ut in user_timeline_entries
    ] == [
        (parse_time("12:10"), Decimal("2000.000000"), Decimal("2000.000000"), Decimal("2000.000000")),
        (parse_time("12:30"), Decimal("2000.000000"), Decimal("2100.000000"), Decimal("2000.000000")),
        (parse_time("12:45"), Decimal("2000.000000"), Decimal("2250.000000"), Decimal("2000.000000")),
        (parse_time("12:50"), Decimal("2960.000000"), Decimal("3210.000000"), Decimal("2960.000000")),
    ]


@pytest.mark.asyncio
async def test_refresh_only_a_few(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
) -> None:
    # Create test data with multiple products
    await make_data("""
                11:59, 12:10, 12:20, 12:40, 12:50
    AAPL:         100,      ,   110,   120,
    GOOGL:        200,      ,      ,   210,
    Alice/AAPL:      ,    10,      ,      ,     8
    Alice/GOOGL:     ,     5,      ,      ,
    """)

    # Fetch cashflows
    sorted_cashflow_rows = await connection.fetch(
        f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
        """
    )
    sorted_cashflows = [Cashflow(*cf) for cf in sorted_cashflow_rows]

    # Refresh cumulative cashflows
    cumulative_cashflows = await refresh_cumulative_cashflows(connection, sorted_cashflows, None)

    # Only include events before 12:40
    cumulative_cashflows = [
        ccf for ccf in cumulative_cashflows if ccf.timestamp < parse_time("12:40")
    ]

    # Refresh continuous aggregate for price updates
    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # Fetch price updates before 12:40
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

    # Sort events
    sorted_events = sorted(cumulative_cashflows + price_updates, key=lambda e: e.timestamp)

    # Refresh user_product_timeline
    user_product_timeline_entries = await refresh_user_product_timeline(
        connection, granularity, sorted_events, None, None
    )

    # Refresh user_timeline with limited events
    user_timeline_entries = await refresh_user_timeline(
        connection, granularity, user_product_timeline_entries, {}
    )

    # Fetch results
    user_timeline_rows = await connection.fetch(
        f"""
        SELECT {", ".join(f.name for f in fields(UserTimelineEntry))}
        FROM user_timeline_cache_{granularity.suffix}
        WHERE user_id = $1
        ORDER BY "timestamp"
        """,
        alice,
    )
    user_timeline_entries = [UserTimelineEntry(*ut) for ut in user_timeline_rows]

    # Should only have the first two entries (up to 12:30)
    assert [
        (ut.timestamp, ut.net_investment, ut.market_value)
        for ut in user_timeline_entries
    ] == [
        (parse_time("12:10"), Decimal("2000.000000"), Decimal("2000.000000")),
        (parse_time("12:30"), Decimal("2000.000000"), Decimal("2100.000000")),
    ]


@pytest.mark.asyncio
async def test_with_seed_values(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
) -> None:
    # Create test data with multiple products
    await make_data("""
                11:59, 12:10, 12:20, 12:40, 12:50
    AAPL:         100,      ,   110,   120,
    GOOGL:        200,      ,      ,   210,
    Alice/AAPL:      ,    10,      ,      ,     8
    Alice/GOOGL:     ,     5,      ,      ,
    """)

    # Fetch cashflows
    sorted_cashflow_rows = await connection.fetch(
        f"""
        SELECT {", ".join(f.name for f in fields(Cashflow))}
        FROM cashflow
        ORDER BY "timestamp"
        """
    )
    sorted_cashflows = [Cashflow(*cf) for cf in sorted_cashflow_rows]

    # Refresh cumulative cashflows
    cumulative_cashflows = await refresh_cumulative_cashflows(connection, sorted_cashflows, None)

    # Refresh continuous aggregate for price updates
    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # Fetch price updates
    price_update_rows = await connection.fetch(
        f"""
        SELECT {", ".join(f.name for f in fields(PriceUpdate))}
        FROM price_update_{granularity.suffix}
        ORDER BY "timestamp"
        """
    )
    price_updates = [PriceUpdate(*pu) for pu in price_update_rows]

    # Sort events
    sorted_events = sorted(cumulative_cashflows + price_updates, key=lambda e: e.timestamp)

    # Refresh user_product_timeline
    user_product_timeline_entries = await refresh_user_product_timeline(
        connection, granularity, sorted_events, None, None
    )

    # Split into two phases
    # First phase: process events up to 12:30
    first_phase_entries = [
        upt for upt in user_product_timeline_entries if upt.timestamp <= parse_time("12:30")
    ]
    second_phase_entries = [
        upt for upt in user_product_timeline_entries if upt.timestamp > parse_time("12:30")
    ]

    # First refresh
    await refresh_user_timeline(connection, granularity, first_phase_entries, {})

    # Build seed from first phase results
    seed_user_product_timeline: dict[UUID, dict[UUID, UserProductTimelineEntry]] = {}
    for upt in first_phase_entries:
        seed_user_product_timeline.setdefault(upt.user_id, {})[upt.product_id] = upt

    # Second refresh with seed
    await refresh_user_timeline(connection, granularity, second_phase_entries, seed_user_product_timeline)

    # Fetch final results
    user_timeline_rows = await connection.fetch(
        f"""
        SELECT {", ".join(f.name for f in fields(UserTimelineEntry))}
        FROM user_timeline_cache_{granularity.suffix}
        WHERE user_id = $1
        ORDER BY "timestamp"
        """,
        alice,
    )
    user_timeline_entries = [UserTimelineEntry(*ut) for ut in user_timeline_rows]

    # Should have all four entries (same as first test)
    assert [
        (ut.timestamp, ut.net_investment, ut.market_value, ut.cost_basis)
        for ut in user_timeline_entries
    ] == [
        (parse_time("12:10"), Decimal("2000.000000"), Decimal("2000.000000"), Decimal("2000.000000")),
        (parse_time("12:30"), Decimal("2000.000000"), Decimal("2100.000000"), Decimal("2000.000000")),
        (parse_time("12:45"), Decimal("2000.000000"), Decimal("2250.000000"), Decimal("2000.000000")),
        (parse_time("12:50"), Decimal("2960.000000"), Decimal("3210.000000"), Decimal("2960.000000")),
    ]
