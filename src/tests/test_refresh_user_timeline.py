from dataclasses import fields
from decimal import Decimal
from typing import AsyncIterator, Awaitable, Callable
from uuid import UUID

import asyncpg
import pytest

from performance.granularities import GRANULARITIES
from performance.iter_utils import cursor_to_async_iterator, merge_sorted
from performance.models import (
    Cashflow,
    CumulativeCashflow,
    PriceUpdate,
    UserProductTimelineEntry,
    UserTimelineEntry,
)
from performance.refresh_utils import (
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
) -> None:
    # Create test data with multiple products
    await make_data("""
                    11:59, 12:10, 12:20, 12:40, 12:50
        AAPL:         100,      ,   110,   120,
        GOOGL:        200,      ,      ,   210,
        Alice/AAPL:      ,    10,      ,      ,     8
        Alice/GOOGL:     ,     5,      ,      ,
    """)
    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # Fetch cashflows
    # Refresh cumulative cashflows
    async with connection.transaction():
        cashflow_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            ORDER BY "timestamp"
        """)
        cashflow_iter = cursor_to_async_iterator(cashflow_cursor, Cashflow)
        cumulative_cashflows_iter = refresh_cumulative_cashflows(connection, cashflow_iter, {})

        # Fetch price updates
        price_update_cursor = connection.cursor(
            f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            ORDER BY "timestamp"
            """
        )
        price_update_iter = cursor_to_async_iterator(price_update_cursor, PriceUpdate)
        sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
            price_update_iter, cumulative_cashflows_iter
        )

        # Refresh user_product_timeline
        user_product_timeline_entries = await refresh_user_product_timeline(
            connection, granularity, sorted_events_iter, {}, {}
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
            (
                parse_time("12:10"),
                Decimal("2000.000000"),
                Decimal("2000.000000"),
                Decimal("2000.000000"),
            ),
            (
                parse_time("12:30"),
                Decimal("2000.000000"),
                Decimal("2100.000000"),
                Decimal("2000.000000"),
            ),
            (
                parse_time("12:45"),
                Decimal("2000.000000"),
                Decimal("2250.000000"),
                Decimal("2000.000000"),
            ),
            (
                parse_time("12:50"),
                Decimal("2960.000000"),
                Decimal("3210.000000"),
                Decimal("2960.000002"),
            ),
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

    # Refresh continuous aggregate for price updates
    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # Refresh cumulative cashflows
    async with connection.transaction():
        # Fetch cashflows
        cashflow_cursor = connection.cursor(
            f"""
                SELECT {", ".join(f.name for f in fields(Cashflow))}
                FROM cashflow
                WHERE "timestamp" < $1
                ORDER BY "timestamp"
            """,
            parse_time("12:39"),
        )
        cashflow_iter = cursor_to_async_iterator(cashflow_cursor, Cashflow)
        cumulative_cashflows_iter = refresh_cumulative_cashflows(connection, cashflow_iter, {})

        price_update_cursor = connection.cursor(
            f"""
                SELECT {", ".join(f.name for f in fields(PriceUpdate))}
                FROM price_update_{granularity.suffix}
                WHERE "timestamp" < $1
                ORDER BY "timestamp"
            """,
            parse_time("12:39"),
        )
        price_updates_iter = cursor_to_async_iterator(price_update_cursor, PriceUpdate)

        # Sort events
        sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
            price_updates_iter, cumulative_cashflows_iter
        )

        # Refresh user_product_timeline
        user_product_timeline_entries = await refresh_user_product_timeline(
            connection, granularity, sorted_events_iter, {}, {}
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
            (ut.timestamp, ut.net_investment, ut.market_value) for ut in user_timeline_entries
        ] == [
            (parse_time("12:10"), Decimal("2000.000000"), Decimal("2000.000000")),
            (parse_time("12:30"), Decimal("2000.000000"), Decimal("2100.000000")),
        ]


@pytest.mark.asyncio
async def test_with_seed_values(
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

    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    async with connection.transaction():
        cashflow_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            ORDER BY "timestamp"
        """)
        cashflow_iter = cursor_to_async_iterator(cashflow_cursor, Cashflow)
        cumulative_cashflows_iter = refresh_cumulative_cashflows(connection, cashflow_iter, {})

        price_update_cursor = connection.cursor(
            f"""
                SELECT {", ".join(f.name for f in fields(PriceUpdate))}
                FROM price_update_{granularity.suffix}
                ORDER BY "timestamp"
            """
        )
        price_updates_iter = cursor_to_async_iterator(price_update_cursor, PriceUpdate)

        # Sort events
        sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
            price_updates_iter, cumulative_cashflows_iter
        )

        # Refresh user_product_timeline
        user_product_timeline_entries = await refresh_user_product_timeline(
            connection, granularity, sorted_events_iter, {}, {}
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
        await refresh_user_timeline(
            connection, granularity, second_phase_entries, seed_user_product_timeline
        )

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
            (
                parse_time("12:10"),
                Decimal("2000.000000"),
                Decimal("2000.000000"),
                Decimal("2000.000000"),
            ),
            (
                parse_time("12:30"),
                Decimal("2000.000000"),
                Decimal("2100.000000"),
                Decimal("2000.000000"),
            ),
            (
                parse_time("12:45"),
                Decimal("2000.000000"),
                Decimal("2250.000000"),
                Decimal("2000.000000"),
            ),
            (
                parse_time("12:50"),
                Decimal("2960.000000"),
                Decimal("3210.000000"),
                Decimal("2960.000002"),
            ),
        ]
