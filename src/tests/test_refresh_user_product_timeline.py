from dataclasses import fields
from decimal import Decimal
from typing import AsyncIterator, Awaitable, Callable

import asyncpg
import pytest

from performance.granularities import GRANULARITIES
from performance.iter_utils import cursor_to_async_iterator, merge_sorted
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
        price_update_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            ORDER BY "timestamp"
        """)
        price_updates_iter = cursor_to_async_iterator(price_update_cursor, PriceUpdate)
        sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
            price_updates_iter, cumulative_cashflows_iter
        )

        # act
        async for _ in refresh_user_product_timeline(
            connection, granularity, sorted_events_iter, {}, {}
        ):
            pass

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

    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    async with connection.transaction():
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
        sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
            price_updates_iter, cumulative_cashflows_iter
        )

        # act
        async for _ in refresh_user_product_timeline(
            connection, granularity, sorted_events_iter, {}, {}
        ):
            pass

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
    # With 15min granularity: 11:59 buckets to 12:00, 12:00 buckets to 12:15
    await make_data("""
                    11:59, 12:00, 12:15, 12:30
        AAPL:         100,   150,      ,   120
        Alice/AAPL:      ,      ,    10
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

        price_updates_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            ORDER BY "timestamp"
        """)
        price_updates_iter = cursor_to_async_iterator(price_updates_cursor, PriceUpdate)

        # Mix them together - price updates come first when timestamps are equal
        sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
            price_updates_iter, cumulative_cashflows_iter
        )

        # Now test the full refresh - the market value should use the new price (150)
        async for _ in refresh_user_product_timeline(
            connection, granularity, sorted_events_iter, {}, {}
        ):
            pass

        user_product_timeline_rows = await connection.fetch(
            f"""
                SELECT {", ".join(f.name for f in fields(UserProductTimelineEntry))}
                FROM user_product_timeline_cache_{granularity.suffix}
                WHERE "timestamp" = $1
                ORDER BY "timestamp"
            """,
            parse_time("12:15"),
        )
        user_product_timeline_entries = [
            UserProductTimelineEntry(*upt) for upt in user_product_timeline_rows
        ]

        # Market value should be 10 units * 150 price = 1500
        # (not 10 * 100 = 1000, which would happen if cashflow was processed first)
        assert len(user_product_timeline_entries) == 1
        assert user_product_timeline_entries[0].timestamp == parse_time("12:15")
        assert user_product_timeline_entries[0].units == Decimal("10.000000")
        assert user_product_timeline_entries[0].market_value == Decimal("1500.000000")


@pytest.mark.asyncio
async def test_with_seed_values(
    make_data: Callable[[str], Awaitable[None]], connection: asyncpg.Connection
) -> None:
    # arrange
    await make_data("""
                    11:59, 12:10, 12:20, 12:40, 12:50
        AAPL:         100,      ,   110,   120,
        Alice/AAPL:      ,    10,      ,      ,     8
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

        price_update_cursor = connection.cursor(f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            ORDER BY "timestamp"
        """)
        price_updates_iter = cursor_to_async_iterator(price_update_cursor, PriceUpdate)
        sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
            price_updates_iter, cumulative_cashflows_iter
        )

        # act
        async for _ in refresh_user_product_timeline(
            connection, granularity, sorted_events_iter, {}, {}
        ):
            pass

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
