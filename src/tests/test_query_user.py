from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID

import asyncpg
import pytest

from performance.granularities import GRANULARITIES
from performance.interface import get_user_timeline, refresh
from performance.models import UserTimelineEntry
from tests.utils import parse_time, user_timeline_eq


@pytest.mark.asyncio
async def test_get_user_timeline_without_refresh(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
    googl: UUID,
) -> None:
    """Test query with multiple products, before refresh"""
    # arrange
    await make_data("""
                    11:59, 12:10
        AAPL:         100,
        GOOGL:        200,
        Alice/AAPL:      ,    10
        Alice/GOOGL:     ,     5
    """)

    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # act - query WITHOUT refresh
    timeline = await get_user_timeline(connection, alice, granularity)

    # assert
    assert len(timeline) == 1
    assert user_timeline_eq(
        timeline[0],
        UserTimelineEntry(
            user_id=alice,
            timestamp=parse_time("12:10"),
            net_investment=Decimal("2000"),
            market_value=Decimal("2000"),
            deposits=Decimal("2000"),
            buy_cost=Decimal("2000"),
            cost_basis=Decimal("2000.000000"),
        ),
    )


@pytest.mark.asyncio
async def test_get_user_timeline_with_refresh(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
    googl: UUID,
) -> None:
    """Test query with multiple products, after refresh"""
    # arrange
    await make_data("""
                    11:59, 12:10
        AAPL:         100,
        GOOGL:        200,
        Alice/AAPL:      ,    10
        Alice/GOOGL:     ,     5
    """)

    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # Refresh to populate cache
    await refresh(connection)

    # act - query WITH refresh
    timeline = await get_user_timeline(connection, alice, granularity)

    # assert
    assert len(timeline) == 1
    assert user_timeline_eq(
        timeline[0],
        UserTimelineEntry(
            user_id=alice,
            timestamp=parse_time("12:10"),
            net_investment=Decimal("2000"),
            market_value=Decimal("2000"),
            deposits=Decimal("2000"),
            buy_cost=Decimal("2000"),
            cost_basis=Decimal("2000"),
        ),
    )


@pytest.mark.asyncio
async def test_get_user_timeline_cached_and_fresh(
    make_data: Callable[[str], Awaitable[None]], connection: asyncpg.Connection, alice: UUID
) -> None:
    """Test query combines cached and fresh data"""
    # arrange - create initial data and refresh (cached)
    await make_data("""
                    11:59, 12:10
        AAPL:         100,
        GOOGL:        200,
        Alice/AAPL:      ,    10
        Alice/GOOGL:     ,     5
    """)

    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )
    await refresh(connection)

    # arrange - add fresh data WITHOUT refresh
    await make_data("""
                    12:14, 12:20
        AAPL:         110,
        Alice/AAPL:      ,     5
    """)
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # act - query should combine cached + fresh
    timeline = await get_user_timeline(connection, alice, granularity)

    # assert
    expected = [
        # Cached entry at 12:10
        UserTimelineEntry(
            user_id=alice,
            timestamp=parse_time("12:10"),
            net_investment=Decimal("2000"),
            market_value=Decimal("2000"),
            deposits=Decimal("2000"),
            buy_cost=Decimal("2000"),
            cost_basis=Decimal("2000"),
        ),
        # Fresh entry from price update (12:14 bucketed to 12:15)
        UserTimelineEntry(
            user_id=alice,
            timestamp=parse_time("12:15"),
            net_investment=Decimal("2000"),
            market_value=Decimal("2100"),
            deposits=Decimal("2000"),
            buy_cost=Decimal("2000"),
            cost_basis=Decimal("2000"),
        ),
        # Fresh entry from cashflow (12:20)
        # 15 AAPL @ $110 + 5 GOOGL @ $200 = $2650
        UserTimelineEntry(
            user_id=alice,
            timestamp=parse_time("12:20"),
            net_investment=Decimal("2550"),
            market_value=Decimal("2650"),
            deposits=Decimal("2550"),
            buy_cost=Decimal("2550"),
            cost_basis=Decimal("2550"),
        ),
    ]
    assert len(timeline) == len(expected)
    assert all(user_timeline_eq(act, exp) for act, exp in zip(timeline, expected))
