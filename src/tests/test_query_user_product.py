from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID
import asyncpg
import pytest

from performance.granularities import GRANULARITIES
from performance.interface import get_user_product_timeline, refresh
from performance.models import UserProductTimelineEntry
from tests.utils import parse_time


@pytest.mark.asyncio
async def test_get_user_product_timeline_without_refresh(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
) -> None:
    """Test query with one price update and one cashflow, before refresh"""
    # arrange
    await make_data("""
                    11:59, 12:10
        AAPL:         100,
        Alice/AAPL:      ,    10
    """)

    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # act - query WITHOUT refresh
    timeline = await get_user_product_timeline(connection, alice, aapl, granularity)

    # assert
    assert timeline == [
        UserProductTimelineEntry(
            user_id=alice,
            product_id=aapl,
            timestamp=parse_time("12:10"),
            units=Decimal("10.000000"),
            net_investment=Decimal("1000.000000"),
            deposits=Decimal("1000.000000"),
            withdrawals=Decimal("0.000000"),
            fees=Decimal("0.000000"),
            buy_units=Decimal("10.000000"),
            sell_units=Decimal("0.000000"),
            buy_cost=Decimal("1000.000000"),
            sell_proceeds=Decimal("0.000000"),
            market_value=Decimal("1000.000000"),
        )
    ]


@pytest.mark.asyncio
async def test_get_user_product_timeline_with_refresh(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
) -> None:
    """Test query with one price update and one cashflow, after refresh"""
    # arrange
    await make_data("""
                    11:59, 12:10
        AAPL:         100,
        Alice/AAPL:      ,    10
    """)

    granularity = GRANULARITIES[0]
    await connection.execute(
        f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
    )

    # Refresh to populate cache
    await refresh(connection)

    # act - query WITH refresh
    timeline = await get_user_product_timeline(connection, alice, aapl, granularity)

    # assert
    assert timeline == [
        UserProductTimelineEntry(
            user_id=alice,
            product_id=aapl,
            timestamp=parse_time("12:10"),
            units=Decimal("10.000000"),
            net_investment=Decimal("1000.000000"),
            deposits=Decimal("1000.000000"),
            withdrawals=Decimal("0.000000"),
            fees=Decimal("0.000000"),
            buy_units=Decimal("10.000000"),
            sell_units=Decimal("0.000000"),
            buy_cost=Decimal("1000.000000"),
            sell_proceeds=Decimal("0.000000"),
            market_value=Decimal("1000.000000"),
        )
    ]


@pytest.mark.asyncio
async def test_get_user_product_timeline_cached_and_fresh(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
    aapl: UUID,
) -> None:
    """Test query combines cached and fresh data"""
    # arrange - create initial data and refresh (cached)
    await make_data("""
                    11:59, 12:10
        AAPL:         100,
        Alice/AAPL:      ,    10
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
    timeline = await get_user_product_timeline(connection, alice, aapl, granularity)

    # assert
    assert timeline == [
        # Cached entry
        UserProductTimelineEntry(
            user_id=alice,
            product_id=aapl,
            timestamp=parse_time("12:10"),
            units=Decimal("10.000000"),
            net_investment=Decimal("1000.000000"),
            deposits=Decimal("1000.000000"),
            withdrawals=Decimal("0.000000"),
            fees=Decimal("0.000000"),
            buy_units=Decimal("10.000000"),
            sell_units=Decimal("0.000000"),
            buy_cost=Decimal("1000.000000"),
            sell_proceeds=Decimal("0.000000"),
            market_value=Decimal("1000.000000"),
        ),
        # Fresh entry from price update (12:14 bucketed to 12:15)
        UserProductTimelineEntry(
            user_id=alice,
            product_id=aapl,
            timestamp=parse_time("12:15"),
            units=Decimal("10.000000"),
            net_investment=Decimal("1000.000000"),
            deposits=Decimal("1000.000000"),
            withdrawals=Decimal("0.000000"),
            fees=Decimal("0.000000"),
            buy_units=Decimal("10.000000"),
            sell_units=Decimal("0.000000"),
            buy_cost=Decimal("1000.000000"),
            sell_proceeds=Decimal("0.000000"),
            market_value=Decimal("1100.000000000000"),
        ),
        # Fresh entry from cashflow (12:20)
        UserProductTimelineEntry(
            user_id=alice,
            product_id=aapl,
            timestamp=parse_time("12:20"),
            units=Decimal("15.000000"),
            net_investment=Decimal("1550.000000"),
            deposits=Decimal("1550.000000"),
            withdrawals=Decimal("0.000000"),
            fees=Decimal("0.000000"),
            buy_units=Decimal("15.000000"),
            sell_units=Decimal("0.000000"),
            buy_cost=Decimal("1550.000000"),
            sell_proceeds=Decimal("0.000000"),
            market_value=Decimal("1650.000000000000"),
        ),
    ]
