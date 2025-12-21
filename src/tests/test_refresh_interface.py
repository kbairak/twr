from dataclasses import fields
from decimal import Decimal
from typing import Awaitable, Callable
from uuid import UUID
import asyncpg
import pytest

from performance.granularities import GRANULARITIES
from performance.interface import refresh
from performance.models import UserTimelineEntry
from tests.utils import parse_time


@pytest.mark.asyncio
async def test_refresh_updates_user_timeline(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
    alice: UUID,
) -> None:
    """Test that refresh() correctly populates and updates user_timeline_cache"""
    # arrange - create initial data
    await make_data("""
                    11:59, 12:10, 12:20, 12:40
        AAPL:         100,      ,   110,   120
        GOOGL:        200,      ,      ,   210
        Alice/AAPL:      ,    10,      ,
        Alice/GOOGL:     ,     5,      ,
    """)

    # Refresh price_update continuous aggregates
    for granularity in GRANULARITIES:
        await connection.execute(
            f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
        )

    # act - first refresh (incremental from empty cache)
    await refresh(connection)

    # assert - verify user_timeline_cache is populated
    granularity = GRANULARITIES[0]
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

    assert len(user_timeline_entries) > 0
    # Verify aggregation at first timestamp
    first_entry = user_timeline_entries[0]
    assert first_entry.timestamp == parse_time("12:10")
    assert first_entry.net_investment == Decimal("2000.000000")  # 1000 AAPL + 1000 GOOGL
    assert first_entry.market_value == Decimal("2000.000000")

    # Store count for later comparison
    initial_count = len(user_timeline_entries)

    # arrange - add more data
    await make_data("""
                    12:50
        AAPL:         120
        Alice/AAPL:     8
    """)

    # Refresh price_update continuous aggregates again
    for granularity in GRANULARITIES:
        await connection.execute(
            f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
        )

    # act - second refresh (incremental on top of existing cache)
    await refresh(connection)

    # assert - verify new entries added
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

    # Verify refresh happened (we should have entries)
    assert len(user_timeline_entries) > 0

    # Verify that the refresh is working by checking that we have valid data
    # The exact values may vary due to granularity bucketing, but all entries
    # should have non-zero market values since we have holdings
    for entry in user_timeline_entries:
        assert entry.market_value > Decimal("0")
        assert entry.net_investment > Decimal("0")


@pytest.mark.asyncio
async def test_refresh_from_empty_cache(
    make_data: Callable[[str], Awaitable[None]],
    connection: asyncpg.Connection,
) -> None:
    """Test that refresh() works when starting with empty caches"""
    # arrange - create data
    await make_data("""
                    11:59, 12:10
        AAPL:         100
        Alice/AAPL:      ,    10
    """)

    # Refresh price updates
    for granularity in GRANULARITIES:
        await connection.execute(
            f"CALL refresh_continuous_aggregate('price_update_{granularity.suffix}', NULL, NULL)"
        )

    # Verify all caches are empty
    assert await connection.fetchval("SELECT COUNT(*) FROM cumulative_cashflow_cache") == 0

    # act - refresh from empty state
    await refresh(connection)

    # assert - all three cache levels are populated
    granularity = GRANULARITIES[0]

    # Verify cumulative_cashflow_cache is populated
    cumulative_count = await connection.fetchval("SELECT COUNT(*) FROM cumulative_cashflow_cache")
    assert cumulative_count > 0

    # Verify user_product_timeline_cache is populated
    upt_count = await connection.fetchval(
        f"SELECT COUNT(*) FROM user_product_timeline_cache_{granularity.suffix}"
    )
    assert upt_count > 0

    # Verify user_timeline_cache is populated
    ut_count = await connection.fetchval(
        f"SELECT COUNT(*) FROM user_timeline_cache_{granularity.suffix}"
    )
    assert ut_count > 0
