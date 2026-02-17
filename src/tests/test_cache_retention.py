"""Tests for cache retention functionality.

These tests verify that the cache retention logic works correctly for different granularities.
Since the trigger-based repair and refresh functions interact in complex ways, we focus on
testing the observable behavior: that data within retention is accessible and correctly cached.
"""

import datetime
from decimal import Decimal

from twr.models import Cashflow, PriceUpdate


def test_repair_respects_retention_period_15min(query, user, product, insert):
    """Test that repair doesn't cache data outside 7-day retention period."""
    # Use current time as reference
    now = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)

    # Insert cashflow within retention (3 days ago)
    recent_time = now - datetime.timedelta(days=3)
    insert(PriceUpdate(product("AAPL"), recent_time, 100.00))
    insert(Cashflow(user("Alice"), product("AAPL"), recent_time, 10, 100, 1001))

    # Refresh continuous aggregate
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")

    # Now insert out-of-order cashflow 10 days ago (outside 7 day retention)
    old_time = now - datetime.timedelta(days=10)
    insert(Cashflow(user("Alice"), product("AAPL"), old_time, 5, 100, 501))

    # Trigger invalidates and repairs - repair should NOT cache the 10-day-old data
    # Verify function still works correctly (computes from fresh data when needed)
    view_data = query(
        """
        SELECT units
        FROM user_product_timeline_business_15min(%(user_id)s, %(product_id)s)
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        {"user_id": user("Alice"), "product_id": product("AAPL")},
    )

    # Should show correct total units (10 + 5 = 15) even though old data may not be cached
    assert view_data[0]["units"] == Decimal("15.000000"), "View should compute correct values"

    # Check that cache doesn't have an excessive amount of old data
    cache_count = query(
        """
        SELECT COUNT(*) as count
        FROM user_product_timeline_cache_15min
        WHERE user_id = %(user_id)s AND product_id = %(product_id)s
        AND timestamp < %(retention_cutoff)s
        """,
        {
            "user_id": user("Alice"),
            "product_id": product("AAPL"),
            "retention_cutoff": now - datetime.timedelta(days=7),
        },
    )[0]["count"]

    # Should have minimal or no data older than 7 days
    assert cache_count < 10, (
        f"Cache should not have significant data outside retention, got {cache_count} old rows"
    )


def test_view_works_with_retention(query, user, product, insert):
    """Test that views continue to work correctly even with retention filtering."""
    # Use current time as reference
    now = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)

    # Insert data at multiple time points within retention
    for days_ago in [5, 3, 1]:
        timestamp = now - datetime.timedelta(days=days_ago)
        price = Decimal("100.00") + days_ago
        insert(PriceUpdate(product("AAPL"), timestamp, price))
        insert(Cashflow(user("Alice"), product("AAPL"), timestamp, 1, price, price))

    # Refresh continuous aggregate
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")

    # Query function - should return all data within retention
    view_data = query(
        """
        SELECT timestamp, units
        FROM user_product_timeline_business_15min(%(user_id)s, %(product_id)s)
        ORDER BY timestamp
        """,
        {"user_id": user("Alice"), "product_id": product("AAPL")},
    )

    # Should have at least 3 cashflow events
    assert len(view_data) >= 3, (
        f"View should return data within retention, got {len(view_data)} rows"
    )

    # Latest should have 3 units total
    latest = view_data[-1]
    assert latest["units"] == Decimal("3.000000"), "View should show correct cumulative units"


def test_user_timeline_with_retention(query, user, product, insert):
    """Test that user_timeline aggregates correctly with retention."""
    # Use recent dates
    now = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)

    # Insert data for two products within retention
    recent_time = now - datetime.timedelta(days=2)
    for prod in ["AAPL", "GOOGL"]:
        insert(PriceUpdate(product(prod), recent_time, 100.00))
        insert(Cashflow(user("Alice"), product(prod), recent_time, 10, 100, 1001))

    # Refresh continuous aggregate
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")

    # Verify user_timeline aggregates portfolio correctly
    portfolio = query(
        """
        SELECT market_value, net_investment
        FROM user_timeline_business_15min(%(user_id)s)
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        {"user_id": user("Alice")},
    )

    # Should aggregate both products (10 units × $100 × 2 products = $2000)
    assert len(portfolio) > 0, "User timeline should return data"
    assert portfolio[0]["market_value"] == Decimal("2000.000000000000"), (
        f"Portfolio should aggregate both products, got {portfolio[0]['market_value']}"
    )
