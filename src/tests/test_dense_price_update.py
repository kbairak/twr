"""Tests for _dense_price_update gap-filling behavior."""

import datetime


def test_dense_price_update_forward_fill_from_seed(query, product):
    """Test that _dense_price_update forward-fills from seed when p_after excludes earlier price updates.

    Scenario: AAPL has a price at 11:00, GOOGL has prices at 14:00, 15:00, 16:00.
    When querying with p_after='11:00', the CROSS JOIN creates (AAPL × {14:00, 15:00, 16:00, 17:00}) pairs.
    Without a seed mechanism, AAPL wouldn't have price_update_timestamp for 14:00-16:00.
    With the fix, AAPL gets price_update_timestamp=11:00 for those timestamps (forward-filled from seed).
    """
    query(
        """
        INSERT INTO price_update (product_id, timestamp, price) VALUES
        (%(aapl)s, '2025-01-01 10:00:00', 100.00),
        (%(aapl)s, '2025-01-01 16:00:00', 150.00),
        (%(googl)s, '2025-01-01 13:00:00', 200.00),
        (%(googl)s, '2025-01-01 14:00:00', 200.00),
        (%(googl)s, '2025-01-01 15:00:00', 200.00)
        """,
        {"aapl": product("AAPL"), "googl": product("GOOGL")},
    )

    query("CALL refresh_continuous_aggregate('price_update_1h', NULL, NULL)")

    rows = query(
        """
        SELECT product_id, "timestamp", price_update_timestamp
        FROM _dense_price_update_1h(NULL, '2025-01-01 11:00:00'::timestamptz, NULL)
        ORDER BY product_id, "timestamp"
        """,
    )

    aapl_rows = [r for r in rows if r["product_id"] == product("AAPL")]

    expected_aapl = [
        {
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(2025, 1, 1, 14, 0, tzinfo=datetime.timezone.utc),
            "price_update_timestamp": datetime.datetime(2025, 1, 1, 11, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(2025, 1, 1, 15, 0, tzinfo=datetime.timezone.utc),
            "price_update_timestamp": datetime.datetime(2025, 1, 1, 11, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(2025, 1, 1, 16, 0, tzinfo=datetime.timezone.utc),
            "price_update_timestamp": datetime.datetime(2025, 1, 1, 11, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(2025, 1, 1, 17, 0, tzinfo=datetime.timezone.utc),
            "price_update_timestamp": datetime.datetime(2025, 1, 1, 17, 0, tzinfo=datetime.timezone.utc),
        },
    ]

    assert aapl_rows == expected_aapl


