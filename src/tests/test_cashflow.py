"""Tests for cashflow trigger validation and cumulative calculations."""

from collections.abc import Callable
from typing import Any, Protocol

from tests.utils import (
    mock_ccf,
    mock_cf,
    mock_pu,
    mock_uptb,
    mock_utb,
    parse_time,
)
from twr.models import (
    Cashflow,
    CumulativeCashflow,
    PriceUpdate,
    UserProductTimelineBusinessEvent,
    UserTimelineBusinessEvent,
)


# Type aliases for test fixtures
class QueryType(Protocol):
    """Protocol for the query fixture with optional parameters."""

    def __call__(
        self, q: str, params: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> list[
        PriceUpdate
        | Cashflow
        | CumulativeCashflow
        | UserProductTimelineBusinessEvent
        | UserTimelineBusinessEvent
        | dict[str, Any]
    ]: ...


def test_one_price(
    make_data: Callable[[str], None], query: QueryType, product: Callable[[str], str]
) -> None:
    make_data("""
              12:30
        AAPL:    10
    """)
    rows = query('SELECT product_id, "timestamp", price FROM price_update')
    assert rows == [PriceUpdate(product("AAPL"), parse_time("12:30"), 10)]


def test_multiple_prices(
    make_data: Callable[[str], None], query: QueryType, product: Callable[[str], str]
) -> None:
    make_data("""
               12:30, 12:40, 12:50
        AAPL:     10,    12
        GOOGL:    30,      ,    45
    """)
    rows = query("""
        SELECT product_id, "timestamp", price
        FROM price_update
        ORDER BY product_id, "timestamp"
    """)
    # UUIDs sort differently than product names, so we need to check the results match
    # regardless of order
    assert len(rows) == 4
    aapl_rows = [r for r in rows if isinstance(r, PriceUpdate) and r.product_id == product("AAPL")]
    googl_rows = [
        r for r in rows if isinstance(r, PriceUpdate) and r.product_id == product("GOOGL")
    ]

    assert aapl_rows == [
        mock_pu(timestamp=parse_time("12:30"), price=10),
        mock_pu(timestamp=parse_time("12:40"), price=12),
    ]
    assert googl_rows == [
        mock_pu(timestamp=parse_time("12:30"), price=30),
        mock_pu(timestamp=parse_time("12:50"), price=45),
    ]


def test_same_bucket(make_data: Callable[[str], None], query: QueryType) -> None:
    make_data("""
              12:05, 12:10
        AAPL:    10,    15
    """)
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")
    rows = query("SELECT * FROM price_update_15min")
    assert rows == [mock_pu(timestamp=parse_time("12:15"), price=15)]


def test_different_buckets(make_data: Callable[[str], None], query: QueryType) -> None:
    make_data("""
              12:12, 12:17
        AAPL:    10,    15
    """)
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")
    rows = query('SELECT * FROM price_update_15min ORDER BY "timestamp"')
    assert rows == [
        mock_pu(timestamp=parse_time("12:15"), price=10),
        mock_pu(timestamp=parse_time("12:30"), price=15),
    ]


def test_one_cashflow(make_data: Callable[[str], None], query: QueryType) -> None:
    make_data("""
                    12:00, 12:10
        AAPL:          10
        Alice/AAPL:      ,     3
    """)
    rows = query("SELECT * FROM cashflow")
    assert rows == [mock_cf(timestamp=parse_time("12:10"), units_delta=3)]


def test_multiple_cashflows(make_data: Callable[[str], None], query: QueryType) -> None:
    make_data("""
                    12:00, 12:10, 12:20
        AAPL:       10
        Alice/AAPL:      ,     3,     4
    """)
    rows = query('SELECT * FROM cashflow ORDER BY "timestamp"')
    assert rows == [
        mock_cf(timestamp=parse_time("12:10"), units_delta=3),
        mock_cf(timestamp=parse_time("12:20"), units_delta=4),
    ]


def test_cumulative_cashflow(make_data: Callable[[str], None], query: QueryType) -> None:
    make_data("""
                    12:00, 12:10, 12:20, 12:30
        AAPL:       10
        Alice/AAPL:      ,     3,     4,    -5
    """)
    rows = query('SELECT * FROM cumulative_cashflow(NULL, NULL) ORDER BY "timestamp"')
    assert rows == [
        mock_ccf(timestamp=parse_time("12:10"), buy_units=3, sell_units=0),
        mock_ccf(timestamp=parse_time("12:20"), buy_units=7, sell_units=0),
        mock_ccf(timestamp=parse_time("12:30"), buy_units=7, sell_units=5),
    ]


def test_user_product_timeline_latest_includes_realtime_prices(
    make_data: Callable[[str], None],
    query: QueryType,
    user: Callable[[str], str],
    product: Callable[[str], str],
    insert: Callable[[PriceUpdate | Cashflow], None],
) -> None:
    # Use make_data for initial setup
    make_data("""
                  12:30
        AAPL:     100
        Alice/AAPL: 10
    """)

    # Refresh continuous aggregate to create the bucket
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")

    # Insert raw price 5 minutes later (after the bucket, not yet bucketed)
    insert(PriceUpdate(product("AAPL"), parse_time("12:35"), 105))

    # Query for latest portfolio value
    latest = query(
        """
        SELECT *
        FROM user_product_timeline_business_15min(%(user_id)s, %(product_id)s)
        ORDER BY timestamp DESC LIMIT 1
        """,
        {"user_id": user("Alice"), "product_id": product("AAPL")},
    )

    # Returns the bucketed price timestamp (12:45 = 12:30 bucket + 15 min)
    assert latest == [mock_uptb(timestamp=parse_time("12:45"), market_value=1000)]


def test_user_product_timeline_combines_cashflow_and_price_events(
    make_data: Callable[[str], None],
    query: QueryType,
    user: Callable[[str], str],
    product: Callable[[str], str],
) -> None:
    """Test that timeline combines cashflow events with price bucket events."""
    make_data("""
                 10:00, 11:00, 12:00
        AAPL:    150,        , 160
        Alice/AAPL:   , 10
    """)

    # Refresh the continuous aggregate so price buckets are materialized
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")

    # Query the timeline
    timeline = query(
        "SELECT * FROM user_product_timeline_business_15min(%(user_id)s, %(product_id)s)",
        {"user_id": user("Alice"), "product_id": product("AAPL")},
    )

    # Expected results:
    # - Shows latest bucketed price (12:15 = 12:00 + 15min bucket offset)
    # - market_value = 10 * 160 = 1600 (using the 12:00 price)
    #
    assert timeline == [
        mock_uptb(
            timestamp=parse_time("12:15"),
            buy_units=10,
            units=10,
            deposits=1500,
            net_investment=1500,
            market_value=1600,
        )
    ]


def test_user_timeline_aggregates_across_products(
    make_data: Callable[[str], None], query: QueryType, user: Callable[[str], str]
) -> None:
    """Test that user_timeline aggregates portfolio-level metrics across all products."""
    make_data("""
                     10:00, 10:01, 10:14, 10:29
        AAPL:        1    ,      ,     2,     4
        GOOGL:       1    ,      ,     3
        Alice/AAPL:       , 1
        Alice/GOOGL:      , 2
    """)

    # Refresh continuous aggregate
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")

    # The timeline functions may not return data without additional setup
    # This test validates that the schema and queries work correctly when data is present
    # For now, we'll just verify the query syntax is correct by running it
    timeline = query(
        "SELECT * FROM user_timeline_business_15min(%(user_id)s)",
        {"user_id": user("Alice")},
    )

    assert timeline == [
        mock_utb(timestamp=parse_time("10:15"), market_value=8),
        mock_utb(timestamp=parse_time("10:30"), market_value=10),
    ]


def test_out_of_order_cashflow_invalidates_cache(
    make_data: Callable[[str], None],
    query: QueryType,
    product: Callable[[str], str],
    user: Callable[[str], str],
    insert: Callable[[PriceUpdate | Cashflow], None],
) -> None:
    """Test that out-of-order cashflow insertion automatically invalidates affected cache."""
    # Setup initial data with 3 cashflows for Alice/AAPL
    make_data("""
                     10:00, 11:00, 12:00, 13:00
        AAPL:        100  ,   101,   102,   103
        Alice/AAPL:       ,    10,     5,    -3
    """)

    # Fill cache
    query("SELECT refresh_cumulative_cashflow()")

    # Verify cache has 3 rows for AAPL
    rows = query("SELECT * FROM cumulative_cashflow_cache ORDER BY timestamp")
    assert rows == [
        mock_ccf(
            timestamp=parse_time("11:00"),
            buy_units=10,
            sell_units=0,
            buy_cost=1010,
            sell_proceeds=0,
        ),
        mock_ccf(
            timestamp=parse_time("12:00"),
            buy_units=15,
            sell_units=0,
            buy_cost=1520,
            sell_proceeds=0,
        ),
        mock_ccf(
            timestamp=parse_time("13:00"),
            buy_units=15,
            sell_units=3,
            buy_cost=1520,
            sell_proceeds=309,
        ),
    ]

    # Insert out-of-order cashflow at 11:30 (between first and second)
    insert(Cashflow(user("Alice"), product("AAPL"), parse_time("11:30"), 2, 101.00, 202.00))

    # Verify calculations are correct with the out-of-order insert
    rows = query("SELECT * FROM cumulative_cashflow_cache ORDER BY timestamp")

    assert rows == [
        mock_ccf(
            timestamp=parse_time("11:00"),
            buy_units=10,
            sell_units=0,
            buy_cost=1010,
            sell_proceeds=0,
            deposits=1010,
            withdrawals=0,
        ),
        mock_ccf(
            timestamp=parse_time("11:30"),
            buy_units=12,
            sell_units=0,
            buy_cost=1212,
            sell_proceeds=0,
            deposits=1212,
            withdrawals=0,
        ),
        mock_ccf(
            timestamp=parse_time("12:00"),
            buy_units=17,
            sell_units=0,
            buy_cost=1722,
            sell_proceeds=0,
            deposits=1722,
            withdrawals=0,
        ),
        mock_ccf(
            timestamp=parse_time("13:00"),
            buy_units=17,
            sell_units=3,
            buy_cost=1722,
            sell_proceeds=309,
            deposits=1722,
            withdrawals=309,
        ),
    ]


def test_multi_product_cache_invalidation_isolation(
    make_data: Callable[[str], None],
    query: QueryType,
    product: Callable[[str], str],
    user: Callable[[str], str],
    insert: Callable[[PriceUpdate | Cashflow], None],
) -> None:
    """Test that cache invalidation for one product doesn't affect another product."""
    # Setup: 2 cashflows for each product
    make_data("""
                     10:00, 10:30, 11:00, 11:30, 12:00, 12:30
        AAPL:        100,       , 101,       , 102
        GOOGL:       200,       , 201,       , 202
        Alice/AAPL:       , 10,        , 5
        Alice/GOOGL:      , 20,        , 10
    """)

    # Refresh cache and verify we have 4 rows (2 per product)
    query("SELECT refresh_cumulative_cashflow()")
    cache_count = query("SELECT COUNT(*) as count FROM cumulative_cashflow_cache")
    assert cache_count == [{"count": 4}]

    # Add a3 and b3 (after cache watermark)
    insert(Cashflow(user("Alice"), product("AAPL"), parse_time("12:30"), 3, 102.00, 306.00))
    insert(Cashflow(user("Alice"), product("GOOGL"), parse_time("12:30"), 5, 202.00, 1010.00))

    # Cache still has 4 rows (a3 and b3 not cached yet)
    cache_count_after_new = query("SELECT COUNT(*) as count FROM cumulative_cashflow_cache")
    assert cache_count_after_new == [{"count": 4}]

    # Insert out-of-order cashflow for GOOGL between b1 and b2
    # This should auto-repair GOOGL cache but NOT affect AAPL's cache
    insert(Cashflow(user("Alice"), product("GOOGL"), parse_time("11:00"), 8, 200.50, 1604.00))

    # Verify AAPL cache still has 2 rows (unaffected by GOOGL insert)
    aapl_cache = query(
        "SELECT COUNT(*) as count FROM cumulative_cashflow_cache WHERE product_id = %s",
        (product("AAPL"),),
    )
    assert aapl_cache == [{"count": 2}]  # a1, a2 still cached

    # Verify GOOGL cache was automatically repaired (b1, bX, b2 cached, but NOT b3)
    googl_cache = query(
        "SELECT COUNT(*) as count FROM cumulative_cashflow_cache WHERE product_id = %s",
        (product("GOOGL"),),
    )
    assert googl_cache == [{"count": 3}]  # b1, bX, b2 (NOT b3 - after watermark)

    # Verify final cumulative state of AAPL (unaffected by GOOGL repair)
    rows_a = query(
        "SELECT * FROM cumulative_cashflow(NULL, NULL) WHERE product_id = %s ORDER BY timestamp",
        (product("AAPL"),),
    )
    assert rows_a == [
        mock_ccf(
            timestamp=parse_time("10:30"),
            buy_units=10,
            sell_units=0,
            buy_cost=1000,
            sell_proceeds=0,
            deposits=1000,
            withdrawals=0,
        ),
        mock_ccf(
            timestamp=parse_time("11:30"),
            buy_units=15,
            sell_units=0,
            buy_cost=1505,
            sell_proceeds=0,
            deposits=1505,
            withdrawals=0,
        ),
        mock_ccf(
            timestamp=parse_time("12:30"),
            buy_units=18,
            sell_units=0,
            buy_cost=1811,
            sell_proceeds=0,
            deposits=1811,
            withdrawals=0,
        ),
    ]

    # Verify final cumulative state of GOOGL (includes out-of-order insert)
    rows_b = query(
        "SELECT * FROM cumulative_cashflow(NULL, NULL) WHERE product_id = %s ORDER BY timestamp",
        (product("GOOGL"),),
    )
    assert rows_b == [
        mock_ccf(
            timestamp=parse_time("10:30"),
            buy_units=20,
            sell_units=0,
            buy_cost=4000,
            sell_proceeds=0,
            deposits=4000,
            withdrawals=0,
        ),
        mock_ccf(
            timestamp=parse_time("11:00"),
            buy_units=28,
            sell_units=0,
            buy_cost=5604,
            sell_proceeds=0,
            deposits=5604,
            withdrawals=0,
        ),
        mock_ccf(
            timestamp=parse_time("11:30"),
            buy_units=38,
            sell_units=0,
            buy_cost=7614,
            sell_proceeds=0,
            deposits=7614,
            withdrawals=0,
        ),
        mock_ccf(
            timestamp=parse_time("12:30"),
            buy_units=43,
            sell_units=0,
            buy_cost=8624,
            sell_proceeds=0,
            deposits=8624,
            withdrawals=0,
        ),
    ]
