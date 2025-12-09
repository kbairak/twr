"""Tests for cashflow trigger validation and cumulative calculations."""

import datetime
from decimal import Decimal
from unittest import mock

import pytest
from psycopg2.errors import RaiseException
from tests.utils import parse_time


def test_one_price(make_data, query):
    make_data("""
              12:30
        AAPL:    10
    """)
    rows = query("""
        SELECT p.name, pp."timestamp", pp.price
        FROM price_update pp
            INNER JOIN product p ON pp.product_id = p.id
    """)
    assert rows == [
        {
            "name": "AAPL",
            "timestamp": parse_time("12:30"),
            "price": Decimal("10.000000"),
        }
    ]


def test_multiple_prices(make_data, query):
    make_data("""
               12:30, 12:40, 12:50
        AAPL:     10,    12
        GOOGL:    30,      ,    45
    """)
    rows = query("""
        SELECT product.name, price_update."timestamp", price_update.price
        FROM price_update
            INNER JOIN product ON price_update.product_id = product.id
        ORDER BY product.name, price_update."timestamp"
    """)
    assert rows == [
        {
            "name": "AAPL",
            "price": Decimal("10.000000"),
            "timestamp": parse_time("12:30"),
        },
        {
            "name": "AAPL",
            "price": Decimal("12.000000"),
            "timestamp": parse_time("12:40"),
        },
        {
            "name": "GOOGL",
            "price": Decimal("30.000000"),
            "timestamp": parse_time("12:30"),
        },
        {
            "name": "GOOGL",
            "price": Decimal("45.000000"),
            "timestamp": parse_time("12:50"),
        },
    ]


def test_same_bucket(make_data, query):
    make_data("""
              12:05, 12:10
        AAPL:    10,    15
    """)
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")
    rows = query("""
        SELECT p.name, pp.bucket, pp.price
        FROM price_update_15min pp
            INNER JOIN product p ON pp.product_id = p.id
    """)
    assert rows == [
        {"name": "AAPL", "bucket": parse_time("12:00"), "price": Decimal("15.000000")},
    ]


def test_different_buckets(make_data, query):
    make_data("""
              12:12, 12:17
        AAPL:    10,    15
    """)
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")
    rows = query("""
        SELECT p.name, pp.bucket, pp.price
        FROM price_update_15min pp
            INNER JOIN product p ON pp.product_id = p.id
    """)
    assert rows == [
        {"name": "AAPL", "bucket": parse_time("12:00"), "price": Decimal("10.000000")},
        {"name": "AAPL", "bucket": parse_time("12:15"), "price": Decimal("15.000000")},
    ]


def test_one_cashflow(make_data, query):
    make_data("""
                    12:00, 12:10
        AAPL:       10
        Alice/AAPL:      ,     3
    """)
    rows = query("""
        SELECT u.name AS user_name, p.name AS product_name, c.timestamp, c.units_delta,
               c.execution_price, c.execution_money, c.user_money, c.fees
        FROM cashflow c
            INNER JOIN "user" u ON c.user_id = u.id
            INNER JOIN product p ON c.product_id = p.id
    """)
    assert rows == [
        {
            "execution_money": Decimal("30.000000"),
            "execution_price": Decimal("10.000000"),
            "fees": Decimal("0.000000"),
            "product_name": "AAPL",
            "timestamp": parse_time("12:10"),
            "units_delta": Decimal("3.000000"),
            "user_money": Decimal("30.000000"),
            "user_name": "Alice",
        },
    ]


def test_multiple_cashflows(make_data, query):
    make_data("""
                    12:00, 12:10, 12:20
        AAPL:       10
        Alice/AAPL:      ,     3,     4
    """)
    rows = query("""
        SELECT u.name AS user_name, p.name AS product_name, c.timestamp, c.units_delta,
               c.execution_price, c.execution_money, c.user_money, c.fees
        FROM cashflow c
            INNER JOIN "user" u ON c.user_id = u.id
            INNER JOIN product p ON c.product_id = p.id
        ORDER BY c."timestamp"
    """)
    assert rows == [
        {
            "execution_money": Decimal("30.000000"),
            "execution_price": Decimal("10.000000"),
            "fees": Decimal("0.000000"),
            "product_name": "AAPL",
            "timestamp": parse_time("12:10"),
            "units_delta": Decimal("3.000000"),
            "user_money": Decimal("30.000000"),
            "user_name": "Alice",
        },
        {
            "execution_money": Decimal("40.000000"),
            "execution_price": Decimal("10.000000"),
            "fees": Decimal("0.000000"),
            "product_name": "AAPL",
            "timestamp": parse_time("12:20"),
            "units_delta": Decimal("4.000000"),
            "user_money": Decimal("40.000000"),
            "user_name": "Alice",
        },
    ]


def test_cumulative_cashflow(make_data, query):
    make_data("""
                    12:00, 12:10, 12:20, 12:30
        AAPL:       10
        Alice/AAPL:      ,     3,     4,    -5
    """)
    rows = query("""
        SELECT u.name AS user_name, p.name AS product_name, cc."timestamp", cc.units_held
        FROM cumulative_cashflow cc
            INNER JOIN "user" u ON cc.user_id = u.id
            INNER JOIN product p ON cc.product_id = p.id
        ORDER BY cc."timestamp"
    """)
    assert rows == [
        {
            "user_name": "Alice",
            "product_name": "AAPL",
            "timestamp": parse_time("12:10"),
            "units_held": Decimal("3.000000"),
        },
        {
            "user_name": "Alice",
            "product_name": "AAPL",
            "timestamp": parse_time("12:20"),
            "units_held": Decimal("7.000000"),
        },
        {
            "user_name": "Alice",
            "product_name": "AAPL",
            "timestamp": parse_time("12:30"),
            "units_held": Decimal("2.000000"),
        },
    ]


def test_user_product_timeline_includes_realtime_prices(
    make_data, query, user, product
):
    """Test that include_realtime=true includes unbucketed price data for current portfolio value."""
    # Use make_data for initial setup
    make_data("""
                  12:30
        AAPL:     100
        Alice/AAPL: 10
    """)

    # Refresh continuous aggregate to create the bucket
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")

    # Insert raw price 5 minutes later (after the bucket, not yet bucketed)
    query(
        "INSERT INTO price_update (product_id, timestamp, price) VALUES (%s, %s, %s)",
        (product("AAPL"), parse_time("12:35"), 105),
    )

    # Query for latest portfolio value
    latest = query(
        """
        SELECT timestamp, market_value
        FROM user_product_timeline_15min
        WHERE user_id = %(user_id)s AND product_id = %(product_id)s
        ORDER BY timestamp DESC LIMIT 1
        """,
        {"user_id": user("Alice"), "product_id": product("AAPL")},
    )

    # Should return the raw price (5 minutes after base), not the bucketed price
    assert latest == [
        {
            "timestamp": parse_time("12:35"),
            "market_value": Decimal("1050.000000000000"),
        }
    ]


def test_user_product_timeline_combines_cashflow_and_price_events(
    make_data, query, user, product
):
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
        """
        SELECT *
        FROM user_product_timeline_15min
        WHERE user_id = %(user_id)s AND product_id = %(product_id)s
        ORDER BY timestamp
        """,
        {"user_id": user("Alice"), "product_id": product("AAPL")},
    )

    # Expected results:
    # - 10:00 price event NOT included (no cashflows before it, so lcs.units_held IS NULL)
    # - 11:00 cashflow event: bought 10 shares, execution_price=$150 (from market price at 10:00)
    #   net_investment = execution_money + fees = (10 * 150) + 0 = 1500
    #   market_value = 10 * 150 = 1500
    # - 12:00 price bucket event: holdings carried forward, price=$160
    #   market_value = 10 * 160 = 1600

    assert timeline == [
        {
            "market_value": Decimal("1500.000000000000"),
            "net_investment": Decimal("1500.000000"),
            "product_id": product("AAPL"),
            "timestamp": parse_time("11:00"),
            "buy_cost": Decimal("1500.000000000000"),
            "buy_units": Decimal("10.000000"),
            "deposits": Decimal("1500.000000"),
            "fees": Decimal("0"),
            "sell_proceeds": Decimal("0"),
            "sell_units": Decimal("0"),
            "withdrawals": Decimal("0"),
            "units_held": Decimal("10.000000"),
            "user_id": user("Alice"),
        },
        {
            "market_value": Decimal("1600.000000000000"),
            "net_investment": Decimal("1500.000000"),
            "product_id": product("AAPL"),
            "timestamp": parse_time("12:00"),
            "buy_cost": Decimal("1500.000000000000"),
            "buy_units": Decimal("10.000000"),
            "deposits": Decimal("1500.000000"),
            "fees": Decimal("0"),
            "sell_proceeds": Decimal("0"),
            "sell_units": Decimal("0"),
            "withdrawals": Decimal("0"),
            "units_held": Decimal("10.000000"),
            "user_id": user("Alice"),
        },
    ]


def test_user_timeline_aggregates_across_products(make_data, query, user, product):
    """Test that user_timeline aggregates portfolio-level metrics across all products."""
    make_data("""
                     10:00, 11:00, 12:00
        AAPL:        150
        GOOGL:       2800
        Alice/AAPL:       , 10
        Alice/GOOGL:            ,     , 5
    """)

    # Refresh continuous aggregate
    query("CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL)")

    # Query user_timeline
    timeline = query(
        """
        SELECT *
        FROM user_timeline_15min
        WHERE user_id = %(user_id)s
        ORDER BY timestamp
        """,
        {"user_id": user("Alice")},
    )

    # Expected results:
    # - 11:00: Only AAPL position
    #   AAPL: net_investment=1500, market_value=10*150=1500
    #   Portfolio: net_investment=1500, market_value=1500
    # - 12:00: Both AAPL (carried forward) and GOOGL
    #   AAPL: net_investment=1500, market_value=10*150=1500 (carried forward)
    #   GOOGL: net_investment=14000, market_value=5*2800=14000
    #   Portfolio: net_investment=1500+14000=15500, market_value=1500+14000=15500

    assert timeline == [
        {
            "timestamp": parse_time("11:00"),
            "buy_cost": Decimal("1500.000000000000"),
            "buy_units": Decimal("10.000000"),
            "deposits": Decimal("1500.000000"),
            "fees": Decimal("0"),
            "market_value": Decimal("1500.000000000000"),
            "net_investment": Decimal("1500.000000"),
            "sell_proceeds": Decimal("0"),
            "sell_units": Decimal("0"),
            "withdrawals": Decimal("0"),
            "cost_basis": Decimal("1500.000000000000"),
            "sell_basis": Decimal("0"),
            "user_id": user("Alice"),
        },
        {
            "timestamp": parse_time("12:00"),
            "buy_cost": Decimal("15500.000000000000"),
            "buy_units": Decimal("15.000000"),
            "deposits": Decimal("15500.000000"),
            "fees": Decimal("0"),
            "market_value": Decimal("15500.000000000000"),
            "net_investment": Decimal("15500.000000"),
            "sell_proceeds": Decimal("0"),
            "sell_units": Decimal("0"),
            "withdrawals": Decimal("0"),
            "cost_basis": Decimal("15500.000000000000"),
            "sell_basis": Decimal("0"),
            "user_id": user("Alice"),
        },
    ]


def test_cashflow_trigger_derives_missing_fields(query, product, user):
    """Test that cashflow trigger correctly derives missing fields from provided ones."""
    # Insert price data
    query(
        """
        INSERT INTO price_update (product_id, timestamp, price) VALUES
        (%s, '2025-01-01 10:00:00', 101.00)
        """,
        (product("AAPL"),),
    )

    # Test: Provide units_delta, execution_price, fees -> trigger derives execution_money, user_money
    result = query(
        """
        INSERT INTO cashflow (user_id, product_id, timestamp, units_delta, execution_price, fees)
        VALUES (%(user_id)s, %(product_id)s, '2025-01-01 10:30:00', 10, 101.00, 5.00)
        RETURNING *
        """,
        {"user_id": user("Alice"), "product_id": product("AAPL")},
    )

    assert result == [
        {
            "execution_money": Decimal("1010.000000"),
            "execution_price": Decimal("101.000000"),
            "fees": Decimal("5.000000"),
            "id": mock.ANY,
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 10, 30, tzinfo=datetime.timezone.utc
            ),
            "units_delta": Decimal("10.000000"),
            "user_id": user("Alice"),
            "user_money": Decimal("1015.000000"),
        },
    ]


def test_cashflow_trigger_validates_consistency(query, user, product):
    """Test that trigger validates consistency when more than 3 fields provided."""
    # Try to insert inconsistent data (all 5 fields, but math doesn't add up)
    with pytest.raises(RaiseException, match="Inconsistent data"):
        query(
            """
            INSERT INTO cashflow (user_id, product_id, timestamp, units_delta, execution_price,
                                  execution_money, user_money, fees)
            VALUES (%(user_id)s, %(product_id)s, '2025-01-01 10:30:00', 10, 101.00, 999.00,
                    1015.00, 5.00)
            """,
            {"user_id": user("Alice"), "product_id": product("AAPL")},
        )


def test_cumulative_cashflow_calculations(make_data, query, product, user):
    """Test cumulative cashflow view calculates running totals correctly."""
    # Setup: Insert price data using make_data
    make_data("""
              09:00, 10:00, 11:00, 12:00
        AAPL: 100,   101,   102,   103
    """)

    # Insert 3 cashflows with custom fees: 2 buys, 1 sell
    query(
        """
        INSERT INTO cashflow (user_id, product_id, timestamp, units_delta, execution_price, fees) VALUES
        (%(user_id)s, %(product_id)s, %(t1)s, 10, 101.00, 5.00),   -- Buy 10 @ 101
        (%(user_id)s, %(product_id)s, %(t2)s, 5, 102.00, 10.00),   -- Buy 5 @ 102
        (%(user_id)s, %(product_id)s, %(t3)s, -3, 103.00, 3.00)    -- Sell 3 @ 103
        """,
        {
            "user_id": user("Alice"),
            "product_id": product("AAPL"),
            "t1": parse_time("10:30"),
            "t2": parse_time("11:30"),
            "t3": parse_time("12:30"),
        },
    )

    # Query cumulative view
    rows = query(
        """
        SELECT *
        FROM cumulative_cashflow
        ORDER BY timestamp
        """
    )

    assert rows == [
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("1015.000000"),
            "product_id": product("AAPL"),
            "timestamp": parse_time("10:30"),
            "buy_cost": Decimal("1010.000000000000"),
            "buy_units": Decimal("10.000000"),
            "deposits": Decimal("1015.000000"),
            "fees": Decimal("5.000000"),
            "sell_proceeds": Decimal("0"),
            "sell_units": Decimal("0"),
            "withdrawals": Decimal("0"),
            "units_held": Decimal("10.000000"),
            "user_id": user("Alice"),
        },
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("1535.000000"),
            "product_id": product("AAPL"),
            "timestamp": parse_time("11:30"),
            "buy_cost": Decimal("1520.000000000000"),
            "buy_units": Decimal("15.000000"),
            "deposits": Decimal("1535.000000"),
            "fees": Decimal("15.000000"),
            "sell_proceeds": Decimal("0"),
            "sell_units": Decimal("0"),
            "withdrawals": Decimal("0"),
            "units_held": Decimal("15.000000"),
            "user_id": user("Alice"),
        },
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("1229.000000"),
            "product_id": product("AAPL"),
            "timestamp": parse_time("12:30"),
            "buy_cost": Decimal("1520.000000000000"),
            "buy_units": Decimal("15.000000"),
            "deposits": Decimal("1535.000000"),
            "fees": Decimal("18.000000"),
            "sell_proceeds": Decimal("309.000000"),
            "sell_units": Decimal("3.000000"),
            "withdrawals": Decimal("306.000000"),
            "units_held": Decimal("12.000000"),
            "user_id": user("Alice"),
        },
    ]


def test_out_of_order_cashflow_invalidates_cache(query, product, user):
    """Test that out-of-order cashflow insertion automatically invalidates affected cache."""
    # Setup
    query(
        """
        INSERT INTO price_update (product_id, timestamp, price) VALUES
        (%(appl)s, '2025-01-01 10:00:00', 101.00),
        (%(appl)s, '2025-01-01 11:00:00', 102.00),
        (%(appl)s, '2025-01-01 12:00:00', 103.00),
        (%(googl)s, '2025-01-01 13:00:00', 200.00)
        """,
        {"appl": product("AAPL"), "googl": product("GOOGL")},
    )

    # Insert 3 cashflows for Alice/AAPL and one for Alice/GOOGL
    query(
        """
        INSERT INTO cashflow (user_id, product_id, timestamp, units_delta, execution_price, fees) VALUES
        (%(user_id)s, %(appl)s, '2025-01-01 10:30:00', 10, 101.00, 5.00),
        (%(user_id)s, %(appl)s, '2025-01-01 11:30:00', 5, 102.00, 10.00),
        (%(user_id)s, %(appl)s, '2025-01-01 12:30:00', -3, 103.00, 3.00),
        (%(user_id)s, %(googl)s, '2025-01-01 13:00:00', 10, 200.00, 5.00)
        """,
        {"user_id": user("Alice"), "appl": product("AAPL"), "googl": product("GOOGL")},
    )

    # Fill cache
    query("SELECT refresh_cumulative_cashflow()")

    # Verify cache
    rows = query(
        "SELECT * FROM cumulative_cashflow_cache WHERE product_id = %s ORDER BY timestamp",
        (product("AAPL"),),
    )
    assert rows == [
        {
            "buy_cost": Decimal("1010.000000"),
            "buy_units": Decimal("10.000000"),
            "cashflow_id": mock.ANY,
            "deposits": Decimal("1015.000000"),
            "fees": Decimal("5.000000"),
            "net_investment": Decimal("1015.000000"),
            "product_id": product("AAPL"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 10, 30, tzinfo=datetime.timezone.utc
            ),
            "units_held": Decimal("10.000000"),
            "user_id": user("Alice"),
            "withdrawals": Decimal("0.000000"),
        },
        {
            "buy_cost": Decimal("1520.000000"),
            "buy_units": Decimal("15.000000"),
            "cashflow_id": mock.ANY,
            "deposits": Decimal("1535.000000"),
            "fees": Decimal("15.000000"),
            "net_investment": Decimal("1535.000000"),
            "product_id": product("AAPL"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 11, 30, tzinfo=datetime.timezone.utc
            ),
            "units_held": Decimal("15.000000"),
            "user_id": user("Alice"),
            "withdrawals": Decimal("0.000000"),
        },
        {
            "buy_cost": Decimal("1520.000000"),
            "buy_units": Decimal("15.000000"),
            "cashflow_id": mock.ANY,
            "deposits": Decimal("1535.000000"),
            "fees": Decimal("18.000000"),
            "net_investment": Decimal("1229.000000"),
            "product_id": product("AAPL"),
            "sell_proceeds": Decimal("309.000000"),
            "sell_units": Decimal("3.000000"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 12, 30, tzinfo=datetime.timezone.utc
            ),
            "units_held": Decimal("12.000000"),
            "user_id": user("Alice"),
            "withdrawals": Decimal("306.000000"),
        },
    ]

    # Insert out-of-order cashflow at 11:00 (between first and second)
    query(
        """
        INSERT INTO cashflow (user_id, product_id, timestamp, units_delta, execution_price, fees)
        VALUES (%(user_id)s, %(product_id)s, '2025-01-01 11:00:00', 2, 101.50, 1.00)
        """,
        {"user_id": user("Alice"), "product_id": product("AAPL")},
    )

    # Verify calculations are correct with the out-of-order insert
    rows = query(
        "SELECT * FROM cumulative_cashflow WHERE product_id = %s ORDER BY timestamp",
        (product("AAPL"),),
    )

    assert rows == [
        {
            "buy_cost": Decimal("1010.000000"),
            "buy_units": Decimal("10.000000"),
            "cashflow_id": mock.ANY,
            "deposits": Decimal("1015.000000"),
            "fees": Decimal("5.000000"),
            "net_investment": Decimal("1015.000000"),
            "product_id": product("AAPL"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 10, 30, tzinfo=datetime.timezone.utc
            ),
            "units_held": Decimal("10.000000"),
            "user_id": user("Alice"),
            "withdrawals": Decimal("0.000000"),
        },
        {
            "buy_cost": Decimal("1213.000000"),
            "buy_units": Decimal("12.000000"),
            "cashflow_id": mock.ANY,
            "deposits": Decimal("1219.000000"),
            "fees": Decimal("6.000000"),
            "net_investment": Decimal("1219.000000"),
            "product_id": product("AAPL"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 11, 0, tzinfo=datetime.timezone.utc
            ),
            "units_held": Decimal("12.000000"),
            "user_id": user("Alice"),
            "withdrawals": Decimal("0.000000"),
        },
        {
            "buy_cost": Decimal("1723.000000"),
            "buy_units": Decimal("17.000000"),
            "cashflow_id": mock.ANY,
            "deposits": Decimal("1739.000000"),
            "fees": Decimal("16.000000"),
            "net_investment": Decimal("1739.000000"),
            "product_id": product("AAPL"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 11, 30, tzinfo=datetime.timezone.utc
            ),
            "units_held": Decimal("17.000000"),
            "user_id": user("Alice"),
            "withdrawals": Decimal("0.000000"),
        },
        {
            "buy_cost": Decimal("1723.000000"),
            "buy_units": Decimal("17.000000"),
            "cashflow_id": mock.ANY,
            "deposits": Decimal("1739.000000"),
            "fees": Decimal("19.000000"),
            "net_investment": Decimal("1433.000000"),
            "product_id": product("AAPL"),
            "sell_proceeds": Decimal("309.000000"),
            "sell_units": Decimal("3.000000"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 12, 30, tzinfo=datetime.timezone.utc
            ),
            "units_held": Decimal("14.000000"),
            "user_id": user("Alice"),
            "withdrawals": Decimal("306.000000"),
        },
    ]


def test_multi_product_cache_invalidation_isolation(query, product, user):
    """Test that cache invalidation for one product doesn't affect another product."""

    query(
        """
        INSERT INTO price_update (product_id, timestamp, price) VALUES
        (%(product_a)s, '2025-01-01 10:00:00', 100.00),
        (%(product_a)s, '2025-01-01 11:00:00', 101.00),
        (%(product_a)s, '2025-01-01 12:00:00', 102.00),
        (%(product_b)s, '2025-01-01 10:00:00', 200.00),
        (%(product_b)s, '2025-01-01 11:00:00', 201.00),
        (%(product_b)s, '2025-01-01 12:00:00', 202.00)
        """,
        {"product_a": product("AAPL"), "product_b": product("GOOGL")},
    )

    # Insert 2 cashflows for each product
    # Product A: a1(10:30), a2(11:30)
    # Product B: b1(10:30), b2(11:30)
    query(
        """
        INSERT INTO cashflow (user_id, product_id, timestamp, units_delta, execution_price, fees) VALUES
        (%(user_id)s, %(product_a)s, '2025-01-01 10:30:00', 10, 100.00, 1.00),  -- a1
        (%(user_id)s, %(product_a)s, '2025-01-01 11:30:00', 5, 101.00, 1.00),   -- a2
        (%(user_id)s, %(product_b)s, '2025-01-01 10:30:00', 20, 200.00, 2.00),  -- b1
        (%(user_id)s, %(product_b)s, '2025-01-01 11:30:00', 10, 201.00, 2.00)   -- b2
        """,
        {
            "user_id": user("Alice"),
            "product_a": product("AAPL"),
            "product_b": product("GOOGL"),
        },
    )

    # Refresh cache and verify we have 4 rows (2 per product)
    query("SELECT refresh_cumulative_cashflow()")
    cache_count = query("SELECT COUNT(*) as count FROM cumulative_cashflow_cache")
    assert cache_count == [{"count": 4}]

    # Add a3 and b3 (after cache watermark)
    query(
        """
        INSERT INTO cashflow (user_id, product_id, timestamp, units_delta, execution_price, fees) VALUES
        (%(user_id)s, %(product_a)s, '2025-01-01 12:30:00', 3, 102.00, 1.00),   -- a3
        (%(user_id)s, %(product_b)s, '2025-01-01 12:30:00', 5, 202.00, 2.00)    -- b3
        """,
        {
            "user_id": user("Alice"),
            "product_a": product("AAPL"),
            "product_b": product("GOOGL"),
        },
    )

    # Cache still has 4 rows (a3 and b3 not cached yet)
    cache_count_after_new = query(
        "SELECT COUNT(*) as count FROM cumulative_cashflow_cache"
    )
    assert cache_count_after_new == [{"count": 4}]

    # Insert out-of-order cashflow for product B between b1 and b2
    # This should auto-repair product B cache but NOT affect product A's cache
    query(
        """
        INSERT INTO cashflow (user_id, product_id, timestamp, units_delta, execution_price, fees)
        VALUES (%(user_id)s, %(product_b)s, '2025-01-01 11:00:00', 8, 200.50, 1.50)  -- bX
        """,
        {"user_id": user("Alice"), "product_b": product("GOOGL")},
    )

    # Verify product A still has both cached rows (a1, a2) - unaffected by product B insert
    product_a_cache = query(
        """
        SELECT COUNT(*) as count
        FROM cumulative_cashflow_cache ccc
        JOIN cashflow cf ON ccc.cashflow_id = cf.id
        WHERE cf.product_id = %(product_a)s
        """,
        {"product_a": product("AAPL")},
    )
    assert product_a_cache == [{"count": 2}]  # a1, a2 still cached

    # Verify product B cache was automatically repaired (b1, bX, b2 cached, but NOT b3)
    product_b_cache = query(
        """
        SELECT COUNT(*) as count
        FROM cumulative_cashflow_cache ccc
        JOIN cashflow cf ON ccc.cashflow_id = cf.id
        WHERE cf.product_id = %(product_b)s
        """,
        {"product_b": product("GOOGL")},
    )
    assert product_b_cache == [{"count": 3}]  # b1, bX, b2 (NOT b3 - after watermark)

    # Verify final state of both products
    rows_a = query(
        """
        SELECT *
        FROM cumulative_cashflow
        WHERE user_id = %(user_id)s AND product_id = %(product_a)s
        ORDER BY timestamp
        """,
        {"user_id": user("Alice"), "product_a": product("AAPL")},
    )

    assert rows_a == [
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("1001.000000"),
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 10, 30, tzinfo=datetime.timezone.utc
            ),
            "buy_cost": Decimal("1000.000000"),
            "buy_units": Decimal("10.000000"),
            "deposits": Decimal("1001.000000"),
            "fees": Decimal("1.000000"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "withdrawals": Decimal("0.000000"),
            "units_held": Decimal("10.000000"),
            "user_id": user("Alice"),
        },
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("1507.000000"),
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 11, 30, tzinfo=datetime.timezone.utc
            ),
            "buy_cost": Decimal("1505.000000"),
            "buy_units": Decimal("15.000000"),
            "deposits": Decimal("1507.000000"),
            "fees": Decimal("2.000000"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "withdrawals": Decimal("0.000000"),
            "units_held": Decimal("15.000000"),
            "user_id": user("Alice"),
        },
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("1814.000000"),
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 12, 30, tzinfo=datetime.timezone.utc
            ),
            "buy_cost": Decimal("1811.000000000000"),
            "buy_units": Decimal("18.000000"),
            "deposits": Decimal("1814.000000"),
            "fees": Decimal("3.000000"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "withdrawals": Decimal("0.000000"),
            "units_held": Decimal("18.000000"),
            "user_id": user("Alice"),
        },
    ]

    rows_b = query(
        """
        SELECT *
        FROM cumulative_cashflow
        WHERE user_id = %(user_id)s AND product_id = %(product_b)s
        ORDER BY timestamp
        """,
        {"user_id": user("Alice"), "product_b": product("GOOGL")},
    )

    assert rows_b == [
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("4002.000000"),
            "product_id": product("GOOGL"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 10, 30, tzinfo=datetime.timezone.utc
            ),
            "buy_cost": Decimal("4000.000000"),
            "buy_units": Decimal("20.000000"),
            "deposits": Decimal("4002.000000"),
            "fees": Decimal("2.000000"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "withdrawals": Decimal("0.000000"),
            "units_held": Decimal("20.000000"),
            "user_id": user("Alice"),
        },
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("5607.500000"),
            "product_id": product("GOOGL"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 11, 0, tzinfo=datetime.timezone.utc
            ),
            "buy_cost": Decimal("5604.000000"),
            "buy_units": Decimal("28.000000"),
            "deposits": Decimal("5607.500000"),
            "fees": Decimal("3.500000"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "withdrawals": Decimal("0.000000"),
            "units_held": Decimal("28.000000"),
            "user_id": user("Alice"),
        },
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("7619.500000"),
            "product_id": product("GOOGL"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 11, 30, tzinfo=datetime.timezone.utc
            ),
            "buy_cost": Decimal("7614.000000"),
            "buy_units": Decimal("38.000000"),
            "deposits": Decimal("7619.500000"),
            "fees": Decimal("5.500000"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "withdrawals": Decimal("0.000000"),
            "units_held": Decimal("38.000000"),
            "user_id": user("Alice"),
        },
        {
            "cashflow_id": mock.ANY,
            "net_investment": Decimal("8631.500000"),
            "product_id": product("GOOGL"),
            "timestamp": datetime.datetime(
                2025, 1, 1, 12, 30, tzinfo=datetime.timezone.utc
            ),
            "buy_cost": Decimal("8624.000000000000"),
            "buy_units": Decimal("43.000000"),
            "deposits": Decimal("8631.500000"),
            "fees": Decimal("7.500000"),
            "sell_proceeds": Decimal("0.000000"),
            "sell_units": Decimal("0.000000"),
            "withdrawals": Decimal("0.000000"),
            "units_held": Decimal("43.000000"),
            "user_id": user("Alice"),
        },
    ]
