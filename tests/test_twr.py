"""
Tests for TWR database operations.
"""

import os
import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from testcontainers.postgres import PostgresContainer

from main import TWRDatabase

# Disable ryuk to avoid port conflicts
os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"


@pytest.fixture(scope="session")
def twr_uncleared():
    """Session-scoped fixture: PostgreSQL container and TWRDatabase instance."""
    # Start PostgreSQL container - testcontainers will find an available random port
    postgres = PostgresContainer("postgres:16")
    postgres.start()

    try:
        # Create TWRDatabase instance
        database = TWRDatabase(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            dbname=postgres.dbname,
            user=postgres.username,
            password=postgres.password,
        )

        # Run migrations once for the entire test session
        database.run_migrations()

        yield database
    finally:
        # Ensure container is stopped and removed after all tests
        postgres.stop()


@pytest.fixture(scope="function")
def twr(twr_uncleared):
    """Function-scoped fixture: clear database before each test."""
    # Clear database before the test runs
    twr_uncleared.clear()
    return twr_uncleared


@pytest.fixture()
def at():
    """Usage:

    def test_something(at):
        assert at(1) - at(0) == timedelta(seconds=1)
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=1)

    def _at(index):
        return start + timedelta(seconds=index)

    return _at


def test_database_isolation(twr):
    """Test that database is cleared between tests."""
    # This test should start with an empty database
    # If the previous test's data wasn't cleared, this would fail

    # Verify no products exist
    products = twr._execute_query("SELECT * FROM product", fetch=True)
    assert len(products) == 0

    # Verify no prices exist
    prices = twr._execute_query("SELECT * FROM product_price", fetch=True)
    assert len(prices) == 0


def test_add_price(twr):
    """Test adding a product price."""
    # Add a price
    twr.add_price("nvidia", 186.26)

    # Verify the price was added
    results = twr._execute_query(
        """
        SELECT p.name, pp.price
        FROM product_price pp
        JOIN product p ON pp.product_id = p.id
        WHERE p.name = %s
    """,
        ("nvidia",),
        fetch=True,
    )
    assert results == [{"name": "nvidia", "price": Decimal("186.260000")}]

    # Verify other tables are empty (only product and product_price should have data)
    users = twr._execute_query('SELECT * FROM "user"', fetch=True)
    assert users == []

    cash_flows = twr._execute_query("SELECT * FROM user_cash_flow", fetch=True)
    assert cash_flows == []

    # Verify views are empty
    user_product_timeline = twr._execute_query(
        "SELECT * FROM user_product_timeline", fetch=True
    )
    assert user_product_timeline == []

    user_timeline = twr._execute_query("SELECT * FROM user_timeline", fetch=True)
    assert user_timeline == []


def test_one_price_one_cashflow(twr, at):
    """Test adding one price and one cash flow, verify all tables and views."""
    # Add a price for nvidia at $10/unit
    twr.add_price("nvidia", 10, timestamp=at(0))

    # Add a cash flow for Alice buying $100 worth of nvidia (= 10 units)
    twr.add_cashflow("Alice", "nvidia", money=100, timestamp=at(1))

    # Verify product table
    products = twr._execute_query("SELECT name FROM product", fetch=True)
    assert products == [{"name": "nvidia"}]

    # Verify product_price table
    prices = twr._execute_query(
        "SELECT p.name, pp.price, pp.timestamp FROM product_price pp JOIN product p ON pp.product_id = p.id",
        fetch=True,
    )
    assert prices == [
        {"name": "nvidia", "price": Decimal("10.000000"), "timestamp": at(0)}
    ]

    # Verify user table
    users = twr._execute_query('SELECT name FROM "user"', fetch=True)
    assert users == [{"name": "Alice"}]

    # Verify user_cash_flow table
    cash_flows = twr._execute_query(
        """
        SELECT u.name as user_name,
               p.name as product_name,
               ucf.units,
               ucf.deposit,
               ucf.cumulative_units,
               ucf.cumulative_deposits,
               ucf.period_return,
               ucf.cumulative_twr_factor,
               ucf.timestamp
        FROM user_cash_flow ucf
            JOIN "user" u ON ucf.user_id = u.id
            JOIN product p ON ucf.product_id = p.id
        """,
        fetch=True,
    )
    assert cash_flows == [
        {
            "user_name": "Alice",
            "product_name": "nvidia",
            "units": Decimal("10.000000"),
            "deposit": Decimal("100.000000"),
            "cumulative_units": Decimal("10.000000"),
            "cumulative_deposits": Decimal("100.000000"),
            "period_return": Decimal("0.000000"),
            "cumulative_twr_factor": Decimal("1.000000"),
            "timestamp": at(1),
        }
    ]

    # Verify user_product_timeline view
    user_product_timeline = twr._execute_query(
        """
        SELECT u.name as user_name,
               p.name as product_name,
               upt.holdings,
               upt.net_deposits,
               upt.current_price,
               upt.current_value,
               upt.current_twr,
               upt.timestamp
        FROM user_product_timeline upt
        JOIN "user" u ON upt.user_id = u.id
        JOIN product p ON upt.product_id = p.id
        """,
        fetch=True,
    )
    assert user_product_timeline == [
        {
            "user_name": "Alice",
            "product_name": "nvidia",
            "holdings": Decimal("10.000000"),
            "net_deposits": Decimal("100.000000"),
            "current_price": Decimal("10.000000"),
            "current_value": Decimal("100.000000"),
            "current_twr": Decimal("0.000000"),
            "timestamp": at(1),
        }
    ]

    # Verify user_timeline view
    user_timeline = twr._execute_query(
        """
        SELECT u.name as user_name, ut.total_net_deposits, ut.total_value, ut.value_weighted_twr, ut.timestamp
        FROM user_timeline ut
        JOIN "user" u ON ut.user_id = u.id""",
        fetch=True,
    )
    assert user_timeline == [
        {
            "user_name": "Alice",
            "total_net_deposits": Decimal("100.000000"),
            "total_value": Decimal("100.000000"),
            "value_weighted_twr": Decimal("0.000000"),
            "timestamp": at(1),
        }
    ]


def test_price_increase_50_percent(twr, at):
    """Test: user deposits $100 at $10/unit, then price increases by 50%."""
    # Add initial price at $10/unit
    twr.add_price("nvidia", 10, timestamp=at(0))

    # Alice buys $100 worth of nvidia (= 10 units)
    twr.add_cashflow("Alice", "nvidia", money=100, timestamp=at(1))

    # Price increases by 50% to $15/unit
    twr.add_price("nvidia", 15, timestamp=at(2))

    # Verify product table
    products = twr._execute_query("SELECT name FROM product", fetch=True)
    assert products == [{"name": "nvidia"}]

    # Verify product_price table
    prices = twr._execute_query(
        "SELECT p.name, pp.price, pp.timestamp FROM product_price pp JOIN product p ON pp.product_id = p.id ORDER BY pp.timestamp",
        fetch=True,
    )
    assert prices == [
        {"name": "nvidia", "price": Decimal("10.000000"), "timestamp": at(0)},
        {"name": "nvidia", "price": Decimal("15.000000"), "timestamp": at(2)},
    ]

    # Verify user table
    users = twr._execute_query('SELECT name FROM "user"', fetch=True)
    assert users == [{"name": "Alice"}]

    # Verify user_cash_flow table
    cash_flows = twr._execute_query(
        """
        SELECT u.name as user_name,
               p.name as product_name,
               ucf.units,
               ucf.deposit,
               ucf.cumulative_units,
               ucf.cumulative_deposits,
               ucf.period_return,
               ucf.cumulative_twr_factor,
               ucf.timestamp
        FROM user_cash_flow ucf
            JOIN "user" u ON ucf.user_id = u.id
            JOIN product p ON ucf.product_id = p.id
        """,
        fetch=True,
    )
    assert cash_flows == [
        {
            "user_name": "Alice",
            "product_name": "nvidia",
            "units": Decimal("10.000000"),
            "deposit": Decimal("100.000000"),
            "cumulative_units": Decimal("10.000000"),
            "cumulative_deposits": Decimal("100.000000"),
            "period_return": Decimal("0.000000"),
            "cumulative_twr_factor": Decimal("1.000000"),
            "timestamp": at(1),
        }
    ]

    # Verify user_product_timeline view
    user_product_timeline = twr._execute_query(
        """
        SELECT u.name as user_name,
               p.name as product_name,
               upt.holdings,
               upt.net_deposits,
               upt.current_price,
               upt.current_value,
               upt.current_twr,
               upt.timestamp
        FROM user_product_timeline upt
        JOIN "user" u ON upt.user_id = u.id
        JOIN product p ON upt.product_id = p.id
        ORDER BY timestamp
        """,
        fetch=True,
    )
    assert user_product_timeline == [
        {
            "user_name": "Alice",
            "product_name": "nvidia",
            "holdings": Decimal("10.000000"),
            "net_deposits": Decimal("100.000000"),
            "current_price": Decimal("10.000000"),
            "current_value": Decimal("100.000000"),
            "current_twr": Decimal("0.000000"),
            "timestamp": at(1),
        },
        {
            "user_name": "Alice",
            "product_name": "nvidia",
            "holdings": Decimal("10.000000"),
            "net_deposits": Decimal("100.000000"),
            "current_price": Decimal("15.000000"),
            "current_value": Decimal("150.000000"),
            "current_twr": Decimal("0.500000"),  # 50% return
            "timestamp": at(2),
        },
    ]

    # Verify user_timeline view
    user_timeline = twr._execute_query(
        """
        SELECT u.name as user_name, ut.total_net_deposits, ut.total_value, ut.value_weighted_twr, ut.timestamp
        FROM user_timeline ut
        JOIN "user" u ON ut.user_id = u.id
        ORDER BY timestamp""",
        fetch=True,
    )
    assert user_timeline == [
        {
            "user_name": "Alice",
            "total_net_deposits": Decimal("100.000000"),
            "total_value": Decimal("100.000000"),
            "value_weighted_twr": Decimal("0.000000"),
            "timestamp": at(1),
        },
        {
            "user_name": "Alice",
            "total_net_deposits": Decimal("100.000000"),
            "total_value": Decimal("150.000000"),
            "value_weighted_twr": Decimal("0.500000"),  # 50% return
            "timestamp": at(2),
        },
    ]


def test_price_up_then_down(twr, at):
    """Test: $100 at $10/unit, price goes to $15, then back down to $10."""
    # Add initial price at $10/unit
    twr.add_price("nvidia", 10, timestamp=at(0))

    # Alice buys $100 worth of nvidia (= 10 units)
    twr.add_cashflow("Alice", "nvidia", money=100, timestamp=at(1))

    # Price increases by 50% to $15/unit
    twr.add_price("nvidia", 15, timestamp=at(2))

    # Price goes back down to $10/unit
    twr.add_price("nvidia", 10, timestamp=at(3))

    # Verify product table
    products = twr._execute_query("SELECT name FROM product", fetch=True)
    assert products == [{"name": "nvidia"}]

    # Verify product_price table
    prices = twr._execute_query(
        "SELECT p.name, pp.price, pp.timestamp FROM product_price pp JOIN product p ON pp.product_id = p.id ORDER BY pp.timestamp",
        fetch=True,
    )
    assert prices == [
        {"name": "nvidia", "price": Decimal("10.000000"), "timestamp": at(0)},
        {"name": "nvidia", "price": Decimal("15.000000"), "timestamp": at(2)},
        {"name": "nvidia", "price": Decimal("10.000000"), "timestamp": at(3)},
    ]

    # Verify user table
    users = twr._execute_query('SELECT name FROM "user"', fetch=True)
    assert users == [{"name": "Alice"}]

    # Verify user_cash_flow table
    cash_flows = twr._execute_query(
        """
        SELECT u.name as user_name,
               p.name as product_name,
               ucf.units,
               ucf.deposit,
               ucf.cumulative_units,
               ucf.cumulative_deposits,
               ucf.period_return,
               ucf.cumulative_twr_factor,
               ucf.timestamp
        FROM user_cash_flow ucf
            JOIN "user" u ON ucf.user_id = u.id
            JOIN product p ON ucf.product_id = p.id
        """,
        fetch=True,
    )
    assert cash_flows == [
        {
            "user_name": "Alice",
            "product_name": "nvidia",
            "units": Decimal("10.000000"),
            "deposit": Decimal("100.000000"),
            "cumulative_units": Decimal("10.000000"),
            "cumulative_deposits": Decimal("100.000000"),
            "period_return": Decimal("0.000000"),
            "cumulative_twr_factor": Decimal("1.000000"),
            "timestamp": at(1),
        }
    ]

    # Verify user_product_timeline view
    user_product_timeline = twr._execute_query(
        """
        SELECT u.name as user_name,
               p.name as product_name,
               upt.holdings,
               upt.net_deposits,
               upt.current_price,
               upt.current_value,
               upt.current_twr,
               upt.timestamp
        FROM user_product_timeline upt
        JOIN "user" u ON upt.user_id = u.id
        JOIN product p ON upt.product_id = p.id
        ORDER BY timestamp
        """,
        fetch=True,
    )
    assert user_product_timeline == [
        {
            "user_name": "Alice",
            "product_name": "nvidia",
            "holdings": Decimal("10.000000"),
            "net_deposits": Decimal("100.000000"),
            "current_price": Decimal("10.000000"),
            "current_value": Decimal("100.000000"),
            "current_twr": Decimal("0.000000"),
            "timestamp": at(1),
        },
        {
            "user_name": "Alice",
            "product_name": "nvidia",
            "holdings": Decimal("10.000000"),
            "net_deposits": Decimal("100.000000"),
            "current_price": Decimal("15.000000"),
            "current_value": Decimal("150.000000"),
            "current_twr": Decimal("0.500000"),  # 50% return at peak
            "timestamp": at(2),
        },
        {
            "user_name": "Alice",
            "product_name": "nvidia",
            "holdings": Decimal("10.000000"),
            "net_deposits": Decimal("100.000000"),
            "current_price": Decimal("10.000000"),
            "current_value": Decimal("100.000000"),
            "current_twr": Decimal("0.000000"),  # Back to 0% return
            "timestamp": at(3),
        },
    ]

    # Verify user_timeline view
    user_timeline = twr._execute_query(
        """
        SELECT u.name as user_name, ut.total_net_deposits, ut.total_value, ut.value_weighted_twr, ut.timestamp
        FROM user_timeline ut
        JOIN "user" u ON ut.user_id = u.id
        ORDER BY timestamp""",
        fetch=True,
    )
    assert user_timeline == [
        {
            "user_name": "Alice",
            "total_net_deposits": Decimal("100.000000"),
            "total_value": Decimal("100.000000"),
            "value_weighted_twr": Decimal("0.000000"),
            "timestamp": at(1),
        },
        {
            "user_name": "Alice",
            "total_net_deposits": Decimal("100.000000"),
            "total_value": Decimal("150.000000"),
            "value_weighted_twr": Decimal("0.500000"),  # 50% return at peak
            "timestamp": at(2),
        },
        {
            "user_name": "Alice",
            "total_net_deposits": Decimal("100.000000"),
            "total_value": Decimal("100.000000"),
            "value_weighted_twr": Decimal("0.000000"),  # Back to 0% return
            "timestamp": at(3),
        },
    ]
