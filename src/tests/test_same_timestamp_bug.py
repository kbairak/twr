"""Test for same-timestamp buy and sell aggregation bug."""

import datetime
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Protocol

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


def test_buy_and_sell_at_same_timestamp(
    query: QueryType,
    product: Callable[[str], str],
    user: Callable[[str], str],
    insert: Callable[[PriceUpdate | Cashflow], None],
) -> None:
    """
    Test that when both buys and sells occur at the same timestamp,
    they are aggregated correctly (not netted out).

    This test reproduces a bug in the _fresh_cf function where aggregating
    by timestamp causes buys and sells to be incorrectly netted together.
    """
    # Clear cache tables to avoid interference from other tests
    query("TRUNCATE TABLE cumulative_cashflow_cache CASCADE")

    # Setup: Insert price data
    insert(
        PriceUpdate(
            product("AAPL"),
            datetime.datetime(2025, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
            100.00,
        )
    )
    insert(
        PriceUpdate(
            product("AAPL"),
            datetime.datetime(2025, 1, 1, 12, 0, tzinfo=datetime.timezone.utc),
            105.00,
        )
    )

    # Insert a buy and a sell at the SAME timestamp (12:00)
    # Buy 10 units @ 100 with fees=10: user_money = 1010
    # Sell 5 units @ 105 with fees=5: user_money = -520 (proceeds=525, fees=5)
    insert(
        Cashflow(
            user("Alice"),
            product("AAPL"),
            datetime.datetime(2025, 1, 1, 12, 0, tzinfo=datetime.timezone.utc),
            10,
            100.00,
            1010.00,
        )
    )
    insert(
        Cashflow(
            user("Alice"),
            product("AAPL"),
            datetime.datetime(2025, 1, 1, 12, 0, tzinfo=datetime.timezone.utc),
            -5,
            105.00,
            -520.00,
        )
    )

    # Query cumulative cashflow
    rows = query(
        """
        SELECT user_id, product_id, timestamp,
               buy_units, sell_units, buy_cost, sell_proceeds, deposits, withdrawals,
               buy_units - sell_units AS units_held,
               deposits - withdrawals AS net_investment,
               deposits - buy_cost + withdrawals - sell_proceeds AS fees
        FROM cumulative_cashflow(NULL, NULL)
        ORDER BY timestamp
        """
    )

    # Expected behavior: buys and sells should be tracked separately
    # buy_units = 10, sell_units = 5, buy_cost = 1000, sell_proceeds = 525
    # deposits = 1010, withdrawals = 520
    # fees = 1010 - 1000 + 520 - 525 = 5

    expected = [
        {
            "user_id": user("Alice"),
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(2025, 1, 1, 12, 0, tzinfo=datetime.timezone.utc),
            "buy_units": Decimal("10.000000"),  # NOT 5
            "sell_units": Decimal("5.000000"),  # NOT 0
            "buy_cost": Decimal("1000.000000"),  # NOT 475
            "sell_proceeds": Decimal("525.000000"),  # NOT 0
            "deposits": Decimal("1010.000000"),  # NOT 490
            "withdrawals": Decimal("520.000000"),  # NOT 0
            "units_held": Decimal("5.000000"),
            "net_investment": Decimal("490.000000"),
            "fees": Decimal("5.000000"),  # NOT 490
        }
    ]

    assert rows == expected, f"Expected {expected}, but got {rows}"


def test_multiple_buys_and_sells_at_same_timestamp(
    query: QueryType,
    product: Callable[[str], str],
    user: Callable[[str], str],
    insert: Callable[[PriceUpdate | Cashflow], None],
) -> None:
    """
    Test aggregation with multiple buys and sells at the same timestamp.

    This is a more complex case where we have:
    - 2 buys at the same timestamp
    - 1 sell at the same timestamp
    """
    # Clear cache tables to avoid interference from other tests
    query("TRUNCATE TABLE cumulative_cashflow_cache CASCADE")

    # Setup: Insert price data
    insert(
        PriceUpdate(
            product("AAPL"),
            datetime.datetime(2025, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
            100.00,
        )
    )

    # Insert 2 buys and 1 sell at the SAME timestamp (10:00)
    # Buy 10 units @ 100: user_money = 1005 (fees=5)
    # Buy 8 units @ 102: user_money = 820 (fees=4)
    # Sell 3 units @ 105: user_money = -312 (fees=3)
    insert(
        Cashflow(
            user("Alice"),
            product("AAPL"),
            datetime.datetime(2025, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
            10,
            100.00,
            1005.00,
        )
    )
    insert(
        Cashflow(
            user("Alice"),
            product("AAPL"),
            datetime.datetime(2025, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
            8,
            102.00,
            820.00,
        )
    )
    insert(
        Cashflow(
            user("Alice"),
            product("AAPL"),
            datetime.datetime(2025, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
            -3,
            105.00,
            -312.00,
        )
    )

    # Query cumulative cashflow
    rows = query(
        """
        SELECT user_id, product_id, timestamp,
               buy_units, sell_units, buy_cost, sell_proceeds, deposits, withdrawals,
               buy_units - sell_units AS units_held,
               deposits - withdrawals AS net_investment,
               deposits - buy_cost + withdrawals - sell_proceeds AS fees
        FROM cumulative_cashflow(NULL, NULL)
        ORDER BY timestamp
        """
    )

    # Expected behavior:
    # buy_units = 10 + 8 = 18
    # sell_units = 3
    # buy_cost = 1000 + 816 = 1816
    # sell_proceeds = 315
    # deposits = 1005 + 820 = 1825
    # withdrawals = 312
    # fees = 1825 - 1816 + 312 - 315 = 6

    expected = [
        {
            "user_id": user("Alice"),
            "product_id": product("AAPL"),
            "timestamp": datetime.datetime(2025, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
            "buy_units": Decimal("18.000000"),
            "sell_units": Decimal("3.000000"),
            "buy_cost": Decimal("1816.000000"),
            "sell_proceeds": Decimal("315.000000"),
            "deposits": Decimal("1825.000000"),
            "withdrawals": Decimal("312.000000"),
            "units_held": Decimal("15.000000"),
            "net_investment": Decimal("1513.000000"),
            "fees": Decimal("6.000000"),
        }
    ]

    assert rows == expected, f"Expected {expected}, but got {rows}"
