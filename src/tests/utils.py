import datetime
from decimal import Decimal

from performance.models import UserProductTimelineEntry, UserTimelineEntry


def parse_time(text: str) -> datetime.datetime:
    t = datetime.datetime.strptime(text, "%H:%M")
    return datetime.datetime.now(datetime.timezone.utc).replace(
        hour=t.hour, minute=t.minute, second=0, microsecond=0
    )


def user_product_timeline_eq(
    actual: UserProductTimelineEntry,
    expected: UserProductTimelineEntry,
    tolerance: Decimal = Decimal("0.00001"),
) -> bool:
    """Compare two UserProductTimelineEntry objects with tolerance for Decimal rounding.

    Allows small differences which can occur from caching avg_buy_price/avg_sell_price at 6-digit
    precision.
    """
    return (
        actual.user_id == expected.user_id
        and actual.product_id == expected.product_id
        and actual.timestamp == expected.timestamp
        and abs(actual.units - expected.units) <= tolerance
        and abs(actual.net_investment - expected.net_investment) <= tolerance
        and abs(actual.deposits - expected.deposits) <= tolerance
        and abs(actual.withdrawals - expected.withdrawals) <= tolerance
        and abs(actual.fees - expected.fees) <= tolerance
        and abs(actual.buy_units - expected.buy_units) <= tolerance
        and abs(actual.sell_units - expected.sell_units) <= tolerance
        and abs(actual.buy_cost - expected.buy_cost) <= tolerance
        and abs(actual.sell_proceeds - expected.sell_proceeds) <= tolerance
        and abs(actual.avg_buy_price - expected.avg_buy_price) <= tolerance
        and abs(actual.avg_sell_price - expected.avg_sell_price) <= tolerance
        and abs(actual.market_value - expected.market_value) <= tolerance
    )


def user_timeline_eq(
    actual: UserTimelineEntry, expected: UserTimelineEntry, tolerance: Decimal = Decimal("0.00001")
) -> bool:
    """Compare two UserTimelineEntry objects with tolerance for Decimal rounding.

    Allows small differences in cost_basis and sell_basis which can accumulate
    rounding errors from cached avg_buy_price values.
    """
    return (
        actual.user_id == expected.user_id
        and actual.timestamp == expected.timestamp
        and abs(actual.net_investment - expected.net_investment) <= tolerance
        and abs(actual.market_value - expected.market_value) <= tolerance
        and abs(actual.deposits - expected.deposits) <= tolerance
        and abs(actual.withdrawals - expected.withdrawals) <= tolerance
        and abs(actual.fees - expected.fees) <= tolerance
        and abs(actual.buy_cost - expected.buy_cost) <= tolerance
        and abs(actual.sell_proceeds - expected.sell_proceeds) <= tolerance
        and abs(actual.cost_basis - expected.cost_basis) <= tolerance
        and abs(actual.sell_basis - expected.sell_basis) <= tolerance
    )
