import datetime
from typing import Any
from unittest import mock

from twr.models import (
    Cashflow,
    CumulativeCashflow,
    PriceUpdate,
    UserProductTimelineBusinessEvent,
    UserTimelineBusinessEvent,
)


def parse_time(text: str) -> datetime.datetime:
    t = datetime.datetime.strptime(text, "%H:%M")
    return datetime.datetime.now(datetime.timezone.utc).replace(
        hour=t.hour, minute=t.minute, second=0, microsecond=0
    )


def map_to_model(
    dct: dict[str, Any],
) -> (
    PriceUpdate
    | Cashflow
    | CumulativeCashflow
    | UserProductTimelineBusinessEvent
    | UserTimelineBusinessEvent
    | dict[str, Any]
):
    try:
        return PriceUpdate(**dct)
    except TypeError:
        pass
    try:
        return Cashflow(**dct)
    except TypeError:
        pass
    try:
        return CumulativeCashflow(**dct)
    except TypeError:
        pass
    try:
        return UserProductTimelineBusinessEvent(**dct)
    except TypeError:
        pass
    try:
        return UserTimelineBusinessEvent(**dct)
    except TypeError:
        pass
    return dct


def mock_pu(**kwargs: Any) -> PriceUpdate:
    return PriceUpdate(
        **{"product_id": mock.ANY, "timestamp": mock.ANY, "price": mock.ANY, **kwargs}
    )


def mock_cf(**kwargs: Any) -> Cashflow:
    return Cashflow(
        **{
            "user_id": mock.ANY,
            "product_id": mock.ANY,
            "timestamp": mock.ANY,
            "units_delta": mock.ANY,
            "execution_price": mock.ANY,
            "user_money": mock.ANY,
            "id": mock.ANY,
            **kwargs,
        }
    )


def mock_ccf(**kwargs: Any) -> CumulativeCashflow:
    return CumulativeCashflow(
        **{
            "user_id": mock.ANY,
            "product_id": mock.ANY,
            "timestamp": mock.ANY,
            "buy_units": mock.ANY,
            "sell_units": mock.ANY,
            "buy_cost": mock.ANY,
            "sell_proceeds": mock.ANY,
            "deposits": mock.ANY,
            "withdrawals": mock.ANY,
            **kwargs,
        }
    )


def mock_uptb(**kwargs: Any) -> UserProductTimelineBusinessEvent:
    return UserProductTimelineBusinessEvent(
        **{
            "user_id": mock.ANY,
            "product_id": mock.ANY,
            "timestamp": mock.ANY,
            "buy_units": mock.ANY,
            "sell_units": mock.ANY,
            "buy_cost": mock.ANY,
            "sell_proceeds": mock.ANY,
            "deposits": mock.ANY,
            "withdrawals": mock.ANY,
            "units": mock.ANY,
            "net_investment": mock.ANY,
            "fees": mock.ANY,
            "price": mock.ANY,
            "market_value": mock.ANY,
            "avg_buy_cost": mock.ANY,
            "cost_basis": mock.ANY,
            "unrealized_returns": mock.ANY,
            **kwargs,
        }
    )


def mock_utb(**kwargs: Any) -> UserTimelineBusinessEvent:
    return UserTimelineBusinessEvent(
        **{
            "timestamp": mock.ANY,
            "deposits": mock.ANY,
            "withdrawals": mock.ANY,
            "buy_cost": mock.ANY,
            "sell_proceeds": mock.ANY,
            "buy_units": mock.ANY,
            "sell_units": mock.ANY,
            "net_investment": mock.ANY,
            "fees": mock.ANY,
            "avg_buy_cost": mock.ANY,
            "market_value": mock.ANY,
            "cost_basis": mock.ANY,
            "unrealized_returns": mock.ANY,
            **kwargs,
        }
    )
