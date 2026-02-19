import datetime
import uuid
from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class PriceUpdate:
    product_id: uuid.UUID | str
    timestamp: datetime.datetime
    price: float | Decimal


@dataclass
class Product:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    price_updates: list[PriceUpdate] = field(default_factory=list)

    def price_at(self, timestamp: datetime.datetime) -> float | Decimal | None:
        try:
            last_price_update = self.price_updates[0]
        except IndexError:
            return None
        for price_update in self.price_updates[1:]:
            if price_update.timestamp > timestamp:
                break
        if last_price_update.timestamp > timestamp:
            return None
        return last_price_update.price


@dataclass
class Cashflow:
    user_id: uuid.UUID | str
    product_id: uuid.UUID | str
    timestamp: datetime.datetime
    units_delta: float | Decimal
    execution_price: float | Decimal
    user_money: float | Decimal
    id: uuid.UUID | str = field(default_factory=uuid.uuid4)


@dataclass
class Investment:
    units: float = 0.0


@dataclass
class User:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    cashflows: list[Cashflow] = field(default_factory=list)

    @property
    def investments(self) -> dict[uuid.UUID | str, Investment]:
        result = {}
        for cashflow in self.cashflows:
            result.setdefault(cashflow.product_id, Investment())
            result[cashflow.product_id].units += cashflow.units_delta
        return result


@dataclass
class CumulativeCashflow:
    user_id: uuid.UUID | str
    product_id: uuid.UUID | str
    timestamp: datetime.datetime
    buy_units: float | Decimal
    sell_units: float | Decimal
    buy_cost: float | Decimal
    sell_proceeds: float | Decimal
    deposits: float | Decimal
    withdrawals: float | Decimal


@dataclass
class UserProductTimelineBusinessEvent:
    user_id: uuid.UUID | str
    product_id: uuid.UUID | str
    timestamp: datetime.datetime
    buy_units: float | Decimal
    sell_units: float | Decimal
    buy_cost: float | Decimal
    sell_proceeds: float | Decimal
    deposits: float | Decimal
    withdrawals: float | Decimal
    units: float | Decimal
    net_investment: float | Decimal
    fees: float | Decimal
    price: float | Decimal
    market_value: float | Decimal
    avg_buy_cost: float | Decimal
    cost_basis: float | Decimal
    unrealized_returns: float | Decimal


@dataclass
class UserTimelineBusinessEvent:
    timestamp: datetime.datetime
    deposits: float | Decimal
    withdrawals: float | Decimal
    buy_cost: float | Decimal
    sell_proceeds: float | Decimal
    buy_units: float | Decimal
    sell_units: float | Decimal
    net_investment: float | Decimal
    fees: float | Decimal
    avg_buy_cost: float | Decimal
    market_value: float | Decimal
    cost_basis: float | Decimal
    unrealized_returns: float | Decimal
