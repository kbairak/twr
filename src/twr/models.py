import bisect
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
    _timestamps: list[datetime.datetime] = field(default_factory=list)

    def price_at(self, timestamp: datetime.datetime) -> float | Decimal | None:
        if not self.price_updates:
            return None

        # Binary search to find the rightmost price update with timestamp <= target
        # We need a custom key function since we're searching by timestamp
        idx = bisect.bisect_right(self._timestamps, timestamp)

        # If idx is 0, all price updates are after the target timestamp
        if idx == 0:
            return None

        # Return the price from the last update before or at the timestamp
        return self.price_updates[idx - 1].price


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
