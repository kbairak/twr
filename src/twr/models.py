import datetime
import uuid
from dataclasses import dataclass, field


@dataclass
class PriceUpdate:
    product_id: uuid.UUID
    timestamp: datetime.datetime
    price: float


@dataclass
class Product:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    price_updates: list[PriceUpdate] = field(default_factory=list)

    def price_at(self, timestamp: datetime.datetime) -> float | None:
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
    user_id: uuid.UUID
    product_id: uuid.UUID
    timestamp: datetime.datetime
    units_delta: float
    execution_price: float
    user_money: float


@dataclass
class Investment:
    units: float = 0.0


@dataclass
class User:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    cashflows: list[Cashflow] = field(default_factory=list)

    @property
    def investments(self) -> dict[uuid.UUID, Investment]:
        result = {}
        for cashflow in self.cashflows:
            result.setdefault(cashflow.product_id, Investment())
            result[cashflow.product_id].units += cashflow.units_delta
        return result
