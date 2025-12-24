import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, ClassVar
from uuid import UUID, uuid4


class BasePerformanceEntry(ABC):
    """Base class for dataclasses that can be converted to tuples for database insertion"""

    timestamp: datetime.datetime

    @abstractmethod
    def to_tuple(self) -> tuple[Any, ...]:
        """Convert to tuple for database insertion"""
        raise NotImplementedError


@dataclass
class PriceUpdate(BasePerformanceEntry):
    product_id: UUID
    timestamp: datetime.datetime
    price: Decimal

    def to_tuple(self) -> tuple[UUID, datetime.datetime, Decimal]:
        return (self.product_id, self.timestamp, self.price)


@dataclass
class Cashflow(BasePerformanceEntry):
    DATABASE_FIELDS: ClassVar[tuple[str, ...]] = (
        "user_id",
        "product_id",
        "timestamp",
        "id",
        "units_delta",
        "execution_money",
        "user_money",
    )

    user_id: UUID
    product_id: UUID
    timestamp: datetime.datetime

    id: UUID = field(default_factory=uuid4)
    units_delta: Decimal | None = None
    execution_money: Decimal | None = None
    user_money: Decimal | None = None
    execution_price: Decimal | None = None
    fees: Decimal | None = None

    def __post_init__(self) -> None:
        units_delta, execution_price, execution_money, user_money, fees = (
            self.units_delta,
            self.execution_price,
            self.execution_money,
            self.user_money,
            self.fees,
        )
        # Fill
        while True:
            # Repeate until no change
            found_missing, changed = False, False
            if units_delta is None:
                found_missing = True
                if execution_money is not None and execution_price is not None:
                    self.units_delta = units_delta = execution_money / execution_price
                    changed = True
            if execution_price is None:
                found_missing = True
                if execution_money is not None and units_delta is not None:
                    self.execution_price = execution_price = execution_money / units_delta
                    changed = True
            if execution_money is None:
                found_missing = True
                if units_delta is not None and execution_price is not None:
                    self.execution_money = execution_money = units_delta * execution_price
                    changed = True
                elif user_money is not None and fees is not None:
                    self.execution_money = execution_money = user_money - fees
                    changed = True
            if user_money is None:
                found_missing = True
                if execution_money is not None and fees is not None:
                    self.user_money = user_money = execution_money + fees
                    changed = True
            if fees is None:
                found_missing = True
                if execution_money is not None and user_money is not None:
                    self.fees = fees = user_money - execution_money
                    changed = True
            if not found_missing:
                break
            if not changed:
                raise ValueError("Cannot derive mising values")

        # Validate
        assert units_delta is not None
        assert execution_price is not None
        assert execution_money is not None
        assert user_money is not None
        assert fees is not None
        if abs(units_delta * execution_price - execution_money) >= Decimal("0.01"):
            raise ValueError(
                f"Invalid cashflow, {units_delta=} * {execution_price} != {execution_money}"
            )
        if abs(execution_money + fees - user_money) >= Decimal("0.01"):
            raise ValueError(f"Invalid cashflow, {execution_money=} + {fees=} != {user_money}")

    def to_tuple(
        self,
    ) -> tuple[
        UUID, UUID, datetime.datetime, UUID, Decimal | None, Decimal | None, Decimal | None
    ]:
        """Return minimal tuple for database storage.

        Only stores units_delta, execution_money, user_money.
        execution_price and fees are derived in __post_init__ when reading.
        """
        return (
            self.user_id,
            self.product_id,
            self.timestamp,
            self.id,
            self.units_delta,
            self.execution_money,
            self.user_money,
        )


@dataclass
class CumulativeCashflow(BasePerformanceEntry):
    DATABASE_FIELDS: ClassVar[tuple[str, ...]] = (
        "cashflow_id",
        "user_id",
        "product_id",
        "timestamp",
        "deposits",
        "withdrawals",
        "buy_units",
        "sell_units",
        "buy_cost",
        "sell_proceeds",
    )

    cashflow_id: UUID
    user_id: UUID
    product_id: UUID
    timestamp: datetime.datetime

    # Stored fields (6 fields)
    deposits: Decimal = Decimal("0.000000")
    withdrawals: Decimal = Decimal("0.000000")
    buy_units: Decimal = Decimal("0.000000")
    sell_units: Decimal = Decimal("0.000000")
    buy_cost: Decimal = Decimal("0.000000")
    sell_proceeds: Decimal = Decimal("0.000000")

    # Derived fields (3 fields) - computed in __post_init__
    units: Decimal = field(init=False, default=Decimal("0.000000"))
    net_investment: Decimal = field(init=False, default=Decimal("0.000000"))
    fees: Decimal = field(init=False, default=Decimal("0.000000"))

    def __post_init__(self) -> None:
        # Derive the 3 computed fields
        self.units = self.buy_units - self.sell_units
        self.net_investment = self.deposits - self.withdrawals
        self.fees = self.net_investment - (self.buy_cost - self.sell_proceeds)

    def to_tuple(
        self,
    ) -> tuple[
        UUID, UUID, UUID, datetime.datetime, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal
    ]:
        """Return minimal tuple for database storage.

        Only stores deposits, withdrawals, buy_units, sell_units, buy_cost, sell_proceeds.
        units, net_investment, and fees are derived in __post_init__ when reading.
        """
        return (
            self.cashflow_id,
            self.user_id,
            self.product_id,
            self.timestamp,
            self.deposits,
            self.withdrawals,
            self.buy_units,
            self.sell_units,
            self.buy_cost,
            self.sell_proceeds,
        )


@dataclass
class UserProductTimelineEntry(BasePerformanceEntry):
    DATABASE_FIELDS: ClassVar[tuple[str, ...]] = (
        "user_id",
        "product_id",
        "timestamp",
        "deposits",
        "withdrawals",
        "buy_units",
        "sell_units",
        "buy_cost",
        "sell_proceeds",
        "market_value",
    )

    user_id: UUID
    product_id: UUID
    timestamp: datetime.datetime

    # Stored fields (6 fields)
    deposits: Decimal = Decimal("0.000000")
    withdrawals: Decimal = Decimal("0.000000")
    buy_units: Decimal = Decimal("0.000000")
    sell_units: Decimal = Decimal("0.000000")
    buy_cost: Decimal = Decimal("0.000000")
    sell_proceeds: Decimal = Decimal("0.000000")

    market_value: Decimal = Decimal("0.000000")

    # Derived fields (5 fields) - computed in __post_init__
    units: Decimal = field(init=False, default=Decimal("0.000000"))
    net_investment: Decimal = field(init=False, default=Decimal("0.000000"))
    fees: Decimal = field(init=False, default=Decimal("0.000000"))
    avg_buy_price: Decimal = field(init=False, default=Decimal("0.000000"))
    avg_sell_price: Decimal = field(init=False, default=Decimal("0.000000"))

    def __post_init__(self) -> None:
        # Derive the 5 computed fields
        self.units = self.buy_units - self.sell_units
        self.net_investment = self.deposits - self.withdrawals
        self.fees = self.net_investment - (self.buy_cost - self.sell_proceeds)
        self.avg_buy_price = (
            (self.buy_cost / self.buy_units) if self.buy_units != 0 else Decimal("0.000000")
        )
        self.avg_sell_price = (
            (self.sell_proceeds / self.sell_units) if self.sell_units != 0 else Decimal("0.000000")
        )

    def to_tuple(
        self,
    ) -> tuple[
        UUID,
        UUID,
        datetime.datetime,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
    ]:
        return (
            self.user_id,
            self.product_id,
            self.timestamp,
            self.deposits,
            self.withdrawals,
            self.buy_units,
            self.sell_units,
            self.buy_cost,
            self.sell_proceeds,
            self.market_value,
        )


@dataclass
class UserTimelineEntry(BasePerformanceEntry):
    DATABASE_FIELDS: ClassVar[tuple[str, ...]] = (
        "user_id",
        "timestamp",
        "market_value",
        "deposits",
        "withdrawals",
        "buy_cost",
        "sell_proceeds",
        "cost_basis",
        "sell_basis",
    )

    user_id: UUID
    timestamp: datetime.datetime

    # Stored fields (7 fields)
    market_value: Decimal = Decimal("0.000000")
    deposits: Decimal = Decimal("0.000000")
    withdrawals: Decimal = Decimal("0.000000")
    buy_cost: Decimal = Decimal("0.000000")
    sell_proceeds: Decimal = Decimal("0.000000")
    cost_basis: Decimal = Decimal("0.000000")
    sell_basis: Decimal = Decimal("0.000000")

    # Derived fields (2 fields) - computed in __post_init__
    net_investment: Decimal = Decimal("0.000000")
    fees: Decimal = Decimal("0.000000")

    def __post_init__(self) -> None:
        self.net_investment = self.deposits - self.withdrawals
        self.fees = self.net_investment - (self.buy_cost - self.sell_proceeds)

    def to_tuple(
        self,
    ) -> tuple[
        UUID, datetime.datetime, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal
    ]:
        return (
            self.user_id,
            self.timestamp,
            self.market_value,
            self.deposits,
            self.withdrawals,
            self.buy_cost,
            self.sell_proceeds,
            self.cost_basis,
            self.sell_basis,
        )
