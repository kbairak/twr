import datetime
from dataclasses import dataclass, field
from decimal import Decimal
from uuid import UUID, uuid4


@dataclass
class PriceUpdate:
    product_id: UUID
    timestamp: datetime.datetime
    price: Decimal


@dataclass
class Cashflow:
    user_id: UUID
    product_id: UUID
    timestamp: datetime.datetime

    id: UUID = field(default_factory=uuid4)
    units_delta: Decimal | None = None
    execution_price: Decimal | None = None
    execution_money: Decimal | None = None
    user_money: Decimal | None = None
    fees: Decimal | None = None

    def __post_init__(self):
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


@dataclass
class CumulativeCashflow:
    cashflow_id: UUID
    user_id: UUID
    product_id: UUID
    timestamp: datetime.datetime

    units: Decimal = Decimal("0.000000")
    net_investment: Decimal = Decimal("0.000000")
    deposits: Decimal = Decimal("0.000000")
    withdrawals: Decimal = Decimal("0.000000")
    fees: Decimal = Decimal("0.000000")
    buy_units: Decimal = Decimal("0.000000")
    sell_units: Decimal = Decimal("0.000000")
    buy_cost: Decimal = Decimal("0.000000")
    sell_proceeds: Decimal = Decimal("0.000000")


@dataclass
class UserProductTimelineEntry:
    user_id: UUID
    product_id: UUID
    timestamp: datetime.datetime

    units: Decimal = Decimal("0.000000")
    net_investment: Decimal = Decimal("0.000000")
    deposits: Decimal = Decimal("0.000000")
    withdrawals: Decimal = Decimal("0.000000")
    fees: Decimal = Decimal("0.000000")
    buy_units: Decimal = Decimal("0.000000")
    sell_units: Decimal = Decimal("0.000000")
    buy_cost: Decimal = Decimal("0.000000")
    sell_proceeds: Decimal = Decimal("0.000000")

    market_value: Decimal = Decimal("0.000000")


@dataclass
class UserTimelineEntry:
    user_id: UUID
    timestamp: datetime.datetime

    net_investment: Decimal
    market_value: Decimal

    deposits: Decimal
    withdrawals: Decimal
    fees: Decimal
    buy_units: Decimal
    sell_units: Decimal
    buy_cost: Decimal
    sell_proceeds: Decimal
    cost_basis: Decimal
    sell_basis: Decimal
