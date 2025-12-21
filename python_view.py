from dataclasses import asdict, dataclass
import datetime
from typing import Generator
from uuid import UUID


@dataclass
class PriceUpdate:
    product_id: UUID
    timestamp: datetime.datetime
    price: float


@dataclass
class Cashflow:
    id: UUID
    user_id: UUID
    product_id: UUID
    timestamp: datetime.datetime

    units_delta: float
    execution_price: float
    execution_money: float
    user_money: float
    fees: float


@dataclass
class CumulativeCashflow:
    cashflow_id: UUID
    user_id: UUID
    product_id: UUID
    timestamp: datetime.datetime

    units_held: float = 0.0
    net_investment: float = 0.0
    deposits: float = 0.0
    withdrawals: float = 0.0
    fees: float = 0.0
    buy_units: float = 0.0
    sell_units: float = 0.0
    buy_cost: float = 0.0
    sell_proceeds: float = 0.0


@dataclass
class UserProductTimelineEntry:
    user_id: UUID
    product_id: UUID
    timestamp: datetime.datetime

    units_held: float = 0.0
    net_investment: float = 0.0
    deposits: float = 0.0
    withdrawals: float = 0.0
    fees: float = 0.0
    buy_units: float = 0.0
    sell_units: float = 0.0
    buy_cost: float = 0.0
    sell_proceeds: float = 0.0

    market_value: float = 0.0


@dataclass
class UserTimelineEntry:
    user_id: UUID
    timestamp: datetime.datetime

    net_investment: float
    market_value: float

    deposits: float
    withdrawals: float
    fees: float
    buy_cost: float
    sell_proceeds: float
    avg_buy_price: float
    avg_sell_price: float


def generate_cumulative_cashflows(
    sorted_cashflows: list[Cashflow],
) -> Generator[CumulativeCashflow, None, None]:
    # Up-to-date cumulative data per user_id, product_id
    cumulative_data: dict[tuple[UUID, UUID], CumulativeCashflow] = {}

    for cf in sorted_cashflows:
        start = cumulative_data.get(
            (cf.user_id, cf.product_id),
            CumulativeCashflow(
                cashflow_id=cf.id,
                user_id=cf.user_id,
                product_id=cf.product_id,
                timestamp=cf.timestamp,
            ),
        )
        new = CumulativeCashflow(
            cashflow_id=cf.id,
            user_id=cf.user_id,
            product_id=cf.product_id,
            timestamp=cf.timestamp,
            units_held=start.units_held + cf.units_delta,
            net_investment=start.net_investment + cf.user_money,
            deposits=start.deposits + (cf.user_money if cf.units_delta > 0.0 else 0.0),
            withdrawals=start.withdrawals + (-cf.user_money if cf.units_delta < 0.0 else 0.0),
            fees=start.fees + cf.fees,
            buy_units=start.buy_units + (cf.units_delta if cf.units_delta > 0 else 0),
            sell_units=start.sell_units + (-cf.units_delta if cf.units_delta < 0 else 0),
            buy_cost=start.buy_cost + (cf.execution_money if cf.units_delta > 0 else 0),
            sell_proceeds=start.sell_proceeds + (-cf.execution_money if cf.units_delta < 0 else 0),
        )
        yield new
        cumulative_data[(cf.user_id, cf.product_id)] = new


def generate_user_product_timeline(
    sorted_events: list[CumulativeCashflow | PriceUpdate],
) -> Generator[UserProductTimelineEntry, None, None]:
    last_ccf_per_product_user: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    last_price_per_product: dict[UUID, PriceUpdate] = {}

    for event in sorted_events:
        if isinstance(ccf := event, CumulativeCashflow):
            try:
                pu = last_price_per_product[ccf.product_id]
            except KeyError:
                continue
            kwargs = asdict(ccf)
            del kwargs["id"]
            kwargs["market_value"] = ccf.units_held * pu.price
            result = UserProductTimelineEntry(**kwargs)
            yield result
            last_ccf_per_product_user.setdefault(ccf.product_id, {})[ccf.user_id] = ccf
        elif isinstance(pu := event, PriceUpdate):
            for ccf in last_ccf_per_product_user.get(pu.product_id, {}).values():
                kwargs = asdict(ccf)
                del kwargs["id"]
                kwargs["market_value"] = ccf.units_held * pu.price
                result = UserProductTimelineEntry(**kwargs)
                yield result
                last_ccf_per_product_user.setdefault(pu.product_id, {})[ccf.user_id] = ccf
            last_price_per_product[pu.product_id] = pu


def generate_user_timeline(
    sorted_user_product_timeline: list[UserProductTimelineEntry],
) -> Generator[UserTimelineEntry, None, None]:
    cumulative_data: dict[UUID, dict[UUID, UserProductTimelineEntry]] = {}

    for upt in sorted_user_product_timeline:
        cumulative_data.setdefault(upt.user_id, {})[upt.product_id] = upt
        yield UserTimelineEntry(
            user_id=upt.user_id,
            timestamp=upt.timestamp,
            net_investment=sum(x.net_investment for x in cumulative_data[upt.user_id].values()),
            market_value=sum(x.market_value for x in cumulative_data[upt.user_id].values()),
            deposits=sum(x.deposits for x in cumulative_data[upt.user_id].values()),
            withdrawals=sum(x.withdrawals for x in cumulative_data[upt.user_id].values()),
            fees=sum(x.fees for x in cumulative_data[upt.user_id].values()),
            buy_cost=sum(x.buy_cost for x in cumulative_data[upt.user_id].values()),
            sell_proceeds=sum(x.sell_proceeds for x in cumulative_data[upt.user_id].values()),
            avg_buy_price=sum(
                x.buy_cost / x.buy_units for x in cumulative_data[upt.user_id].values()
            ),
            avg_sell_price=sum(
                x.sell_proceeds / x.sell_units for x in cumulative_data[upt.user_id].values()
            ),
        )
