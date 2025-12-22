from dataclasses import asdict, astuple, fields
import datetime
from decimal import Decimal
from uuid import UUID
import asyncpg

from performance.granularities import Granularity
from performance.models import (
    Cashflow,
    CumulativeCashflow,
    PriceUpdate,
    UserProductTimelineEntry,
    UserTimelineEntry,
)


async def compute_cumulative_cashflows(
    sorted_cashflows: list[Cashflow],
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] | None = None,
) -> list[CumulativeCashflow]:
    if seed_cumulative_cashflows is None:
        seed_cumulative_cashflows = {}

    records: list[CumulativeCashflow] = []
    for cf in sorted_cashflows:
        start = seed_cumulative_cashflows.get(cf.user_id, {}).get(
            cf.product_id,
            CumulativeCashflow(
                cashflow_id=cf.id,
                user_id=cf.user_id,
                product_id=cf.product_id,
                timestamp=cf.timestamp,
            ),
        )

        assert cf.units_delta is not None
        assert cf.execution_money is not None
        assert cf.user_money is not None
        assert cf.fees is not None
        new = CumulativeCashflow(
            cashflow_id=cf.id,
            user_id=cf.user_id,
            product_id=cf.product_id,
            timestamp=cf.timestamp,
            units=start.units + cf.units_delta,
            net_investment=start.net_investment + cf.user_money,
            deposits=start.deposits
            + (cf.user_money if cf.units_delta > Decimal("0.000000") else Decimal("0.000000")),
            withdrawals=start.withdrawals
            + (-cf.user_money if cf.units_delta < Decimal("0.000000") else Decimal("0.000000")),
            fees=start.fees + cf.fees,
            buy_units=start.buy_units
            + (cf.units_delta if cf.units_delta > Decimal("0.000000") else Decimal("0.000000")),
            sell_units=start.sell_units
            + (-cf.units_delta if cf.units_delta < Decimal("0.000000") else Decimal("0.000000")),
            buy_cost=start.buy_cost
            + (
                cf.execution_money if cf.units_delta > Decimal("0.000000") else Decimal("0.000000")
            ),
            sell_proceeds=start.sell_proceeds
            + (
                -cf.execution_money
                if cf.units_delta < Decimal("0.000000")
                else Decimal("0.000000")
            ),
        )
        seed_cumulative_cashflows.setdefault(cf.user_id, {})[cf.product_id] = new
        records.append(new)
    return records


async def refresh_cumulative_cashflows(
    connection: asyncpg.Connection,
    sorted_cashflows: list[Cashflow],
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] | None = None,
) -> list[CumulativeCashflow]:
    records = await compute_cumulative_cashflows(sorted_cashflows, seed_cumulative_cashflows)
    await connection.copy_records_to_table(
        "cumulative_cashflow_cache",
        records=[astuple(r) for r in records],
        columns=[f.name for f in fields(CumulativeCashflow)],
    )
    return records


async def compute_user_product_timeline(
    sorted_events: list[CumulativeCashflow | PriceUpdate],
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] | None = None,
    seed_price_updates: dict[UUID, PriceUpdate] | None = None,
) -> list[UserProductTimelineEntry]:
    if seed_cumulative_cashflows is None:
        seed_cumulative_cashflows = {}
    if seed_price_updates is None:
        seed_price_updates = {}

    records: dict[tuple[UUID, UUID, datetime.datetime], UserProductTimelineEntry] = {}
    for event in sorted_events:
        if isinstance(ccf := event, CumulativeCashflow):
            try:
                pu = seed_price_updates[ccf.product_id]
            except KeyError:
                continue
            kwargs = asdict(ccf)
            del kwargs["cashflow_id"]
            kwargs["market_value"] = ccf.units * pu.price
            upt = UserProductTimelineEntry(**kwargs)
            records[(upt.user_id, upt.product_id, upt.timestamp)] = upt
            seed_cumulative_cashflows.setdefault(ccf.product_id, {})[ccf.user_id] = ccf
        elif isinstance(pu := event, PriceUpdate):
            for ccf in seed_cumulative_cashflows.get(pu.product_id, {}).values():
                kwargs = asdict(ccf)
                del kwargs["cashflow_id"]
                kwargs["timestamp"] = pu.timestamp
                kwargs["market_value"] = ccf.units * pu.price
                upt = UserProductTimelineEntry(**kwargs)
                records[(upt.user_id, upt.product_id, upt.timestamp)] = upt
                seed_cumulative_cashflows.setdefault(pu.product_id, {})[ccf.user_id] = ccf
            seed_price_updates[pu.product_id] = pu
    return list(records.values())


async def refresh_user_product_timeline(
    connection: asyncpg.Connection,
    granularity: Granularity,
    sorted_events: list[CumulativeCashflow | PriceUpdate],
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] | None = None,
    seed_price_updates: dict[UUID, PriceUpdate] | None = None,
) -> list[UserProductTimelineEntry]:
    records = await compute_user_product_timeline(
        sorted_events, seed_cumulative_cashflows, seed_price_updates
    )
    await connection.copy_records_to_table(
        f"user_product_timeline_cache_{granularity.suffix}",
        records=[astuple(upt) for upt in records],
        columns=[f.name for f in fields(UserProductTimelineEntry)],
    )
    return records


async def compute_user_timeline(
    sorted_user_product_timeline: list[UserProductTimelineEntry],
    seed_user_product_timeline: dict[UUID, dict[UUID, UserProductTimelineEntry]],
) -> list[UserTimelineEntry]:
    records: dict[tuple[UUID, datetime.datetime], UserTimelineEntry] = {}
    for upt in sorted_user_product_timeline:
        seed_user_product_timeline.setdefault(upt.user_id, {})[upt.product_id] = upt
        records[(upt.user_id, upt.timestamp)] = UserTimelineEntry(
            user_id=upt.user_id,
            timestamp=upt.timestamp,
            net_investment=(
                sum(x.net_investment for x in seed_user_product_timeline[upt.user_id].values())
                or Decimal("0.000000")
            ),
            market_value=(
                sum(x.market_value for x in seed_user_product_timeline[upt.user_id].values())
                or Decimal("0.000000")
            ),
            deposits=(
                sum(x.deposits for x in seed_user_product_timeline[upt.user_id].values())
                or Decimal("0.000000")
            ),
            withdrawals=(
                sum(x.withdrawals for x in seed_user_product_timeline[upt.user_id].values())
                or Decimal("0.000000")
            ),
            fees=(
                sum(x.fees for x in seed_user_product_timeline[upt.user_id].values())
                or Decimal("0.000000")
            ),
            buy_units=(
                sum(x.buy_units for x in seed_user_product_timeline[upt.user_id].values())
                or Decimal("0.000000")
            ),
            sell_units=(
                sum(x.sell_units for x in seed_user_product_timeline[upt.user_id].values())
                or Decimal("0.000000")
            ),
            buy_cost=(
                sum(x.buy_cost for x in seed_user_product_timeline[upt.user_id].values())
                or Decimal("0.000000")
            ),
            sell_proceeds=(
                sum(x.sell_proceeds for x in seed_user_product_timeline[upt.user_id].values())
                or Decimal("0.000000")
            ),
            cost_basis=(
                sum(
                    x.units * (x.buy_cost / x.buy_units)
                    if x.buy_units > Decimal("0.000000")
                    else Decimal("0.000000")
                    for x in seed_user_product_timeline[upt.user_id].values()
                )
                or Decimal("0.000000")
            ),
            sell_basis=(
                sum(
                    x.sell_units * (x.buy_cost / x.buy_units)
                    if x.buy_units > Decimal("0.000000")
                    else Decimal("0.000000")
                    for x in seed_user_product_timeline[upt.user_id].values()
                )
                or Decimal("0.000000")
            ),
        )
    return list(records.values())


async def refresh_user_timeline(
    connection: asyncpg.Connection,
    granularity: Granularity,
    sorted_user_product_timeline: list[UserProductTimelineEntry],
    seed_user_product_timeline: dict[UUID, dict[UUID, UserProductTimelineEntry]],
) -> list[UserTimelineEntry]:
    records = await compute_user_timeline(sorted_user_product_timeline, seed_user_product_timeline)
    await connection.copy_records_to_table(
        f"user_timeline_cache_{granularity.suffix}",
        records=[astuple(ut) for ut in records],
        columns=[f.name for f in fields(UserTimelineEntry)],
    )
    return records
