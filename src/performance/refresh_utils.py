import datetime
from collections.abc import AsyncIterator
from dataclasses import fields
from decimal import Decimal
from uuid import UUID

import asyncpg
from asyncpg import Connection

from performance.granularities import Granularity
from performance.iter_utils import batch_insert
from performance.models import (
    Cashflow,
    CumulativeCashflow,
    PriceUpdate,
    UserProductTimelineEntry,
    UserTimelineEntry,
)


async def compute_cumulative_cashflows(
    cashflow_cursor: asyncpg.cursor.Cursor,
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] | None = None,
) -> AsyncIterator[CumulativeCashflow]:
    if seed_cumulative_cashflows is None:
        seed_cumulative_cashflows = {}

    async for record in cashflow_cursor:
        cf = Cashflow(*record)
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
        yield new


async def refresh_cumulative_cashflows(
    connection: Connection,
    cashflow_cursor: asyncpg.cursor.Cursor,
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] | None = None,
) -> int:
    cumulative_cashflows = compute_cumulative_cashflows(cashflow_cursor, seed_cumulative_cashflows)
    return await batch_insert(
        connection,
        "cumulative_cashflow_cache",
        cumulative_cashflows,
        columns=[f.name for f in fields(CumulativeCashflow)],
    )


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
            upt = UserProductTimelineEntry(
                user_id=ccf.user_id,
                product_id=ccf.product_id,
                timestamp=ccf.timestamp,
                units=ccf.units,
                net_investment=ccf.net_investment,
                market_value=ccf.units * pu.price,
                deposits=ccf.deposits,
                withdrawals=ccf.withdrawals,
                fees=ccf.fees,
                buy_units=ccf.buy_units,
                sell_units=ccf.sell_units,
                buy_cost=ccf.buy_cost,
                sell_proceeds=ccf.sell_proceeds,
                avg_buy_price=(
                    (ccf.buy_cost / ccf.buy_units).quantize(Decimal("0.000000"))
                    if ccf.buy_units > Decimal("0.000000")
                    else Decimal("0.000000")
                ),
                avg_sell_price=(
                    (ccf.sell_proceeds / ccf.sell_units).quantize(Decimal("0.000000"))
                    if ccf.sell_units > Decimal("0.000000")
                    else Decimal("0.000000")
                ),
            )
            records[(upt.user_id, upt.product_id, upt.timestamp)] = upt
            seed_cumulative_cashflows.setdefault(ccf.product_id, {})[ccf.user_id] = ccf
        elif isinstance(pu := event, PriceUpdate):
            for ccf in seed_cumulative_cashflows.get(pu.product_id, {}).values():
                upt = UserProductTimelineEntry(
                    user_id=ccf.user_id,
                    product_id=ccf.product_id,
                    timestamp=pu.timestamp,
                    units=ccf.units,
                    net_investment=ccf.net_investment,
                    market_value=ccf.units * pu.price,
                    deposits=ccf.deposits,
                    withdrawals=ccf.withdrawals,
                    fees=ccf.fees,
                    buy_units=ccf.buy_units,
                    sell_units=ccf.sell_units,
                    buy_cost=ccf.buy_cost,
                    sell_proceeds=ccf.sell_proceeds,
                    avg_buy_price=(
                        (ccf.buy_cost / ccf.buy_units).quantize(Decimal("0.000000"))
                        if ccf.buy_units > Decimal("0.000000")
                        else Decimal("0.000000")
                    ),
                    avg_sell_price=(
                        (ccf.sell_proceeds / ccf.sell_units).quantize(Decimal("0.000000"))
                        if ccf.sell_units > Decimal("0.000000")
                        else Decimal("0.000000")
                    ),
                )
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
        records=[upt.to_tuple() for upt in records],
        columns=[f.name for f in fields(UserProductTimelineEntry)],
    )
    return records


async def compute_user_timeline(
    sorted_user_product_timeline: list[UserProductTimelineEntry],
    seed_user_product_timeline: dict[UUID, dict[UUID, UserProductTimelineEntry]],
) -> list[UserTimelineEntry]:
    running_totals: dict[UUID, UserTimelineEntry] = {}
    for user_id, dct in seed_user_product_timeline.items():
        running_totals[user_id] = UserTimelineEntry(user_id, datetime.datetime.min)
        for x in dct.values():
            running_totals[user_id].net_investment += x.net_investment
            running_totals[user_id].market_value += x.market_value
            running_totals[user_id].deposits += x.deposits
            running_totals[user_id].withdrawals += x.withdrawals
            running_totals[user_id].fees += x.fees
            running_totals[user_id].buy_units += x.buy_units
            running_totals[user_id].sell_units += x.sell_units
            running_totals[user_id].buy_cost += x.buy_cost
            running_totals[user_id].sell_proceeds += x.sell_proceeds
            running_totals[user_id].cost_basis += x.units * x.avg_buy_price
            running_totals[user_id].sell_basis += x.sell_units * x.avg_buy_price
    records: dict[tuple[UUID, datetime.datetime], UserTimelineEntry] = {}
    for upt in sorted_user_product_timeline:
        prev = seed_user_product_timeline.get(upt.user_id, {}).get(
            upt.product_id,
            UserProductTimelineEntry(upt.user_id, upt.product_id, datetime.datetime.min),
        )
        running_totals.setdefault(
            upt.user_id, UserTimelineEntry(upt.user_id, datetime.datetime.min)
        )
        running_totals[upt.user_id].net_investment += upt.net_investment - prev.net_investment
        running_totals[upt.user_id].market_value += upt.market_value - prev.market_value
        running_totals[upt.user_id].deposits += upt.deposits - prev.deposits
        running_totals[upt.user_id].withdrawals += upt.withdrawals - prev.withdrawals
        running_totals[upt.user_id].fees += upt.fees - prev.fees
        running_totals[upt.user_id].buy_units += upt.buy_units - prev.buy_units
        running_totals[upt.user_id].sell_units += upt.sell_units - prev.sell_units
        running_totals[upt.user_id].buy_cost += upt.buy_cost - prev.buy_cost
        running_totals[upt.user_id].sell_proceeds += upt.sell_proceeds - prev.sell_proceeds
        running_totals[upt.user_id].cost_basis += (
            upt.units * upt.avg_buy_price - prev.units * prev.avg_buy_price
        )
        running_totals[upt.user_id].sell_basis += (
            upt.sell_units * upt.avg_buy_price - prev.sell_units * prev.avg_buy_price
        )

        seed_user_product_timeline.setdefault(upt.user_id, {})[upt.product_id] = upt
        rt = running_totals[upt.user_id]
        records[(upt.user_id, upt.timestamp)] = UserTimelineEntry(
            user_id=rt.user_id,
            timestamp=upt.timestamp,
            net_investment=rt.net_investment,
            market_value=rt.market_value,
            deposits=rt.deposits,
            withdrawals=rt.withdrawals,
            fees=rt.fees,
            buy_units=rt.buy_units,
            sell_units=rt.sell_units,
            buy_cost=rt.buy_cost,
            sell_proceeds=rt.sell_proceeds,
            cost_basis=rt.cost_basis,
            sell_basis=rt.sell_basis,
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
        records=[ut.to_tuple() for ut in records],
        columns=[f.name for f in fields(UserTimelineEntry)],
    )
    return records
