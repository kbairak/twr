import argparse
import datetime
import io
import itertools
import json
import pathlib
import random
from dataclasses import dataclass, field
from typing import Generator, Iterable
import uuid

from twr.utils import get_conn

MARKET_OPEN = datetime.time(9, 30)
MARKET_CLOSE = datetime.time(16, 0)


def _parse_time_interval(interval_str: str) -> datetime.timedelta:
    """Parse time interval string like '2min', '5min', '1h' to timedelta"""
    import re

    match = re.match(r"^(\d+)(min|h|d)$", interval_str.lower())
    if not match:
        raise ValueError(
            f"Invalid interval format: {interval_str}. Use format like '2min', '1h', '1d'"
        )

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "min":
        return datetime.timedelta(minutes=value)
    elif unit == "h":
        return datetime.timedelta(hours=value)
    elif unit == "d":
        return datetime.timedelta(days=value)
    else:
        raise ValueError(f"Unsupported time unit: {unit}")


def is_market_open(ts: datetime.datetime) -> bool:
    # Monday to Friday, market hours
    is_workday = ts.weekday() < 5
    is_market_hours = MARKET_OPEN <= ts.time() <= MARKET_CLOSE
    return is_workday and is_market_hours


def _get_next_tick(now: datetime.datetime, interval: datetime.timedelta) -> datetime.datetime:
    candidate = now - interval
    while not is_market_open(candidate):
        candidate = datetime.datetime.combine(
            candidate.date() - datetime.timedelta(days=1), MARKET_CLOSE
        )
    return candidate


def _get_ticks(interval: datetime.timedelta, tick_count: int) -> Generator[datetime.datetime]:
    yield (last := _get_next_tick(datetime.datetime.now() + interval, interval))
    for _ in range(tick_count - 1):
        yield (last := _get_next_tick(last, interval))


@dataclass
class PriceUpdate:
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
    product_id: uuid.UUID
    timestamp: datetime.datetime
    units_delta: float
    execution_price: float
    user_money: float


@dataclass
class Investment:
    units: float = 0.0
    invested_amount: float = 0.0


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
            result[cashflow.product_id].invested_amount += cashflow.user_money
        return result


def _chunkify[T](iterable: Iterable[T], chunk_size: int) -> Generator[Generator[T]]:
    for _, enumerated_chunk in itertools.groupby(
        enumerate(iterable), lambda i: i[0] // chunk_size
    ):
        yield (item for _, item in enumerated_chunk)


def generate(days: int, price_update_frequency: str, product_count: int, user_count: int):
    trading_duration = days * (
        datetime.datetime.combine(datetime.date.today(), MARKET_CLOSE)
        - datetime.datetime.combine(datetime.date.today(), MARKET_OPEN)
    )
    interval = _parse_time_interval(price_update_frequency)
    tick_count = round(trading_duration / interval)

    ticks = sorted(list(_get_ticks(interval, tick_count)))
    products = {(p := Product()).id: p for _ in range(product_count)}

    for tick in ticks:
        for product in products.values():
            try:
                last_price = product.price_updates[-1].price
            except IndexError:
                last_price = 10 + 100 * random.random()
            while (next_price := last_price + random.random() - 0.5) <= 0:
                pass
            product.price_updates.append(PriceUpdate(timestamp=tick, price=next_price))

    users = {(u := User()).id: u for _ in range(user_count)}

    # Distribute cashflows between ticks
    timestamp = ticks[0] + datetime.timedelta(seconds=1)
    end = ticks[-1] - datetime.timedelta(seconds=1)
    step = (end - timestamp) / round(tick_count * product_count / 9)
    while timestamp < end:
        user = random.choice(list(users.values()))
        investments = user.investments
        if len(investments) > 0 and random.random() < 0.9:
            product = products[random.choice(list(investments.keys()))]
        else:
            product = random.choice(list(products.values()))
        units = investments.get(product.id, Investment()).units
        while True:
            price = product.price_at(timestamp)
            assert price is not None
            cashflow = Cashflow(
                product_id=product.id,
                timestamp=timestamp,
                units_delta=(units_delta := random.random() - 0.5),
                execution_price=price,
                user_money=units_delta * price + random.random(),
            )
            if units + cashflow.units_delta >= 0:
                break
        user.cashflows.append(cashflow)
        timestamp += step

    conn = get_conn()
    cur = conn.cursor()

    for chunk in _chunkify(
        (
            (product, price_update)
            for product in products.values()
            for price_update in product.price_updates
        ),
        1_000_000,
    ):
        buffer = io.StringIO()
        for product, price_update in chunk:
            buffer.write(f"{product.id}\t{price_update.timestamp}\t{price_update.price}\n")
        buffer.seek(0)
        cur.copy_from(
            buffer, "price_update", columns=("product_id", "timestamp", "price"), sep="\t"
        )
        conn.commit()

    for chunk in _chunkify(
        ((user, cashflow) for user in users.values() for cashflow in user.cashflows),
        1_000_000,
    ):
        buffer = io.StringIO()
        for user, cashflow in chunk:
            buffer.write(
                f"{user.id}\t{cashflow.product_id}\t{cashflow.timestamp}\t{cashflow.units_delta}\t"
                f"{cashflow.execution_price}\t{cashflow.user_money}\n"
            )
        buffer.seek(0)
        cur.copy_from(
            buffer,
            "cashflow",
            columns=(
                "user_id",
                "product_id",
                "timestamp",
                "units_delta",
                "execution_price",
                "user_money",
            ),
            sep="\t",
        )
        conn.commit()

    migrations_dir = pathlib.Path(__file__).parent.parent.parent / "migrations"
    granularities_file = migrations_dir / "granularities.json"

    with open(granularities_file) as f:
        GRANULARITIES = json.load(f)

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    for g in GRANULARITIES:
        cur.execute(f"CALL refresh_continuous_aggregate('price_update_{g['suffix']}', NULL, NULL)")

    return users, products, ticks


parser = argparse.ArgumentParser()
parser.add_argument("--days", type=int, default=10)
parser.add_argument("--price-update-frequency", type=str, default="14min")
parser.add_argument("--users", type=int, default=1000)
parser.add_argument("--products", type=int, default=500)


def main():
    args = parser.parse_args()
    users, products, ticks = generate(
        args.days, args.price_update_frequency, args.products, args.users
    )
    print(
        f"Trading duration: {args.days}d\n"
        f"Start           : {ticks[0].isoformat()}\n"
        f"End             : {ticks[-1].isoformat()}\n"
        f"Users           : {args.users}\n"
        f"Products        : {args.products}\n"
        f"Ticks           : {len(ticks)}\n"
        f"Price updates   : {len(ticks) * args.products}\n"
        f"Cashflows       : {round(len(ticks) * args.products / 9)}"
    )


if __name__ == "__main__":
    main()
