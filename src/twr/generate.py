import argparse
import datetime
import io
import itertools
import json
import pathlib
import random
import uuid
from typing import Generator, Iterable, cast

from twr.models import Cashflow, Investment, PriceUpdate, Product, User
from twr.utils import connection

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


def _get_previous_tick(now: datetime.datetime, interval: datetime.timedelta) -> datetime.datetime:
    candidate = now - interval
    while not is_market_open(candidate):
        candidate = datetime.datetime.combine(
            candidate.date() - datetime.timedelta(days=1), MARKET_CLOSE
        )
    return candidate


def _get_ticks(
    interval: datetime.timedelta, duration: datetime.timedelta
) -> Generator[datetime.datetime]:
    last = now = datetime.datetime.now() + interval
    while now - last < duration:
        yield (last := _get_previous_tick(last, interval))


def _chunkify[T](iterable: Iterable[T], chunk_size: int) -> Generator[Generator[T]]:
    for _, enumerated_chunk in itertools.groupby(
        enumerate(iterable), lambda i: i[0] // chunk_size
    ):
        yield (item for _, item in enumerated_chunk)


def _jitter() -> datetime.timedelta:
    return datetime.timedelta(milliseconds=1_000 * random.random() - 500)


def generate(
    days: int, price_update_frequency: str, product_count: int, user_count: int
) -> tuple[list[uuid.UUID], list[uuid.UUID], list[datetime.datetime]]:
    interval, duration = (
        _parse_time_interval(price_update_frequency),
        datetime.timedelta(days=days * 7 / 5),  # Convert calendar to trading days
    )
    ticks = sorted(list(_get_ticks(interval, duration)))

    products_list: list[Product] = []
    products_dict: dict[uuid.UUID | str, Product] = {}
    for _ in range(product_count):
        products_list.append((product := Product()))
        products_dict[product.id] = product
        last_price = 10 + 100 * random.random()
        for tick in ticks:
            # Lets drop some price updates randomly to simulate gaps
            if product.price_updates and random.random() < 0.03:
                continue
            while (next_price := last_price + random.random() - 0.5) <= 0:
                pass
            timestamp = tick + _jitter()
            product.price_updates.append(
                PriceUpdate(product_id=product.id, timestamp=timestamp, price=next_price)
            )
            product._timestamps.append(timestamp)
            last_price = next_price

    users_list: list[User] = []
    users_dict: dict[uuid.UUID, User] = {}
    # Track investments incrementally to avoid O(n²) recomputation
    user_investments: dict[uuid.UUID, dict[uuid.UUID | str, Investment]] = {}
    for _ in range(user_count):
        users_list.append((u := User()))
        users_dict[u.id] = u
        user_investments[u.id] = {}

    # Distribute cashflows between ticks
    start = ticks[0] + datetime.timedelta(seconds=1)
    end = ticks[-1] - datetime.timedelta(seconds=1)
    cashflow_count = round(len(ticks) * product_count / 9)
    cashflow_ticks = sorted(start + random.random() * (end - start) for _ in range(cashflow_count))
    for timestamp in cashflow_ticks:
        user = random.choice(users_list)
        investments = user_investments[user.id]
        if len(investments) > 0 and random.random() < 0.9:
            product = products_dict[random.choice(list(investments.keys()))]
        else:
            product = random.choice(products_list)
        units = investments.get(product.id, Investment()).units
        while units + (units_delta := random.random() - 0.5) < 0:
            pass
        market_price = product.price_at(timestamp)
        assert market_price is not None
        user.cashflows.append(
            Cashflow(
                user_id=user.id,
                product_id=product.id,
                timestamp=timestamp,
                units_delta=units_delta,
                # Add a 10% discrepancy between market and execution price
                execution_price=(
                    execution_price := float(market_price) * (1 + 0.1 * (random.random() - 0.5))
                ),
                # Add an up to 1$ fee
                user_money=units_delta * execution_price + random.random(),
            )
        )
        # Update investments incrementally
        investments.setdefault(product.id, Investment())
        investments[product.id].units += units_delta

    with connection() as conn:
        cur = conn.cursor()

        for chunk in _chunkify(
            (
                (product, price_update)
                for product in products_list
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
            ((user, cashflow) for user in users_list for cashflow in user.cashflows),
            1_000_000,
        ):
            buffer = io.StringIO()
            for user, cashflow in chunk:
                buffer.write(
                    f"{user.id}\t{cashflow.product_id}\t{cashflow.timestamp}\t"
                    f"{cashflow.units_delta}\t{cashflow.execution_price}\t{cashflow.user_money}\n"
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

        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("VACUUM ANALYZE price_update")
        cur.execute("VACUUM ANALYZE cashflow")

        migrations_dir = pathlib.Path(__file__).parent.parent.parent / "migrations"
        granularities_file = migrations_dir / "granularities.json"

        with open(granularities_file) as f:
            GRANULARITIES = json.load(f)

        for g in GRANULARITIES:
            cur.execute(
                f"CALL refresh_continuous_aggregate('price_update_{g['suffix']}', NULL, NULL)"
            )
            cur.execute(f"VACUUM ANALYZE price_update_{g['suffix']}")

    return (
        list(users_dict.keys()),
        cast(list[uuid.UUID], list(products_dict.keys())),
        ticks,
    )


parser = argparse.ArgumentParser()
parser.add_argument("--days", type=int, default=10)
parser.add_argument("--price-update-frequency", type=str, default="14min")
parser.add_argument("--users", type=int, default=1000)
parser.add_argument("--products", type=int, default=500)


def main() -> None:
    args = parser.parse_args()
    _, _, ticks = generate(args.days, args.price_update_frequency, args.products, args.users)
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
