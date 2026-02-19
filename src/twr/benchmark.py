import datetime
import itertools
import random
import sys
import time
import uuid
from typing import Any, Callable

from twr.drop import drop_and_recreate_schema
from twr.generate import generate, parser
from twr.migrate import run_all_migrations
from twr.utils import GRANULARITIES, Granularity, connection


def _mean(query_times: list[float]) -> float:
    return sum(query_times) / len(query_times)


def _cv(query_times: list[float]) -> float:
    """Coefficient of variation (std dev / mean)"""
    mean = _mean(query_times)
    variance = sum((x - mean) ** 2 for x in query_times) / len(query_times)
    std_dev = variance**0.5
    return std_dev / mean if mean > 0 else float("inf")


def _measure(prefix: str, func1: Callable[[], Any], func2: Callable[..., None]) -> None:
    query_times: list[float] = []
    warmup = 10
    message = ""
    cv = 0.0

    while (
        (
            len(query_times) < warmup + 10  # At least 10 after warmup
            or _cv(query_times[warmup:]) > 0.3  # CV of all samples after warmup
        )
        and len(query_times) < 1000
    ):
        args: Any = func1()
        start_time = time.time()
        func2(args)
        end_time = time.time()
        query_times.append(end_time - start_time)

        if len(query_times) > warmup:
            cv = _cv(query_times[warmup:])
            if sys.stdout.isatty():
                print(f"\r{' ' * len(message)}\r", end="")
                avg_ms = _mean(query_times[warmup:]) * 1000
                message = f"{prefix}: {avg_ms:7.2f}ms (CV={cv:.3f}, n={len(query_times) - warmup})"
                print(message, end="", flush=True)

    if sys.stdout.isatty():
        print()
    else:
        print(
            f"{prefix}: {_mean(query_times[warmup:]) * 1000:7.2f}ms (CV={cv:.3f}, "
            f"n={len(query_times) - warmup})"
        )


def _query_granularity(
    user_ids: list[uuid.UUID], product_ids: list[uuid.UUID], suffix: str
) -> None:
    with connection() as conn:
        cur = conn.cursor()

        def query1() -> tuple[uuid.UUID, uuid.UUID]:
            return random.choice(user_ids), random.choice(product_ids)

        def query2(args: tuple[uuid.UUID, uuid.UUID]) -> None:
            user_id, product_id = args
            cur.execute(
                f"SELECT user_product_timeline_business_{suffix}(%s, %s)",
                (str(user_id), str(product_id)),
            )
            cur.fetchall()

        _measure(f"    - user_product_timeline_business_{suffix:5}", query1, query2)

        def query3() -> uuid.UUID:
            return random.choice(user_ids)

        def query4(user_id: uuid.UUID) -> None:
            cur.execute(f"SELECT user_timeline_business_{suffix}(%s)", (str(user_id),))
            cur.fetchall()

        _measure(f"    - user_timeline_business_{suffix:5}        ", query3, query4)


def _clear_cache(cutoff: datetime.datetime) -> None:
    with connection() as conn:
        cur = conn.cursor()
        for table in ["cumulative_cashflow_cache"] + [
            f"user_product_timeline_cache_{g['suffix']}" for g in GRANULARITIES
        ]:
            cur.execute(f"DELETE FROM {table} WHERE timestamp > %s", (cutoff,))
            cur.execute(f"VACUUM ANALYZE {table}")


def main() -> None:
    args = parser.parse_args()
    msg = (
        f"benchmark --days={args.days} --price-update-frequency={args.price_update_frequency} "
        f"--products={args.products} --users={args.users}"
    )
    print(f"\n{msg}\n{'=' * len(msg)}")

    with connection() as conn:
        drop_and_recreate_schema(conn)
    with connection() as conn:
        run_all_migrations(conn)

    print("\n⚙️ Event generation: ", end="", flush=True)
    tic = time.time()
    user_ids, product_ids, ticks = generate(
        args.days, args.price_update_frequency, args.products, args.users
    )
    print(f"{time.time() - tic:.2f}s")

    print("\n🔍 Querying with 0% cache")

    for g in GRANULARITIES:
        _query_granularity(user_ids, product_ids, g["suffix"])

    print("\n🔄 Refreshing cache")
    with connection() as conn:
        cur = conn.cursor()

        print("    - refresh_cumulative_cashflow         : ", end="", flush=True)
        tic = time.time()
        cur.execute("SELECT refresh_cumulative_cashflow()")
        print(f"{time.time() - tic: 6.2f}s")

        cur.execute("VACUUM ANALYZE cumulative_cashflow_cache")

        for g in GRANULARITIES:
            print(f"    - refresh_user_product_timeline_{g['suffix']:5} : ", end="", flush=True)
            tic = time.time()
            cur.execute(f"SELECT refresh_user_product_timeline_{g['suffix']}()")
            print(f"{time.time() - tic: 6.2f}s")

            cur.execute(f"VACUUM ANALYZE user_product_timeline_cache_{g['suffix']}")

    print("\n🔍 Querying with 100% cache")
    for g in GRANULARITIES:
        _query_granularity(user_ids, product_ids, g["suffix"])

    cutoffs: dict[datetime.datetime, list[tuple[float, Granularity]]] = {}
    for n, g in itertools.product((0.25, 0.5, 0.75), GRANULARITIES):
        if g["cache_retention"]:
            days = int(g["cache_retention"].split()[0])
            start = max(ticks[0], ticks[-1] - datetime.timedelta(days=days))
        else:
            start = ticks[0]
        duration = ticks[-1] - start
        timestamp = start + n * duration
        cutoffs.setdefault(timestamp, []).append((n, g))

    for timestamp in sorted(cutoffs.keys(), reverse=True):
        _clear_cache(timestamp)
        for n, g in cutoffs[timestamp]:
            print(
                f"\n🔍 Querying {g['suffix']:5} with {n * 100}% cache "
                f"(cutoff: {timestamp.isoformat()})"
            )
            _query_granularity(user_ids, product_ids, g["suffix"])


if __name__ == "__main__":
    main()
