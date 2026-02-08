import datetime
import itertools
import random
import time
import uuid
from twr.generate import parser, generate
from twr.utils import Granularity, GRANULARITIES, connection
from twr.drop import drop_and_recreate_schema
from twr.migrate import run_all_migrations


def _query_granularity(
    user_ids: list[uuid.UUID], product_ids: list[uuid.UUID], suffix: str
) -> None:
    with connection() as conn:
        cur = conn.cursor()

        query_timings: list[float] = []
        big_tic = time.time()
        while time.time() - big_tic < 5:
            user_id = random.choice(user_ids)
            product_id = random.choice(product_ids)
            small_tic = time.time()
            cur.execute(
                f"SELECT * FROM user_product_timeline_business_{suffix}(%s, %s)",
                (str(user_id), str(product_id)),
            )
            cur.fetchall()
            query_timings.append(time.time() - small_tic)
        avg = 1_000 * sum(query_timings) / len(query_timings)
        print(f"  user_product_timeline_business_{suffix:5}: {avg:.2f}ms")

        query_timings: list[float] = []
        big_tic = time.time()
        while time.time() - big_tic < 5:
            user_id = random.choice(user_ids)
            small_tic = time.time()
            cur.execute(f"SELECT * FROM user_timeline_business_{suffix}(%s)", (str(user_id),))
            cur.fetchall()
            query_timings.append(time.time() - small_tic)
        avg = 1_000 * sum(query_timings) / len(query_timings)
        print(f"  user_timeline_business_{suffix:5}        : {avg:.2f}ms")


def _clear_cache(cutoff: datetime.datetime) -> None:
    with connection() as conn:
        cur = conn.cursor()
        for table in ["cumulative_cashflow_cache"] + [
            f"user_product_timeline_cache_{g['suffix']}" for g in GRANULARITIES
        ]:
            cur.execute(f"DELETE FROM {table} WHERE timestamp > %s", (cutoff,))


def main() -> None:
    args = parser.parse_args()
    print(
        f"benchmark --days={args.days} --price-update-frequency={args.price_update_frequency} "
        f"--products={args.products} --users={args.users}"
    )

    with connection() as conn:
        drop_and_recreate_schema(conn)
    with connection() as conn:
        run_all_migrations(conn)

    tic = time.time()
    users, products, ticks = generate(
        args.days, args.price_update_frequency, args.products, args.users
    )
    user_ids, product_ids = list(users.keys()), list(products.keys())
    print(f"Event generation took {time.time() - tic:.2f}s")

    print("Querying with 0% cache")

    for g in GRANULARITIES:
        _query_granularity(user_ids, product_ids, g["suffix"])

    print("Refreshing cache")
    with connection() as conn:
        cur = conn.cursor()

        tic = time.time()
        cur.execute("SELECT refresh_cumulative_cashflow()")
        print(f"  refresh_cumulative_cashflow        : {time.time() - tic:.2f}s")

        cur.execute("VACUUM ANALYZE cumulative_cashflow_cache")

        for g in GRANULARITIES:
            tic = time.time()
            cur.execute(f"SELECT refresh_user_product_timeline_{g['suffix']}()")
            print(f"  refresh_user_product_timeline_{g['suffix']:5}: {time.time() - tic:.2f}s")

            cur.execute(f"VACUUM ANALYZE user_product_timeline_cache_{g['suffix']}")

    print("Querying with 100% cache")
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
                f"Querying {g['suffix']:5} with {n * 100}% cache (cutoff: {timestamp.isoformat()})"
            )
            _query_granularity(user_ids, product_ids, g["suffix"])


if __name__ == "__main__":
    main()
