import datetime
import functools
from copy import deepcopy
from dataclasses import fields
from typing import Any, AsyncIterator, Awaitable, Callable
from uuid import UUID

import asyncpg

from performance import settings
from performance.granularities import GRANULARITIES, Granularity
from performance.iter_utils import (
    async_iterator_to_list,
    cursor_to_async_iterator,
    list_to_async_iterator,
    merge_sorted,
)
from performance.models import (
    Cashflow,
    CumulativeCashflow,
    PriceUpdate,
    UserProductTimelineEntry,
    UserTimelineEntry,
)
from performance.refresh_utils import (
    compute_cumulative_cashflows,
    compute_user_product_timeline,
    compute_user_timeline,
    refresh_cumulative_cashflows,
    refresh_user_product_timeline,
    refresh_user_timeline,
)


def _transaction(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @functools.wraps(func)
    async def decorated(connection: asyncpg.Connection, *args: Any, **kwargs: Any) -> Any:
        async with connection.transaction():
            return await func(connection, *args, **kwargs)

    return decorated


@_transaction
async def add_cashflows(connection: asyncpg.Connection, *cashflows: Cashflow) -> None:
    min_user_product_timestamps: dict[tuple[UUID, UUID], datetime.datetime] = {}
    min_user_timestamps: dict[UUID, datetime.datetime] = {}
    for cf in cashflows:
        min_user_product_timestamps[(cf.user_id, cf.product_id)] = min(
            min_user_product_timestamps.get(
                (cf.user_id, cf.product_id),
                datetime.datetime.max.replace(tzinfo=datetime.timezone.utc),
            ),
            cf.timestamp,
        )
        min_user_timestamps[cf.user_id] = min(
            min_user_timestamps.get(
                cf.user_id, datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)
            ),
            cf.timestamp,
        )

    # Reverse-zip min_timestamps into user_ids, product_ids, timestamps
    keys, timestamps_tuple = zip(*min_user_product_timestamps.items())
    user_ids_tuple, product_ids_tuple = zip(*keys)
    user_ids_for_user_product, product_ids_for_user_product, timestamps_for_user_product = (
        list(user_ids_tuple),
        list(product_ids_tuple),
        list(timestamps_tuple),
    )
    user_ids_tuple_user, timestamps_tuple_user = zip(*min_user_timestamps.items())
    user_ids_for_user, timestamps_for_user = list(user_ids_tuple_user), list(timestamps_tuple_user)

    # Invalidate cumulative_cashflow_cache after out-of-order inserts
    await connection.execute(
        """
            WITH min_cashflows AS (
                SELECT unnest($1::uuid[]) AS user_id,
                       unnest($2::uuid[]) AS product_id,
                       unnest($3::timestamptz[]) AS "timestamp"
            )
            DELETE FROM cumulative_cashflow_cache ccc
            USING min_cashflows cf
            WHERE ccc.user_id = cf.user_id AND
                  ccc.product_id = cf.product_id AND
                  ccc."timestamp" >= cf."timestamp"
        """,
        user_ids_for_user_product,
        product_ids_for_user_product,
        timestamps_for_user_product,
    )

    # Invalidate user_product_timeline_cache after out-of-order inserts
    for granularity in GRANULARITIES:
        await connection.execute(
            f"""
                WITH min_cashflows AS (
                    SELECT unnest($1::uuid[]) AS user_id,
                           unnest($2::uuid[]) AS product_id,
                           unnest($3::timestamptz[]) AS "timestamp"
                )
                DELETE FROM user_product_timeline_cache_{granularity.suffix} upt
                USING min_cashflows cf
                WHERE upt.user_id = cf.user_id AND
                      upt.product_id = cf.product_id AND
                      upt."timestamp" >= cf."timestamp"
            """,
            user_ids_for_user_product,
            product_ids_for_user_product,
            timestamps_for_user_product,
        )

    # Invalidate user_timeline_cache after out-of-order inserts
    for granularity in GRANULARITIES:
        await connection.execute(
            f"""
                WITH min_cashflows AS (
                    SELECT unnest($1::uuid[]) AS user_id,
                           unnest($2::timestamptz[]) AS "timestamp"
                )
                DELETE FROM user_timeline_cache_{granularity.suffix} ut
                USING min_cashflows cf
                WHERE ut.user_id = cf.user_id AND
                      ut."timestamp" >= cf."timestamp"
            """,
            user_ids_for_user,
            timestamps_for_user,
        )

    # Insert new cashflows
    await connection.copy_records_to_table(
        "cashflow",
        records=[cf.to_tuple() for cf in cashflows],
        columns=[f.name for f in fields(Cashflow)],
    )

    # Repair cumulative cashlows
    sorted_cashflow_cursor = connection.cursor(
        f"""
            WITH min_user_product_timestamps AS (
                SELECT unnest($1::uuid[]) AS user_id,
                       unnest($2::uuid[]) AS product_id,
                       unnest($3::timestamptz[]) AS "timestamp"
            )
            SELECT {", ".join(f"cf.{f.name}" for f in fields(Cashflow))}
            FROM cashflow cf
                INNER JOIN min_user_product_timestamps mupt
                    ON cf.user_id = mupt.user_id AND
                       cf.product_id = mupt.product_id AND
                       cf."timestamp" >= mupt."timestamp"
            WHERE cf."timestamp" <= (SELECT COALESCE(MAX("timestamp"), 'Infinity'::timestamptz)
                                     FROM cumulative_cashflow_cache)
            ORDER BY cf."timestamp" ASC
        """,
        user_ids_for_user_product,
        product_ids_for_user_product,
        timestamps_for_user_product,
        prefetch=settings.PREFETCH_COUNT,
    )
    sorted_cashflow_iter = cursor_to_async_iterator(sorted_cashflow_cursor, Cashflow)
    seed_cumulative_cashflow_cursor = connection.cursor(
        f"""
            WITH min_user_products AS (
                SELECT unnest($1::uuid[]) AS user_id, unnest($2::uuid[]) AS product_id
            )
            SELECT DISTINCT ON (user_id, product_id)
                {", ".join(f"ccc.{f.name}" for f in fields(CumulativeCashflow))}
            FROM cumulative_cashflow_cache ccc
                INNER JOIN min_user_products mup
                    ON ccc.user_id = mup.user_id AND ccc.product_id = mup.product_id
            ORDER BY user_id, product_id, "timestamp" DESC
        """,
        user_ids_for_user_product,
        product_ids_for_user_product,
        prefetch=settings.PREFETCH_COUNT,
    )
    seed_cumulative_cashflow_iter = cursor_to_async_iterator(
        seed_cumulative_cashflow_cursor, CumulativeCashflow
    )
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    async for ccf in seed_cumulative_cashflow_iter:
        seed_cumulative_cashflows.setdefault(ccf.user_id, {})[ccf.product_id] = ccf

    sorted_cumulative_cashflows_iter = refresh_cumulative_cashflows(
        connection, sorted_cashflow_iter, seed_cumulative_cashflows
    )
    # Evaluate this because we will need it for multiple granularities
    sorted_cumulative_cashflows = await async_iterator_to_list(sorted_cumulative_cashflows_iter)

    # Repair user-product-timeline
    for granularity in GRANULARITIES:
        sorted_price_update_cursor = connection.cursor(
            f"""
                WITH min_user_product_timestamps AS (
                    SELECT unnest($1::uuid[]) AS product_id,
                           unnest($2::timestamptz[]) AS "timestamp"
                )
                SELECT {", ".join(f"pu.{f.name}" for f in fields(PriceUpdate))}
                FROM price_update_{granularity.suffix} pu
                    INNER JOIN min_user_product_timestamps mupt
                        ON pu.product_id = mupt.product_id AND pu."timestamp" >= mupt."timestamp"
                WHERE pu."timestamp" <= (SELECT COALESCE(MAX("timestamp"), 'Infinity'::timestamptz)
                                         FROM user_product_timeline_cache_{granularity.suffix})
                ORDER BY pu."timestamp" ASC

            """,
            product_ids_for_user_product,
            timestamps_for_user_product,
            prefetch=settings.PREFETCH_COUNT,
        )
        sorted_price_update_iter = cursor_to_async_iterator(
            sorted_price_update_cursor, PriceUpdate
        )
        sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
            sorted_price_update_iter, list_to_async_iterator(sorted_cumulative_cashflows)
        )
        seed_price_update_rows = await connection.fetch(
            f"""
                WITH min_price_timestamps AS (
                    SELECT unnest($1::uuid[]) AS product_id,
                            unnest($2::timestamptz[]) AS "timestamp"
                )
                SELECT DISTINCT ON (pu.product_id)
                    {", ".join(f"pu.{f.name}" for f in fields(PriceUpdate))}
                FROM price_update_{granularity.suffix} pu
                    INNER JOIN min_price_timestamps mpt
                        ON pu.product_id = mpt.product_id
                WHERE pu."timestamp" < mpt."timestamp"
                ORDER BY pu.product_id, pu."timestamp" DESC
            """,
            product_ids_for_user_product,
            timestamps_for_user_product,
        )
        seed_price_updates: dict[UUID, PriceUpdate] = {
            pu["product_id"]: PriceUpdate(*pu) for pu in seed_price_update_rows
        }
        async for _ in refresh_user_product_timeline(
            connection,
            granularity,
            sorted_events_iter,
            seed_cumulative_cashflows,
            seed_price_updates,
        ):
            pass

        # Repair user-timeline
        min_affected_timestamp = min(timestamps_for_user)

        # Build seed from latest entries before min_timestamp
        seed_upt_rows = await connection.fetch(
            f"""
                WITH user_ids AS (SELECT unnest($1::uuid[]) AS user_id)
                SELECT DISTINCT ON (upt.user_id, upt.product_id)
                    {", ".join(f"upt.{f.name}" for f in fields(UserProductTimelineEntry))}
                FROM user_product_timeline_cache_{granularity.suffix} upt
                    INNER JOIN user_ids u
                        ON upt.user_id = u.user_id
                WHERE upt."timestamp" < $2
                ORDER BY upt.user_id, upt.product_id, upt."timestamp" DESC
            """,
            user_ids_for_user,
            min_affected_timestamp,
        )
        seed_user_product_timeline: dict[UUID, dict[UUID, UserProductTimelineEntry]] = {}
        for upt_row in seed_upt_rows:
            seed_user_product_timeline.setdefault(upt_row["user_id"], {})[
                upt_row["product_id"]
            ] = UserProductTimelineEntry(*upt_row)

        # Fetch ALL user_product_timeline entries >= min_affected_timestamp for affected users
        # (not just the ones we just refreshed, which only include affected products)
        sorted_user_product_cursor = connection.cursor(
            f"""
                WITH user_ids AS (SELECT unnest($1::uuid[]) AS user_id)
                SELECT {", ".join(f"upt.{f.name}" for f in fields(UserProductTimelineEntry))}
                FROM user_product_timeline_cache_{granularity.suffix} upt
                    INNER JOIN user_ids u
                        ON upt.user_id = u.user_id
                WHERE upt."timestamp" >= $2
                ORDER BY upt."timestamp" ASC
            """,
            user_ids_for_user,
            min_affected_timestamp,
            prefetch=settings.PREFETCH_COUNT,
        )
        sorted_user_product_iter = cursor_to_async_iterator(
            sorted_user_product_cursor, UserProductTimelineEntry
        )

        await refresh_user_timeline(
            connection,
            granularity,
            sorted_user_product_iter,
            seed_user_product_timeline,
        )

    # Cleanup old cache entries based on retention policy
    for granularity in GRANULARITIES:
        if granularity.cache_retention is None:
            continue

        # Clean up user_product_timeline_cache
        await connection.execute(
            f"""
                DELETE FROM user_product_timeline_cache_{granularity.suffix} upt
                WHERE upt."timestamp" < NOW() - INTERVAL '{granularity.cache_retention}'
                  AND (upt.user_id, upt.product_id, upt."timestamp") NOT IN (
                      SELECT DISTINCT ON (user_id, product_id) user_id, product_id, "timestamp"
                      FROM user_product_timeline_cache_{granularity.suffix}
                      ORDER BY user_id, product_id, "timestamp" DESC
                  )
            """
        )

        # Clean up user_timeline_cache
        await connection.execute(
            f"""
                DELETE FROM user_timeline_cache_{granularity.suffix} ut
                WHERE ut."timestamp" < NOW() - INTERVAL '{granularity.cache_retention}'
                  AND (ut.user_id, ut."timestamp") NOT IN (
                      SELECT DISTINCT ON (user_id) user_id, "timestamp"
                      FROM user_timeline_cache_{granularity.suffix}
                      ORDER BY user_id, "timestamp" DESC
                  )
            """
        )


@_transaction
async def add_price_update(connection: asyncpg.Connection, *price_updates: PriceUpdate) -> None:
    await connection.copy_records_to_table(
        "price_update",
        records=[pu.to_tuple() for pu in price_updates],
        columns=[f.name for f in fields(PriceUpdate)],
    )


@_transaction
async def get_user_product_timeline(
    connection: asyncpg.Connection,
    user_id: UUID,
    product_id: UUID,
    granularity: Granularity,
) -> list[UserProductTimelineEntry]:
    # Fetch all cached entries
    cached_rows = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(UserProductTimelineEntry))}
            FROM user_product_timeline_cache_{granularity.suffix}
            WHERE user_id = $1 AND product_id = $2
            ORDER BY "timestamp" ASC
        """,
        user_id,
        product_id,
    )
    cached_entries = [UserProductTimelineEntry(*row) for row in cached_rows]

    # Get watermark (latest timestamp in cache)
    watermark = (
        cached_entries[-1].timestamp
        if cached_entries
        else datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    )

    # Get seed cumulative cashflow (latest at or before watermark)
    seed_ccf_for_compute_ccf: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    seed_ccf_for_compute_upt: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    ccf_row = await connection.fetchrow(
        f"""
            SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
            FROM cumulative_cashflow_cache
            WHERE user_id = $1 AND product_id = $2 AND "timestamp" <= $3
            ORDER BY "timestamp" DESC
            LIMIT 1
        """,
        user_id,
        product_id,
        watermark,
    )
    if ccf_row:
        ccf = CumulativeCashflow(*ccf_row)
        # For compute_cumulative_cashflows: seed[user_id][product_id]
        seed_ccf_for_compute_ccf[user_id] = {product_id: ccf}
        # For compute_user_product_timeline: seed[product_id][user_id]
        seed_ccf_for_compute_upt[product_id] = {user_id: ccf}

    # Fetch cashflows after watermark
    async with connection.transaction():
        sorted_cashflow_cursor = connection.cursor(
            f"""
                SELECT {", ".join(f.name for f in fields(Cashflow))}
                FROM cashflow
                WHERE user_id = $1 AND product_id = $2 AND "timestamp" > $3
                ORDER BY "timestamp" ASC
            """,
            user_id,
            product_id,
            watermark,
            prefetch=settings.PREFETCH_COUNT,
        )
        sorted_cashflow_iter = cursor_to_async_iterator(sorted_cashflow_cursor, Cashflow)

        # Compute cumulative cashflows for fresh data
        sorted_cumulative_cashflows_iter = compute_cumulative_cashflows(
            sorted_cashflow_iter, seed_ccf_for_compute_ccf
        )

    # Get seed price update (latest at or before watermark)
    seed_price_updates: dict[UUID, PriceUpdate] = {}
    seed_price_update_row = await connection.fetchrow(
        f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            WHERE product_id = $1 AND "timestamp" <= $2
            ORDER BY "timestamp" DESC
            LIMIT 1
        """,
        product_id,
        watermark,
    )
    if seed_price_update_row:
        seed_price_updates[product_id] = PriceUpdate(*seed_price_update_row)

    # Fetch price updates after watermark
    sorted_price_update_cursor = connection.cursor(
        f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            WHERE product_id = $1 AND "timestamp" > $2
            ORDER BY "timestamp" ASC
        """,
        product_id,
        watermark,
        prefetch=settings.PREFETCH_COUNT,
    )
    sorted_price_update_iter = cursor_to_async_iterator(sorted_price_update_cursor, PriceUpdate)

    # Merge events and sort
    sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
        sorted_price_update_iter, sorted_cumulative_cashflows_iter
    )

    # Compute fresh entries
    fresh_entries = []
    async for entry in compute_user_product_timeline(
        sorted_events_iter, seed_ccf_for_compute_upt, seed_price_updates
    ):
        fresh_entries.append(entry)

    # Return cached + fresh combined
    return cached_entries + fresh_entries


@_transaction
async def get_user_timeline(
    connection: asyncpg.Connection,
    user_id: UUID,
    granularity: Granularity,
) -> list[UserTimelineEntry]:
    # Fetch all cached entries
    cached_rows = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(UserTimelineEntry))}
            FROM user_timeline_cache_{granularity.suffix}
            WHERE user_id = $1
            ORDER BY "timestamp" ASC
        """,
        user_id,
    )
    cached_entries = [UserTimelineEntry(*row) for row in cached_rows]

    # Get watermark (latest timestamp in cache)
    watermark = (
        cached_entries[-1].timestamp
        if cached_entries
        else datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    )

    # Get seed cumulative cashflows (latest per product at or before watermark)
    seed_ccf_for_compute_ccf: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    seed_ccf_for_compute_upt: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    ccf_rows = await connection.fetch(
        f"""
            SELECT DISTINCT ON (product_id)
                {", ".join(f.name for f in fields(CumulativeCashflow))}
            FROM cumulative_cashflow_cache
            WHERE user_id = $1 AND "timestamp" <= $2
            ORDER BY product_id, "timestamp" DESC
        """,
        user_id,
        watermark,
    )
    for row in ccf_rows:
        ccf = CumulativeCashflow(*row)
        # For compute_cumulative_cashflows: seed[user_id][product_id]
        seed_ccf_for_compute_ccf.setdefault(user_id, {})[ccf.product_id] = ccf
        # For compute_user_product_timeline: seed[product_id][user_id]
        seed_ccf_for_compute_upt.setdefault(ccf.product_id, {})[user_id] = ccf

    # Fetch cashflows after watermark
    async with connection.transaction():
        sorted_cashflow_cursor = connection.cursor(
            f"""
                SELECT {", ".join(f.name for f in fields(Cashflow))}
                FROM cashflow
                WHERE user_id = $1 AND "timestamp" > $2
                ORDER BY "timestamp" ASC
            """,
            user_id,
            watermark,
            prefetch=settings.PREFETCH_COUNT,
        )
        sorted_cashflow_iter = cursor_to_async_iterator(sorted_cashflow_cursor, Cashflow)

        # Compute cumulative cashflows for fresh data
        sorted_cumulative_cashflows_iter = compute_cumulative_cashflows(
            sorted_cashflow_iter, seed_ccf_for_compute_ccf
        )

    # Get seed price updates (latest per product at or before watermark)
    seed_price_updates: dict[UUID, PriceUpdate] = {}
    seed_price_update_rows = await connection.fetch(
        f"""
            SELECT DISTINCT ON (product_id)
                {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            WHERE product_id IN (SELECT DISTINCT product_id FROM cashflow WHERE user_id = $1)
              AND "timestamp" <= $2
            ORDER BY product_id, "timestamp" DESC
        """,
        user_id,
        watermark,
    )
    for pu in seed_price_update_rows:
        price_update = PriceUpdate(*pu)
        seed_price_updates[price_update.product_id] = price_update

    # Fetch price updates after watermark
    sorted_price_update_cursor = connection.cursor(
        f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            WHERE product_id IN (SELECT DISTINCT product_id FROM cashflow WHERE user_id = $1)
              AND "timestamp" > $2
            ORDER BY "timestamp" ASC
        """,
        user_id,
        watermark,
        prefetch=settings.PREFETCH_COUNT,
    )
    sorted_price_update_iter = cursor_to_async_iterator(sorted_price_update_cursor, PriceUpdate)

    # Merge events and sort
    sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
        sorted_price_update_iter, sorted_cumulative_cashflows_iter
    )

    # Compute fresh user_product_timeline entries
    fresh_upt_iter = compute_user_product_timeline(
        sorted_events_iter, seed_ccf_for_compute_upt, seed_price_updates
    )

    # Get seed user_product_timeline (latest per product at or before watermark)
    seed_upt_rows = await connection.fetch(
        f"""
            SELECT DISTINCT ON (product_id)
                {", ".join(f.name for f in fields(UserProductTimelineEntry))}
            FROM user_product_timeline_cache_{granularity.suffix}
            WHERE user_id = $1 AND "timestamp" <= $2
            ORDER BY product_id, "timestamp" DESC
        """,
        user_id,
        watermark,
    )
    seed_user_product_timeline: dict[UUID, dict[UUID, UserProductTimelineEntry]] = {}
    for upt_row in seed_upt_rows:
        upt_entry = UserProductTimelineEntry(*upt_row)
        seed_user_product_timeline.setdefault(user_id, {})[upt_entry.product_id] = upt_entry

    # Compute fresh user_timeline entries
    fresh_entries = await compute_user_timeline(
        fresh_upt_iter,
        seed_user_product_timeline,
    )

    # Return cached + fresh combined
    return cached_entries + fresh_entries


@_transaction
async def refresh(connection: asyncpg.Connection) -> None:
    # Get last cumulative_cashflow per user-product
    seed_cumulative_cashflow_rows: list[asyncpg.Record] = await connection.fetch(f"""
        SELECT DISTINCT ON (user_id, product_id)
            {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        ORDER BY user_id, product_id, "timestamp" DESC
    """)
    # Build two structures: one for compute_cumulative_cashflows (user_id -> product_id)
    # and one for compute_user_product_timeline (product_id -> user_id)
    seed_cumulative_cashflows_by_user: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    seed_cumulative_cashflows_by_product: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    cumulative_cashflows_watermark = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    for ccf_row in seed_cumulative_cashflow_rows:
        ccf = CumulativeCashflow(*ccf_row)
        seed_cumulative_cashflows_by_user.setdefault(ccf.user_id, {})[ccf.product_id] = ccf
        seed_cumulative_cashflows_by_product.setdefault(ccf.product_id, {})[ccf.user_id] = ccf
        cumulative_cashflows_watermark = max(cumulative_cashflows_watermark, ccf.timestamp)

    cashflow_cursor = connection.cursor(
        f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            WHERE "timestamp" > $1
            ORDER BY "timestamp" ASC
        """,
        cumulative_cashflows_watermark,
        prefetch=settings.PREFETCH_COUNT,
    )
    cashflow_iter = cursor_to_async_iterator(cashflow_cursor, Cashflow)
    sorted_cumulative_cashflows_iter = refresh_cumulative_cashflows(
        connection, cashflow_iter, seed_cumulative_cashflows_by_user
    )
    sorted_cumulative_cashflows = await async_iterator_to_list(sorted_cumulative_cashflows_iter)

    for granularity in GRANULARITIES:
        # Refresh user_product_timeline_cache
        seed_price_update_rows: list[asyncpg.Record] = await connection.fetch(f"""
            SELECT DISTINCT ON (product_id) {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            ORDER BY product_id, "timestamp" DESC
        """)
        seed_price_updates: dict[UUID, PriceUpdate] = {
            pu["product_id"]: PriceUpdate(*pu) for pu in seed_price_update_rows
        }
        sorted_price_update_cursor = connection.cursor(
            f"""
                SELECT {", ".join(f.name for f in fields(PriceUpdate))}
                FROM price_update_{granularity.suffix}
                WHERE "timestamp" > (SELECT COALESCE(MAX("timestamp"), '-Infinity'::timestamptz)
                                    FROM user_product_timeline_cache_{granularity.suffix})
                ORDER BY "timestamp" ASC
            """,
            prefetch=settings.PREFETCH_COUNT,
        )
        sorted_price_update_iter = cursor_to_async_iterator(
            sorted_price_update_cursor, PriceUpdate
        )
        sorted_events_iter: AsyncIterator[CumulativeCashflow | PriceUpdate] = merge_sorted(
            sorted_price_update_iter, list_to_async_iterator(sorted_cumulative_cashflows)
        )
        sorted_user_product_iter = refresh_user_product_timeline(
            connection,
            granularity,
            sorted_events_iter,
            deepcopy(seed_cumulative_cashflows_by_product),
            seed_price_updates,
        )

        # Refresh user_timeline_cache

        # Get seed: latest UserProductTimelineEntry per (user_id, product_id) before watermark
        seed_upt_rows: list[asyncpg.Record] = await connection.fetch(f"""
            SELECT DISTINCT ON (user_id, product_id)
                {", ".join(f.name for f in fields(UserProductTimelineEntry))}
            FROM user_product_timeline_cache_{granularity.suffix}
            WHERE "timestamp" <= (SELECT COALESCE(MAX("timestamp"), 'Infinity'::timestamptz)
                                  FROM user_timeline_cache_{granularity.suffix})
            ORDER BY user_id, product_id, "timestamp" DESC
        """)

        seed_user_product_timeline: dict[UUID, dict[UUID, UserProductTimelineEntry]] = {}
        for upt_row in seed_upt_rows:
            seed_user_product_timeline.setdefault(upt_row["user_id"], {})[
                upt_row["product_id"]
            ] = UserProductTimelineEntry(*upt_row)

        await refresh_user_timeline(
            connection, granularity, sorted_user_product_iter, seed_user_product_timeline
        )
