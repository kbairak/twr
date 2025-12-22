from dataclasses import astuple, fields
import datetime
import functools
from uuid import UUID

import asyncpg


from performance.granularities import GRANULARITIES, Granularity
from performance.models import (
    Cashflow,
    CumulativeCashflow,
    PriceUpdate,
    UserProductTimelineEntry,
    UserTimelineEntry,
)
from performance.utils import (
    compute_cumulative_cashflows,
    compute_user_product_timeline,
    compute_user_timeline,
    refresh_cumulative_cashflows,
    refresh_user_product_timeline,
    refresh_user_timeline,
)


def _transaction(func):
    @functools.wraps(func)
    async def decorated(connection: asyncpg.Connection, *args, **kwargs):
        async with connection.transaction():
            return await func(connection, *args, **kwargs)

    return decorated


@_transaction
async def add_cashflows(connection: asyncpg.Connection, *cashflows: Cashflow):
    min_user_product_timestamps: dict[tuple[UUID, UUID], datetime.datetime] = {}
    min_user_timestamps: dict[UUID, datetime.datetime] = {}
    min_product_timestamps: dict[UUID, datetime.datetime] = {}
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
        min_product_timestamps[cf.product_id] = min(
            min_product_timestamps.get(
                cf.product_id, datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)
            ),
            cf.timestamp,
        )

    # Reverse-zip min_timestamps into user_ids, product_ids, timestamps
    keys, timestamps_for_user_product = zip(*min_user_product_timestamps.items())
    user_ids_for_user_product, product_ids_for_user_product = zip(*keys)
    user_ids_for_user_product, product_ids_for_user_product, timestamps_for_user_product = (
        list(user_ids_for_user_product),
        list(product_ids_for_user_product),
        list(timestamps_for_user_product),
    )
    user_ids_for_user, timestamps_for_user = zip(*min_user_timestamps.items())
    user_ids_for_user, timestamps_for_user = list(user_ids_for_user), list(timestamps_for_user)
    product_ids_for_product, timestamps_for_product = zip(*min_product_timestamps.items())
    product_ids_for_product, timestamps_for_product = (
        list(product_ids_for_product),
        list(timestamps_for_product),
    )

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
        records=[astuple(cf) for cf in cashflows],
        columns=[f.name for f in fields(Cashflow)],
    )

    # Repair cumulative cashlows
    sorted_cashflow_rows = await connection.fetch(
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
    )
    sorted_cashflows = [Cashflow(*cf) for cf in sorted_cashflow_rows]
    seed_cumulative_cashflow_rows = await connection.fetch(
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
    )
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    for ccf in seed_cumulative_cashflow_rows:
        seed_cumulative_cashflows.setdefault(ccf["user_id"], {})[ccf["product_id"]] = (
            CumulativeCashflow(*ccf)
        )
    sorted_cumulative_cashflows = await refresh_cumulative_cashflows(
        connection, sorted_cashflows, seed_cumulative_cashflows
    )

    # Repair user-product-timeline
    for granularity in GRANULARITIES:
        sorted_price_update_rows = await connection.fetch(
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
        )
        sorted_price_updates = [PriceUpdate(*pu) for pu in sorted_price_update_rows]
        sorted_events = sorted(
            sorted_cumulative_cashflows + sorted_price_updates,
            key=lambda e: (e.timestamp, isinstance(e, CumulativeCashflow)),
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
        await refresh_user_product_timeline(
            connection,
            granularity,
            sorted_events,
            seed_cumulative_cashflows,
            seed_price_updates,
        )

    # Repair user-timeline
    for granularity in GRANULARITIES:
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
        for upt in seed_upt_rows:
            seed_user_product_timeline.setdefault(upt["user_id"], {})[upt["product_id"]] = (
                UserProductTimelineEntry(*upt)
            )

        # Fetch ALL user_product_timeline entries >= min_affected_timestamp for affected users
        # (not just the ones we just refreshed, which only include affected products)
        sorted_user_product_rows = await connection.fetch(
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
        )
        sorted_user_product_timeline = [
            UserProductTimelineEntry(*upt) for upt in sorted_user_product_rows
        ]

        await refresh_user_timeline(
            connection,
            granularity,
            sorted_user_product_timeline,
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
async def add_price_update(connection: asyncpg.Connection, *price_updates: PriceUpdate):
    await connection.copy_records_to_table(
        "price_update",
        records=[astuple(pu) for pu in price_updates],
        columns=[f.name for f in fields(PriceUpdate)],
    )


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
    sorted_cashflow_rows = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            WHERE user_id = $1 AND product_id = $2 AND "timestamp" > $3
            ORDER BY "timestamp" ASC
        """,
        user_id,
        product_id,
        watermark,
    )
    sorted_cashflows = [Cashflow(*cf) for cf in sorted_cashflow_rows]

    # Compute cumulative cashflows for fresh data
    sorted_cumulative_cashflows = await compute_cumulative_cashflows(
        sorted_cashflows, seed_ccf_for_compute_ccf
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
    sorted_price_update_rows = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            WHERE product_id = $1 AND "timestamp" > $2
            ORDER BY "timestamp" ASC
        """,
        product_id,
        watermark,
    )
    sorted_price_updates = [PriceUpdate(*pu) for pu in sorted_price_update_rows]

    # Merge events and sort
    sorted_events = sorted(
        sorted_cumulative_cashflows + sorted_price_updates,
        key=lambda e: (e.timestamp, isinstance(e, CumulativeCashflow)),
    )

    # Compute fresh entries
    fresh_entries = await compute_user_product_timeline(
        sorted_events,
        seed_ccf_for_compute_upt,
        seed_price_updates,
    )

    # Return cached + fresh combined
    return cached_entries + fresh_entries


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
    sorted_cashflow_rows = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            WHERE user_id = $1 AND "timestamp" > $2
            ORDER BY "timestamp" ASC
        """,
        user_id,
        watermark,
    )
    sorted_cashflows = [Cashflow(*cf) for cf in sorted_cashflow_rows]

    # Compute cumulative cashflows for fresh data
    sorted_cumulative_cashflows = await compute_cumulative_cashflows(
        sorted_cashflows, seed_ccf_for_compute_ccf
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
    sorted_price_update_rows = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            WHERE product_id IN (SELECT DISTINCT product_id FROM cashflow WHERE user_id = $1)
              AND "timestamp" > $2
            ORDER BY "timestamp" ASC
        """,
        user_id,
        watermark,
    )
    sorted_price_updates = [PriceUpdate(*pu) for pu in sorted_price_update_rows]

    # Merge events and sort
    sorted_events = sorted(
        sorted_cumulative_cashflows + sorted_price_updates,
        key=lambda e: (e.timestamp, isinstance(e, CumulativeCashflow)),
    )

    # Compute fresh user_product_timeline entries
    fresh_upt_entries = await compute_user_product_timeline(
        sorted_events,
        seed_ccf_for_compute_upt,
        seed_price_updates,
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
    for upt in seed_upt_rows:
        upt_entry = UserProductTimelineEntry(*upt)
        seed_user_product_timeline.setdefault(user_id, {})[upt_entry.product_id] = upt_entry

    # Compute fresh user_timeline entries
    fresh_entries = await compute_user_timeline(
        fresh_upt_entries,
        seed_user_product_timeline,
    )

    # Return cached + fresh combined
    return cached_entries + fresh_entries


@_transaction
async def refresh(connection: asyncpg.Connection):
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
    for ccf in seed_cumulative_cashflow_rows:
        ccf_obj = CumulativeCashflow(*ccf)
        seed_cumulative_cashflows_by_user.setdefault(ccf["user_id"], {})[ccf["product_id"]] = ccf_obj
        seed_cumulative_cashflows_by_product.setdefault(ccf["product_id"], {})[ccf["user_id"]] = ccf_obj
        cumulative_cashflows_watermark = max(cumulative_cashflows_watermark, ccf["timestamp"])

    sorted_cashflow_rows: list[asyncpg.Record] = await connection.fetch(
        f"""
            SELECT {", ".join(f.name for f in fields(Cashflow))}
            FROM cashflow
            WHERE "timestamp" > $1
            ORDER BY "timestamp" ASC
        """,
        cumulative_cashflows_watermark,
    )
    sorted_cashflows = [Cashflow(*record) for record in sorted_cashflow_rows]
    sorted_cumulative_cashflows = await refresh_cumulative_cashflows(
        connection, sorted_cashflows, seed_cumulative_cashflows_by_user
    )

    for granularity in GRANULARITIES:
        seed_price_update_rows: list[asyncpg.Record] = await connection.fetch(f"""
            SELECT DISTINCT ON (product_id) {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            ORDER BY product_id, "timestamp" DESC
        """)
        seed_price_updates: dict[UUID, PriceUpdate] = {
            pu["product_id"]: PriceUpdate(*pu) for pu in seed_price_update_rows
        }
        sorted_price_update_rows: list[asyncpg.Record] = await connection.fetch(f"""
            SELECT {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            WHERE "timestamp" > (SELECT COALESCE(MAX("timestamp"), '-Infinity'::timestamptz)
                                 FROM user_product_timeline_cache_{granularity.suffix})
            ORDER BY "timestamp" ASC
        """)
        sorted_price_updates = [PriceUpdate(*pu) for pu in sorted_price_update_rows]
        sorted_events = sorted(
            sorted_cumulative_cashflows + sorted_price_updates,
            key=lambda e: (e.timestamp, isinstance(e, CumulativeCashflow)),
        )
        await refresh_user_product_timeline(
            connection, granularity, sorted_events, seed_cumulative_cashflows_by_product, seed_price_updates
        )

    # Refresh user_timeline_cache
    for granularity in GRANULARITIES:
        # Fetch UserProductTimelineEntry objects after watermark
        sorted_upt_rows: list[asyncpg.Record] = await connection.fetch(f"""
            SELECT {", ".join(f.name for f in fields(UserProductTimelineEntry))}
            FROM user_product_timeline_cache_{granularity.suffix}
            WHERE "timestamp" > (SELECT COALESCE(MAX("timestamp"), '-Infinity'::timestamptz)
                                 FROM user_timeline_cache_{granularity.suffix})
            ORDER BY "timestamp" ASC
        """)
        sorted_user_product_timeline = [UserProductTimelineEntry(*upt) for upt in sorted_upt_rows]

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
        for upt in seed_upt_rows:
            seed_user_product_timeline.setdefault(upt["user_id"], {})[upt["product_id"]] = (
                UserProductTimelineEntry(*upt)
            )

        await refresh_user_timeline(
            connection,
            granularity,
            sorted_user_product_timeline,
            seed_user_product_timeline,
        )
