from dataclasses import astuple, fields
import datetime
import functools
from uuid import UUID

import asyncpg


from performance.granularities import GRANULARITIES
from performance.models import Cashflow, CumulativeCashflow, PriceUpdate
from performance.utils import refresh_cumulative_cashflows, refresh_user_product_timeline


def _transaction(func):
    @functools.wraps(func)
    async def decorated(connection: asyncpg.Connection, *args, **kwargs):
        async with connection.transaction():
            return await func(connection, *args, **kwargs)

    return decorated


@_transaction
async def add_cashflows(connection: asyncpg.Connection, *cashflows: Cashflow):
    min_timestamps: dict[tuple[UUID, UUID], datetime.datetime] = {}
    for cf in cashflows:
        min_timestamps[(cf.user_id, cf.product_id)] = min(
            min_timestamps.get(
                (cf.user_id, cf.product_id),
                datetime.datetime.max.replace(tzinfo=datetime.timezone.utc),
            ),
            cf.timestamp,
        )

    # Reverse-zip min_timestamps into user_ids, product_ids, timestamps
    keys, timestamps = zip(*min_timestamps.items())
    user_ids, product_ids = zip(*keys)
    user_ids, product_ids, timestamps = list(user_ids), list(product_ids), list(timestamps)

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
        user_ids,
        product_ids,
        timestamps,
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
            user_ids,
            product_ids,
            timestamps,
        )

    # Insert new cashflows
    await connection.copy_records_to_table(
        "cashflow",
        records=[astuple(cf) for cf in cashflows],
        columns=[f.name for f in fields(Cashflow)],
    )

    # Repair cumulative_cashflow_cache
    cumulative_cashflows_watermark = await connection.fetchval("""
        SELECT MAX("timestamp")
        FROM cumulative_cashflow_cache
    """)
    sorted_cashflow_rows = await connection.fetch(
        f"""
            WITH min_cashflows AS (
                SELECT unnest($1::uuid[]) AS user_id,
                       unnest($2::uuid[]) AS product_id,
                       unnest($3::timestamptz[]) AS "timestamp"
            )
            SELECT {", ".join(f"cf.{f.name}" for f in fields(Cashflow))}
            FROM cashflow cf
                INNER JOIN min_cashflows mcf
                    ON cf.user_id = mcf.user_id AND
                       cf.product_id = mcf.product_id AND
                       cf."timestamp" >= mcf."timestamp"
            WHERE cf."timestamp" <= COALESCE($4, 'Infinity'::timestamptz)
            ORDER BY cf."timestamp" ASC
        """,
        user_ids,
        product_ids,
        timestamps,
        cumulative_cashflows_watermark,
    )
    sorted_cashflows = [Cashflow(*cf) for cf in sorted_cashflow_rows]

    seed_cumulative_cashflow_rows = await connection.fetch(
        f"""
            WITH min_cashflows AS (
                SELECT unnest($1::uuid[]) AS user_id, unnest($2::uuid[]) AS product_id
            )
            SELECT DISTINCT ON (user_id, product_id)
                {", ".join(f"ccc.{f.name}" for f in fields(CumulativeCashflow))}
            FROM cumulative_cashflow_cache ccc
                INNER JOIN min_cashflows mcf
                    ON ccc.user_id = mcf.user_id AND ccc.product_id = mcf.product_id
            ORDER BY user_id, product_id, "timestamp" DESC
        """,
        user_ids,
        product_ids,
    )
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    for ccf in seed_cumulative_cashflow_rows:
        seed_cumulative_cashflows.setdefault(ccf["user_id"], {})[ccf["product_id"]] = (
            CumulativeCashflow(*ccf)
        )
    sorted_cumulative_cashflows = await refresh_cumulative_cashflows(
        connection, sorted_cashflows, seed_cumulative_cashflows
    )

    for granularity in GRANULARITIES:
        min_price_timestamps: dict[UUID, datetime.datetime] = {}
        for (_, product_id), timestamp in min_timestamps.items():
            min_price_timestamps[product_id] = min(
                min_price_timestamps.get(
                    product_id,
                    datetime.datetime.max.replace(tzinfo=datetime.timezone.utc),
                ),
                timestamp,
            )
            # Reverse-zip min_timestamps into product_ids, timestamps
            product_ids, timestamps = zip(*min_price_timestamps.items())
            price_watermark = await connection.fetchval(f"""
                SELECT MAX("timestamp")
                FROM user_product_timeline_cache_{granularity.suffix}
            """)
            sorted_price_update_rows = await connection.fetch(
                f"""
                    WITH min_price_timestamps AS (
                        SELECT unnest($1::uuid[]) AS product_id,
                               unnest($2::timestamptz[]) AS "timestamp"
                    )
                    SELECT {", ".join(f"pu.{f.name}" for f in fields(PriceUpdate))}
                    FROM price_update_{granularity.suffix} pu
                        INNER JOIN min_price_timestamps mpt
                            ON pu.product_id = mpt.product_id AND pu."timestamp" >= mpt."timestamp"
                    WHERE pu."timestamp" <= COALESCE($3, 'Infinity'::timestamptz)
                    ORDER BY pu."timestamp" ASC

                """,
                product_ids,
                timestamps,
                price_watermark,
            )
            sorted_price_updates = [PriceUpdate(*pu) for pu in sorted_price_update_rows]
            sorted_events = sorted(
                sorted_cumulative_cashflows + sorted_price_updates, key=lambda e: e.timestamp
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
                product_ids,
                timestamps,
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


@_transaction
async def refresh(connection: asyncpg.Connection):
    # Get last cumulative_cashflow per user-product
    seed_cumulative_cashflow_rows: list[asyncpg.Record] = await connection.fetch(f"""
        SELECT DISTINCT ON (user_id, product_id)
            {", ".join(f.name for f in fields(CumulativeCashflow))}
        FROM cumulative_cashflow_cache
        ORDER BY user_id, product_id, "timestamp" DESC
    """)
    seed_cumulative_cashflows: dict[UUID, dict[UUID, CumulativeCashflow]] = {}
    cumulative_cashflows_watermark = datetime.datetime.min
    for ccf in seed_cumulative_cashflow_rows:
        seed_cumulative_cashflows.setdefault(ccf["user_id"], {})[ccf["product_id"]] = (
            CumulativeCashflow(*ccf)
        )
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
    await refresh_cumulative_cashflows(connection, sorted_cashflows, seed_cumulative_cashflows)

    for granularity in GRANULARITIES:
        seed_price_update_rows: list[asyncpg.Record] = await connection.fetch(f"""
            SELECT DISTINCT ON (product_id) {", ".join(f.name for f in fields(PriceUpdate))}
            FROM price_update_{granularity.suffix}
            ORDER BY product_id, "timestamp" DESC
        """)
        seed_price_updates: dict[UUID, PriceUpdate] = {
            pu["product_id"]: PriceUpdate(*pu) for pu in seed_price_update_rows
        }
        user_product_timeline_watermark = await connection.fetchval(f"""
            SELECT MAX("timestamp")
            FROM user_product_timeline_cache_{granularity.suffix}
        """)
        sorted_cumulative_cashflow_rows: list[asyncpg.Record] = await connection.fetch(
            f"""
                SELECT {", ".join(f.name for f in fields(CumulativeCashflow))}
                FROM cumulative_cashflow_cache
                WHERE timestamp > $1
                ORDER BY "timestamp" ASC
            """,
            user_product_timeline_watermark,
        )
        sorted_cumulative_cashflows = [
            CumulativeCashflow(*ccf) for ccf in sorted_cumulative_cashflow_rows
        ]
        sorted_price_update_rows: list[asyncpg.Record] = await connection.fetch(
            f"""
                SELECT {", ".join(f.name for f in fields(PriceUpdate))}
                FROM price_update_{granularity.suffix}
                WHERE "timestamp" > $1
                ORDER BY "timestamp" ASC
            """,
        )
        sorted_price_updates = [PriceUpdate(*pu) for pu in sorted_price_update_rows]
        sorted_events = sorted(
            sorted_cumulative_cashflows + sorted_price_updates, key=lambda e: e.timestamp
        )
        await refresh_user_product_timeline(
            connection, granularity, sorted_events, seed_cumulative_cashflows, seed_price_updates
        )
