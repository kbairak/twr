"""Utilities for streaming/iterating over database records"""

import datetime
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Sequence
from uuid import UUID

import asyncpg
from asyncpg.cursor import CursorFactory

from performance.models import BasePerformanceEntry


async def batch_insert[T: BasePerformanceEntry](
    connection: asyncpg.Connection,
    table_name: str,
    entries: AsyncIterator[T],
    columns: Sequence[str],
    batch_size: int = 10_000,
) -> AsyncIterator[T]:
    """Insert entries into database table in batches.

    Args:
        connection: Database connection
        table_name: Table name to insert into
        entries: Async iterator of BasePerformanceEntry objects
        columns: Column names for insertion
        batch_size: Number of entries per batch
    """
    batch = []
    total = 0

    async for entry in entries:
        batch.append(entry.to_tuple())

        if len(batch) >= batch_size:
            await _batch_insert_with_conflict_handling(connection, table_name, batch, columns)
            total += len(batch)
            batch.clear()
        yield entry

    # Insert remaining entries
    if batch:
        await _batch_insert_with_conflict_handling(connection, table_name, batch, columns)
        total += len(batch)


async def _batch_insert_with_conflict_handling(
    connection: asyncpg.Connection,
    table_name: str,
    records: list[tuple],
    columns: Sequence[str],
) -> None:
    """Insert records with ON CONFLICT DO NOTHING to handle duplicates."""
    if not records:
        return

    # Build arrays for each column
    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
    column_names = ", ".join(f'"{col}"' for col in columns)

    # Use unnest to insert multiple rows from arrays
    unnest_expr = ", ".join(f"unnest(${i+1}::{_infer_array_type(records[0][i])}[])" for i in range(len(columns)))

    query = f"""
        INSERT INTO {table_name} ({column_names})
        SELECT {unnest_expr}
        ON CONFLICT DO NOTHING
    """

    # Transpose records: list of tuples -> tuple of lists
    columns_data = tuple([record[i] for record in records] for i in range(len(columns)))

    await connection.execute(query, *columns_data)


def _infer_array_type(value) -> str:
    """Infer PostgreSQL array type from Python value."""
    if isinstance(value, UUID):
        return "uuid"
    elif isinstance(value, datetime.datetime):
        return "timestamptz"
    elif isinstance(value, Decimal):
        return "numeric"
    elif isinstance(value, int):
        return "bigint"
    elif isinstance(value, float):
        return "float8"
    elif isinstance(value, str):
        return "text"
    else:
        # Fallback
        return "text"


async def cursor_to_async_iterator[T: BasePerformanceEntry](
    cursor: CursorFactory, cls: type[T]
) -> AsyncIterator[T]:
    async for record in cursor:
        yield cls(*record)


async def merge_sorted[T: BasePerformanceEntry](
    *iterators: AsyncIterator[T],
) -> AsyncIterator[T]:
    """Merge sorted async iterators into a single sorted async iterator by timestamp.

    All input iterators must be sorted by timestamp.

    Args:
        *iterators: Variable number of sorted async iterators

    Yields:
        Merged items in sorted order by timestamp
    """
    if not iterators:
        return

    # Track active iterators with their current items
    active: list[tuple[T, AsyncIterator[T]]] = []

    # Prime all iterators
    for iterator in iterators:
        try:
            item = await anext(iterator)
            active.append((item, iterator))
        except StopAsyncIteration:
            pass  # Iterator is empty

    # Merge loop
    while active:
        # Find the item with the smallest timestamp
        min_idx = 0
        for i in range(1, len(active)):
            if active[i][0].timestamp < active[min_idx][0].timestamp:
                min_idx = i

        # Yield the smallest item
        item, iterator = active[min_idx]
        yield item

        # Try to get the next item from that iterator
        try:
            next_item = await anext(iterator)
            active[min_idx] = (next_item, iterator)
        except StopAsyncIteration:
            # Iterator exhausted, remove it
            active.pop(min_idx)


async def async_iterator_to_list[T](async_iterator: AsyncIterator[T]) -> list[T]:
    result = []
    async for item in async_iterator:
        result.append(item)
    return result


async def list_to_async_iterator[T](lst: Sequence[T]) -> AsyncIterator[T]:
    for item in lst:
        yield item
