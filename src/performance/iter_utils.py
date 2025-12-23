"""Utilities for streaming/iterating over database records"""

from collections.abc import AsyncIterator

import asyncpg
from asyncpg.cursor import CursorFactory

from performance.models import BasePerformanceEntry


async def batch_insert(
    connection: asyncpg.Connection,
    table_name: str,
    records: AsyncIterator[BasePerformanceEntry],
    columns: list[str],
    batch_size: int = 10000,
) -> int:
    """Insert records into database table in batches.

    Args:
        connection: Database connection
        table_name: Table name to insert into
        records: Async iterator of BasePerformanceEntry objects
        columns: Column names for insertion
        batch_size: Number of records per batch

    Returns:
        Total number of records inserted
    """
    batch = []
    total = 0

    async for record in records:
        batch.append(record.to_tuple())

        if len(batch) >= batch_size:
            await connection.copy_records_to_table(table_name, records=batch, columns=columns)
            total += len(batch)
            batch.clear()

    # Insert remaining records
    if batch:
        await connection.copy_records_to_table(table_name, records=batch, columns=columns)
        total += len(batch)

    return total


async def deduplicate_by_timestamp[E: BasePerformanceEntry](
    records: AsyncIterator[E],
) -> AsyncIterator[E]:
    """Deduplicate records by timestamp, keeping the last record for each timestamp.

    When multiple records have the same timestamp, only the last one is yielded.
    This function holds onto a record until it sees one with a different timestamp.

    Args:
        records: Async iterator of records (must be sorted by timestamp)

    Yields:
        Deduplicated records
    """
    current: E | None = None

    async for record in records:
        if current is None:
            # First record
            current = record
        elif record.timestamp != current.timestamp:
            # Different timestamp - yield the previous record and update current
            yield current
            current = record
        else:
            # Same timestamp - replace current (we keep the last one)
            current = record

    # Yield the last record
    if current is not None:
        yield current


async def cursor_to_async_iterator[T: BasePerformanceEntry](
    cursor: CursorFactory, cls: type[T]
) -> AsyncIterator[T]:
    async for record in cursor:
        yield cls(*record)


async def merge_sorted[E: BasePerformanceEntry](
    *iterators: AsyncIterator[E],
) -> AsyncIterator[E]:
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
    active: list[tuple[E, AsyncIterator[E]]] = []

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
