import datetime
from collections.abc import AsyncIterator
from dataclasses import dataclass

import asyncpg
import pytest

from performance.iter_utils import batch_insert, merge_sorted
from performance.models import BasePerformanceEntry


@dataclass
class SampleEntry(BasePerformanceEntry):
    """Sample dataclass for iter_utils tests"""

    timestamp: datetime.datetime
    value: int

    def to_tuple(self) -> tuple[datetime.datetime, int]:
        return (self.timestamp, self.value)


@pytest.mark.asyncio
async def test_batch_insert(connection: asyncpg.Connection) -> None:
    """Test batch_insert with small batch size"""
    # Create test table
    await connection.execute("""
        CREATE TEMP TABLE test_batch (
            timestamp TIMESTAMPTZ NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    # Create test records
    async def record_generator() -> AsyncIterator[SampleEntry]:
        for i in range(25):
            yield SampleEntry(
                timestamp=datetime.datetime(2024, 1, 1, 0, i, tzinfo=datetime.timezone.utc),
                value=i,
            )

    # Insert with batch_size=10 (should create 3 batches: 10, 10, 5)
    rows = []
    async for ro in batch_insert(
        connection, "test_batch", record_generator(), columns=["timestamp", "value"], batch_size=10
    ):
        rows.append(ro)

    # Verify data was inserted
    assert len(rows) == 25
    for i, row in enumerate(rows):
        assert row.value == i


@pytest.mark.asyncio
async def test_merge_sorted() -> None:
    """Test merge_sorted with multiple iterators"""

    async def gen1() -> AsyncIterator[SampleEntry]:
        # Even timestamps
        for i in [0, 2, 4]:
            yield SampleEntry(
                timestamp=datetime.datetime(2024, 1, 1, i, 0, tzinfo=datetime.timezone.utc),
                value=i * 10,
            )

    async def gen2() -> AsyncIterator[SampleEntry]:
        # Odd timestamps
        for i in [1, 3, 5]:
            yield SampleEntry(
                timestamp=datetime.datetime(2024, 1, 1, i, 0, tzinfo=datetime.timezone.utc),
                value=i * 10,
            )

    result = []
    async for record in merge_sorted(gen1(), gen2()):
        result.append(record)

    assert len(result) == 6
    # Verify sorted by timestamp
    for i, record in enumerate(result):
        assert record.value == i * 10


@pytest.mark.asyncio
async def test_merge_sorted_three_iterators() -> None:
    """Test merge_sorted with three iterators"""

    async def gen1() -> AsyncIterator[SampleEntry]:
        for i in [0, 3, 6]:
            yield SampleEntry(
                timestamp=datetime.datetime(2024, 1, 1, i, 0, tzinfo=datetime.timezone.utc),
                value=i,
            )

    async def gen2() -> AsyncIterator[SampleEntry]:
        for i in [1, 4, 7]:
            yield SampleEntry(
                timestamp=datetime.datetime(2024, 1, 1, i, 0, tzinfo=datetime.timezone.utc),
                value=i,
            )

    async def gen3() -> AsyncIterator[SampleEntry]:
        for i in [2, 5, 8]:
            yield SampleEntry(
                timestamp=datetime.datetime(2024, 1, 1, i, 0, tzinfo=datetime.timezone.utc),
                value=i,
            )

    result = []
    async for record in merge_sorted(gen1(), gen2(), gen3()):
        result.append(record)

    assert len(result) == 9
    for i, record in enumerate(result):
        assert record.value == i


@pytest.mark.asyncio
async def test_merge_sorted_empty_iterators() -> None:
    """Test merge_sorted handles empty iterators"""

    async def empty_gen() -> AsyncIterator[SampleEntry]:
        return
        yield  # Make it a generator

    async def gen_with_data() -> AsyncIterator[SampleEntry]:
        for i in range(3):
            yield SampleEntry(
                timestamp=datetime.datetime(2024, 1, 1, i, 0, tzinfo=datetime.timezone.utc),
                value=i,
            )

    result = []
    async for record in merge_sorted(empty_gen(), gen_with_data(), empty_gen()):
        result.append(record)

    assert len(result) == 3
