import datetime
from collections.abc import AsyncIterator
from dataclasses import dataclass
from decimal import Decimal
from uuid import uuid4

import pytest

from performance.iter_utils import batch_insert, deduplicate_by_timestamp, merge_sorted
from performance.models import BasePerformanceEntry


@dataclass
class SampleEntry(BasePerformanceEntry):
    """Sample dataclass for iter_utils tests"""

    timestamp: datetime.datetime
    value: int

    def to_tuple(self):
        return (self.timestamp, self.value)


@pytest.mark.asyncio
async def test_batch_insert(connection):
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
    total = await batch_insert(
        connection,
        "test_batch",
        record_generator(),
        columns=["timestamp", "value"],
        batch_size=10,
    )

    assert total == 25

    # Verify data was inserted
    rows = await connection.fetch("SELECT * FROM test_batch ORDER BY value")
    assert len(rows) == 25
    for i, row in enumerate(rows):
        assert row["value"] == i


@pytest.mark.asyncio
async def test_deduplicate_by_timestamp(connection):
    """Test deduplicate_by_timestamp keeps last record for each timestamp"""

    async def record_generator() -> AsyncIterator[SampleEntry]:
        ts1 = datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        ts2 = datetime.datetime(2024, 1, 1, 1, 0, tzinfo=datetime.timezone.utc)

        # Two records with ts1 (should keep value=2)
        yield SampleEntry(timestamp=ts1, value=1)
        yield SampleEntry(timestamp=ts1, value=2)

        # Three records with ts2 (should keep value=5)
        yield SampleEntry(timestamp=ts2, value=3)
        yield SampleEntry(timestamp=ts2, value=4)
        yield SampleEntry(timestamp=ts2, value=5)

    result = []
    async for record in deduplicate_by_timestamp(record_generator()):
        result.append(record)

    assert len(result) == 2
    assert result[0].value == 2  # Last with ts1
    assert result[1].value == 5  # Last with ts2


@pytest.mark.asyncio
async def test_deduplicate_by_timestamp_no_duplicates(connection):
    """Test deduplicate_by_timestamp with no duplicates"""

    async def record_generator() -> AsyncIterator[SampleEntry]:
        for i in range(5):
            yield SampleEntry(
                timestamp=datetime.datetime(2024, 1, 1, i, 0, tzinfo=datetime.timezone.utc),
                value=i,
            )

    result = []
    async for record in deduplicate_by_timestamp(record_generator()):
        result.append(record)

    assert len(result) == 5
    for i, record in enumerate(result):
        assert record.value == i


@pytest.mark.asyncio
async def test_merge_sorted(connection):
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
async def test_merge_sorted_three_iterators(connection):
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
async def test_merge_sorted_empty_iterators(connection):
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
