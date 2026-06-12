from uuid import uuid4

import pytest

from app.repository import get_stats, process_event


@pytest.mark.asyncio
async def test_get_stats_contains_required_counters(db_pool):
    data = await get_stats(db_pool)

    required_counters = {
        "received",
        "stream_enqueued",
        "unique_processed",
        "duplicate_dropped",
        "process_errors",
    }

    assert "counters" in data
    assert required_counters.issubset(set(data["counters"].keys()))


@pytest.mark.asyncio
async def test_unique_processing_updates_topic_stats(db_pool, make_event):
    topic = f"test.stats.{uuid4().hex[:8]}"
    event = make_event(topic=topic)

    await process_event(
        pool=db_pool,
        event=event,
        worker_name="pytest-worker",
    )

    data = await get_stats(db_pool)

    topic_stat = next(
        item for item in data["topics"]
        if item["topic"] == topic
    )

    assert topic_stat["unique_count"] == 1
    assert topic_stat["duplicate_count"] == 0


@pytest.mark.asyncio
async def test_duplicate_processing_updates_topic_duplicate_stats(db_pool, make_event):
    topic = f"test.dup.{uuid4().hex[:8]}"
    event = make_event(topic=topic)

    await process_event(
        pool=db_pool,
        event=event,
        worker_name="pytest-worker-1",
    )

    await process_event(
        pool=db_pool,
        event=event,
        worker_name="pytest-worker-2",
    )

    data = await get_stats(db_pool)

    topic_stat = next(
        item for item in data["topics"]
        if item["topic"] == topic
    )

    assert topic_stat["unique_count"] == 1
    assert topic_stat["duplicate_count"] == 1