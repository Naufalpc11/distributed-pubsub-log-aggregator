import asyncio
from uuid import uuid4

import pytest

from app.repository import process_event


@pytest.mark.asyncio
async def test_concurrent_duplicate_processing_saves_one_row(db_pool, make_event):
    topic = f"test.concurrent.{uuid4().hex[:8]}"
    event = make_event(topic=topic)

    results = await asyncio.gather(
        *[
            process_event(
                pool=db_pool,
                event=event,
                worker_name=f"pytest-worker-{index}",
            )
            for index in range(20)
        ]
    )

    async with db_pool.acquire() as conn:
        total_rows = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM processed_events
            WHERE topic = $1 AND event_id = $2
            """,
            event.topic,
            event.event_id,
        )

    assert results.count("processed") == 1
    assert results.count("duplicate_dropped") == 19
    assert total_rows == 1


@pytest.mark.asyncio
async def test_concurrent_unique_events_are_all_processed(db_pool, make_event):
    topic = f"test.concurrent.unique.{uuid4().hex[:8]}"

    events = [
        make_event(
            topic=topic,
            event_id=f"unique-{index}-{uuid4().hex[:8]}",
        )
        for index in range(10)
    ]

    results = await asyncio.gather(
        *[
            process_event(
                pool=db_pool,
                event=event,
                worker_name=f"pytest-worker-{index}",
            )
            for index, event in enumerate(events)
        ]
    )

    async with db_pool.acquire() as conn:
        total_rows = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM processed_events
            WHERE topic = $1
            """,
            topic,
        )

    assert results.count("processed") == 10
    assert total_rows == 10