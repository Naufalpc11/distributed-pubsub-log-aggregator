import pytest

from app.repository import process_event


async def count_event(pool, topic: str, event_id: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM processed_events
            WHERE topic = $1 AND event_id = $2
            """,
            topic,
            event_id,
        )


@pytest.mark.asyncio
async def test_process_unique_event_returns_processed(db_pool, make_event):
    event = make_event()

    status = await process_event(
        pool=db_pool,
        event=event,
        worker_name="pytest-worker",
    )

    assert status == "processed"


@pytest.mark.asyncio
async def test_process_duplicate_event_returns_duplicate_dropped(db_pool, make_event):
    event = make_event()

    first_status = await process_event(
        pool=db_pool,
        event=event,
        worker_name="pytest-worker",
    )

    second_status = await process_event(
        pool=db_pool,
        event=event,
        worker_name="pytest-worker",
    )

    assert first_status == "processed"
    assert second_status == "duplicate_dropped"


@pytest.mark.asyncio
async def test_duplicate_event_is_stored_only_once(db_pool, make_event):
    event = make_event()

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

    total = await count_event(
        db_pool,
        topic=event.topic,
        event_id=event.event_id,
    )

    assert total == 1