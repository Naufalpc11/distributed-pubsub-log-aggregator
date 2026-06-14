import pytest

from app.db import create_pool
from app.repository import get_events, process_event


@pytest.mark.asyncio
async def test_event_persists_across_database_reconnect(db_pool, make_event):
    event = make_event()

    await process_event(
        pool=db_pool,
        event=event,
        worker_name="pytest-worker-before-recreate",
    )

    new_pool = await create_pool()

    try:
        events = await get_events(
            pool=new_pool,
            topic=event.topic,
            limit=10,
        )
    finally:
        await new_pool.close()

    assert any(
        item["event_id"] == event.event_id
        for item in events
    )
