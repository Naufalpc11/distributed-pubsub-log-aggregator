import pytest

from app.repository import process_event


@pytest.mark.asyncio
async def test_worker_processing_is_idempotent(db_pool, make_event):
    event = make_event()

    results = []

    for _ in range(3):
        status = await process_event(
            pool=db_pool,
            event=event,
            worker_name="pytest-worker",
        )

        results.append(status)

    assert results == [
        "processed",
        "duplicate_dropped",
        "duplicate_dropped",
    ]


@pytest.mark.asyncio
async def test_audit_log_records_processed_and_duplicate_status(db_pool, make_event):
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

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT status
            FROM audit_logs
            WHERE topic = $1 AND event_id = $2
            ORDER BY id
            """,
            event.topic,
            event.event_id,
        )

    statuses = [row["status"] for row in rows]

    assert "processed" in statuses
    assert "duplicate_dropped" in statuses