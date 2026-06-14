import json
from datetime import datetime, timezone

import pytest

from app import worker as worker_module
from app.models import EventIn


class FakeRedis:
    def __init__(self, attempt: int = 1):
        self.attempt = attempt
        self.acked = []
        self.hdeleted = []
        self.deadletters = []
        self.claimed_called = False

    async def xack(self, *args):
        self.acked.append(args)

    async def hdel(self, *args):
        self.hdeleted.append(args)

    async def hincrby(self, *args):
        return self.attempt

    async def xadd(self, *args):
        self.deadletters.append(args)
        return "deadletter-1"

    async def xautoclaim(self, *args, **kwargs):
        self.claimed_called = True

        event = EventIn(
            topic="pending.test",
            event_id="pending-001",
            timestamp=datetime.now(timezone.utc),
            source="pytest",
            payload={"pending": True},
        )

        return (
            "0-0",
            [
                (
                    "1-0",
                    {
                        "event": json.dumps(event.model_dump(mode="json")),
                    },
                )
            ],
            [],
        )


class FakePool:
    def __init__(self):
        self.stats = []
        self.audit = []


def make_fields() -> dict:
    event = EventIn(
        topic="retry.test",
        event_id="retry-001",
        timestamp=datetime.now(timezone.utc),
        source="pytest",
        payload={"retry": True},
    )

    return {
        "event": json.dumps(event.model_dump(mode="json")),
    }


@pytest.mark.asyncio
async def test_worker_acks_only_after_success(monkeypatch):
    fake_redis = FakeRedis()
    fake_pool = FakePool()

    async def fake_process_event(pool, event, worker_name):
        return "processed"

    monkeypatch.setattr(worker_module, "process_event", fake_process_event)

    await worker_module.handle_message(
        redis_client=fake_redis,
        pool=fake_pool,
        message_id="1-0",
        fields=make_fields(),
        worker_name="pytest-worker",
    )

    assert len(fake_redis.acked) == 1
    assert len(fake_redis.hdeleted) == 1


@pytest.mark.asyncio
async def test_failed_message_is_not_acked_before_max_retry(monkeypatch):
    fake_redis = FakeRedis(attempt=1)
    fake_pool = FakePool()

    async def fake_process_event(pool, event, worker_name):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(worker_module, "process_event", fake_process_event)
    monkeypatch.setattr(worker_module, "calculate_backoff_seconds", lambda attempt: 0)

    await worker_module.handle_message(
        redis_client=fake_redis,
        pool=fake_pool,
        message_id="1-0",
        fields=make_fields(),
        worker_name="pytest-worker",
    )

    assert len(fake_redis.acked) == 0
    assert len(fake_redis.deadletters) == 0


@pytest.mark.asyncio
async def test_failed_message_is_deadlettered_after_max_retry(monkeypatch):
    fake_redis = FakeRedis(attempt=worker_module.MAX_RETRIES)
    fake_pool = FakePool()

    async def fake_process_event(pool, event, worker_name):
        raise RuntimeError("simulated permanent failure")

    async def fake_increment_stat(pool, name, delta=1):
        pool.stats.append((name, delta))

    async def fake_record_audit_log(pool, topic, event_id, status, message, worker_name):
        pool.audit.append(
            {
                "topic": topic,
                "event_id": event_id,
                "status": status,
                "message": message,
                "worker_name": worker_name,
            }
        )

    monkeypatch.setattr(worker_module, "process_event", fake_process_event)
    monkeypatch.setattr(worker_module, "increment_stat", fake_increment_stat)
    monkeypatch.setattr(worker_module, "record_audit_log", fake_record_audit_log)

    await worker_module.handle_message(
        redis_client=fake_redis,
        pool=fake_pool,
        message_id="1-0",
        fields=make_fields(),
        worker_name="pytest-worker",
    )

    assert len(fake_redis.acked) == 1
    assert len(fake_redis.deadletters) == 1
    assert ("process_errors", 1) in fake_pool.stats
    assert ("dead_lettered", 1) in fake_pool.stats
    assert fake_pool.audit[0]["status"] == "dead_lettered"


@pytest.mark.asyncio
async def test_pending_recovery_claims_and_processes_messages(monkeypatch):
    fake_redis = FakeRedis()
    fake_pool = FakePool()
    handled_messages = []

    async def fake_handle_message(redis_client, pool, message_id, fields, worker_name):
        handled_messages.append(message_id)

    monkeypatch.setattr(worker_module, "handle_message", fake_handle_message)

    recovered = await worker_module.recover_pending_messages(
        redis_client=fake_redis,
        pool=fake_pool,
        worker_name="pytest-worker",
    )

    assert recovered == 1
    assert fake_redis.claimed_called is True
    assert handled_messages == ["1-0"]