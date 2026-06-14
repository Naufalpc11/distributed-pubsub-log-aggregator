import asyncio
import json
import logging
import socket
from typing import Any

from app.config import (
    CONSUMER_GROUP,
    MAX_RETRIES,
    PENDING_IDLE_MS,
    READ_BLOCK_MS,
    READ_COUNT,
    RETRY_BACKOFF_BASE_SECONDS,
    STREAM_NAME,
    WORKER_NAME,
)
from app.db import create_pool
from app.models import EventIn
from app.redis_client import create_redis_client, ensure_consumer_group
from app.repository import (
    increment_stat,
    process_event,
    record_audit_log,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def calculate_backoff_seconds(attempt: int) -> float:
    safe_attempt = max(1, attempt)
    return RETRY_BACKOFF_BASE_SECONDS * (2 ** (safe_attempt - 1))


def extract_event_identity(
    fields: dict[str, Any],
    fallback_event_id: str,
) -> tuple[str, str]:
    try:
        raw_event = fields.get("event", "{}")
        event_data = json.loads(raw_event)

        topic = event_data.get("topic", "unknown")
        event_id = event_data.get("event_id", fallback_event_id)

        return topic, event_id
    except Exception:
        return "unknown", fallback_event_id


async def dead_letter_message(
    redis_client,
    pool,
    message_id: str,
    fields: dict[str, Any],
    worker_name: str,
    error: Exception,
) -> None:
    topic, event_id = extract_event_identity(fields, message_id)

    await increment_stat(pool, "process_errors", 1)
    await increment_stat(pool, "dead_lettered", 1)

    await record_audit_log(
        pool=pool,
        topic=topic,
        event_id=event_id,
        status="dead_lettered",
        message=f"max retries exceeded: {error}",
        worker_name=worker_name,
    )

    await redis_client.xadd(
        f"{STREAM_NAME}:deadletter",
        {
            "original_message_id": message_id,
            "worker_name": worker_name,
            "error": str(error),
            "fields": json.dumps(fields),
        },
    )


async def handle_message(
    redis_client,
    pool,
    message_id: str,
    fields: dict[str, Any],
    worker_name: str,
) -> None:
    try:
        raw_event = fields["event"]
        event = EventIn.model_validate_json(raw_event)

        status = await process_event(
            pool=pool,
            event=event,
            worker_name=worker_name,
        )

        await redis_client.xack(
            STREAM_NAME,
            CONSUMER_GROUP,
            message_id,
        )

        await redis_client.hdel(
            f"{STREAM_NAME}:retry-count",
            message_id,
        )

        logger.info(
            "%s | topic=%s | event_id=%s | redis_id=%s",
            status,
            event.topic,
            event.event_id,
            message_id,
        )

    except Exception as exc:
        attempt = await redis_client.hincrby(
            f"{STREAM_NAME}:retry-count",
            message_id,
            1,
        )

        if attempt >= MAX_RETRIES:
            await dead_letter_message(
                redis_client=redis_client,
                pool=pool,
                message_id=message_id,
                fields=fields,
                worker_name=worker_name,
                error=exc,
            )

            await redis_client.xack(
                STREAM_NAME,
                CONSUMER_GROUP,
                message_id,
            )

            await redis_client.hdel(
                f"{STREAM_NAME}:retry-count",
                message_id,
            )

            logger.exception(
                "dead_lettered | redis_id=%s | attempt=%s | error=%s",
                message_id,
                attempt,
                exc,
            )

        else:
            backoff_seconds = calculate_backoff_seconds(attempt)

            logger.warning(
                "retry_scheduled | redis_id=%s | attempt=%s/%s | backoff=%.2fs | error=%s",
                message_id,
                attempt,
                MAX_RETRIES,
                backoff_seconds,
                exc,
            )

            await asyncio.sleep(backoff_seconds)

            # Penting:
            # Tidak melakukan XACK di sini.
            # Message dibiarkan pending agar bisa di-retry/recovered oleh worker.
            return


async def recover_pending_messages(
    redis_client,
    pool,
    worker_name: str,
) -> int:
    claimed = await redis_client.xautoclaim(
        name=STREAM_NAME,
        groupname=CONSUMER_GROUP,
        consumername=worker_name,
        min_idle_time=PENDING_IDLE_MS,
        start_id="0-0",
        count=READ_COUNT,
    )

    if len(claimed) == 3:
        _, messages, _ = claimed
    else:
        _, messages = claimed

    if not messages:
        return 0

    logger.info(
        "pending_recovery | worker=%s | claimed=%s | redis_ids=%s",
        worker_name,
        len(messages),
        ",".join(message_id for message_id, _ in messages),
    )

    for message_id, fields in messages:
        await handle_message(
            redis_client=redis_client,
            pool=pool,
            message_id=message_id,
            fields=fields,
            worker_name=worker_name,
        )

    return len(messages)


async def read_new_messages(
    redis_client,
    pool,
    worker_name: str,
) -> None:
    response = await redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=worker_name,
        streams={STREAM_NAME: ">"},
        count=READ_COUNT,
        block=READ_BLOCK_MS,
    )

    if not response:
        return

    for _, messages in response:
        for message_id, fields in messages:
            await handle_message(
                redis_client=redis_client,
                pool=pool,
                message_id=message_id,
                fields=fields,
                worker_name=worker_name,
            )


async def run_worker() -> None:
    worker_name = WORKER_NAME or f"worker-{socket.gethostname()}"

    pool = await create_pool()
    redis_client = await create_redis_client()

    await ensure_consumer_group(redis_client)

    logger.info(
        "worker started | worker_name=%s | stream=%s | group=%s | max_retries=%s | pending_idle_ms=%s",
        worker_name,
        STREAM_NAME,
        CONSUMER_GROUP,
        MAX_RETRIES,
        PENDING_IDLE_MS,
    )

    try:
        while True:
            await recover_pending_messages(
                redis_client=redis_client,
                pool=pool,
                worker_name=worker_name,
            )

            await read_new_messages(
                redis_client=redis_client,
                pool=pool,
                worker_name=worker_name,
            )

    finally:
        await redis_client.aclose()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_worker())
