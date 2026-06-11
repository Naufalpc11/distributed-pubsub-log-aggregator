import asyncio
import json
import logging
import socket

from app.config import CONSUMER_GROUP, STREAM_NAME, WORKER_NAME
from app.db import create_pool
from app.models import EventIn
from app.redis_client import create_redis_client, ensure_consumer_group
from app.repository import increment_stat, process_event


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


async def run_worker() -> None:
    worker_name = WORKER_NAME or f"worker-{socket.gethostname()}"

    pool = await create_pool()
    redis_client = await create_redis_client()

    await ensure_consumer_group(redis_client)

    logger.info(
        "worker started | worker_name=%s | stream=%s | group=%s",
        worker_name,
        STREAM_NAME,
        CONSUMER_GROUP,
    )

    try:
        while True:
            response = await redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=worker_name,
                streams={STREAM_NAME: ">"},
                count=10,
                block=5000,
            )

            if not response:
                continue

            for _, messages in response:
                for message_id, fields in messages:
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

                        logger.info(
                            "%s | topic=%s | event_id=%s | redis_id=%s",
                            status,
                            event.topic,
                            event.event_id,
                            message_id,
                        )

                    except Exception as exc:
                        await increment_stat(pool, "process_errors", 1)

                        await redis_client.xack(
                            STREAM_NAME,
                            CONSUMER_GROUP,
                            message_id,
                        )

                        logger.exception(
                            "failed to process message_id=%s | error=%s",
                            message_id,
                            exc,
                        )

    finally:
        await redis_client.aclose()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_worker())