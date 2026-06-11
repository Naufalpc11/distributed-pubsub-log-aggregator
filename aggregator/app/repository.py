import json
from typing import Any

import asyncpg

from app.models import EventIn


async def increment_stat(conn_or_pool: Any, name: str, delta: int = 1) -> None:
    await conn_or_pool.execute(
        """
        INSERT INTO stats (name, value, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (name)
        DO UPDATE SET
            value = stats.value + $2,
            updated_at = NOW()
        """,
        name,
        delta,
    )


async def process_event(
    pool: asyncpg.Pool,
    event: EventIn,
    worker_name: str,
) -> str:
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="read_committed"):
            inserted = await conn.fetchrow(
                """
                INSERT INTO processed_events (
                    topic,
                    event_id,
                    event_timestamp,
                    source,
                    payload,
                    processed_by
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                ON CONFLICT (topic, event_id)
                DO NOTHING
                RETURNING id
                """,
                event.topic,
                event.event_id,
                event.timestamp,
                event.source,
                json.dumps(event.payload),
                worker_name,
            )

            if inserted:
                status = "processed"
                message = "event inserted once"

                await increment_stat(conn, "unique_processed", 1)

                await conn.execute(
                    """
                    INSERT INTO topic_stats (topic, unique_count, duplicate_count, updated_at)
                    VALUES ($1, 1, 0, NOW())
                    ON CONFLICT (topic)
                    DO UPDATE SET
                        unique_count = topic_stats.unique_count + 1,
                        updated_at = NOW()
                    """,
                    event.topic,
                )
            else:
                status = "duplicate_dropped"
                message = "duplicate ignored because topic and event_id already exist"

                await increment_stat(conn, "duplicate_dropped", 1)

                await conn.execute(
                    """
                    INSERT INTO topic_stats (topic, unique_count, duplicate_count, updated_at)
                    VALUES ($1, 0, 1, NOW())
                    ON CONFLICT (topic)
                    DO UPDATE SET
                        duplicate_count = topic_stats.duplicate_count + 1,
                        updated_at = NOW()
                    """,
                    event.topic,
                )

            await conn.execute(
                """
                INSERT INTO audit_logs (
                    topic,
                    event_id,
                    status,
                    message,
                    worker_name
                )
                VALUES ($1, $2, $3, $4, $5)
                """,
                event.topic,
                event.event_id,
                status,
                message,
                worker_name,
            )

            return status


async def get_events(
    pool: asyncpg.Pool,
    topic: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    limit = max(1, min(limit, 500))

    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                """
                SELECT
                    topic,
                    event_id,
                    event_timestamp,
                    source,
                    payload,
                    processed_by,
                    created_at
                FROM processed_events
                WHERE topic = $1
                ORDER BY id DESC
                LIMIT $2
                """,
                topic,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    topic,
                    event_id,
                    event_timestamp,
                    source,
                    payload,
                    processed_by,
                    created_at
                FROM processed_events
                ORDER BY id DESC
                LIMIT $1
                """,
                limit,
            )

    result = []

    for row in rows:
        payload = row["payload"]

        if isinstance(payload, str):
            payload = json.loads(payload)

        result.append(
            {
                "topic": row["topic"],
                "event_id": row["event_id"],
                "timestamp": row["event_timestamp"],
                "source": row["source"],
                "payload": payload,
                "processed_by": row["processed_by"],
                "created_at": row["created_at"],
            }
        )

    return result


async def get_stats(pool: asyncpg.Pool) -> dict[str, Any]:
    async with pool.acquire() as conn:
        stat_rows = await conn.fetch(
            """
            SELECT name, value
            FROM stats
            ORDER BY name
            """
        )

        topic_rows = await conn.fetch(
            """
            SELECT topic, unique_count, duplicate_count
            FROM topic_stats
            ORDER BY topic
            """
        )

    counters = {row["name"]: row["value"] for row in stat_rows}

    topics = [
        {
            "topic": row["topic"],
            "unique_count": row["unique_count"],
            "duplicate_count": row["duplicate_count"],
        }
        for row in topic_rows
    ]

    return {
        "counters": counters,
        "topics": topics,
    }