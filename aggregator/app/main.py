import json
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Query, Request

from app.config import CONSUMER_GROUP, STREAM_NAME
from app.db import create_pool
from app.models import EventIn
from app.redis_client import create_redis_client, ensure_consumer_group
from app.repository import get_events, get_stats, increment_stat


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.start_time = time.time()
    app.state.db_pool = await create_pool()
    app.state.redis = await create_redis_client()

    await ensure_consumer_group(app.state.redis)
    initial_stats = await get_stats(app.state.db_pool)
    app.state.start_counters = initial_stats["counters"]

    yield

    await app.state.redis.aclose()
    await app.state.db_pool.close()


app = FastAPI(
    title="Distributed Pub-Sub Log Aggregator",
    description="UAS Sistem Terdistribusi: Pub-Sub log aggregator dengan idempotent consumer dan deduplication.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "aggregator-api",
    }


@app.get("/ready")
async def ready(request: Request) -> dict[str, Any]:
    db_value = await request.app.state.db_pool.fetchval("SELECT 1")
    redis_ping = await request.app.state.redis.ping()

    return {
        "status": "ready",
        "database": db_value == 1,
        "redis": redis_ping is True,
    }


@app.post("/publish", status_code=202)
async def publish_event(
    request: Request,
    payload: EventIn | list[EventIn] = Body(...),
) -> dict[str, Any]:
    events = payload if isinstance(payload, list) else [payload]

    if not events or len(events) > 1000:
        raise HTTPException(
            status_code=422,
            detail="publish accepts between 1 and 1000 events",
        )

    result = await enqueue_events(request, events)

    if isinstance(payload, EventIn):
        return result["items"][0] | {
            "status": "accepted",
            "stream": STREAM_NAME,
        }

    return result


async def enqueue_events(
    request: Request,
    events: list[EventIn],
) -> dict[str, Any]:
    pipe = request.app.state.redis.pipeline(transaction=False)

    for event in events:
        pipe.xadd(
            STREAM_NAME,
            {
                "event": json.dumps(event.model_dump(mode="json")),
            },
        )

    redis_message_ids = await pipe.execute()

    await increment_stat(request.app.state.db_pool, "received", len(events))
    await increment_stat(request.app.state.db_pool, "stream_enqueued", len(events))

    return {
        "status": "accepted",
        "count": len(events),
        "stream": STREAM_NAME,
        "items": [
            {
                "topic": event.topic,
                "event_id": event.event_id,
                "redis_message_id": redis_message_ids[index],
            }
            for index, event in enumerate(events)
        ],
    }


@app.post("/publish/batch", status_code=202, deprecated=True)
async def publish_batch_events(
    request: Request,
    events: list[EventIn] = Body(..., min_length=1, max_length=1000),
) -> dict[str, Any]:
    return await enqueue_events(request, events)


@app.get("/events")
async def list_events(
    request: Request,
    topic: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    items = await get_events(
        request.app.state.db_pool,
        topic=topic,
        limit=limit,
    )

    return {
        "count": len(items),
        "items": items,
    }


@app.get("/stats")
async def stats(request: Request) -> dict[str, Any]:
    data = await get_stats(request.app.state.db_pool)

    uptime_seconds = round(time.time() - request.app.state.start_time, 2)

    return {
        "service": "aggregator-api",
        "uptime_seconds": uptime_seconds,
        **data,
    }


@app.get("/metrics")
async def metrics(
    request: Request,
    include_topics: bool = False,
) -> dict[str, Any]:
    data = await get_stats(request.app.state.db_pool)
    counters = data["counters"]
    uptime_seconds = max(time.time() - request.app.state.start_time, 0.001)

    received = int(counters.get("received", 0))
    unique_processed = int(counters.get("unique_processed", 0))
    duplicate_dropped = int(counters.get("duplicate_dropped", 0))
    processed_total = unique_processed + duplicate_dropped
    start_counters = request.app.state.start_counters
    accepted_since_start = max(
        received - int(start_counters.get("received", 0)),
        0,
    )
    processed_since_start = max(
        processed_total
        - int(start_counters.get("unique_processed", 0))
        - int(start_counters.get("duplicate_dropped", 0)),
        0,
    )

    stream_length = await request.app.state.redis.xlen(STREAM_NAME)
    dead_letter_length = await request.app.state.redis.xlen(
        f"{STREAM_NAME}:deadletter"
    )
    pending_summary = await request.app.state.redis.xpending(
        STREAM_NAME,
        CONSUMER_GROUP,
    )
    pending_count = (
        int(pending_summary.get("pending", 0))
        if isinstance(pending_summary, dict)
        else int(pending_summary[0])
    )
    group_rows = await request.app.state.redis.xinfo_groups(STREAM_NAME)
    group_data = next(
        (
            group
            for group in group_rows
            if group.get("name") == CONSUMER_GROUP
        ),
        {},
    )

    response = {
        "uptime_seconds": round(uptime_seconds, 2),
        "counters": counters,
        "since_api_start": {
            "accepted": accepted_since_start,
            "processed": processed_since_start,
        },
        "rates": {
            "accepted_per_second": round(
                accepted_since_start / uptime_seconds,
                2,
            ),
            "processed_per_second": round(
                processed_since_start / uptime_seconds,
                2,
            ),
            "duplicate_rate": round(
                duplicate_dropped / processed_total,
                4,
            )
            if processed_total
            else 0.0,
        },
        "queue": {
            "stream_length": stream_length,
            "pending": pending_count,
            "consumer_group_lag": int(group_data.get("lag") or 0),
            "unprocessed_estimate": max(
                int(counters.get("stream_enqueued", 0)) - processed_total,
                0,
            ),
            "dead_letter_stream_length": dead_letter_length,
        },
    }

    if include_topics:
        response["topics"] = data["topics"]

    return response
