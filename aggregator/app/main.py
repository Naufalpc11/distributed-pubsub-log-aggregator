import json
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Query, Request

from app.config import STREAM_NAME
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
    event: EventIn,
    request: Request,
) -> dict[str, Any]:
    event_json = json.dumps(event.model_dump(mode="json"))

    redis_message_id = await request.app.state.redis.xadd(
        STREAM_NAME,
        {
            "event": event_json,
        },
    )

    await increment_stat(request.app.state.db_pool, "received", 1)
    await increment_stat(request.app.state.db_pool, "stream_enqueued", 1)

    return {
        "status": "accepted",
        "topic": event.topic,
        "event_id": event.event_id,
        "stream": STREAM_NAME,
        "redis_message_id": redis_message_id,
    }


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
async def metrics(request: Request) -> dict[str, Any]:
    data = await get_stats(request.app.state.db_pool)

    return {
        "metrics": data["counters"],
        "topics": data["topics"],
    }