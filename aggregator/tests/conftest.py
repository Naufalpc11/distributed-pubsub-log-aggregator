import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import pytest_asyncio

from app.db import create_pool
from app.models import EventIn


@pytest.fixture
def api_base_url() -> str:
    return os.getenv("API_BASE_URL", "http://localhost:8080")


@pytest.fixture
def make_event():
    def _make_event(
        topic: str | None = None,
        event_id: str | None = None,
        source: str = "pytest",
    ) -> EventIn:
        suffix = uuid4().hex[:12]

        return EventIn(
            topic=topic or f"test.topic.{suffix}",
            event_id=event_id or f"test-{suffix}",
            timestamp=datetime.now(timezone.utc),
            source=source,
            payload={
                "test": True,
                "suffix": suffix,
            },
        )

    return _make_event


@pytest_asyncio.fixture
async def db_pool():
    pool = await create_pool()

    try:
        yield pool
    finally:
        await pool.close()