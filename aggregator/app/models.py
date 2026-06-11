from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator


class EventIn(BaseModel):
    topic: str = Field(
        ...,
        min_length=1,
        max_length=100,
        pattern=r"^[a-zA-Z0-9_.-]+$",
        examples=["auth.login"],
    )
    event_id: str = Field(
        ...,
        min_length=1,
        max_length=150,
        examples=["evt-001"],
    )
    timestamp: datetime
    source: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["publisher-1"],
    )
    payload: dict[str, Any]

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)