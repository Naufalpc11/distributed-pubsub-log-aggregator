from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.models import EventIn


def valid_event_data() -> dict:
    return {
        "topic": "auth.login",
        "event_id": "evt-001",
        "timestamp": "2026-06-11T10:00:00Z",
        "source": "pytest",
        "payload": {
            "user_id": "u001",
            "action": "login",
        },
    }


def test_valid_event_is_accepted():
    event = EventIn(**valid_event_data())

    assert event.topic == "auth.login"
    assert event.event_id == "evt-001"
    assert event.timestamp.tzinfo is not None
    assert event.payload["action"] == "login"


def test_naive_timestamp_is_normalized_to_utc():
    data = valid_event_data()
    data["timestamp"] = datetime(2026, 6, 11, 10, 0, 0)

    event = EventIn(**data)

    assert event.timestamp.tzinfo is not None
    assert event.timestamp.utcoffset() == timezone.utc.utcoffset(event.timestamp)


def test_invalid_topic_is_rejected():
    data = valid_event_data()
    data["topic"] = "auth login invalid"

    with pytest.raises(ValidationError):
        EventIn(**data)


def test_empty_event_id_is_rejected():
    data = valid_event_data()
    data["event_id"] = ""

    with pytest.raises(ValidationError):
        EventIn(**data)


def test_invalid_timestamp_is_rejected():
    data = valid_event_data()
    data["timestamp"] = "not-a-valid-date"

    with pytest.raises(ValidationError):
        EventIn(**data)


def test_payload_must_be_object():
    data = valid_event_data()
    data["payload"] = ["not", "object"]

    with pytest.raises(ValidationError):
        EventIn(**data)