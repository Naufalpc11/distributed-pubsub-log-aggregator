import time
from datetime import datetime, timezone
from uuid import uuid4

import httpx


def wait_for_api(api_base_url: str) -> None:
    for _ in range(20):
        try:
            response = httpx.get(f"{api_base_url}/health", timeout=3)

            if response.status_code == 200:
                return

        except httpx.HTTPError:
            pass

        time.sleep(0.5)

    raise AssertionError("API is not ready")


def make_api_event() -> dict:
    suffix = uuid4().hex[:12]

    return {
        "topic": f"api.test.{suffix[:8]}",
        "event_id": f"api-{suffix}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "pytest-api",
        "payload": {
            "test": True,
            "suffix": suffix,
        },
    }


def test_health_endpoint(api_base_url):
    wait_for_api(api_base_url)

    response = httpx.get(f"{api_base_url}/health", timeout=5)

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_ready_endpoint(api_base_url):
    wait_for_api(api_base_url)

    response = httpx.get(f"{api_base_url}/ready", timeout=5)
    data = response.json()

    assert response.status_code == 200
    assert data["status"] == "ready"
    assert data["database"] is True
    assert data["redis"] is True


def test_publish_endpoint_accepts_event(api_base_url):
    wait_for_api(api_base_url)

    event = make_api_event()

    response = httpx.post(
        f"{api_base_url}/publish",
        json=event,
        timeout=5,
    )

    data = response.json()

    assert response.status_code == 202
    assert data["status"] == "accepted"
    assert data["topic"] == event["topic"]
    assert data["event_id"] == event["event_id"]


def test_published_event_appears_in_events_endpoint(api_base_url):
    wait_for_api(api_base_url)

    event = make_api_event()

    publish_response = httpx.post(
        f"{api_base_url}/publish",
        json=event,
        timeout=5,
    )

    assert publish_response.status_code == 202

    found = False

    for _ in range(20):
        response = httpx.get(
            f"{api_base_url}/events",
            params={
                "topic": event["topic"],
                "limit": 500,
            },
            timeout=5,
        )

        items = response.json()["items"]

        found = any(
            item["event_id"] == event["event_id"]
            for item in items
        )

        if found:
            break

        time.sleep(0.5)

    assert found is True


def test_stats_endpoint_has_counters(api_base_url):
    wait_for_api(api_base_url)

    response = httpx.get(f"{api_base_url}/stats", timeout=5)
    data = response.json()

    assert response.status_code == 200
    assert "counters" in data
    assert "received" in data["counters"]
    assert "unique_processed" in data["counters"]
    assert "duplicate_dropped" in data["counters"]