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

def test_publish_batch_endpoint_accepts_events(api_base_url):
    wait_for_api(api_base_url)

    events = [
        make_api_event(),
        make_api_event(),
        make_api_event(),
    ]

    response = httpx.post(
        f"{api_base_url}/publish/batch",
        json=events,
        timeout=10,
    )

    data = response.json()

    assert response.status_code == 202
    assert data["status"] == "accepted"
    assert data["count"] == 3
    assert len(data["items"]) == 3


def test_publish_endpoint_accepts_batch_events(api_base_url):
    wait_for_api(api_base_url)

    events = [
        make_api_event(),
        make_api_event(),
        make_api_event(),
    ]

    response = httpx.post(
        f"{api_base_url}/publish",
        json=events,
        timeout=10,
    )

    data = response.json()

    assert response.status_code == 202
    assert data["status"] == "accepted"
    assert data["count"] == 3
    assert len(data["items"]) == 3


def test_published_batch_events_appear_in_events_endpoint(api_base_url):
    wait_for_api(api_base_url)

    events = [
        make_api_event(),
        make_api_event(),
        make_api_event(),
    ]

    response = httpx.post(
        f"{api_base_url}/publish/batch",
        json=events,
        timeout=10,
    )

    assert response.status_code == 202

    expected_event_ids = {
        event["event_id"]
        for event in events
    }

    found_event_ids = set()

    for _ in range(30):
        for event in events:
            events_response = httpx.get(
                f"{api_base_url}/events",
                params={
                    "topic": event["topic"],
                    "limit": 500,
                },
                timeout=5,
            )

            items = events_response.json()["items"]

            for item in items:
                if item["event_id"] in expected_event_ids:
                    found_event_ids.add(item["event_id"])

        if found_event_ids == expected_event_ids:
            break

        time.sleep(0.5)

    assert found_event_ids == expected_event_ids


def test_metrics_endpoint_has_rates_and_queue_metrics(api_base_url):
    wait_for_api(api_base_url)

    response = httpx.get(f"{api_base_url}/metrics", timeout=5)
    data = response.json()

    assert response.status_code == 200
    assert "counters" in data
    assert "accepted_per_second" in data["rates"]
    assert "processed_per_second" in data["rates"]
    assert "duplicate_rate" in data["rates"]
    assert "stream_length" in data["queue"]
    assert "pending" in data["queue"]
    assert "consumer_group_lag" in data["queue"]
    assert "dead_letter_stream_length" in data["queue"]
