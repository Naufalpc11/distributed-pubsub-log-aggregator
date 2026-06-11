import os
import random
import time
from datetime import datetime, timezone

import requests


TARGET_URL = os.getenv("TARGET_URL", "http://aggregator-api:8080/publish")
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "20000"))
DUPLICATE_RATIO = float(os.getenv("DUPLICATE_RATIO", "0.30"))

TOPICS = [
    "auth.login",
    "payment.created",
    "order.placed",
    "inventory.updated",
]


def health_url() -> str:
    if TARGET_URL.endswith("/publish"):
        return TARGET_URL.replace("/publish", "/health")

    return TARGET_URL.rstrip("/") + "/health"


def wait_for_api() -> None:
    url = health_url()

    for attempt in range(1, 31):
        try:
            response = requests.get(url, timeout=3)

            if response.status_code == 200:
                print(f"Aggregator API ready: {url}")
                return

        except requests.RequestException:
            pass

        print(f"Waiting for aggregator API... attempt={attempt}")
        time.sleep(2)

    raise RuntimeError("Aggregator API is not ready")


def make_unique_event(index: int) -> dict:
    topic = random.choice(TOPICS)

    return {
        "topic": topic,
        "event_id": f"evt-{index:08d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "publisher-simulator",
        "payload": {
            "sequence": index,
            "message": f"log message {index}",
            "severity": random.choice(["INFO", "WARN", "ERROR"]),
        },
    }


def build_events() -> tuple[list[dict], int, int]:
    unique_total = int(TOTAL_EVENTS * (1 - DUPLICATE_RATIO))
    duplicate_total = TOTAL_EVENTS - unique_total

    unique_events = [
        make_unique_event(index)
        for index in range(1, unique_total + 1)
    ]

    duplicate_events = [
        random.choice(unique_events).copy()
        for _ in range(duplicate_total)
    ]

    events = unique_events + duplicate_events
    random.shuffle(events)

    return events, unique_total, duplicate_total


def main() -> None:
    wait_for_api()

    events, unique_total, duplicate_total = build_events()

    print("Publisher started")
    print(f"Target URL       : {TARGET_URL}")
    print(f"Total events     : {TOTAL_EVENTS}")
    print(f"Unique events    : {unique_total}")
    print(f"Duplicate events : {duplicate_total}")
    print(f"Duplicate ratio  : {DUPLICATE_RATIO:.2f}")

    accepted = 0
    failed = 0
    started = time.time()

    for index, event in enumerate(events, start=1):
        try:
            response = requests.post(
                TARGET_URL,
                json=event,
                timeout=5,
            )

            if response.status_code in (200, 202):
                accepted += 1
            else:
                failed += 1
                print(
                    f"Failed status={response.status_code} body={response.text[:200]}"
                )

        except requests.RequestException as exc:
            failed += 1
            print(f"Request failed: {exc}")

        if index % 1000 == 0:
            print(f"Progress: sent={index}/{TOTAL_EVENTS}")

    elapsed = time.time() - started

    print("Publisher finished")
    print(f"Accepted : {accepted}")
    print(f"Failed   : {failed}")
    print(f"Elapsed  : {elapsed:.2f}s")


if __name__ == "__main__":
    main()