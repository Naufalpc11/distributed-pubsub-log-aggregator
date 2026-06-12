import http from "k6/http";
import { check, sleep } from "k6";
import exec from "k6/execution";

const TARGET_URL = __ENV.TARGET_URL || "http://aggregator-api:8080";
const TOTAL_EVENTS = Number(__ENV.TOTAL_EVENTS || 20000);
const DUPLICATE_RATIO = Number(__ENV.DUPLICATE_RATIO || 0.30);
const RUN_ID = __ENV.RUN_ID || "uasfinal001";

const UNIQUE_TOTAL = Math.floor(TOTAL_EVENTS * (1 - DUPLICATE_RATIO));

export const options = {
  scenarios: {
    publish_events: {
      executor: "shared-iterations",
      vus: 20,
      iterations: TOTAL_EVENTS,
      maxDuration: "5m",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<1000"],
  },
};

export default function () {
  const i = exec.scenario.iterationInTest;

  const uniqueIndex = i < UNIQUE_TOTAL ? i : i % UNIQUE_TOTAL;

  const topics = [
    "auth.login",
    "payment.created",
    "order.placed",
    "inventory.updated",
  ];

  const topic = topics[uniqueIndex % topics.length];

  const event = {
    topic: topic,
    event_id: `k6-${RUN_ID}-${uniqueIndex}`,
    timestamp: new Date().toISOString(),
    source: "k6-load-test",
    payload: {
      sequence: i,
      unique_index: uniqueIndex,
      duplicate: i >= UNIQUE_TOTAL,
      message: `load test event ${i}`,
    },
  };

  const response = http.post(
    `${TARGET_URL}/publish`,
    JSON.stringify(event),
    {
      headers: {
        "Content-Type": "application/json",
      },
    }
  );

  check(response, {
    "publish status is 202": (r) => r.status === 202,
  });

  sleep(0.01);
}