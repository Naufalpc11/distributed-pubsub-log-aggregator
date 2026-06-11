import os


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://appuser:apppass@postgres:5432/appdb",
)

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://redis:6379/0",
)

STREAM_NAME = os.getenv("STREAM_NAME", "events")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "log-workers")
WORKER_NAME = os.getenv("WORKER_NAME", "worker-default")