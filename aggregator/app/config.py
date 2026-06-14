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

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE_SECONDS = float(os.getenv("RETRY_BACKOFF_BASE_SECONDS", "0.5"))
PENDING_IDLE_MS = int(os.getenv("PENDING_IDLE_MS", "10000"))
READ_BLOCK_MS = int(os.getenv("READ_BLOCK_MS", "5000"))
READ_COUNT = int(os.getenv("READ_COUNT", "10"))