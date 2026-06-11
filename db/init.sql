CREATE TABLE IF NOT EXISTS processed_events (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    processed_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_processed_event UNIQUE (topic, event_id)
);

CREATE TABLE IF NOT EXISTS stats (
    name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS topic_stats (
    topic TEXT PRIMARY KEY,
    unique_count BIGINT NOT NULL DEFAULT 0,
    duplicate_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT NOT NULL,
    worker_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO stats (name, value)
VALUES
    ('received', 0),
    ('stream_enqueued', 0),
    ('unique_processed', 0),
    ('duplicate_dropped', 0),
    ('process_errors', 0)
ON CONFLICT (name) DO NOTHING;