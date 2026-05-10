CREATE TABLE
    fetch_cache (
        id SERIAL PRIMARY KEY,
        source_key VARCHAR(100) NOT NULL,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL,
        status VARCHAR(20) NOT NULL,
        row_count INTEGER,
        source_url TEXT,
        payload JSONB NOT NULL
    );

CREATE UNIQUE INDEX idx_fetch_cache_source ON fetch_cache (source_key);

CREATE INDEX idx_fetch_cache_expires ON fetch_cache (expires_at);