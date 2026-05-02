-- Run this once against your PostgreSQL database before starting the server.
-- With Docker Compose: docker compose exec postgres psql -U emailworker -d emailworker -f /dev/stdin < migrations/001_create_jobs.sql

CREATE TABLE IF NOT EXISTS jobs (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    status      TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending','processing','done','failed','cancelled')),
    webhook_url TEXT        NOT NULL,
    csv_path    TEXT        NOT NULL,
    total_rows  INT         NOT NULL DEFAULT 0,
    error_msg   TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);
