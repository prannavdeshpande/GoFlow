-- Recipient rows are stored separately so the worker can read and update them in PostgreSQL.

CREATE TABLE IF NOT EXISTS job_recipients (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    email       TEXT        NOT NULL,
    data        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status      TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending','processing','sent','failed')),
    attempts    INT         NOT NULL DEFAULT 0,
    error_msg   TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_recipients_job_id ON job_recipients(job_id);
CREATE INDEX IF NOT EXISTS idx_job_recipients_status  ON job_recipients(status);