CREATE OR REPLACE SCHEMA steam_review.PIPELINE_META;
select catalog STEAM_REVIEW;
CREATE OR REPLACE TABLE steam_review.PIPELINE_META.CURSOR_STATE_DAILY_INGESTION (
   app_id              BIGINT PRIMARY KEY,
    last_cursor         TEXT,             -- optional: last cursor used, mostly for debugging
    last_run_at         TIMESTAMPTZ,
    last_run_status     TEXT           -- 'success' | 'failed' | 'running'
);

CREATE OR REPLACE TABLE steam_review.PIPELINE_META.cursor_state_backfill (
    app_id              BIGINT PRIMARY KEY,
    last_cursor         TEXT,             -- REQUIRED, not optional — this is how you resume
    oldest_seen_timestamp TIMESTAMPTZ,    -- tracks how far back you've reached
    pages_completed      INT DEFAULT 0,
    status               TEXT DEFAULT 'not_started',  -- 'not_started' | 'in_progress' | 'complete' | 'failed'
    last_run_at          TIMESTAMPTZ,
    consecutive_failures INT DEFAULT 0,
    updated_at           TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE steam_review.PIPELINE_META.API_INGESTION_RUN (
    run_id          STRING DEFAULT UUID_STRING(),   -- Snowflake can auto-generate a UUID
    app_id           STRING NOT NULL,
    started_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ended_at        TIMESTAMP_NTZ,
    start_cursor    STRING,
    end_cursor      STRING,
    rows_loaded     NUMBER DEFAULT 0,
    status          STRING,       -- 'RUNNING' | 'SUCCESS' | 'FAILED'
    failure_reason  STRING,
    CONSTRAINT pk_run_id PRIMARY KEY (run_id)
);


GRANT USAGE ON DATABASE steam_review TO ROLE AIRFLOW_ROLE;
GRANT USAGE ON SCHEMA steam_review.PIPELINE_META TO ROLE AIRFLOW_ROLE;
GRANT SELECT, INSERT ON TABLE steam_review.PIPELINE_META.CURSOR_STATE_DAILY_INGESTION TO ROLE AIRFLOW_ROLE;