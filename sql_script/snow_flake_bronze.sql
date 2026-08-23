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


CREATE EXTERNAL VOLUME iceberg_external_volume
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'my-s3-location'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://your-bucket/path/'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789:role/your-volume-role'
      )
    );


CREATE OR REPLACE FILE FORMAT steam_review.BRONZE.json_ff
TYPE = JSON;

CREATE STORAGE INTEGRATION steam_storage_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-account>:role/snowflake-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket/raw/');

DESC STORAGE INTEGRATION steam_storage_int;


CREATE OR REPLACE STAGE steam_review.BRONZE.steam_stage
URL='s3://steam-bucket/raw/'
STORAGE_INTEGRATION = steam_storage_int
FILE_FORMAT = steam_review.BRONZE.json_ff;

CREATE OR REPLACE TABLE steam_review.BRONZE.RAW_REVIEW_LANDING (
    raw_data VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PIPE steam_review.BRONZE.steam_pipe
AUTO_INGEST = TRUE
AS
COPY INTO steam_review.BRONZE.RAW_REVIEW_LANDING (raw_data)
FROM (
    SELECT $1
    FROM @steam_review.BRONZE.steam_stage
)
FILE_FORMAT = (TYPE = 'JSON');

DESCRIBE PIPE steam_review.BRONZE.steam_pipe;
alter pipe steam_review.BRONZE.steam_pipe refresh;

CREATE OR REPLACE STREAM steam_review.BRONZE.raw_review_landing_stream
ON TABLE steam_review.BRONZE.RAW_REVIEW_LANDING;

CREATE OR REPLACE DYNAMIC ICEBERG TABLE steam_review.BRONZE.RAW_REVIEW(
    review_id                       STRING,
    -- author (flattened, all strings)
    author_steam_id                 STRING,
    author_num_games_owned          STRING,
    author_num_reviews              STRING,
    author_playtime_forever         STRING,
    author_playtime_last_two_weeks  STRING,
    author_playtime_at_review       STRING,
    author_deck_playtime_at_review  STRING,
    author_last_played              STRING,
    language                        STRING,
    review                          STRING,
    timestamp_created               STRING,
    timestamp_updated               STRING,
    voted_up                        STRING,
    votes_up                        STRING,
    votes_funny                     STRING,
    weighted_vote_score             STRING,
    comment_count                   STRING,
    steam_purchase                  STRING,
    received_for_free               STRING,
    written_during_early_access     STRING,
    developer_response              STRING,
    timestamp_dev_responded         STRING,
    primarily_steam_deck            STRING,
    reactions                       STRING,
    ingested_at                     TIMESTAMP_NTZ
)
    TARGET_LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
    CATALOG = 'SNOWFLAKE' -- don't change unless you want to use other catalog!!!
    EXTERNAL_VOLUME = 'iceberg_external_volume'
    BASE_LOCATION = 'bronze'
AS
SELECT
    r.value:recommendationid::STRING AS review_id,
    r.value:author:steamid::STRING AS author_steam_id,
    r.value:author:num_games_owned::STRING AS author_num_games_owned,
    r.value:author:num_reviews::STRING AS author_num_reviews,
    r.value:author:playtime_forever::STRING AS author_playtime_forever,
    r.value:author:playtime_last_two_weeks::STRING AS author_playtime_last_two_weeks,
    r.value:author:playtime_at_review::STRING AS author_playtime_at_review,
    COALESCE(r.value:author:deck_playtime_at_review::STRING, 'N/A') AS author_deck_playtime_at_review,
    r.value:author:last_played::STRING AS author_last_played,
    r.value:language::STRING AS language,
    r.value:review::STRING AS review,
    r.value:timestamp_created::STRING AS timestamp_created,
    r.value:timestamp_updated::STRING AS timestamp_updated,
    r.value:voted_up::STRING AS voted_up,
    r.value:votes_up::STRING AS votes_up,
    r.value:votes_funny::STRING AS votes_funny,
    r.value:weighted_vote_score::STRING AS weighted_vote_score,
    r.value:comment_count::STRING AS comment_count,
    r.value:steam_purchase::STRING AS steam_purchase,
    r.value:received_for_free::STRING AS received_for_free,
    r.value:written_during_early_access::STRING AS written_during_early_access,
    r.value:developer_response::STRING AS developer_response,
    r.value:timestamp_dev_responded::STRING AS timestamp_dev_responded,
    r.value:primarily_steam_deck::STRING AS primarily_steam_deck,
    r.value:reactions AS reactions,
    l.ingested_at
FROM steam_review.BRONZE.RAW_REVIEW_LANDING l,
     LATERAL FLATTEN(input => l.raw_data:reviews) r;