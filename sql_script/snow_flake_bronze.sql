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

CREATE OR REPLACE ICEBERG TABLE  STEAM_REVIEW.BRONZE.RAW_REVIEW (
  recommendationid            STRING,

    -- author (flattened, all strings)
    author_steamid               STRING,
    author_num_games_owned       STRING,
    author_num_reviews           STRING,
    author_playtime_forever      STRING,
    author_playtime_last_two_weeks STRING,
    author_playtime_at_review    STRING,
    author_deck_playtime_at_review STRING,
    author_last_played           STRING,

    language                     STRING,
    review                       STRING,
    timestamp_created            STRING,
    timestamp_updated             STRING,
    voted_up                     STRING,
    votes_up                     STRING,
    votes_funny                  STRING,
    weighted_vote_score          STRING,
    comment_count                STRING,
    steam_purchase               STRING,
    received_for_free            STRING,
    written_during_early_access  STRING,
    developer_response           STRING,
    timestamp_dev_responded      STRING,
    primarily_steam_deck         STRING,

    reactions                    ARRAY(STRING),
    ingested_at                  TIMESTAMP_LTZ
)
CATALOG = 'SNOWFLAKE' -- don't change unless you want to use other catalog!!!
EXTERNAL_VOLUME = 'iceberg_external_volume'
BASE_LOCATION = 'bronze/'; -- The folder (path) inside your external cloud storage where the Iceberg table’s data and metadata will live.


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

CREATE OR REPLACE PIPE steam_review.BRONZE.steam_pipe
AUTO_INGEST = TRUE
AS
COPY INTO STEAM_REVIEW.BRONZE.RAW_REVIEW  (
    recommendationid,
    author_steamid,
    author_num_games_owned,
    author_num_reviews,
    author_playtime_forever,
    author_playtime_last_two_weeks,
    author_playtime_at_review,
    author_deck_playtime_at_review,
    author_last_played,
    language,
    review,
    timestamp_created,
    timestamp_updated,
    voted_up,
    votes_up,
    votes_funny,
    weighted_vote_score,
    comment_count,
    steam_purchase,
    received_for_free,
    written_during_early_access,
    developer_response,
    timestamp_dev_responded,
    primarily_steam_deck,
    reactions,
    ingested_at
)
FROM (
    SELECT
        $1:recommendationid::STRING, --$1--->entire json object for that row
        $1:author:steamid::STRING,
        $1:author:num_games_owned::STRING,
        $1:author:num_reviews::STRING,
        $1:author:playtime_forever::STRING,
        $1:author:playtime_last_two_weeks::STRING,
        $1:author:playtime_at_review::STRING,
        $1:author:deck_playtime_at_review::STRING,
        $1:author:last_played::STRING,
        $1:language::STRING,
        $1:review::STRING,
        $1:timestamp_created::STRING,
        $1:timestamp_updated::STRING,
        $1:voted_up::STRING,
        $1:votes_up::STRING,
        $1:votes_funny::STRING,
        $1:weighted_vote_score::STRING,
        $1:comment_count::STRING,
        $1:steam_purchase::STRING,
        $1:received_for_free::STRING,
        $1:written_during_early_access::STRING,
        $1:developer_response::STRING,
        $1:timestamp_dev_responded::STRING,
        $1:primarily_steam_deck::STRING,
        CAST($1:reactions AS ARRAY(STRING)),
        current_timestamp()
    FROM @steam_review.BRONZE.steam_stage
)
FILE_FORMAT = (TYPE = 'JSON');