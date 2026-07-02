BEGIN;

CREATE SCHEMA IF NOT EXISTS bronze;

--Raw Review Table

--should never delete the meta data
CREATE TABLE IF NOT EXISTS bronze.ingestion_run (
  app_id BIGINT NOT NULL,
  run_id UUID PRIMARY KEY,
  status TEXT  NOT NULL, -- success/failed/running
  start_cursor TEXT,  --beginning of execution cursor key
  end_cursor TEXT,  --last cursor: will be updated when cursor changes bc: page changes, prorgam crush, script terminal
  last_success_cursor TEXT, -- recovery from crash
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  rows_fetched INTEGER, --how many rows we read from this database
  error_type TEXT, --error type for ingest fail
  error_message TEXT -- error detail
);

CREATE TABLE IF NOT EXISTS bronze.raw_review (
  raw_id BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE, -- used for scheule transformation task
  app_id BIGINT NOT NULL,
  review_id BIGINT NOT NULL,
  hash_raw_json VARCHAR(64) NOT NULL,  -- hex SHA-256 length for check updated review
  raw_json JSONB NOT NULL,
  -- used for pipeline broken anaylyze, data delay, tell pipeline healthy status
  -- also ue to compare API and UI Pipelne 
  -- freshness lag, extraction delay, consistency
  inserted_at timestamptz Not NULL DEFAULT now(), 
  run_id UUID 
    CONSTRAINT fk_raw_review_run
    REFERENCES bronze.ingestion_run(run_id) 
    ON DELETE RESTRICT, --delete the raw review should not delete the ingestion run as it as metadata table
  PRIMARY KEY(app_id, review_id, hash_raw_json)
);

-- raw id for transformation task schedule, run_id for failure analysis
CREATE INDEX IF NOT EXISTS idx_raw_review_raw_id ON bronze.raw_review(raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_review_run_id ON bronze.raw_review(run_id);

CREATE SCHEMA IF NOT EXISTS silver;

-- doc abput field explaination https://partner.steamgames.com/doc/store/getreviews
CREATE TABLE IF NOT EXISTS silver.clean_latest_review (
  app_id BIGINT NOT NULL,
  review_id BIGINT NOT NULL,
  language VARCHAR(20) NOT NULL,
  review_text TEXT NOT NULL DEFAULT '',
  timestamp_created timestamptz NOT NULL,
  timestamp_updated timestamptz NOT NULL,
  voted_positive BOOLEAN NOT NULL, --entry for voted_up
  votes_helpful INTEGER, 
  votes_funny INTEGER,
  weighted_vote_score DOUBLE PRECISION,
  review_comment_count INTEGER, --entry for comments_count
  steam_purchase BOOLEAN NOT NULL, 
  received_for_free BOOLEAN NOT NULL, 
  written_during_early_access BOOLEAN NOT NULL, 
  primarily_play_on_steam_deck BOOLEAN NOT NULL, 
  source_raw_id BIGINT NOT NULL, --for tracking source it comes from
  PRIMARY KEY(app_id, review_id)
);

CREATE TABLE IF NOT EXISTS silver.clean_latest_review_player_info (
  app_id BIGINT NOT NULL,
  review_id BIGINT NOT NULL,
  steam_user_id BIGINT NOT NULL,
  num_games_owned BIGINT NOT NULL,
  total_playtime_mins INTEGER NOT NULL,  --playtime_forver
  playtime_last_two_weeks_mins INTEGER NOT NULL,
  playtime_at_review_mins INTEGER NOT NULL,
  last_played timestamptz NOT NULL,
  PRIMARY KEY(app_id, review_id),
  CONSTRAINT fk_playerinfo_review
    FOREIGN KEY (app_id, review_id)
    REFERENCES silver.clean_latest_review (app_id, review_id)
    ON DELETE CASCADE
);

-- log for tracking the transformation
CREATE TABLE IF NOT EXISTS silver.transform_checkpoint ( 
  pipeline_name TEXT PRIMARY KEY, 
  last_raw_id_processed BIGINT NOT NULL DEFAULT 0, 
  finished_at timestamptz NOT NULL DEFAULT now() 
);


COMMIT;