BEGIN;

CREATE SCHEMA IF NOT EXISTS bronze;

--Raw Review Table


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
  app_id BIGINT NOT NULL,
  review_id BIGINT NOT NULL,
  hash_raw_json VARCHAR(64) NOT NULL,  -- hex SHA-256 length for check updated review
  raw_json JSONB NOT NULL,
  run_id UUID 
    CONSTRAINT fk_raw_review_run
    REFERENCES bronze.ingestion_run(run_id) 
    ON DELETE SET NULL,
  PRIMARY KEY(app_id, review_id, hash_raw_json)
);



CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.clean_latest_review (
  app_id BIGINT NOT NULL,
  review_id BIGINT NOT NULL,
  language VARCHAR(20) NOT NULL,
  review_text TEXT NOT NULL DEFAULT '',
  timestamp_created timestamptz NOT NULL,
  timestamp_updated timestamptz NOT NULL,
  voted_up BOOLEAN NOT NULL, 
  votes_helpful INTEGER, 
  votes_funny INTEGER,
  weighted_vote_score DOUBLE PRECISION,
  comment_count INTEGER,
  steam_purchase BOOLEAN NOT NULL, 
  received_for_free BOOLEAN NOT NULL, 
  written_during_early_access BOOLEAN NOT NULL, 
  primarily_play_on_steam_deck BOOLEAN NOT NULL, 
  PRIMARY KEY(app_id, review_id)
);

CREATE TABLE IF NOT EXISTS silver.clean_latest_review_player_info (
  app_id BIGINT NOT NULL,
  review_id BIGINT NOT NULL,
  steam_user_id BIGINT NOT NULL,
  PRIMARY KEY(app_id, review_id),
  CONSTRAINT fk_playerinfo_review
    FOREIGN KEY (app_id, review_id)
    REFERENCES silver.clean_latest_review (app_id, review_id)
    ON DELETE CASCADE
);

COMMIT;