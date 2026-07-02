CREATE SCHEMA IF NOT EXISTS `steam-review-project-494120.bronze`
OPTIONS(location = "northamerica-northeast2");

CREATE TABLE IF NOT EXISTS `steam-review-project-494120.bronze.ingestion_run` (
  pipeline_name STRING NOT NULL,
  environment STRING NOT NULL,

  app_id INT64 NOT NULL,
  run_id STRING NOT NULL,
  status STRING NOT NULL,

  start_cursor STRING,
  end_cursor STRING,
  last_success_cursor STRING,

  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  finished_at TIMESTAMP,

  rows_fetched INT64,
  rows_inserted INT64,
  rows_deduplicated INT64,

  error_type STRING,
  error_message STRING,

  PRIMARY KEY(run_id) NOT ENFORCED

)
PARTITION BY DATE(started_at)
CLUSTER BY app_id, status;

CREATE TABLE IF NOT EXISTS `steam-review-project-494120.bronze.raw_review` (
  raw_id STRING NOT NULL,
  app_id INT64 NOT NULL,
  review_id INT64 NOT NULL,
  hash_raw_json STRING NOT NULL,
  raw_json JSON NOT NULL,
  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  run_id STRING NOT NULL,
    CONSTRAINT fk_raw_review_run
    FOREIGN KEY (run_id)
    REFERENCES `steam-review-project-494120.bronze.ingestion_run` (run_id) 
    NOT ENFORCED,
  PRIMARY KEY(raw_id) NOT ENFORCED
)
PARTITION BY DATE(inserted_at)
CLUSTER BY app_id, review_id;




CREATE SCHEMA IF NOT EXISTS `steam-review-project-494120.silver`
OPTIONS(location = "northamerica-northeast2");

-- doc abput field explaination https://partner.steamgames.com/doc/store/getreviews
CREATE TABLE IF NOT EXISTS `steam-review-project-494120.silver.clean_latest_review` (
  app_id INT64 NOT NULL,
  review_id INT64 NOT NULL,
  language STRING NOT NULL,
  review_text STRING DEFAULT '' NOT NULL,
  timestamp_created TIMESTAMP NOT NULL,
  timestamp_updated TIMESTAMP NOT NULL,
  voted_positive BOOLEAN NOT NULL, --entry for voted_up
  votes_helpful INT64, 
  votes_funny INT64,
  weighted_vote_score FLOAT64,
  review_comment_count INT64, --entry for comments_count
  steam_purchase BOOLEAN NOT NULL, 
  received_for_free BOOLEAN NOT NULL, 
  written_during_early_access BOOLEAN NOT NULL, 
  primarily_play_on_steam_deck BOOLEAN NOT NULL, 
  source_raw_id STRING NOT NULL, --for tracking source it comes from
  PRIMARY KEY(app_id, review_id) NOT ENFORCED
)
PARTITION BY DATE(timestamp_created)
CLUSTER BY app_id, voted_positive;



CREATE TABLE IF NOT EXISTS `steam-review-project-494120.silver.clean_latest_review_player_info` (
  app_id INT64 NOT NULL,
  review_id INT64 NOT NULL,
  steam_user_id INT64 NOT NULL,
  num_games_owned INT64 NOT NULL,
  total_playtime_mins INT64 NOT NULL,  --playtime_forver
  playtime_last_two_weeks_mins INT64 NOT NULL,
  playtime_at_review_mins INT64 NOT NULL,
  last_played TIMESTAMP NOT NULL,

  PRIMARY KEY(app_id, review_id) NOT ENFORCED,
  CONSTRAINT fk_playerinfo_review
    FOREIGN KEY (app_id, review_id)
    REFERENCES `steam-review-project-494120.silver.clean_latest_review` (app_id, review_id)
    NOT ENFORCED
)

CLUSTER BY app_id, review_id;

-- log for tracking the transformation
CREATE TABLE IF NOT EXISTS `steam-review-project-494120.silver.transform_checkpoint` ( 
  pipeline_name STRING NOT NULL, 
  last_raw_id_processed INT64 DEFAULT 0 NOT NULL, 
  finished_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  PRIMARY KEY(pipeline_name) NOT ENFORCED
)

CLUSTER BY pipeline_name;


--- BIG QUERY accepts INTEGER OR INT64, THEY are same thing
--- INT , SMALLINT , INTEGER , BIGINT , TINYINT , and BYTEINT are aliases for INT64
--- FLOAT64 is FLOAT 64


--- maybe??
-- raw_id = app_id + review_id + hash_raw_json
-- for idempotency