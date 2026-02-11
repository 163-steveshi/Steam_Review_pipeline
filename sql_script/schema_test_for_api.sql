--use chatgpt to generate the tested column input

BEGIN;

INSERT INTO bronze.ingestion_run (
  app_id,
  run_id,
  status,
  start_cursor,
  started_at
) VALUES (
  570,
  '11111111-1111-1111-1111-111111111111',
  'running',
  'cursor_0',
  now()
);
INSERT INTO bronze.raw_review (
  app_id,
  review_id,
  hash_raw_json,
  raw_json,
  run_id
) VALUES (
  570,
  100001,
  'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  '{"review": "Great game!", "votes_helpful": 42, "voted_up": true}',
  '11111111-1111-1111-1111-111111111111'
);

INSERT INTO silver.clean_latest_review (
  app_id,
  review_id,
  language,
  review_text,
  timestamp_created,
  timestamp_updated,
  voted_up,
  votes_helpful,
  votes_funny,
  weighted_vote_score,
  comment_count,
  steam_purchase,
  received_for_free,
  written_during_early_access,
  primarily_play_on_steam_deck
) VALUES (
  570,
  100001,
  'en',
  'Great game!',
  now(),
  now(),
  TRUE,
  42,
  3,
  0.97,
  1,
  TRUE,
  FALSE,
  FALSE,
  FALSE
);

INSERT INTO silver.clean_latest_review_player_info (
  app_id,
  review_id,
  steam_user_id
) VALUES (
  570,
  100001,
  76561199859117575
);

--assertuon test
DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM bronze.ingestion_run
      WHERE app_id = 570
        AND run_id = '11111111-1111-1111-1111-111111111111'
    ) THEN
      RAISE EXCEPTION 'FAIL: ingestion_run row not inserted';
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM bronze.raw_review
      WHERE app_id = 570
        AND review_id = 100001
        AND run_id = '11111111-1111-1111-1111-111111111111'
    ) THEN
      RAISE EXCEPTION 'FAIL: raw_review row not inserted or run_id not set';
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM silver.clean_latest_review
      WHERE app_id = 570
        AND review_id = 100001
    ) THEN
      RAISE EXCEPTION 'FAIL: clean_latest_review row not inserted';
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM silver.clean_latest_review_player_info
      WHERE app_id = 570
        AND review_id = 100001
    ) THEN
      RAISE EXCEPTION 'FAIL: clean_latest_review_player_info row not inserted';
    END IF;

    RAISE NOTICE 'PASS: setup inserts present';
END $$;


-- SELECT * FROM bronze.raw_review;
-- SELECT * FROM bronze.ingestion_run;
-- SELECT * FROM silver.clean_latest_review;
-- SELECT * FROM silver.clean_latest_review_player_info;

-- test on delete set null
DELETE FROM bronze.ingestion_run
WHERE run_id = '11111111-1111-1111-1111-111111111111';

DO $$
  DECLARE check_run_id text;
  BEGIN
    -- The raw_review row should still exist...
    IF NOT EXISTS (
      SELECT 1 FROM bronze.raw_review
      WHERE app_id = 570 AND review_id = 100001
    ) THEN
      RAISE EXCEPTION 'FAIL: raw_review row disappeared (expected it to remain)';
    END IF;

    -- ...but its run_id should now be NULL (SET NULL)
    SELECT run_id INTO check_run_id
    FROM bronze.raw_review
    WHERE app_id = 570 AND review_id = 100001;

    IF check_run_id IS NOT NULL THEN
      RAISE EXCEPTION 'FAIL: expected raw_review.run_id to be NULL after deleting ingestion_run, got: %', check_run_id;
    END IF;

    RAISE NOTICE 'PASS: ON DELETE SET NULL worked (raw_review.run_id is NULL)';
END $$;

--test on delete cascade
DELETE FROM silver.clean_latest_review
WHERE app_id = 570 AND review_id = 100001;
DO $$
  BEGIN
    -- After deleting the parent review, the player_info child row should be gone (CASCADE)
    IF EXISTS (
      SELECT 1 FROM silver.clean_latest_review_player_info
      WHERE app_id = 570 AND review_id = 100001
    ) THEN
      RAISE EXCEPTION 'FAIL: expected player_info to be deleted (cascade) but row still exists';
    END IF;

    RAISE NOTICE 'PASS: ON DELETE CASCADE worked (player_info removed)';
END $$;

-- roll back for clean test data
ROLLBACK;