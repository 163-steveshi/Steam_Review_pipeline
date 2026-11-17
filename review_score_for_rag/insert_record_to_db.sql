WITH ranked AS (
    SELECT app_id, review_id,
           ROW_NUMBER() OVER (PARTITION BY app_id, text_hash
                              ORDER BY timestamp_created) AS dup_rank,
           COUNT(*)     OVER (PARTITION BY app_id, text_hash)       AS dup_count
    FROM gold.review_rag_score
    WHERE COALESCE(reject_reason, '{}') <@ ARRAY['duplicate']      -- NULL, {}, or {duplicate}
)
UPDATE gold.review_rag_score g
SET dup_rank      = r.dup_rank,
    dup_count     = r.dup_count,
    reject_reason = CASE WHEN r.dup_rank > 1 THEN ARRAY['duplicate'] ELSE NULL END
FROM ranked r
WHERE g.app_id = r.app_id
  AND g.review_id = r.review_id
  -- only touch rows that actually change (fewer writes, faster reruns)
  AND (g.dup_rank  IS DISTINCT FROM r.dup_rank
    OR g.dup_count IS DISTINCT FROM r.dup_count
    OR g.reject_reason IS DISTINCT FROM
       CASE WHEN r.dup_rank > 1 THEN ARRAY['duplicate'] ELSE NULL END);


SELECT review.app_id, review.review_id, review.source_raw_id,
	game.source_raw_id as game_source_raw_id, game.name as game_name,
	game.app_type, game.genres, review.review_text, review.language,
	review.voted_positive,
    review.steam_purchase,
    review.received_for_free,
    review.written_during_early_access,
    review.primarily_play_on_steam_deck,

    -- Playtime & Timestamps
    player_info.playtime_at_review_mins,
    review.timestamp_created,
    review.timestamp_updated,

    -- Trust & Ranking Signals
    review.votes_helpful,
    review.votes_funny,
    review.weighted_vote_score

   
FROM silver.clean_latest_review as review
INNER JOIN silver.clean_latest_review_player_info as player_info
	on review.review_id = player_info.review_id and review.app_id = player_info.app_id
INNER JOIN silver.dim_game as game
	on review.app_id = game.app_id
WHERE review.app_id = 4001890;


-- check the review overall length in the percetage cut

SELECT
  percentile_cont(ARRAY[0.5, 0.75, 0.9, 0.95, 0.99])
    WITHIN GROUP (ORDER BY length(review_text)) AS char_percentiles,
  count(*) FILTER (WHERE length(review_text) > 1200)::float / count(*) AS share_over_1200
FROM gold.review_rag_score
WHERE is_rag_eligible = true;   -- your 

