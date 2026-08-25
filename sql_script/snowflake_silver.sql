CREATE OR REPLACE DYNAMIC TABLE STEAM_REVIEW.silver.steam_reviews_flagged
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Intermediate table: casts + flags validity, nothing is dropped here'
AS
SELECT
    review_id,
    app_id,
    author_steam_id,

    author_num_games_owned                                   AS author_num_games_owned_raw,
    TRY_CAST(author_num_games_owned AS BIGINT)                AS author_num_games_owned,

    author_num_reviews                                        AS author_num_reviews_raw,
    TRY_CAST(author_num_reviews AS BIGINT)                    AS author_num_reviews,

    author_playtime_forever                                   AS author_playtime_forever_raw,
    TRY_CAST(author_playtime_forever AS BIGINT)               AS author_playtime_forever_mins,

    author_playtime_last_two_weeks                            AS author_playtime_last_two_weeks_raw,
    TRY_CAST(author_playtime_last_two_weeks AS BIGINT)        AS author_playtime_last_two_weeks_mins,

    author_playtime_at_review                                 AS author_playtime_at_review_raw,
    TRY_CAST(author_playtime_at_review AS BIGINT)             AS author_playtime_at_review_mins,

    author_deck_playtime_at_review                            AS author_deck_playtime_at_review_raw,
    TRY_CAST(author_deck_playtime_at_review AS BIGINT)        AS author_deck_playtime_at_review_mins,

    author_last_played                                        AS author_last_played_raw,
    TRY_CAST(author_last_played AS TIMESTAMP_NTZ)             AS author_last_played_timestamp,

    language,
    review,

    timestamp_created                                         AS timestamp_created_raw,
    TRY_CAST(timestamp_created AS TIMESTAMP_NTZ)              AS timestamp_created,

    timestamp_updated                                         AS timestamp_updated_raw,
    TRY_CAST(timestamp_updated AS TIMESTAMP_NTZ)              AS timestamp_updated,

    voted_up                                                  AS voted_up_raw,
    TRY_CAST(voted_up AS BOOLEAN)                             AS voted_positive,

    votes_up                                                  AS votes_up_raw,
    TRY_CAST(votes_up AS BIGINT)                              AS votes_helpful,

    votes_funny                                               AS votes_funny_raw,
    TRY_CAST(votes_funny AS BIGINT)                           AS votes_funny,

    weighted_vote_score                                       AS weighted_vote_score_raw,
    TRY_CAST(weighted_vote_score AS FLOAT)                    AS weighted_vote_score,

    comment_count                                             AS comment_count_raw,
    TRY_CAST(comment_count AS BIGINT)                         AS comment_count,

    steam_purchase                                            AS steam_purchase_raw,
    TRY_CAST(steam_purchase AS BOOLEAN)                       AS is_steam_purchase,

    received_for_free                                         AS received_for_free_raw,
    TRY_CAST(received_for_free AS BOOLEAN)                    AS is_received_for_free,

    written_during_early_access                               AS written_during_early_access_raw,
    TRY_CAST(written_during_early_access AS BOOLEAN)          AS is_written_during_early_access,

    developer_response,

    timestamp_dev_responded                                   AS timestamp_dev_responded_raw,
    TRY_CAST(timestamp_dev_responded AS TIMESTAMP_NTZ)        AS timestamp_dev_responded,

    primarily_steam_deck                                      AS primarily_steam_deck_raw,
    TRY_CAST(primarily_steam_deck AS BOOLEAN)                 AS is_primarily_steam_deck_player,

    reactions                                                 AS reactions_raw,
    TRY_PARSE_JSON(reactions)                                 AS reactions,

    ingested_at,

    ARRAY_CONSTRUCT_COMPACT(
        -- ===== REVIEW GRAIN =====
        CASE WHEN review_id IS NULL THEN 'review:miss_review_id' END,
        CASE WHEN app_id IS NULL THEN 'review:miss_app_id' END,
        CASE WHEN review IS NULL THEN 'review:miss_review_text' END,
        CASE WHEN language IS NULL THEN 'review:miss_language' END,

        CASE WHEN timestamp_created IS NULL THEN 'review:miss_timestamp_created' END,
        CASE WHEN timestamp_created IS NOT NULL
             AND TRY_CAST(timestamp_created AS TIMESTAMP_NTZ) IS NULL
             THEN 'review:timestamp_created_malformed' END,

        CASE WHEN timestamp_updated IS NOT NULL
             AND TRY_CAST(timestamp_updated AS TIMESTAMP_NTZ) IS NULL
             THEN 'review:timestamp_updated_malformed' END,

        CASE WHEN voted_up IS NULL THEN 'review:miss_voted_up' END,
        CASE WHEN voted_up IS NOT NULL
             AND TRY_CAST(voted_up AS BOOLEAN) IS NULL
             THEN 'review:voted_up_malformed' END,

        CASE WHEN votes_up IS NOT NULL
             AND TRY_CAST(votes_up AS BIGINT) IS NULL
             THEN 'review:votes_up_malformed' END,

        CASE WHEN votes_funny IS NOT NULL
             AND TRY_CAST(votes_funny AS BIGINT) IS NULL
             THEN 'review:votes_funny_malformed' END,

        CASE WHEN weighted_vote_score IS NOT NULL
             AND TRY_CAST(weighted_vote_score AS FLOAT) IS NULL
             THEN 'review:weighted_vote_score_malformed' END,

        CASE WHEN comment_count IS NOT NULL
             AND TRY_CAST(comment_count AS BIGINT) IS NULL
             THEN 'review:comment_count_malformed' END,

        CASE WHEN steam_purchase IS NOT NULL
             AND TRY_CAST(steam_purchase AS BOOLEAN) IS NULL
             THEN 'review:steam_purchase_malformed' END,

        CASE WHEN received_for_free IS NOT NULL
             AND TRY_CAST(received_for_free AS BOOLEAN) IS NULL
             THEN 'review:received_for_free_malformed' END,

        CASE WHEN written_during_early_access IS NOT NULL
             AND TRY_CAST(written_during_early_access AS BOOLEAN) IS NULL
             THEN 'review:written_during_early_access_malformed' END,

        CASE WHEN timestamp_dev_responded IS NOT NULL
             AND TRY_CAST(timestamp_dev_responded AS TIMESTAMP_NTZ) IS NULL
             THEN 'review:timestamp_dev_responded_malformed' END,

        CASE WHEN primarily_steam_deck IS NOT NULL
             AND TRY_CAST(primarily_steam_deck AS BOOLEAN) IS NULL
             THEN 'review:primarily_steam_deck_malformed' END,

        CASE WHEN reactions IS NOT NULL
             AND TRY_PARSE_JSON(reactions) IS NULL
             THEN 'review:reactions_malformed' END
    ) AS review_failure_reasons,

    ARRAY_CONSTRUCT_COMPACT(
        -- ===== AUTHOR GRAIN =====
        CASE WHEN author_steam_id IS NULL THEN 'author:miss_author_steam_id' END,

        CASE WHEN author_num_games_owned IS NOT NULL
             AND TRY_CAST(author_num_games_owned AS BIGINT) IS NULL
             THEN 'author:num_games_owned_malformed' END,

        CASE WHEN author_num_reviews IS NOT NULL
             AND TRY_CAST(author_num_reviews AS BIGINT) IS NULL
             THEN 'author:num_reviews_malformed' END,

        CASE WHEN author_playtime_forever IS NOT NULL
             AND TRY_CAST(author_playtime_forever AS BIGINT) IS NULL
             THEN 'author:playtime_forever_malformed' END,

        CASE WHEN author_playtime_last_two_weeks IS NOT NULL
             AND TRY_CAST(author_playtime_last_two_weeks AS BIGINT) IS NULL
             THEN 'author:playtime_last_two_weeks_malformed' END,

        CASE WHEN author_playtime_at_review IS NOT NULL
             AND TRY_CAST(author_playtime_at_review AS BIGINT) IS NULL
             THEN 'author:playtime_at_review_malformed' END,

        CASE WHEN author_deck_playtime_at_review IS NOT NULL
             AND TRY_CAST(author_deck_playtime_at_review AS BIGINT) IS NULL
             THEN 'author:deck_playtime_at_review_malformed' END,

        CASE WHEN author_last_played IS NOT NULL
             AND TRY_CAST(author_last_played AS TIMESTAMP_NTZ) IS NULL
             THEN 'author:last_played_malformed' END
    ) AS review_player_failure_reasons

FROM STEAM_REVIEW.BRONZE.RAW_REVIEW;