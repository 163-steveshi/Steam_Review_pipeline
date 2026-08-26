CREATE OR REPLACE DYNAMIC TABLE STEAM_REVIEW.silver.steam_reviews_flagged
  TARGET_LAG = '5 mins'
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

CREATE TABLE IF NOT EXISTS STEAM_REVIEW.SILVER.dim_reviews_scd1 (
    review_id                         STRING PRIMARY KEY,
    app_id                            STRING,
    language                          STRING,
    review                            STRING,
    timestamp_created                 TIMESTAMP_NTZ,
    timestamp_updated                 TIMESTAMP_NTZ,
    voted_positive                    BOOLEAN,
    votes_helpful                     BIGINT,
    votes_funny                       BIGINT,
    weighted_vote_score                FLOAT,
    comment_count                     BIGINT,
    is_steam_purchase                 BOOLEAN,
    is_received_for_free              BOOLEAN,
    is_written_during_early_access    BOOLEAN,
    developer_response                VARCHAR,
    timestamp_dev_responded           TIMESTAMP_NTZ,
    is_primarily_steam_deck_player    BOOLEAN,
    reactions                         VARIANT,
    ingested_at                       TIMESTAMP_NTZ,
    _dlt_updated_at                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- when the merge statement write the data
);

CREATE STREAM IF NOT EXISTS STEAM_REVIEW.SILVER.steam_reviews_flagged_stream
  ON DYNAMIC TABLE STEAM_REVIEW.SILVER.steam_reviews_flagged
  APPEND_ONLY = FALSE; -- need operation like update

CREATE TASK IF NOT EXISTS STEAM_REVIEW.SILVER.task_merge_dim_reviews_scd1
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '10 mins'
  WHEN SYSTEM$STREAM_HAS_DATA('STEAM_REVIEW.SILVER.steam_reviews_flagged_stream')
AS
MERGE INTO STEAM_REVIEW.SILVER.dim_reviews_scd1 AS tgt
USING (
    SELECT *
    FROM STEAM_REVIEW.SILVER.steam_reviews_flagged_stream
    WHERE METADATA$ACTION = 'INSERT'
      AND ARRAY_SIZE(review_failure_reasons) = 0   -- only clean rows flow to gold
) AS src
ON tgt.review_id = src.review_id

WHEN MATCHED AND src.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
        app_id                          = src.app_id,
        language                        = src.language,
        review                          = src.review,
        timestamp_created               = src.timestamp_created,
        timestamp_updated               = src.timestamp_updated,
        voted_positive                  = src.voted_positive,
        votes_helpful                   = src.votes_helpful,
        votes_funny                     = src.votes_funny,
        weighted_vote_score             = src.weighted_vote_score,
        comment_count                   = src.comment_count,
        is_steam_purchase               = src.is_steam_purchase,
        is_received_for_free            = src.is_received_for_free,
        is_written_during_early_access  = src.is_written_during_early_access,
        developer_response              = src.developer_response,
        timestamp_dev_responded         = src.timestamp_dev_responded,
        is_primarily_steam_deck_player  = src.is_primarily_steam_deck_player,
        reactions                       = src.reactions,
        ingested_at                     = src.ingested_at,
        _dlt_updated_at                 = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        review_id, app_id, language, review,
        timestamp_created, timestamp_updated,
        voted_positive, votes_helpful, votes_funny, weighted_vote_score,
        comment_count, is_steam_purchase, is_received_for_free,
        is_written_during_early_access, developer_response,
        timestamp_dev_responded, is_primarily_steam_deck_player,
        reactions, ingested_at, _dlt_updated_at
    )
    VALUES (
        src.review_id, src.app_id, src.language, src.review,
        src.timestamp_created, src.timestamp_updated,
        src.voted_positive, src.votes_helpful, src.votes_funny, src.weighted_vote_score,
        src.comment_count, src.is_steam_purchase, src.is_received_for_free,
        src.is_written_during_early_access, src.developer_response,
        src.timestamp_dev_responded, src.is_primarily_steam_deck_player,
        src.reactions, src.ingested_at, CURRENT_TIMESTAMP()
    );

ALTER TASK STEAM_REVIEW.SILVER.task_merge_dim_review_scd1 RESUME;


CREATE TABLE IF NOT EXISTS STEAM_REVIEW.SILVER.dim_player_info_scd1 (
    author_steam_id                        STRING PRIMARY KEY,
    author_num_games_owned                 BIGINT,
    author_num_reviews                     BIGINT,
    author_playtime_forever_mins           BIGINT,
    author_playtime_last_two_weeks_mins    BIGINT,
    author_playtime_at_review_mins         BIGINT,
    author_deck_playtime_at_review_mins    BIGINT,
    author_last_played_timestamp           TIMESTAMP_NTZ,
    ingested_at                            TIMESTAMP_NTZ,
    _dlt_updated_at                        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STREAM IF NOT EXISTS STEAM_REVIEW.SILVER.steam_reviews_flagged_stream_player
  ON DYNAMIC TABLE STEAM_REVIEW.SILVER.steam_reviews_flagged
  APPEND_ONLY = FALSE;
CREATE TASK IF NOT EXISTS STEAM_REVIEW.SILVER.task_merge_dim_player_info_scd1
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '10 mins'
  WHEN SYSTEM$STREAM_HAS_DATA('STEAM_REVIEW.SILVER.steam_reviews_flagged_stream_player')
AS
MERGE INTO  STEAM_REVIEW.SILVER.dim_player_info_scd1 AS tgt
USING (
    SELECT
        author_steam_id,
        author_num_games_owned,
        author_num_reviews,
        author_playtime_forever_mins,
        author_playtime_last_two_weeks_mins,
        author_playtime_at_review_mins,
        author_deck_playtime_at_review_mins,
        author_last_played_timestamp,
        ingested_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY author_steam_id
                ORDER BY ingested_at DESC, timestamp_updated DESC NULLS LAST
            ) AS rn
        FROM STEAM_REVIEW.SILVER.steam_reviews_flagged_stream_player
        WHERE METADATA$ACTION = 'INSERT'
          AND ARRAY_SIZE(review_player_failure_reasons) = 0   -- only clean author rows flow to gold
    )
    WHERE rn = 1   -- one row per author per batch, most recent wins
) AS src
ON tgt.author_steam_id = src.author_steam_id

WHEN MATCHED THEN
    UPDATE SET
        author_num_games_owned                 = src.author_num_games_owned,
        author_num_reviews                     = src.author_num_reviews,
        author_playtime_forever_mins           = src.author_playtime_forever_mins,
        author_playtime_last_two_weeks_mins    = src.author_playtime_last_two_weeks_mins,
        author_playtime_at_review_mins         = src.author_playtime_at_review_mins,
        author_deck_playtime_at_review_mins    = src.author_deck_playtime_at_review_mins,
        author_last_played_timestamp           = src.author_last_played_timestamp,
        ingested_at                             = src.ingested_at,
        _dlt_updated_at                         = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        author_steam_id, author_num_games_owned, author_num_reviews,
        author_playtime_forever_mins, author_playtime_last_two_weeks_mins,
        author_playtime_at_review_mins, author_deck_playtime_at_review_mins,
        author_last_played_timestamp, ingested_at, _dlt_updated_at
    )
    VALUES (
        src.author_steam_id, src.author_num_games_owned, src.author_num_reviews,
        src.author_playtime_forever_mins, src.author_playtime_last_two_weeks_mins,
        src.author_playtime_at_review_mins, src.author_deck_playtime_at_review_mins,
        src.author_last_played_timestamp, src.ingested_at, CURRENT_TIMESTAMP()
    );

ALTER TASK STEAM_REVIEW.SILVER.task_merge_dim_player_info_scd1 RESUME;