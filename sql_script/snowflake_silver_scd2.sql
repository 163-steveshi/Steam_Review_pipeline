CREATE OR REPLACE TABLE STEAM_REVIEW.SILVER.dim_reviews_scd2 (
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
    start_timestamp                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    end_timestamp                     TIMESTAMP_NTZ,
    is_current                        BOOLEAN
);

CREATE OR REPLACE STREAM STEAM_REVIEW.SILVER.steam_reviews_flagged_stream_scd2
  ON DYNAMIC TABLE STEAM_REVIEW.SILVER.steam_reviews_flagged
  APPEND_ONLY = FALSE; -- need operation like update

CREATE OR REPLACE TASK STEAM_REVIEW.SILVER.task_merge_dim_reviews_scd2
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '10 minutes'
  WHEN SYSTEM$STREAM_HAS_DATA('STEAM_REVIEW.SILVER.steam_reviews_flagged_stream')
AS
MERGE INTO STEAM_REVIEW.SILVER.dim_reviews_scd2 AS Target
USING (
    WITH Clean_Staging AS (
        SELECT * FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY review_id
                    ORDER BY ingested_at DESC, timestamp_updated DESC NULLS LAST
                ) AS rn
            FROM STEAM_REVIEW.SILVER.steam_reviews_flagged_stream_scd2
            WHERE METADATA$ACTION = 'INSERT'
              AND ARRAY_SIZE(review_failure_reasons) = 0
        )
        WHERE rn = 1
    )
    -- 1. Brand new record
    SELECT
        S.review_id, S.app_id, S.language, S.review,
        S.timestamp_created, S.timestamp_updated, S.voted_positive,
        S.votes_helpful, S.votes_funny, S.weighted_vote_score,
        S.comment_count, S.is_steam_purchase, S.is_received_for_free,
        S.is_written_during_early_access, S.developer_response,
        S.timestamp_dev_responded, S.is_primarily_steam_deck_player,
        S.reactions, S.ingested_at,
        CURRENT_TIMESTAMP() AS start_timestamp,
        NULL::TIMESTAMP_NTZ AS end_timestamp,
        TRUE AS is_current,
        'INSERT' AS Action_Type
    FROM Clean_Staging S
    LEFT JOIN STEAM_REVIEW.SILVER.dim_reviews_scd2 AS T
        ON S.review_id = T.review_id AND T.is_current = TRUE
    WHERE T.review_id IS NULL

    UNION ALL

    -- 2. Expire existing current row that changed
    SELECT
        T.review_id, T.app_id, T.language, T.review,
        T.timestamp_created, T.timestamp_updated, T.voted_positive,
        T.votes_helpful, T.votes_funny, T.weighted_vote_score,
        T.comment_count, T.is_steam_purchase, T.is_received_for_free,
        T.is_written_during_early_access, T.developer_response,
        T.timestamp_dev_responded, T.is_primarily_steam_deck_player,
        T.reactions, T.ingested_at,
        T.start_timestamp,
        CURRENT_TIMESTAMP() AS end_timestamp,
        FALSE AS is_current,
        'UPDATE_EXPIRE' AS Action_Type
    FROM Clean_Staging S
    INNER JOIN STEAM_REVIEW.SILVER.dim_reviews_scd2 AS T
        ON S.review_id = T.review_id AND T.is_current = TRUE
    WHERE
           S.app_id                         IS DISTINCT FROM T.app_id
        OR S.language                       IS DISTINCT FROM T.language
        OR S.review                         IS DISTINCT FROM T.review
        OR S.timestamp_created               IS DISTINCT FROM T.timestamp_created
        OR S.timestamp_updated               IS DISTINCT FROM T.timestamp_updated
        OR S.voted_positive                  IS DISTINCT FROM T.voted_positive
        OR S.votes_helpful                   IS DISTINCT FROM T.votes_helpful
        OR S.votes_funny                     IS DISTINCT FROM T.votes_funny
        OR S.weighted_vote_score             IS DISTINCT FROM T.weighted_vote_score
        OR S.comment_count                   IS DISTINCT FROM T.comment_count
        OR S.is_steam_purchase               IS DISTINCT FROM T.is_steam_purchase
        OR S.is_received_for_free            IS DISTINCT FROM T.is_received_for_free
        OR S.is_written_during_early_access  IS DISTINCT FROM T.is_written_during_early_access
        OR S.developer_response              IS DISTINCT FROM T.developer_response
        OR S.timestamp_dev_responded         IS DISTINCT FROM T.timestamp_dev_responded
        OR S.is_primarily_steam_deck_player  IS DISTINCT FROM T.is_primarily_steam_deck_player
        OR S.reactions                       IS DISTINCT FROM T.reactions

    UNION ALL

    -- 3. New active version of a changed row
    SELECT
        S.review_id, S.app_id, S.language, S.review,
        S.timestamp_created, S.timestamp_updated, S.voted_positive,
        S.votes_helpful, S.votes_funny, S.weighted_vote_score,
        S.comment_count, S.is_steam_purchase, S.is_received_for_free,
        S.is_written_during_early_access, S.developer_response,
        S.timestamp_dev_responded, S.is_primarily_steam_deck_player,
        S.reactions, S.ingested_at,
        CURRENT_TIMESTAMP() AS start_timestamp,
        NULL::TIMESTAMP_NTZ AS end_timestamp,
        TRUE AS is_current,
        'UPDATE_INSERT' AS Action_Type
    FROM Clean_Staging S
    INNER JOIN STEAM_REVIEW.SILVER.dim_reviews_scd2 AS T
        ON S.review_id = T.review_id AND T.is_current = TRUE
    WHERE
           S.app_id                         IS DISTINCT FROM T.app_id
        OR S.language                       IS DISTINCT FROM T.language
        OR S.review                         IS DISTINCT FROM T.review
        OR S.timestamp_created               IS DISTINCT FROM T.timestamp_created
        OR S.timestamp_updated               IS DISTINCT FROM T.timestamp_updated
        OR S.voted_positive                  IS DISTINCT FROM T.voted_positive
        OR S.votes_helpful                   IS DISTINCT FROM T.votes_helpful
        OR S.votes_funny                     IS DISTINCT FROM T.votes_funny
        OR S.weighted_vote_score             IS DISTINCT FROM T.weighted_vote_score
        OR S.comment_count                   IS DISTINCT FROM T.comment_count
        OR S.is_steam_purchase               IS DISTINCT FROM T.is_steam_purchase
        OR S.is_received_for_free            IS DISTINCT FROM T.is_received_for_free
        OR S.is_written_during_early_access  IS DISTINCT FROM T.is_written_during_early_access
        OR S.developer_response              IS DISTINCT FROM T.developer_response
        OR S.timestamp_dev_responded         IS DISTINCT FROM T.timestamp_dev_responded
        OR S.is_primarily_steam_deck_player  IS DISTINCT FROM T.is_primarily_steam_deck_player
        OR S.reactions                       IS DISTINCT FROM T.reactions
) AS Source
ON Target.review_id = Source.review_id
   AND Target.is_current = TRUE
   AND Source.Action_Type = 'UPDATE_EXPIRE'

WHEN MATCHED THEN
    UPDATE SET
        Target.is_current = FALSE,
        Target.end_timestamp = Source.end_timestamp

WHEN NOT MATCHED THEN
    INSERT (
        review_id, app_id, language, review,
        timestamp_created, timestamp_updated,
        voted_positive, votes_helpful, votes_funny, weighted_vote_score,
        comment_count, is_steam_purchase, is_received_for_free,
        is_written_during_early_access, developer_response,
        timestamp_dev_responded, is_primarily_steam_deck_player,
        reactions, ingested_at, start_timestamp, end_timestamp, is_current
    )
    VALUES (
        Source.review_id, Source.app_id, Source.language, Source.review,
        Source.timestamp_created, Source.timestamp_updated,
        Source.voted_positive, Source.votes_helpful, Source.votes_funny, Source.weighted_vote_score,
        Source.comment_count, Source.is_steam_purchase, Source.is_received_for_free,
        Source.is_written_during_early_access, Source.developer_response,
        Source.timestamp_dev_responded, Source.is_primarily_steam_deck_player,
        Source.reactions, Source.ingested_at, Source.start_timestamp, Source.end_timestamp, Source.is_current
    );
ALTER TASK STEAM_REVIEW.SILVER.task_merge_dim_reviews_scd2 RESUME;



CREATE OR REPLACE TABLE STEAM_REVIEW.SILVER.dim_player_infos_scd2 (
    author_steam_id                        STRING PRIMARY KEY,
    author_num_games_owned                 BIGINT,
    author_num_reviews                     BIGINT,
    author_playtime_forever_mins           BIGINT,
    author_playtime_last_two_weeks_mins    BIGINT,
    author_playtime_at_review_mins         BIGINT,
    author_deck_playtime_at_review_mins    BIGINT,
    author_last_played_timestamp           TIMESTAMP_NTZ,
    ingested_at                            TIMESTAMP_NTZ,
    start_timestamp                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    end_timestamp                     TIMESTAMP_NTZ,
    is_current                        BOOLEAN
);

CREATE OR REPLACE STREAM  STEAM_REVIEW.SILVER.steam_reviews_flagged_stream_player_info_scd2
  ON DYNAMIC TABLE STEAM_REVIEW.SILVER.steam_reviews_flagged
  APPEND_ONLY = FALSE;
  
CREATE OR REPLACE TASK STEAM_REVIEW.SILVER.task_merge_dim_player_info_scd2
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '10 minutes'
  WHEN SYSTEM$STREAM_HAS_DATA('STEAM_REVIEW.SILVER.steam_reviews_flagged_stream_player_info_scd2')
AS
MERGE INTO STEAM_REVIEW.SILVER.dim_player_infos_scd2 AS Target
USING (
    WITH Clean_Staging AS (
        SELECT * FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY  author_steam_id
                    ORDER BY ingested_at DESC, timestamp_updated DESC NULLS LAST
                ) AS rn
            FROM STEAM_REVIEW.SILVER.steam_reviews_flagged_stream_player_info_scd2
            WHERE METADATA$ACTION = 'INSERT'
              AND ARRAY_SIZE(review_failure_reasons) = 0
        )
        WHERE rn = 1
    )
    -- 1. Brand new record
    SELECT
        S.author_steam_id,
        S.author_num_games_owned,
        S.author_num_reviews,
        S.author_playtime_forever_mins,
        S.author_playtime_last_two_weeks_mins,
        S.author_playtime_at_review_mins,
        S.author_deck_playtime_at_review_mins,
        S.author_last_played_timestamp, 
        S.ingested_at,
        CURRENT_TIMESTAMP() AS start_timestamp,
        NULL::TIMESTAMP_NTZ AS end_timestamp,
        TRUE AS is_current,
        'INSERT' AS Action_Type
    FROM Clean_Staging S
    LEFT JOIN STEAM_REVIEW.SILVER.dim_player_infos_scd2 AS T
        ON S.author_steam_id = T.author_steam_id AND T.is_current = TRUE
    WHERE T.author_steam_id IS NULL

    UNION ALL

    -- 2. Expire existing current row that changed
    SELECT
        T.author_steam_id,
        T.author_num_games_owned,
        T.author_num_reviews,
        T.author_playtime_forever_mins,
        T.author_playtime_last_two_weeks_mins,
        T.author_playtime_at_review_mins,
        T.author_deck_playtime_at_review_mins,
        T.author_last_played_timestamp, 
        T.ingested_at,
        T.start_timestamp,
        CURRENT_TIMESTAMP() AS end_timestamp,
        FALSE AS is_current,
        'UPDATE_EXPIRE' AS Action_Type
    FROM Clean_Staging S
    INNER JOIN STEAM_REVIEW.SILVER.dim_player_infos_scd2 AS T
        ON S.author_steam_id = T.author_steam_id AND T.is_current = TRUE
    WHERE
        S.author_num_games_owned IS DISTINCT FROM T.author_num_games_owned
        OR S.author_num_reviews IS DISTINCT FROM T.author_num_reviews
        OR S.author_playtime_forever_mins IS DISTINCT FROM T.author_playtime_forever_mins
        OR S.author_playtime_last_two_weeks_mins IS DISTINCT FROM T.author_playtime_last_two_weeks_mins
        OR S.author_playtime_at_review_mins IS DISTINCT FROM T.author_playtime_at_review_mins
        OR S.author_deck_playtime_at_review_mins IS DISTINCT FROM T.author_deck_playtime_at_review_mins
        OR S.author_last_played_timestamp IS DISTINCT FROM T.author_last_played_timestamp

    UNION ALL

    -- 3. New active version of a changed row
    SELECT
        S.author_steam_id,
        S.author_num_games_owned,
        S.author_num_reviews,
        S.author_playtime_forever_mins,
        S.author_playtime_last_two_weeks_mins,
        S.author_playtime_at_review_mins,
        S.author_deck_playtime_at_review_mins,
        S.author_last_played_timestamp, 
        S.ingested_at,
        CURRENT_TIMESTAMP() AS start_timestamp,
        NULL::TIMESTAMP_NTZ AS end_timestamp,
        TRUE AS is_current,
        'UPDATE_INSERT' AS Action_Type
    FROM Clean_Staging S
    INNER JOIN STEAM_REVIEW.SILVER.dim_player_infos_scd2 AS T
        ON S.author_steam_id = T.author_steam_id AND T.is_current = TRUE
    WHERE
        S.author_num_games_owned IS DISTINCT FROM T.author_num_games_owned
        OR S.author_num_reviews IS DISTINCT FROM T.author_num_reviews
        OR S.author_playtime_forever_mins IS DISTINCT FROM T.author_playtime_forever_mins
        OR S.author_playtime_last_two_weeks_mins IS DISTINCT FROM T.author_playtime_last_two_weeks_mins
        OR S.author_playtime_at_review_mins IS DISTINCT FROM T.author_playtime_at_review_mins
        OR S.author_deck_playtime_at_review_mins IS DISTINCT FROM T.author_deck_playtime_at_review_mins
        OR S.author_last_played_timestamp IS DISTINCT FROM T.author_last_played_timestamp
             
) AS Source
ON Target.author_steam_id = Source.author_steam_id
   AND Target.is_current = TRUE
   AND Source.Action_Type = 'UPDATE_EXPIRE'

WHEN MATCHED THEN
    UPDATE SET
        Target.is_current = FALSE,
        Target.end_timestamp = Source.end_timestamp

WHEN NOT MATCHED THEN
    INSERT (
        author_steam_id,
        author_num_games_owned,
        author_num_reviews,
        author_playtime_forever_mins,
        author_playtime_last_two_weeks_mins,
        author_playtime_at_review_mins,
        author_deck_playtime_at_review_mins,
        author_last_played_timestamp, 
        ingested_at,
        start_timestamp,
        end_timestamp,
        is_current
    )
    VALUES (
        Source.author_steam_id,
        Source.author_num_games_owned,
        Source.author_num_reviews,
        Source.author_playtime_forever_mins,
        Source.author_playtime_last_two_weeks_mins,
        Source.author_playtime_at_review_mins,
        Source.author_deck_playtime_at_review_mins,
        Source.author_last_played_timestamp, 
        Source.ingested_at, Source.start_timestamp, Source.end_timestamp, Source.is_current
    );
ALTER TASK STEAM_REVIEW.SILVER.task_merge_dim_player_info_scd2 RESUME;