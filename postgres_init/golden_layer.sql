CREATE TABLE IF NOT EXISTS gold.review_rag_score (
    -- keys / lineage
    app_id                  BIGINT      NOT NULL,
    review_id               BIGINT      NOT NULL,
    source_raw_id           BIGINT      NOT NULL,      -- review lineage
    game_source_raw_id      BIGINT,                    -- which dim_game snapshot was joined (NULL = not fetched yet)

    -- game context (denormalized for chunk text)
    game_name               TEXT,
    app_type                TEXT,
    genres                  JSONB,

    -- content
    review_text             TEXT        NOT NULL,
    language                VARCHAR(20) NOT NULL,

    -- metadata filters for retrieval
    voted_positive          BOOLEAN     NOT NULL,
    timestamp_created       TIMESTAMPTZ NOT NULL,
    timestamp_updated       TIMESTAMPTZ NOT NULL,
    steam_purchase          BOOLEAN     NOT NULL,
    received_for_free       BOOLEAN     NOT NULL,
    written_during_early_access  BOOLEAN NOT NULL,
    primarily_play_on_steam_deck BOOLEAN NOT NULL,
    playtime_at_review_mins INTEGER,                   -- from player_info

    -- trust / ranking signals
    votes_helpful           INTEGER,
    votes_funny             INTEGER,
    weighted_vote_score     DOUBLE PRECISION,

    -- quality features
    word_count              INTEGER     NOT NULL,
    unique_word_ratio       NUMERIC(5,4),
    letter_ratio            NUMERIC(5,4),
    has_url                 BOOLEAN     NOT NULL,
    has_char_spam           BOOLEAN     NOT NULL,
    text_hash               CHAR(32)    NOT NULL,
    dup_rank                INTEGER     NOT NULL,

    -- decision
    reject_reason           TEXT[],                      -- NULL = eligible
    is_rag_eligible         BOOLEAN GENERATED ALWAYS AS (COALESCE(cardinality(reject_reason), 0) = 0) STORED,
    scoring_version         TEXT        NOT NULL,      -- e.g. 'v1' when you tune thresholds
    scored_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    dup_count INTEGER,
    PRIMARY KEY (app_id, review_id)
);
CREATE INDEX ON gold.review_rag_score (is_rag_eligible, app_id);