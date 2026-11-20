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
    review_text_for_chunking TEXT       NOT NULL,
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


CREATE TABLE IF NOT EXISTS gold.review_chunks (
    -- keys / lineage
    app_id              BIGINT      NOT NULL,
    review_id           BIGINT      NOT NULL,
    chunk_index         SMALLINT    NOT NULL,
    chunk_count         SMALLINT    NOT NULL,          -- how many chunks this review produced
    text_hash           CHAR(32)    NOT NULL,          -- copied from review_rag_score: detects edited reviews
    chunker_version     TEXT        NOT NULL,          -- e.g. 'v1-300tok-40ovl'
    game_name               TEXT,
    -- content
    content             TEXT        NOT NULL,          -- cleaned chunk text (what the LLM reads)

    -- filters used INSIDE vector search (copied from review_rag_score)
    language            VARCHAR(20) NOT NULL,
    voted_positive      BOOLEAN     NOT NULL,
    timestamp_created   TIMESTAMPTZ NOT NULL,
    playtime_at_review_mins INTEGER,
    genres              TEXT[],                        -- flattened genres JSONB for vector search the filter “does this game have genre X?”

    -- embedding
    embedding           vector(1024),                  -- NULL = waiting to be embedded
    embed_model         TEXT,
    embedded_at         TIMESTAMPTZ,

    chunked_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (app_id, review_id, chunk_index),
    FOREIGN KEY (app_id, review_id)
        REFERENCES gold.review_rag_score (app_id, review_id) ON DELETE CASCADE
);

-- vector search
CREATE INDEX IF NOT EXISTS review_chunks_embedding_hnsw
    ON gold.review_chunks USING hnsw (embedding vector_cosine_ops);

-- filters
CREATE INDEX IF NOT EXISTS review_chunks_genres_gin ON gold.review_chunks USING gin (genres);
CREATE INDEX IF NOT EXISTS review_chunks_app_id     ON gold.review_chunks (app_id);

-- embedding worker: find pending chunks fast
CREATE INDEX IF NOT EXISTS review_chunks_pending
    ON gold.review_chunks (app_id, review_id, chunk_index) WHERE embedding IS NULL;