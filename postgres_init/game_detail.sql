CREATE TABLE IF NOT EXISTS bronze.steam_app_details_raw (
    app_id      INTEGER,

    fetched_at  timestamptz DEFAULT now(),
    success     boolean,
    payload     jsonb,          -- the entire "data" object
    raw_id      BIGSERIAL
);
CREATE INDEX IF NOT EXISTS ix_raw_id ON bronze.steam_app_details_raw (raw_id);


CREATE TABLE IF NOT EXISTS meta.etl_checkpoint (
    pipeline     text PRIMARY KEY,
    last_raw_id  bigint      NOT NULL DEFAULT 0,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.dim_game (
    app_id                      INTEGER     NOT NULL,
    source_raw_id               BIGINT      NOT NULL,
    snapshot_at                 TIMESTAMPTZ NOT NULL,   -- bronze.fetched_at
    name                        TEXT,
    app_type                    TEXT,
    short_description           TEXT,
    is_free                     BOOLEAN,
    release_date                DATE,
    developers                  JSONB,
    genres                      JSONB,
    categories                  JSONB,
    metacritic_score            INTEGER,
    steam_recommendation_count  INTEGER,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (app_id, source_raw_id)
);

CREATE INDEX IF NOT EXISTS ix_dim_game_app_snapshot
    ON silver.dim_game (app_id, snapshot_at DESC);

-- SELECT DISTINCT ON (app_id)
--     app_id,
--     payload->>'name'                                   AS name,
--     payload->>'type'                                   AS app_type,
--     payload->>'short_description'                      AS short_description,
--     (payload->>'is_free')::boolean                     AS is_free,
--     to_date(payload->'release_date'->>'date', 'DD Mon, YYYY') AS release_date,
--     ARRAY(SELECT jsonb_array_elements_text(payload->'developers')) AS developers,
--     ARRAY(SELECT g->>'description' FROM jsonb_array_elements(payload->'genres') g)     AS genres,
--     ARRAY(SELECT c->>'description' FROM jsonb_array_elements(payload->'categories') c) AS categories,
--     (payload->'metacritic'->>'score')::int             AS metacritic_score,
--     (payload->'recommendations'->>'total')::int        AS steam_recommendations
-- FROM bronze.steam_appdetails_raw
-- WHERE success
-- ORDER BY app_id, fetched_at DESC;   -- latest fetch wins


--  JSONB is only faster for specific "search and filter" queries; native SQL arrays are significantly faster for reading, writing, and modifying data.