from inspect import _void
import json
import time
import requests
import psycopg2

from psycopg2 import errors as pg_errors
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    # at most retry expoential grow time limit
    "max_retry_delay": timedelta(minutes=30),
    "email": ["your-team@example.com"],
    "email_on_failure": True,
}

REQUIRED_TABLES = [
    ("bronze", "steam_app_details_raw"),
    ("silver", "clean_latest_review"),
    ("silver", "dim_game"),
    ("meta", "etl_checkpoint"),
]


def _check_tables_exist(hook: PostgresHook) -> None:
    """Fail fast with a clear message if a schema or table is missing."""
    for schema, table in REQUIRED_TABLES:
        schema_exists = hook.get_first(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            parameters=(schema,),
        )
        if not schema_exists:
            raise AirflowFailException(f"Schema '{schema}' does not exist")

        table_exists = hook.get_first(
            "SELECT to_regclass(%s)", parameters=(f"{schema}.{table}",)
        )[0]
        if table_exists is None:
            raise AirflowFailException(f"Table '{schema}.{table}' does not exist")


def fetch_app_detail(
    session: requests.Session, app_id: int
) -> tuple[bool, dict | None]:
    """Returns (success, data). Retries on 429 with backoff."""

    url = "https://store.steampowered.com/api/appdetails"

    max_retries = 3

    for attempt in range(max_retries + 1):
        resp = session.get(url, params={"appids": app_id}, timeout=30)

        if resp.status_code == 429:
            wait = 60 * (attempt + 1)
            print(f"429 on {app_id}, sleeping {wait}s")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        body = resp.json() or {}
        entry = body.get(str(app_id), {})
        return bool(entry.get("success")), entry.get("data")

    raise RuntimeError(f"Still rate-limited after {max_retries} retries")


@dag(
    dag_id="steam_game_detail_process",
    schedule=timedelta(days=45),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["steam", "game_detail"],
)
def steam_app_detail_process():
    @task
    def get_need_update_app_id() -> list[int] | None:

        hook = PostgresHook(postgres_conn_id="steam_review_postgres")
        _check_tables_exist(hook)
        try:
            rows = hook.get_records("""
              WITH latest AS (
                  SELECT DISTINCT ON (app_id) app_id, fetched_at, success
                  FROM bronze.steam_app_details_raw
                  ORDER BY app_id, fetched_at DESC
              )
              SELECT r.app_id
              FROM (SELECT DISTINCT app_id FROM silver.clean_latest_review) r
              LEFT JOIN latest l ON l.app_id = r.app_id
              WHERE l.app_id IS NULL
                OR (l.success     AND l.fetched_at < now() - interval '30 days')
                OR (NOT l.success AND l.fetched_at < now() - interval '7 days')
              LIMIT 500;
          """)
            app_ids = [
                r[0] for r in rows
            ]  # rows are tuples like (123,), unpack to ints

            print(f"{len(app_ids)} app_ids need update")
            return app_ids
        except pg_errors.UndefinedTable as e:  # 42P01 (also covers missing schema)
            raise AirflowFailException(f"Missing table/schema: {e.pgerror}") from e
        except pg_errors.InvalidSchemaName as e:  # 3F000
            raise AirflowFailException(f"Missing schema: {e.pgerror}") from e
        except pg_errors.UndefinedColumn as e:  # 42703 – schema drift
            raise AirflowFailException(f"Column missing: {e.pgerror}") from e
        except psycopg2.OperationalError:
            # connection/timeout problems are often transient -> let Airflow retry
            raise

    @task
    def write_game_detail_to_db(app_ids: list[int]) -> None:
        hook = PostgresHook(postgres_conn_id="steam_review_postgres")
        conn = hook.get_conn()
        session = requests.Session()

        stats = {"ok": 0, "not_found": 0, "error": 0}
        insert_sql = """
            INSERT INTO bronze.steam_app_details_raw (app_id, fetched_at, success, payload)
            VALUES (%s, now(), %s, %s::jsonb)
        """
        sleep_between_calls = 1.6  # ~187 calls / 5 min, under Steam's ~200 limit
        try:
            for i, app_id in enumerate(app_ids, 1):
                try:
                    success, data = fetch_app_detail(session, app_id)
                except Exception as e:  # network error, bad JSON, etc.
                    print(f"[{i}/{len(app_ids)}] {app_id} error: {e}")
                    success, data = False, {"error": str(e)}
                    stats["error"] += 1
                else:
                    stats["ok" if success else "not_found"] += 1

                with conn.cursor() as cur:
                    cur.execute(insert_sql, (app_id, success, json.dumps(data)))
                conn.commit()  # commit per row -> progress survives a crash

                if i % 50 == 0:
                    print(f"progress {i}/{len(app_ids)} {stats}")
                time.sleep(sleep_between_calls)
        finally:
            conn.close()
            session.close()

        print(f"done: {stats}")

        # Fail the task if *everything* errored – likely blocked or Steam is down
        if app_ids and stats["error"] == len(app_ids):
            raise AirflowFailException("All requests failed")

    @task
    def transform_app_detail() -> dict | None:
        hook = PostgresHook(postgres_conn_id="steam_review_postgres")
        _check_tables_exist(hook)
        conn = hook.get_conn()
        pipeline_name = "app_details_bronze_to_silver"
        try:
            with conn.cursor() as cur:
                # 1. make sure the checkpoint row exists (first run -> 0 = process everything)
                cur.execute(
                    """
                      INSERT INTO meta.etl_checkpoint (pipeline, last_raw_id)
                      VALUES (%s, 0)
                      ON CONFLICT (pipeline) DO NOTHING;
                      """,
                    (pipeline_name,),
                )

                # 2. lock it so a concurrent run waits here
                cur.execute(
                    """
                      SELECT last_raw_id FROM meta.etl_checkpoint
                      WHERE pipeline = %s
                      FOR UPDATE;                      -- preventing other concurrent transactions from modifying or locking them until your current transaction finishes (via COMMIT or ROLLBACK)
                      """,
                    (pipeline_name,),
                )
                # cursor.fetchone return the query reuslt in on tuple
                (last_raw_id,) = cur.fetchone()

                # 3. fix the upper bound up front
                cur.execute("SELECT max(raw_id) FROM bronze.steam_app_details_raw")
                (max_raw_id,) = cur.fetchone()

                if max_raw_id is None or max_raw_id <= last_raw_id:
                    conn.commit()
                    print(f"nothing new (checkpoint={last_raw_id})")
                    return {"inserted": 0, "checkpoint": last_raw_id}

                # 4. transform + advance checkpoint
                cur.execute(
                    r"""
                      WITH new_raw AS (
                          SELECT r.raw_id, r.app_id, r.fetched_at, r.payload
                          FROM bronze.steam_app_details_raw r
                          WHERE r.raw_id > %(last_raw_id)s
                            AND r.raw_id <= %(max_raw_id)s
                            AND r.success
                      ),
                      inserted AS (
                          INSERT INTO silver.dim_game (
                              app_id, source_raw_id, snapshot_at,
                              name, app_type, short_description, is_free, release_date,
                              developers, genres, categories, metacritic_score,
                              steam_recommendation_count, loaded_at
                          )
                          SELECT
                              app_id, raw_id, fetched_at,
                              payload->>'name',
                              payload->>'type',
                              payload->>'short_description',
                              (payload->>'is_free')::boolean,
                              CASE
                                  WHEN payload->'release_date'->>'date' ~ '^\d{1,2} [A-Za-z]{3}, \d{4}$'
                                      THEN to_date(payload->'release_date'->>'date', 'DD Mon, YYYY')
                                  WHEN payload->'release_date'->>'date' ~ '^[A-Za-z]{3} \d{1,2}, \d{4}$'
                                      THEN to_date(payload->'release_date'->>'date', 'Mon DD, YYYY')
                              END,
                              COALESCE(payload->'developers', '[]'::jsonb),
                              COALESCE(jsonb_path_query_array(payload, '$.genres[*].description'),     '[]'::jsonb),
                              COALESCE(jsonb_path_query_array(payload, '$.categories[*].description'), '[]'::jsonb),
                              (payload->'metacritic'->>'score')::int,
                              (payload->'recommendations'->>'total')::int,
                              now()
                          FROM new_raw
                          ON CONFLICT (app_id, source_raw_id) DO NOTHING
                          RETURNING 1
                      )
                      UPDATE meta.etl_checkpoint
                      SET last_raw_id = %(max_raw_id)s,
                          updated_at  = now()
                      WHERE pipeline = %(pipeline)s
                      RETURNING last_raw_id, (SELECT count(*) FROM inserted);
                      """,
                    {
                        "pipeline": pipeline_name,
                        "last_raw_id": last_raw_id,
                        "max_raw_id": max_raw_id,
                    },
                )
                new_cp, inserted = cur.fetchone()

            conn.commit()  # everything above lands together, or not at all
            print(
                f"inserted {inserted} snapshots, checkpoint {last_raw_id} -> {new_cp}"
            )
            return {"inserted": inserted, "checkpoint": new_cp}
        except pg_errors.UndefinedTable as e:  # 42P01 (also covers missing schema)
            raise AirflowFailException(f"Missing table/schema: {e.pgerror}") from e
        except pg_errors.InvalidSchemaName as e:  # 3F000
            raise AirflowFailException(f"Missing schema: {e.pgerror}") from e
        except pg_errors.UndefinedColumn as e:  # 42703 – schema drift
            raise AirflowFailException(f"Column missing: {e.pgerror}") from e
        except psycopg2.OperationalError:
            # connection/timeout problems are often transient -> let Airflow retry
            raise

    app_ids = get_need_update_app_id()
    if app_ids:
        write_game_detail_to_db(app_ids) >> transform_app_detail()


steam_app_detail_process()
