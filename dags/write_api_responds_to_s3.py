from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta

from api_based_pipeline.ingestion.api_extract import request_reviews_with_fallback
from api_based_pipeline.ingestion.api_error import SteamError

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    # at most retry expoential grow time limit
    "max_retry_delay": timedelta(minutes=30),
    "email": ["your-team@example.com"],
    "email_on_failure": True,
}


@dag(
    dag_id="store_steam_review_api_repsonds_to_s3",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "app_id": Param(730, type="integer"),
        "filter": Param("recent", type="string", enum=["recent", "all", "updated"]),
        "language": Param("all", type="string"),
        "review_type": Param(
            "all", type="string", enum=["all", "positive", "negative"]
        ),
        "purchase_type": Param(
            "all", type="string", enum=["all", "steam", "non_steam_purchase"]
        ),
        "num_per_page": Param(100, type="integer", minimum=1, maximum=100),
    },
    tags=["steam", "bronze", "ingestion"],
)
def steam_review_ingestion():

    @task
    def get_app_id(**context) -> int:
        # returns actual resolved Python list, not a template string
        return context["params"]["app_id"]

    @task
    def ingest(app_id: int, **context):
        params = context["params"]
        # 1. Initialize the specific database hook using your Connection ID
        db_hook = SnowflakeHook(snowflake_conn_id="snowflake_steam_review_pipeline")
        # 2. Get the pre-configured SQLAlchemy engine natively
        # Option B: get a pandas DataFrame
        sql = f"SELECT * FROM steam_review.PIPELINE_META.CURSOR_STATE_DAILY_INGESTION WHERE app_id = {app_id}"
        df = db_hook.get_pandas_df(sql)
        if df.empty:
            cursor = "*"
        else:
            cursor = df.iloc[0]["CURSOR"]
        print("my_cursor", cursor)
        try:
            # request_reviews_with_fallback(
            #     app_id=app_id,
            #     filter=params["filter"],
            #     language=params["language"],
            #     review_type=params["review_type"],
            #     purchase_type=params["purchase_type"],
            #     num_per_page=params["num_per_page"],
            #     cursor="*",
            # )
            print()
        except SteamError as e:
            # re-raise as AirflowException so it shows clearly as a task failure
            # rather than an ambiguous unhandled exception in the UI
            raise AirflowException(
                f"Steam ingestion failed for app_id={app_id}: {e}"
            ) from e
        finally:
            print("test_finished")

    ingest(app_id=get_app_id())


steam_review_ingestion()
