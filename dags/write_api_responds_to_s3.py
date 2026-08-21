import json


from csv import Error
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta, timezone


from api_based_pipeline.common.model import SteamReviewAPIResponse
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

S3_CONN_ID = "aws_s3_conn"
SNOWFLAKE_CONN_ID = "snowflake_steam_review_pipeline"
S3_BUCKET_NAME = "steam-review-api-responses"


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
        # TODO: wrap into try catch for catch the exception
        # returns actual resolved Python list, not a template string
        return context["params"]["app_id"]

    @task
    def get_cursor(app_id: int) -> str:
        # TODO: wrap into try catch for catch the exception
        db_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        # 2. Get the pre-configured SQLAlchemy engine natively
        # Option B: get a pandas DataFrame
        sql = f"SELECT * FROM steam_review.PIPELINE_META.CURSOR_STATE_DAILY_INGESTION WHERE app_id = {app_id}"
        df = db_hook.get_pandas_df(sql)
        if df.empty:
            cursor = "*"
        else:
            # columnm name is strictly case sensitive
            cursor = df.iloc[0]["LAST_CURSOR"]
        return cursor

    @task
    def request_review(app_id: int, cursor: str, **context) -> SteamReviewAPIResponse:
        params = context["params"]
        # 1. Initialize the specific database hook using your Connection ID

        try:
            api_response = request_reviews_with_fallback(
                app_id=app_id,
                filter=params["filter"],
                language=params["language"],
                review_type=params["review_type"],
                purchase_type=params["purchase_type"],
                num_per_page=params["num_per_page"],
                cursor=cursor,
            )
            return api_response.dict()
        except SteamError as e:
            # re-raise as AirflowException so it shows clearly as a task failure
            # rather than an ambiguous unhandled exception in the UI

            raise AirflowException(
                f"Steam API requested failed for app_id={app_id}: {e}"
            ) from e

    @task
    def write_to_s3(
        app_id: int, api_response: SteamReviewAPIResponse, **context
    ) -> bool:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        bucket_name = S3_BUCKET_NAME
        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        ingestion_date = context["ts"][:10]
        key = f"steam_app_id={app_id}/ingestion_date={ingestion_date}/{run_id}.json"
        try:
            s3_hook.load_string(
                json.dumps(api_response), key=key, bucket_name=bucket_name, replace=True
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            raise RuntimeError(
                f"S3 FILE upload failed for key={key}: [{error_code}] {e}"
            ) from e

        except (EndpointConnectionError, NoCredentialsError) as e:
            raise RuntimeError(
                f"S3 connectivity/credentials issue for key={key}: {e}"
            ) from e
        return True

    @task
    def save_cursor(api_response: SteamReviewAPIResponse, app_id: int):
        # TODO: wrap into try catch for catch the exception
        db_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        merge_sql = """
            MERGE INTO steam_review.PIPELINE_META.CURSOR_STATE_DAILY_INGESTION AS target
            USING (SELECT %(app_id)s AS app_id) AS source
            ON target.app_id = source.app_id
            WHEN MATCHED THEN UPDATE SET
                last_cursor = %(new_cursor)s,
                last_run_at = %(last_run_at)s,
                last_run_status = %(last_run_status)s
            WHEN NOT MATCHED THEN INSERT (app_id, last_cursor, last_run_at, last_run_status)
                VALUES (%(app_id)s, %(new_cursor)s, %(last_run_at)s, %(last_run_status)s)
        """

        db_hook.run(
            merge_sql,
            parameters={
                "app_id": app_id,
                "new_cursor": api_response["cursor"],
                "last_run_at": datetime.now(timezone.utc),
                "last_run_status": "SUCCESS",
            },
        )

    @task
    def record_ingestion_run_result(
        app_id: int,
        started_at: datetime,
        successed: bool,
        start_cursor: str,
        api_response: SteamReviewAPIResponse,
    ):
        db_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        if successed:
            insert_query = """
                INSERT INTO steam_review.PIPELINE_META.API_INGESTION_RUN (app_id, started_at, ended_at, start_cursor, end_cursor, rows_loaded, status)
                VALUES (%(app_id)s, %(started_at)s, %(ended_at)s, %(start_cursor)s, %(end_cursor)s, %(rows_loaded)s, %(status)s)
            """
            db_hook.run(
                insert_query,
                parameters={
                    "app_id": app_id,
                    "started_at": started_at,  # Replace with your actual start variable
                    "ended_at": datetime.now(timezone.utc),
                    "start_cursor": start_cursor,  # Replace with your actual start cursor variable
                    "end_cursor": api_response[
                        "cursor"
                    ],  # Replace with your actual end cursor variable
                    "rows_loaded": api_response["query_summary"][
                        "num_reviews"
                    ],  # Replace with your actual rows loaded variable
                    "status": "SUCCESS",
                },
            )
        # TODO: write the fasle ingetsion run record

    try:
        start_time = datetime.now(timezone.utc)
        app_id = get_app_id()
        cursor = get_cursor(app_id)
        respond = request_review(app_id, cursor)
        sucesss = write_to_s3(app_id, respond)
        if sucesss:
            save_cursor(respond, app_id)
            record_ingestion_run_result(
                app_id=app_id,
                started_at=start_time,
                successed=True,
                start_cursor=cursor,
                api_response=respond,
            )

        # now save the cursor and save the ingestion run status
    except AirflowException as e:
        print()
    except Error as e:
        # save the false ingestion run staatus
        print()


steam_review_ingestion()
