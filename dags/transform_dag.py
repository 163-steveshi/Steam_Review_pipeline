# transform_dag.py
from airflow.sdk import Asset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from datetime import timedelta
from sqlalchemy.exc import SQLAlchemyError

from api_based_pipeline.transform.review_transform import run_transform

bronze_reviews = Asset("postgres://postgres/steam_review/bronze/raw_review")

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
    dag_id="steam_review_transform",
    schedule=[bronze_reviews],
    catchup=False,
    max_active_runs=1,
    params={
        "pipeline_name": Param("bronze_to_silver", type="string"),
        "batch_size": Param(1000, type="integer", minimum=1),
    },
    tags=["steam", "silver", "transform"],
)
def bronze_to_silver_transform():
    @task
    def transform(**context):
        params = context["params"]
        db_hook = PostgresHook(postgres_conn_id="steam_review_postgres")
        # Get the pre-configured SQLAlchemy engine natively
        engine = db_hook.get_sqlalchemy_engine()
        try:
            run_transform(
                engine,
                pipeline_name=params["pipeline_name"],
                batch_size=params["batch_size"],
            )
        except SQLAlchemyError as e:
            raise AirflowException(
                f"Steam Review Transformation Task failed: {e}"
            ) from e
        finally:
            engine.dispose()

    transform()


bronze_to_silver_transform()
