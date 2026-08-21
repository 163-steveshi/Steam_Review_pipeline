Iceberg for domain data that's large, needs to be replayable/shareable across engines, or benefits from lakehouse openness (bronze review data). Native Snowflake tables for operational/control-plane metadata that's small, frequently mutated, and Snowflake-only (cursor state, DAG run logs, data-quality check results).


API
 │
 ▼
S3/raw/YYYY/MM/DD/
 │
 ▼
Iceberg Bronze
 │
 ▼
Snowflake inhouse Silver
 │
 ▼
Gold

FOR THE BRONZE DAG IN AIRFLOW:
extract_api

write_raw_to_s3

load_bronze_iceberg_from_s3

cursor state table use scd1: nobody interested into old cursor state
MERGE INTO PIPELINE_META.CURSOR_STATE t
USING (SELECT %(appid)s AS appid, %(cursor)s AS cursor_value, ...) s
ON t.appid = s.appid
WHEN MATCHED THEN UPDATE SET cursor_value = s.cursor_value, last_updated_at = CURRENT_TIMESTAMP(), status = s.status
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);

cursor_state table alwasy get the latest cursor per app_id
ingetsion_run append only historical record

CREATE TABLE steam_review.PIPELINE_META.CURSOR_STATE (
    appid           STRING,
    cursor_value    STRING,
    status          STRING,
    last_updated_at TIMESTAMP_NTZ,
    dag_run_id      STRING
);

CREATE TABLE PIPELINE_META.INGESTION_RUN (
    run_id          STRING,       -- or auto-increment/UUID
    appid           STRING,
    started_at      TIMESTAMP_NTZ,
    ended_at        TIMESTAMP_NTZ,
    cursor_before   STRING,
    cursor_after    STRING,
    rows_loaded     NUMBER,
    status          STRING        -- 'SUCCESS' | 'FAILED' | 'IN_PROGRESS'
);
STEP1: load the api respond to s3 bucket
DAGS:
DAG 1: steam_reviews_extract — API → S3 only.
get_cursor_task → fetch_pages_task[] → write_s3_task[] → update_cursor_task
need to define s3 bukcet directory format
configure the snowflake role that only for airflow, and set up snowflake connection via airflow ui
also assign the new role to the login snowflake account 

STEP2:
Utilize snowpipe + external stage: steam_reviews_load — S3 → Iceberg only.
detect_new_files_task(integereded with snowpipe) → load_bronze_task → log_run_task
do not use snowpipe stream: it is for low latencey micro batch streaming, costly

S3 → S3 event notification → Snowpipe → Snowflake stage → target table