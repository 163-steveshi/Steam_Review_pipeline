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

1. set up i am role in aws for access a specific s3 bucket (not a i am user)
2. create the external volume in Snowflake, referencing that role's ARN.
Snowflake generates its own AWS IAM user + external ID for  Snowflake account (get this by running     
describe external volume  iceberg_external_volume;)
3. go back to aws and set up the connection:
Update the IAM role's trust policy — run DESC EXTERNAL VOLUME iceberg_external_volume to get Snowflake's generated STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID, then plug those into your IAM role's trust relationship in AWS.

{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::<SNOWFLAKE_ACCOUNT_ID>:user/<SNOWFLAKE_IAM_USER>"
			},
			"Action": "sts:AssumeRole",
			"Condition": {
				"StringEquals": {
					"sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
				}
			}
		}
	]
}
4. create the iceberg table + landing table:
landing table for data get into snowflake

5. create the file format definition for snow pipe
6. creaet the exrnal stage: point to the s3 bucket:
need to create a storage intergation with explictly aws role and location
use describe to describe the new stogra intergation and add the snowflake  iam_arn and external_id into the iam trust policy area
7. create the snowpipe:

8. turn on s3 lambda notification sending
get tye notification channel + configure s3 event notification
DESCRIBE PIPE steam_review.BRONZE.steam_pipe;
find notification_channel amd search for arn:aws:sqs:ca-central-1:123456789012:sf-snowpipe-...


AWS Console → S3 → your bucket → Properties → Event notifications → Create event notification

under Event types, select: Object creation

For destination:

SQS Queue

and select the Snowflake SQS queue ARN obtained from SHOW PIPES.

9. use a task that flatten + insert into the bronze table

the bronze table should be dynamic ice berg table: : Store the final or intermediate results of a declarative SELECT query (including complex joins and aggregations) and update themselves on a schedule + plus external location

10. implement the silver flag table
11. splite the silver review and silver player info
12. add silver player clean, dlq +  silver review clean, dlq 
13. add scd1 and scd2 over the clean data
define stream to track cdc of flag table: each scd task has its own stream:
A stream has one offset. Once Task A consumes the stream, the stream's offset advances. Task B may then see no rows (depending on timing and transaction behavior).

create task for merge into the sc1 table

14. migrate to dbt for using dbt data quality test
15. find a way to host over github actions