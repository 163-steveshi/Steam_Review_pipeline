import requests
import uuid
import json
import time
import hashlib
import argparse
from typing import List, Optional, Any
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, func, select, desc, Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

# # python coding habit: design the data classes for declare stored data type
# # rather than make regular oop object
# from dataclasses import dataclass

from api_based_pipeline.common.model import (
    RawReview,
    IngestionRun,
    SteamReviewAPIResponse,
)
from api_error import *


PURCHASE_TYPE: set[str] = {"all", "non_steam_purchase", "steam"}
ALLOWED_FILTERS: set[str] = {"all", "recent", "updated"}
ALLOWED_LANGUAGE: set[str] = {
    "all",
    "ar",
    "bg",
    "zh-CN",
    "zh-TW",
    "cs",
    "da",
    "nl",
    "en",
    "fi",
    "fr",
    "de",
    "el",
    "hu",
    "id",
    "it",
    "ja",
    "ko",
    "no",
    "pl",
    "pt",
    "pt - BR",
    "ro",
    "ru",
    "es",
    "es-419",
    "sv",
    "th",
    "tr",
    "uk",
    "vi",
}


def request_review(
    app_id: int,
    filter: str,
    review_type: str,
    language: str,
    cursor: str,
    purchase_type: str,
    num_per_page: int,
) -> SteamReviewAPIResponse:

    if not isinstance(app_id, int):
        raise SteamValidationError("app id must be an integer")
    if app_id < 10:
        raise SteamValidationError("app id is too small to be a valid Steam app")
    if filter not in ALLOWED_FILTERS:
        raise SteamValidationError(
            "wrong filter value, only support: 'all', 'recent', 'updated'"
        )

    if language not in ALLOWED_LANGUAGE:
        raise SteamValidationError(
            "Unsupport langauge value" + get_language_supportDoc()
        )

    if purchase_type not in PURCHASE_TYPE:
        raise SteamValidationError(
            "wrong filter value, only support: 'all', 'non_steam_purchase', 'steam'"
        )
    if not isinstance(num_per_page, int):
        raise SteamValidationError("num_per_page must be an integer")
    if num_per_page < 20 or num_per_page > 100:
        raise SteamValidationError("num_per_page must be in the range of 20 to 100")

    url = f"https://store.steampowered.com/appreviews/{app_id}"
    params = {
        "json": 1,
        "filter": filter,
        "language": language,
        "purchase_type": purchase_type,
        "num_per_page": num_per_page,
        "review_type": review_type,
        "cursor": cursor,
    }
    res = requests.get(url, params=params, timeout=10)

    print(res.status_code)
    result = res.json()

    if res.status_code != 200:
        if res.status_code >= 500:
            raise SteamHTTPError(
                "The Steam Server is down, please retry later",
                status_code=res.status_code,
            )

        if res.status_code == 429:
            raise SteamHTTPError(
                "The Steam Server is down, please retry later",
                status_code=res.status_code,
            )
        raise SteamHTTPError(
            "Other HTTP ERROR Please refers to the html code for reference",
            status_code=res.status_code,
        )
    # throw error about steam api
    if result.get("success") != 1:
        raise SteamAPIError(result.get("error"))
    result["app_id"] = app_id
    print(result.get("query_summary"))
    return SteamReviewAPIResponse.model_validate(result)


def get_language_supportDoc() -> str:
    return "You can read the api docs to learn support language value: https://partner.steamgames.com/doc/store/localization/languages"


def load_api_respond_to_bronze_layer(
    session: Session, review_data: SteamReviewAPIResponse, run_id: uuid.UUID
) -> int:

    reviews = []
    for r in review_data.reviews:
        # converts a Pydantic model into a plain Python json
        # exlcude none for handling fale format like empty value sometimes is null or just empty string
        # since the data is structured from api, the chance would be minimum
        # good pratical use when the data is raw or semistructure
        review_json = r.model_dump(mode="json", exclude_none=True)
        reviews.append(
            {
                "app_id": review_data.app_id,
                "review_id": r.recommendationid,
                "hash_raw_json": hash_json(review_json),
                "raw_json": review_json,
                "run_id": run_id,
            }
        )

    insert_row_num = ingest_raw_review(session, reviews)
    print("total " + str(insert_row_num) + " be inserted into the db")
    return insert_row_num


def ingest_raw_review(session: Session, reviews: List[RawReview]):

    stmt_obj = (
        # use the pg_insert for use the postgre on conflict
        pg_insert(RawReview).values(reviews)
        # avoid raise error when meet duplicated value
        .on_conflict_do_nothing(index_elements=["app_id", "review_id", "hash_raw_json"])
        # return to count how many value we insert into db
        .returning(RawReview.review_id)
    )

    result = session.execute(stmt_obj)
    inserted_count = len(result.fetchall())

    return inserted_count


def hash_json(jon_val: dict[str, Any]) -> str:

    # order the json key in ascending order and remove the extra space
    # produce the same deterministic json if the key and value are the same
    text = json.dumps(jon_val, sort_keys=True, separators=(",", ":"))

    # hash the review json into SHA-256 hex
    # so we can collect review and possible it's updated version
    # so we can acheive deduplication
    hash_val = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return hash_val


def choose_start_cursor(session: Session, app_id: int) -> str:

    # get the latest ingestion run success cursor
    last_run = (
        session.execute(
            select(IngestionRun)
            .where(
                IngestionRun.app_id == app_id,
                IngestionRun.last_success_cursor.isnot(None),
            )
            .order_by(desc(IngestionRun.finished_at))
            .limit(1)
        )
        .scalars()
        .one_or_none()
    )

    if last_run is None:
        print("last run is empty")
        return "*"
    now = datetime.now(timezone.utc)
    # resume from the last cursor
    # if the task finished within 1 hour
    if (
        last_run.last_success_cursor is not None
        and last_run.finished_at is not None
        and now <= last_run.finished_at + timedelta(hours=1)
    ):
        print("we have a valid cursor")
        return str(last_run.last_success_cursor)

    # if no successful cursor
    print("we have no success cursor")
    return "*"


def request_reviews_with_fallback(
    appId: int,
    filter: str,
    review_type: str,
    language: str,
    cursor: str,
    purchase_type: str,
    num_per_page: int,
) -> SteamReviewAPIResponse:

    MAX_ATTEMPT = 3

    used_cursor = cursor
    last_error: SteamError | None = None
    for attempt in range(0, MAX_ATTEMPT):
        try:
            return request_review(
                appId,
                filter,
                language,
                review_type,
                used_cursor,
                purchase_type,
                num_per_page,
            )
        # invalid api usage input, no retry
        except SteamValidationError as e:
            last_error = e
            raise e
        except SteamHTTPError as e:

            last_error = e
            # server error wait 10 before retry
            if e.status_code is not None and e.status_code >= 500:
                time.sleep(10)
                continue
            # request api to often retry after 60 seconds
            if e.status_code is not None and e.status_code == 429:
                time.sleep(60)
                continue
            else:
                time.sleep(20)
                continue
        except SteamAPIError as e:
            # for invalid cursor retry again
            if "Invalid cursor" in e.message and attempt < MAX_ATTEMPT:
                used_cursor = "*"
                continue
            # else raise the error and existed
            raise e
    # should never reach here, but just in case
    assert last_error is not None
    raise last_error


def start_ingestion_run(session: Session, app_id: int, start_cursor: str) -> uuid.UUID:
    # generate a randome UNIQUE ID
    run_id = uuid.uuid4()
    run = IngestionRun(
        run_id=run_id,
        rows_fetched=0,
        app_id=app_id,
        status="running",
        start_cursor=start_cursor,
    )
    # mark this row of ingestionRun Meta data as pending insert
    session.add(run)
    # emits the actual INSERT INTO bronze.ingestion_run (...), but don't commit it
    # allow roll back, update and so on
    session.flush()
    return run_id


def mark_ingestion_failure(
    session: Session, error_type: str, error_message: str, run_id: uuid.UUID
):

    session.query(IngestionRun).filter(IngestionRun.run_id == run_id).update(
        {
            IngestionRun.status: "failed",
            IngestionRun.error_type: error_type,
            IngestionRun.error_message: error_message,
            IngestionRun.finished_at: func.now(),
        }
    )


def update_running_ingestion(
    session: Session,
    run_id: uuid.UUID,
    end_cursor: str,
    status: str,
    rows_fetched: int = 0,
):
    # for non review of the steam app, no updated on last success cursor
    if status == "skipped_no_reviews":

        session.query(IngestionRun).filter(IngestionRun.run_id == run_id).update(
            {
                IngestionRun.status: status,
                IngestionRun.end_cursor: end_cursor,
                IngestionRun.rows_fetched: IngestionRun.rows_fetched + rows_fetched,
                IngestionRun.finished_at: func.now(),
            }
        )
    else:
        session.query(IngestionRun).filter(IngestionRun.run_id == run_id).update(
            {
                IngestionRun.status: status,
                IngestionRun.end_cursor: end_cursor,
                IngestionRun.rows_fetched: IngestionRun.rows_fetched + rows_fetched,
                IngestionRun.finished_at: func.now(),
                IngestionRun.last_success_cursor: end_cursor,
            }
        )


# run Ingestion ()
# TODO: need update with session begin
def run_ingestion(
    engine: Engine,
    app_id: int,
    filter: str,
    language: str,
    review_type: str,
    purchase_type: str,
    num_per_page: int,
    max_pages: int,
):
    # session object defintiion: for later create safe
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    with SessionLocal() as session:
        start_cursor = choose_start_cursor(session, app_id)
        run_id = start_ingestion_run(session, app_id, start_cursor)
        cursor = start_cursor
        # Flag determine if the app reivew ingestion is skipped or not
        skipped_ingestion = False
        retry = True  # in case steam api return 0 review in one respond give another shot before mark it finished
        try:
            for _ in range(max_pages):
                reviewData = request_reviews_with_fallback(
                    app_id,
                    filter,
                    language,
                    review_type,
                    cursor,
                    purchase_type,
                    num_per_page,
                )
                # stop condition: the app has no review
                if (
                    reviewData.query_summary.total_reviews == 0
                    and reviewData.query_summary.num_reviews == 0
                ):

                    update_running_ingestion(
                        session,
                        run_id,
                        cursor,
                        "skipped_no_reviews",
                        rows_fetched=0,
                    )
                    session.commit()
                    skipped_ingestion = True
                    break
                # when ingest all review for the current app
                if reviewData.query_summary.num_reviews == 0:
                    if retry:
                        retry = False
                        continue
                    break
                insertedRowNum = load_api_respond_to_bronze_layer(
                    session, reviewData, run_id
                )

                # update the running cursor
                cursor = reviewData.cursor
                update_running_ingestion(
                    session,
                    run_id,
                    cursor,
                    "running",
                    rows_fetched=insertedRowNum,
                )
                session.commit()
                # sleep to avoid steam api frequenty request limit
                time.sleep(3)

            if not skipped_ingestion:
                update_running_ingestion(
                    session,
                    run_id,
                    cursor,
                    "success",
                )
                session.commit()
        except SteamError as e:

            mark_ingestion_failure(
                session,
                e.error_type,
                e.message,
                run_id,
            )
            session.commit()
            raise e
        except Exception as e:
            mark_ingestion_failure(session, type(e).__name__, str(e), run_id)
            session.commit()
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app_id", type=int, required=True)
    parser.add_argument("--filter", default="recent")
    parser.add_argument("--language", default="all")
    parser.add_argument("--review_type", default="all")
    parser.add_argument("--purchase_type", default="all")
    parser.add_argument("--num_per_page", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=50)
    args = parser.parse_args()
    DATABASE_URL = "postgresql+psycopg://root:root@localhost:55432/steam_review"
    engine = create_engine(DATABASE_URL)
    run_ingestion(
        engine,
        args.app_id,
        args.filter,
        args.language,
        args.review_type,
        args.purchase_type,
        args.num_per_page,
        args.max_pages,
    )


if __name__ == "__main__":
    main()

# example of bad
bad_id = {
    "success": 1,
    "query_summary": {
        "num_reviews": 0,
        "review_score": 0,
        "review_score_desc": "No user reviews",
        "total_positive": 0,
        "total_negative": 0,
        "total_reviews": 0,
    },
    "reviews": [],
    "cursor": "*",
}
