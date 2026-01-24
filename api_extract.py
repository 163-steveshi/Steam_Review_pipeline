import requests
import uuid
import json
import time
import hashlib
import argparse
from typing import List, Optional, Any
from datetime import datetime, timedelta
from sqlalchemy import create_engine, func, select, desc, Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

# # python coding habit: design the data classes for declare stored data type
# # rather than make regular oop object
# from dataclasses import dataclass
# industry used construct data model via class definition like pydantic
from pydantic import BaseModel
from api_based_pipeline.model import RawReview, IngestionRun
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


class Author(BaseModel):
    steamid: str
    num_games_owned: Optional[int]
    num_reviews: Optional[int]
    playtime_forever: Optional[int]
    playtime_last_two_weeks: Optional[int]
    playtime_at_review: Optional[int]
    last_played: Optional[int]


class Review(BaseModel):
    recommendationid: str
    author: Author
    language: str
    review: str
    timestamp_created: int
    timestamp_updated: int
    voted_up: bool
    votes_up: int
    votes_funny: int
    weighted_vote_score: float
    comment_count: int
    steam_purchase: bool
    received_for_free: bool
    written_during_early_access: bool
    primarily_steam_deck: bool


class QuerySummary(BaseModel):
    num_reviews: int
    review_score: int
    review_score_desc: str
    total_positive: int
    total_negative: int
    total_reviews: int


class SteamReviewAPIResponse(BaseModel):
    success: int
    query_summary: QuerySummary
    reviews: List[Review]
    cursor: str
    app_id: int


def requestReview(
    appId: int = 3180070,
    filter: str = "recent",
    review_type: str = "all",
    language: str = "all",
    cursor: str = "*",
    purchaseType: str = "all",
    numPerPage: int = 100,
) -> SteamReviewAPIResponse:

    if not isinstance(appId, int):
        raise SteamValidationError("app id must be an integer")
    if appId < 10:
        raise SteamValidationError("app id is too small to be a valid Steam app")
    if filter not in ALLOWED_FILTERS:
        raise SteamValidationError(
            "wrong filter value, only support: 'all', 'recent', 'updated'"
        )

    if language not in ALLOWED_LANGUAGE:
        raise SteamValidationError("Unsupport langauge value" + getLanguageSupportDoc())

    if purchaseType not in PURCHASE_TYPE:
        raise SteamValidationError(
            "wrong filter value, only support: 'all', 'non_steam_purchase', 'steam'"
        )
    if not isinstance(numPerPage, int):
        raise SteamValidationError("num_per_page must be an integer")
    if numPerPage < 20 or numPerPage > 100:
        raise SteamValidationError("num_per_page must be in the range of 20 to 100")

    url = f"https://store.steampowered.com/appreviews/{appId}"
    params = {
        "json": 1,
        "filter": filter,
        "language": language,
        "purchase_type": purchaseType,
        "num_per_page": numPerPage,
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
                statusCode=res.status_code,
            )

        if res.status_code == 429:
            raise SteamHTTPError(
                "The Steam Server is down, please retry later",
                statusCode=res.status_code,
            )
        raise SteamHTTPError(
            "Other HTTP ERROR Please refers to the html code for reference",
            statusCode=res.status_code,
        )
    # throw error about steam api
    if result.get("success") != 1:
        raise SteamAPIError(result.get("error"))
    result["app_id"] = appId
    print(result.get("query_summary"))
    return SteamReviewAPIResponse.model_validate(result)


def getLanguageSupportDoc() -> str:
    return "You can read the api docs to learn support language value: https://partner.steamgames.com/doc/store/localization/languages"


def loadAPIRespondToBronzeLayer(
    session: Session, reviewData: SteamReviewAPIResponse, runId: uuid.UUID
) -> int:

    reviews = []
    for r in reviewData.reviews:
        # converts a Pydantic model into a plain Python json
        # exlcude none for handling fale format like empty value sometimes is null or just empty string
        # since the data is structured from api, the chance would be minimum
        # good pratical use when the data is raw or semistructure
        reviewJson = r.model_dump(mode="json", exclude_none=True)
        reviews.append(
            {
                "app_id": reviewData.app_id,
                "review_id": r.recommendationid,
                "hash_raw_json": hashJson(reviewJson),
                "raw_json": reviewJson,
                "run_id": runId,
            }
        )

    insertRowNum = ingestRawReview(session, reviews)
    print("total " + str(insertRowNum) + "be inserted into the db")
    if insertRowNum == 0:
        print("No more new data be fetched, need to stop the data pipeline")
    return insertRowNum


def ingestRawReview(session: Session, reviews: List[RawReview]):

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


def hashJson(jsonVal: dict[str, Any]) -> str:

    # order the json key in ascending order and remove the extra space
    # produce the same deterministic json if the key and value are the same
    text = json.dumps(jsonVal, sort_keys=True, separators=(",", ":"))

    # hash the review json into SHA-256 hex
    # so we can collect review and possible it's updated version
    # so we can acheive deduplication
    hashVal = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return hashVal


def chooseStartCursor(session: Session, appID: int) -> str:

    # get the latest ingestion run token
    lastRun = (
        session.execute(
            select(IngestionRun)
            .where(IngestionRun.app_id == appID, IngestionRun.status == "success")
            .order_by(desc(IngestionRun.finished_at))
            .limit(1)
        )
        .scalars()
        .one_or_none()
    )

    if lastRun is None:
        print("last run is empty")
        return "*"
    now = datetime.now()
    # resume from the last cursor
    # if the task finished within 1 hour
    if (
        lastRun.last_success_cursor is not None
        and lastRun.finished_at is not None
        and now <= lastRun.finished_at + timedelta(hours=1)
    ):
        print("we have a valid cursor")
        return str(lastRun.last_success_cursor)

    # if no successful cursor
    print("we have no success cursor")
    return "*"


def requestReviewsWithFallback(
    appId: int,
    filter: str = "recent",
    review_type: str = "all",
    language: str = "all",
    cursor: str = "*",
    purchaseType: str = "all",
    numPerPage: int = 100,
) -> SteamReviewAPIResponse:

    MAX_ATTEMPT = 3

    usedCursor = cursor
    lastError: SteamError | None = None
    for attempt in range(0, MAX_ATTEMPT):
        try:
            return requestReview(
                appId,
                filter,
                language,
                review_type,
                usedCursor,
                purchaseType,
                numPerPage,
            )
        # invalid api usage input, no retry
        except SteamValidationError as e:
            lastError = e
            raise e
        except SteamHTTPError as e:

            lastError = e
            # server error wait 10 before retry
            if e.statusCode is not None and e.statusCode >= 500:
                time.sleep(10)
                continue
            # request api to often retry after 60 seconds
            if e.statusCode is not None and e.statusCode == 429:
                time.sleep(60)
                continue
            else:
                time.sleep(20)
                continue
        except SteamAPIError as e:
            # for invalid cursor retry again
            if "Invalid cursor" in e.message and attempt < MAX_ATTEMPT:
                usedCursor = "*"
                continue
            # else raise the error and existed
            raise e
    # should never reach here, but just in case
    assert lastError is not None
    raise lastError


def startIngestionRun(session: Session, appId: int, startCursor: str) -> uuid.UUID:
    # generate a randome UNIQUE ID FROM the
    runId = uuid.uuid4()
    run = IngestionRun(
        run_id=runId,
        app_id=appId,
        status="running",
        start_cursor=startCursor,
    )
    # mark this row of ingestionRun Meta data as pending insert
    session.add(run)
    # emits the actual INSERT INTO bronze.ingestion_run (...), but don't commit it
    # allow roll back, update and so on
    session.flush()
    return runId


def markIngestionSucesss(
    session: Session, runId: uuid.UUID, endCursor: str, rowsFetched: int
):

    session.query(IngestionRun).filter(IngestionRun.run_id == runId).update(
        {
            IngestionRun.status: "success",
            IngestionRun.end_cursor: endCursor,
            IngestionRun.rows_fetched: IngestionRun.rows_fetched + rowsFetched,
            IngestionRun.finished_at: func.now(),
            IngestionRun.last_success_cursor: endCursor,
        }
    )


def markIngestionFailure(
    session: Session, errorType: str, errorMessage: str, runId: uuid.UUID
):

    # TODO: update the error later
    session.query(IngestionRun).filter(IngestionRun.run_id == runId).update(
        {
            IngestionRun.status: "failed",
            IngestionRun.error_type: errorType,
            IngestionRun.error_message: errorMessage,
            IngestionRun.finished_at: func.now(),
        }
    )


def updateRunningIngestion(
    session: Session,
    runId: uuid.UUID,
    endCursor: str,
    status: str,
    rowsFetched: int = 0,
):
    # for non review of the steam app, no updated on last success cursor
    if status == "skipped_no_reviews":

        session.query(IngestionRun).filter(IngestionRun.run_id == runId).update(
            {
                IngestionRun.status: status,
                IngestionRun.end_cursor: endCursor,
                IngestionRun.rows_fetched: IngestionRun.rows_fetched + rowsFetched,
                IngestionRun.finished_at: func.now(),
            }
        )
    else:
        session.query(IngestionRun).filter(IngestionRun.run_id == runId).update(
            {
                IngestionRun.status: status,
                IngestionRun.end_cursor: endCursor,
                IngestionRun.rows_fetched: IngestionRun.rows_fetched + rowsFetched,
                IngestionRun.finished_at: func.now(),
                IngestionRun.last_success_cursor: endCursor,
            }
        )


# run Ingestion ()
def runIngestion(
    engine: Engine,
    appId: int,
    filter: str = "recent",
    language: str = "all",
    review_type: str = "all",
    purchaseType: str = "all",
    numPerPage: int = 100,
    maxPages: int = 50,
):
    # session object defintiion: for later create safe
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    with SessionLocal() as session:
        startCursor = chooseStartCursor(session, appId)
        runId = startIngestionRun(session, appId, startCursor)
        cursor = startCursor
        try:
            for _ in range(maxPages):
                reviewData = requestReviewsWithFallback(
                    appId,
                    filter,
                    language,
                    review_type,
                    cursor,
                    purchaseType,
                    numPerPage,
                )
                # stop condition: the app has no review
                if reviewData.query_summary.total_reviews == 0:

                    updateRunningIngestion(
                        session,
                        runId,
                        endCursor=cursor,
                        status="running",
                        rowsFetched=0,
                    )
                    session.commit()
                    break
                # when ingest all review for the current app
                if len(reviewData.reviews) == 0:
                    break
                insertedRowNum = loadAPIRespondToBronzeLayer(session, reviewData, runId)

                # update the running cursor
                cursor = reviewData.cursor
                updateRunningIngestion(
                    session,
                    runId,
                    endCursor=cursor,
                    status="running",
                    rowsFetched=insertedRowNum,
                )
                session.commit()
            updateRunningIngestion(
                session,
                runId,
                endCursor=cursor,
                status="success",
            )
            session.commit()
        except SteamError as e:

            markIngestionFailure(
                session,
                e.errorType,
                e.message,
                runId,
            )
            session.commit()
            raise e


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app_id", type=int, required=True)
    parser.add_argument("--filter", default="recent")
    parser.add_argument("--language", default="all")
    parser.add_argument("--review_type", default="all")
    parser.add_argument("--purchase_type", default="all")
    parser.add_argument("--num_per_page", type=int, default=100)
    args = parser.parse_args()
    DATABASE_URL = "postgresql+psycopg://root:root@localhost:55432/steam_review"
    engine = create_engine(DATABASE_URL)
    runIngestion(
        engine,
        args.app_id,
        args.filter,
        args.language,
        args.review_type,
        args.purchase_type,
        args.num_per_page,
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
