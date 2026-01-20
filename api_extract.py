import requests

import json
import hashlib
from typing import List, Optional, Any
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert


# # python coding habt: design the data classes for declare stored data type
# # rather than make regular oop object
# from dataclasses import dataclass
# industry used construct data model via class definition
from pydantic import BaseModel
from api_based_pipeline.model import RawReview

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
    weighted_vote_score: str
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
    gameId: int = 3180070,
    filter: str = "all",
    language: str = "all",
    cursor: str = "*",
    purchase_type: str = "all",
    num_per_page: int = 100,
) -> SteamReviewAPIResponse:

    if not isinstance(gameId, int):
        raise ValueError("game id must be an integer")
    if gameId < 10:
        raise ValueError("game id is too small to be a valid Steam app")
    if filter not in ALLOWED_FILTERS:
        raise ValueError("wrong filter value, only support: 'all', 'recent', 'updated'")

    if language not in ALLOWED_LANGUAGE:
        raise ValueError("Unsupport langauge value" + getLanguageSupportDoc())

    if purchase_type not in PURCHASE_TYPE:
        raise ValueError(
            "wrong filter value, only support: 'all', 'non_steam_purchase', 'steam'"
        )
    if not isinstance(num_per_page, int):
        raise ValueError("num_per_page must be an integer")
    if num_per_page < 20 or num_per_page > 100:
        raise ValueError("num_per_page must be in the range of 20 to 100")

    url = f"https://store.steampowered.com/appreviews/{gameId}"
    params = {
        "json": 1,
        "filter": filter,
        "language": language,
        "purchase_type": purchase_type,
        "num_per_page": num_per_page,
        "cursor": cursor,
    }
    res = requests.get(url, params=params, timeout=10)

    print(res.status_code)
    result = res.json()
    # print(result)

    if res.status_code != 200:
        raise RuntimeError("Steam API request failed, please try again with cool down")
    if result.get("success") != 1:
        raise ValueError(result.get("error"))
    result["app_id"] = gameId
    return SteamReviewAPIResponse.model_validate(result)


def getLanguageSupportDoc() -> str:
    return "You can read the api docs to learn support language value: https://partner.steamgames.com/doc/store/localization/languages"


def loadAPIRespondToBronzeLayer(reviewData: SteamReviewAPIResponse) -> bool:

    if reviewData.query_summary.num_reviews == 0:
        raise ValueError(
            "Wrong steam app id: please search the game correct id via: https://steamdb.info/"
        )
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
            }
        )

    insertRowNum = ingestRawReview(reviews)
    print("total " + str(insertRowNum) + "be inserted into the db")
    if insertRowNum == 0:
        print("No more new data be fetched, stop the data pipeline")

    # old orm way need to optimized
    # for r in reviewData.reviews:
    #     review = RawReview(
    #         app_id=reviewData.app_id,
    #         review_id=r.recommendationid,
    #         hash_raw_json=hashJson(r),
    #         raw_json=r,
    #     )
    #     reviews.append(review)
    # # call log the ingestion
    # # ingest the data

    return True


def ingestRawReview(reviews: List[RawReview]):

    DATABASE_URL = "postgresql+psycopg://root:root@localhost:55432/steam_review"

    engine = create_engine(DATABASE_URL)

    with engine.begin() as connection:
        # SQL ALchemy neeeds this object to target table in db and its format
        statementObj = pg_insert(RawReview).values(reviews)
        # use the pg_insert for use the postgre on conflict
        # avoid raise error filter out the error
        # need to reassgin for facing error
        statementObj = statementObj.on_conflict_do_nothing(
            index_elements=["app_id", "review_id", "hash_raw_json"]
        )

        statementObj = statementObj.returning(
            RawReview.review_id
        )  # return to count how many value we insert into db

        result = connection.execute(statementObj)
        # number of rows actual inserted
        insertedCount = len(result.all())
    # with engine.begin() as connection:
    #     result = connection.execute(text("SELECT * FROM bronze.raw_review;"))
    #     rows = result.fetchall()
    #     print(rows)

    # close any idle connection
    engine.dispose()
    return insertedCount


def hashJson(jsonVal: dict[str, Any]) -> str:

    # order the json key in ascending order and remove the extra space
    # produce the same deterministic json if the key and value are the same
    text = json.dumps(jsonVal, sort_keys=True, separators=(",", ":"))

    # hash the review json into SHA-256 hex
    # so we can collect review and possible it's updated version
    # so we can acheive deduplication
    hashVal = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return hashVal


def recordIngestionFailure(error: str, runId: int = -1):
    # when id is -1 mean their is first time crash before try to write partial data to db
    # TODO: replace the below logic to real writing to the db logic
    print("load the error to the db")


def main():
    try:

        reviewData = requestReview(gameId=730)
        loadAPIRespondToBronzeLayer(reviewData)
    except ValueError as valError:

        recordIngestionFailure(str(valError))


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
