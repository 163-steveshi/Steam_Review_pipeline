import requests

import json
from typing import List, Optional

# python coding habt: design the data classes for declare stored data type
# rather than make regular oop object
from dataclasses import dataclass

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


@dataclass
class Author:
    steamid: str
    num_games_owned: Optional[int]
    num_reviews: Optional[int]
    playtime_forever: Optional[int]
    playtime_last_two_weeks: Optional[int]
    playtime_at_review: Optional[int]
    last_played: Optional[int]


@dataclass
class Review:
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


@dataclass
class QuerySummary:
    num_reviews: int
    review_score: int
    review_score_desc: str
    total_positive: int
    total_negative: int
    total_reviews: int


@dataclass
class SteamReviewAPIResponse:
    success: int
    query_summary: QuerySummary
    reviews: List[Review]
    cursor: str


def requestReview(
    gameId: int = 3180070,
    filter: str = "all",
    language: str = "str",
    cursor: str = "*",
    purchase_type: str = "all",
    num_per_page: int = 100,
) -> SteamReviewAPIResponse:

    if not isinstance(gameId, int):
        raise ValueError("game id  must be an integer")
    if gameId < 10:
        raise ValueError("game id is too small to be a valid Steam app")
    if filter not in ALLOWED_FILTERS:
        raise ValueError("wrong filter value, only support ")

    if language not in ALLOWED_LANGUAGE:
        raise ValueError("Unsupport langauge value" + getSupportParamterDoc())

    url = f"https://store.steampowered.com/appreviews/{gameId}?json=1&filter={filter}&language={language}&purchase_type={purchase_type}&num_per_page={num_per_page}&cursor={cursor}"

    res = requests.get(url, timeout=10)

    print(res.status_code)
    print(res.json())
    result = res.json()
    return result


def getSupportParamterDoc() -> str:
    return "You can read the api docs to learn support language value: https://partner.steamgames.com/doc/store/localization/languages"


def main():
    requestReview(gameId=10)


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
