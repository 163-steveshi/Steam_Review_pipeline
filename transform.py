from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine, func, select, desc, Engine, and_, or_
from api_based_pipeline.model import (
    TransformCheckpoint,
    RawReview,
    Review,
    CleanLatestReview,
    CleanLatestReviewPlayerInfo,
)
from typing import Sequence
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
import logging

log = logging.getLogger(__name__)


def choose_resumed_point(session: Session, pipeline_name: str) -> int:
    last_transformed_raw_review_id = (
        session.execute(
            select(TransformCheckpoint.last_raw_id_processed).where(
                TransformCheckpoint.pipeline_name == pipeline_name,
            )
        )
        .scalars()  # return only the actual column value
        .one_or_none()
    )

    if last_transformed_raw_review_id:
        resume_id = (
            session.execute(
                select(RawReview.raw_id)
                .where(
                    RawReview.raw_id > last_transformed_raw_review_id,
                )
                .limit(1)
            )
            .scalars()
            .one_or_none()
        )
        if resume_id:
            return resume_id
        # here mean all raw reviews are currently cleaned
        return -1
    # no clean record yet start with 1st one
    return 0


def choose_transform_task_end_point(session: Session) -> int:

    end_point = (
        session.execute(select(func.max(RawReview.raw_id)).limit(1))
        .scalars()
        .one_or_none()
    )
    if end_point:
        return end_point
    return -1


def read_raw_reivew(
    session: Session, last_id: int, stop_id: int, batch_size: int = 1000
) -> tuple[Sequence[RawReview], int]:
    raw_reviews: Sequence[RawReview] = (
        session.execute(
            select(RawReview)
            .where(and_(RawReview.raw_id > last_id, RawReview.raw_id <= stop_id))
            .order_by(RawReview.raw_id)
            .limit(batch_size)
        )
        .scalars()
        .all()
    )
    if not raw_reviews:
        return [], last_id
    new_last_id = raw_reviews[-1].raw_id
    return raw_reviews, new_last_id


def parse_raw_review(raw_review: Sequence[RawReview]) -> tuple[list[dict], list[dict]]:
    cleaned_reviews: list[dict] = []
    cleaned_player_infos: list[dict] = []
    for review in raw_review:
        # after validate review write it into a map for inserting in a list
        cleaned_review_info = Review.model_validate(review.raw_json)
        cleaned_reviews.append(
            {
                "app_id": review.app_id,
                "review_id": cleaned_review_info.recommendationid,
                "language": cleaned_review_info.language,
                "review_text": cleaned_review_info.review or "",
                "timestamp_created": datetime.fromtimestamp(
                    cleaned_review_info.timestamp_created, tz=timezone.utc
                ),
                "timestamp_updated": datetime.fromtimestamp(
                    cleaned_review_info.timestamp_updated, tz=timezone.utc
                ),
                "voted_positive": cleaned_review_info.voted_up,
                "votes_helpful": cleaned_review_info.votes_up,
                "votes_funny": cleaned_review_info.votes_funny,
                "weighted_vote_score": cleaned_review_info.weighted_vote_score,
                "review_comment_count": cleaned_review_info.comment_count,
                "steam_purchase": cleaned_review_info.steam_purchase,
                "received_for_free": cleaned_review_info.received_for_free,
                "written_during_early_access": cleaned_review_info.written_during_early_access,
                "primarily_play_on_steam_deck": cleaned_review_info.primarily_steam_deck,
                "source_raw_id": review.raw_id,
            }
        )
        cleaned_player_infos.append(
            {
                "app_id": review.app_id,
                "review_id": cleaned_review_info.recommendationid,
                "steam_user_id": cleaned_review_info.author.steamid,
                "num_games_owned": cleaned_review_info.author.num_games_owned,
                "total_playtime_hr": cleaned_review_info.author.playtime_forever,
                "playtime_last_two_weeks_hr": cleaned_review_info.author.playtime_last_two_weeks,
                "playtime_at_review_hr": cleaned_review_info.author.playtime_at_review,
                "last_played": cleaned_review_info.author.last_played,
            }
        )
    return cleaned_reviews, cleaned_player_infos


def insert_cleaned_review(
    session: Session, cleaned_reviews, cleaned_player_infos
) -> tuple[int, int]:
    if not cleaned_reviews or not cleaned_player_infos:
        return 0, 0
    try:
        # use the pg_insert for use the postgre on conflict
        stmt_review = pg_insert(CleanLatestReview).values(cleaned_reviews)
        stmt_review = stmt_review.on_conflict_do_update(
            index_elements=["app_id", "review_id"],
            # update the field
            set_={
                # stm.exclued mean rejeect incoming rows
                "language": stmt_review.excluded.language,
                "review_text": stmt_review.excluded.review_text,
                "timestamp_created": stmt_review.excluded.timestamp_created,
                "timestamp_updated": stmt_review.excluded.timestamp_updated,
                "voted_positive": stmt_review.excluded.voted_positive,
                "votes_helpful": stmt_review.excluded.votes_helpful,
                "votes_funny": stmt_review.excluded.votes_funny,
                "weighted_vote_score": stmt_review.excluded.weighted_vote_score,
                "review_comment_count": stmt_review.excluded.review_comment_count,
                "steam_purchase": stmt_review.excluded.steam_purchase,
                "received_for_free": stmt_review.excluded.received_for_free,
                "written_during_early_access": stmt_review.excluded.written_during_early_access,
                "primarily_play_on_steam_deck": stmt_review.excluded.primarily_play_on_steam_deck,
                "source_raw_id": stmt_review.excluded.source_raw_id,
            },
            where=stmt_review.excluded.timestamp_updated
            > CleanLatestReview.timestamp_updated,
        ).returning(CleanLatestReview.review_id)

        stmt_player_info = pg_insert(CleanLatestReviewPlayerInfo).values(
            cleaned_player_infos
        )
        stmt_player_info = stmt_player_info.on_conflict_do_update(
            index_elements=["app_id", "review_id"],
            set_={
                "steam_user_id": stmt_player_info.excluded.steam_user_id,
                "num_games_owned": stmt_player_info.excluded.num_games_owned,
                "total_playtime_hr": stmt_player_info.excluded.total_playtime_hr,
                "playtime_last_two_weeks_hr": stmt_player_info.excluded.playtime_last_two_weeks_hr,
                "playtime_at_review_hr": stmt_player_info.excluded.playtime_at_review_hr,
                "last_played": stmt_player_info.excluded.last_played,
            },
            where=or_(
                # don't use != here, bad practice
                # if column is nullable, sql treat xx != NULL as return null
                # the sql update think update null to null reject the request
                CleanLatestReviewPlayerInfo.num_games_owned.is_distinct_from(
                    stmt_player_info.excluded.num_games_owned
                ),
                CleanLatestReviewPlayerInfo.total_playtime_hr.is_distinct_from(
                    stmt_player_info.excluded.total_playtime_hr
                ),
                CleanLatestReviewPlayerInfo.playtime_last_two_weeks_hr.is_distinct_from(
                    stmt_player_info.excluded.playtime_last_two_weeks_hr
                ),
                CleanLatestReviewPlayerInfo.playtime_at_review_hr.is_distinct_from(
                    stmt_player_info.excluded.playtime_at_review_hr
                ),
                CleanLatestReviewPlayerInfo.last_played.is_distinct_from(
                    stmt_player_info.excluded.last_played
                ),
            ),
        ).returning(CleanLatestReviewPlayerInfo.steam_user_id)

        review_result = session.execute(stmt_review)
        review_inserted_count = len(review_result.fetchall())

        player_info_result = session.execute(stmt_player_info)
        player_info_inserted_count = len(player_info_result.fetchall())

        # return inserted_count
        return review_inserted_count, player_info_inserted_count
    except SQLAlchemyError as _:
        # session.rollback() TODO: decideing; for trasnfroemation logic people use the with session.begin(): auto commit and auto rollback
        log.exception("Upsert failed in insert_cleaned_review")
        raise
