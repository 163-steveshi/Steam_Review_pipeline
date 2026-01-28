import uuid
from datetime import datetime
from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    Integer,
    Text,
    TIMESTAMP,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase


# func: call common SQL functions like count, sum, avg, min, max, lower


class Base(DeclarativeBase):
    pass


class IngestionRun(Base):
    __tablename__ = "ingestion_run"
    __table_args__ = {"schema": "bronze"}

    app_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    run_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    status: Mapped[str] = mapped_column(Text, nullable=False)
    start_cursor: Mapped[str | None] = mapped_column(Text)
    end_cursor: Mapped[str | None] = mapped_column(Text)
    last_success_cursor: Mapped[str | None] = mapped_column(Text)

    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    rows_fetched: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    error_type: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)


class RawReview(Base):
    __tablename__ = "raw_review"
    __table_args__ = {"schema": "bronze"}

    raw_id: Mapped[int] = mapped_column(
        BigInteger,
        unique=True,
        nullable=False,
        autoincrement=True,  # marked as managed by DB
    )

    app_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    review_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    hash_raw_json: Mapped[str] = mapped_column(String(64), primary_key=True)
    inserted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    raw_json: Mapped[dict] = mapped_column(JSONB, nullable=False)

    run_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("bronze.ingestion_run.run_id", ondelete="SET NOT NULL"),
        nullable=True,
    )


class CleanLatestReview(Base):
    __tablename__ = "clean_latest_review"
    __table_args__ = {"schema": "silver"}

    app_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    review_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    language: Mapped[str] = mapped_column(Text, nullable=False)
    review_text: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("''"),
    )

    timestamp_created: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )
    timestamp_updated: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )

    voted_positive: Mapped[bool] = mapped_column(Boolean, nullable=False)
    votes_helpful: Mapped[int | None] = mapped_column(Integer, nullable=True)
    votes_funny: Mapped[int | None] = mapped_column(Integer, nullable=True)
    weighted_vote_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    review_comment_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    steam_purchase: Mapped[bool] = mapped_column(Boolean, nullable=False)
    received_for_free: Mapped[bool] = mapped_column(Boolean, nullable=False)
    written_during_early_access: Mapped[bool] = mapped_column(Boolean, nullable=False)
    primarily_play_on_steam_deck: Mapped[bool] = mapped_column(Boolean, nullable=False)
    source_raw_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


class CleanLatestReviewPlayerInfo(Base):
    __tablename__ = "clean_latest_review_player_info"
    __table_args__ = (
        ForeignKeyConstraint(
            ["app_id", "review_id"],
            [
                "silver.clean_latest_review.app_id",
                "silver.clean_latest_review.review_id",
            ],
            ondelete="CASCADE",
            name="fk_playerinfo_review",
        ),
        {"schema": "silver"},
    )

    app_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    review_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    steam_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    num_games_owned: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # playtime_* fields (hours)
    total_playtime_hr: Mapped[int] = mapped_column(
        Integer, nullable=False
    )  # playtime_forever
    playtime_last_two_weeks_hr: Mapped[int] = mapped_column(Integer, nullable=False)
    playtime_at_review_hr: Mapped[int] = mapped_column(Integer, nullable=False)

    last_played: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )


class TransformLog(Base):
    __tablename__ = "transform_log"
    __table_args__ = {"schema": "silver"}

    pipeline_name: Mapped[str] = mapped_column(Text, primary_key=True)
    last_raw_id_processed: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )

    # Column name matches your DDL ("finished_atd_at")
    finished_atd_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
