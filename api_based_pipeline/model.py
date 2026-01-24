import uuid
from datetime import datetime
from sqlalchemy import (
    BigInteger,
    Integer,
    Text,
    TIMESTAMP,
    ForeignKey,
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

    app_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    review_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    hash_raw_json: Mapped[str] = mapped_column(String(64), primary_key=True)

    raw_json: Mapped[dict] = mapped_column(JSONB, nullable=False)

    run_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("bronze.ingestion_run.run_id", ondelete="SET NULL"),
        nullable=True,
    )
