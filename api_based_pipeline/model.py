from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, BigInteger, String, ForeignKey, Text, Integer, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB, UUID

# call common SQL functions like count, sum, avg, min, max, lower
from sqlalchemy.sql import func

Base = declarative_base()


class IngestionRun(Base):
    __tablename__ = "ingestion_run"
    __table_args__ = {"schema": "bronze"}
    app_id = Column(BigInteger, nullable=False)
    run_id = Column(
        UUID,
        primary_key=True,
    )

    status = Column(Text, nullable=False)  # success / failed / running
    start_cursor = Column(Text)
    end_cursor = Column(Text)
    last_success_cursor = Column(Text)
    started_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    finished_at = Column(
        TIMESTAMP(timezone=True),
    )

    rows_fetched = Column(Integer)

    error_type = Column(Text)
    error_message = Column(Text)


class RawReview(Base):
    __tablename__ = "raw_review"
    __table_args__ = {"schema": "bronze"}

    app_id = Column(BigInteger, primary_key=True)
    review_id = Column(BigInteger, primary_key=True)
    hash_raw_json = Column(String(64), primary_key=True)

    raw_json = Column(JSONB, nullable=False)

    run_id = Column(
        UUID,
        ForeignKey(
            "bronze.ingestion_run.run_id",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
