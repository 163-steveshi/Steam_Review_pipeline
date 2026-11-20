# use langchian text splitter for cutting the sentences and paragraph
# we also use open api's tokenizer: cl100k_base measuere the context length
# pip install langchain-text-splitters tiktoken

# use chunk size 300 for this split, most steam review text is not exceeds 1200 char


from langchain_text_splitters import RecursiveCharacterTextSplitter
from sqlalchemy import create_engine, text, bindparam, Text
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import Session, sessionmaker
import os
from score import score_review
from dotenv import load_dotenv

load_dotenv("./.env")

CHUNKER_VERSION = "v1-300tok-40ovl"

splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
    encoding_name="cl100k_base",
    chunk_size=300,
    chunk_overlap=40,
    separators=["\n\n", "\n", ". ", "! ", "? ", " ", ""],
)


def review_to_chunks(row: dict) -> list[dict]:
    """One eligible row from gold.review_rag_score -> rows for gold.review_chunks."""
    pieces = splitter.split_text(row["review_text_for_chunking"])
    return [
        {
            "app_id": row["app_id"],
            "review_id": row["review_id"],
            "chunk_index": i,
            "chunk_count": len(pieces),
            "text_hash": row["text_hash"],
            "chunker_version": CHUNKER_VERSION,
            "content": piece,
            "language": row["language"],
            "voted_positive": row["voted_positive"],
            "timestamp_created": row["timestamp_created"],
            "playtime_at_review_mins": row["playtime_at_review_mins"],
            "genres": row["genres"],  # already flattened to list[str]
        }
        for i, piece in enumerate(pieces)
    ]


def main():
    # TODO: create another engine for write to rag vector
    source_engine = create_engine(os.environ["DATABASE_URL_LOCAL"])
    vector_engine = create_engine(os.environ["DATABASE_URL_VECTOR"])

    # ---- 1. Read from the source ----
    with source_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT app_id, review_id, review_text_for_chunking,
                language, voted_positive, playtime_at_review_mins, genres,
                text_hash, timestamp_created
            FROM gold.review_rag_score
            WHERE is_rag_eligible = true
        """)).mappings().all()

    records = []
    for r in rows:
        records.extend(
            review_to_chunks(r)
        )  # extend, not append, if it returns a list of chunks

    insert_sql = text("""
        INSERT INTO gold.review_chunks (
            app_id, review_id, chunk_index, chunk_count, text_hash, chunker_version,
            content, language, voted_positive, timestamp_created,
            playtime_at_review_mins, genres
        ) VALUES (
            :app_id, :review_id, :chunk_index, :chunk_count, :text_hash, :chunker_version,
            :content, :language, :voted_positive, :timestamp_created,
            :playtime_at_review_mins, :genres
        )
        ON CONFLICT (app_id, review_id, chunk_index) DO UPDATE
            SET chunk_count             = EXCLUDED.chunk_count,
                text_hash               = EXCLUDED.text_hash,
                chunker_version         = EXCLUDED.chunker_version,
                content                 = EXCLUDED.content,
                language                = EXCLUDED.language,
                voted_positive          = EXCLUDED.voted_positive,
                timestamp_created       = EXCLUDED.timestamp_created,
                playtime_at_review_mins = EXCLUDED.playtime_at_review_mins,
                genres                  = EXCLUDED.genres,
                -- content changed -> old vector is stale, queue for re-embedding
                embedding               = NULL,
                embed_model             = NULL,
                embedded_at             = NULL,
                chunked_at              = now()
            WHERE gold.review_chunks.text_hash       IS DISTINCT FROM EXCLUDED.text_hash
            OR gold.review_chunks.chunker_version IS DISTINCT FROM EXCLUDED.chunker_version
    """)
    SessionLocal = sessionmaker(bind=vector_engine, autoflush=True)
    with SessionLocal() as session:
        if records:
            session.execute(insert_sql, records)
            session.commit()


if __name__ == "__main__":
    main()
