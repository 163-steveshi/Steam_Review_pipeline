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
    DATABASE_URL_LOCAL = os.environ["DATABASE_URL_LOCAL"]
    engine = create_engine(DATABASE_URL_LOCAL)
    SessionLocal = sessionmaker(bind=engine, autoflush=True)
    with SessionLocal() as session:
        query = text("""
                     SELECT app_id, review_id, review_text_for_chunking, 
                     language, voted_positive, playtime_at_review_mins, genres,
                     text_hash, timestamp_created 
                     FROM gold.review_rag_score
                     where is_rag_eligible = 'true'
                     """)

        # 2. Execute and pass the parameter value
        rows: dict | None = session.execute(query).mappings().all()
        records = []
        if rows:
            for r in rows:
                records.append(review_to_chunks(r))
        print(len(records))


if __name__ == "__main__":
    main()
