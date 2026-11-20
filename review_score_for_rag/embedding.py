import voyageai
import os

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Genres were left out on purpose. They work better as a filter than inside the vector. Here’s the reasoning.

# Embed meaning, filter on facts

# There are two ways to use a piece of metadata in RAG:

# In the vector (the prefix): it influences similarity, i.e. what counts as “close” to the question.
# As a column filter (WHERE genres @> ARRAY['Roguelike']): an exact yes/no.


load_dotenv("./.env")
MODEL = "voyage-4"
DIM = 1024
BATCH_SIZE = 500

client = voyageai.Client(api_key=os.environ["VOYAGE_API_KEY"], max_retries=5)

vector_engine = create_engine(os.environ["DATABASE_URL_VECTOR"])
SELECT_PENDING = text("""
   SELECT app_id,            
    review_id,           
    chunk_index,         
    chunker_version,              
    content,                      
    voted_positive,
    game_name                                 
  FROM gold.review_chunks
  WHERE embedding is null;

""")

UPDATE_EMBEDDING = text("""
    UPDATE gold.review_chunks
    SET embedding   = CAST(:embedding AS vector),
        embed_model = :model,
        embedded_at = now()
    WHERE app_id = :app_id AND review_id = :review_id AND chunk_index = :chunk_index
""")
with vector_engine.connect() as conn:
    rows = conn.execute(SELECT_PENDING).mappings().all()
    print(f"{len(rows)} chunks to embed")

    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        texts = [
            f"Game: {r['game_name'] or 'Unknown'}. "
            f"Review ({'positive' if r['voted_positive'] else 'negative'}): {r['content']}"
            for r in batch
        ]

        result = client.embed(
            texts,
            model=MODEL,
            input_type="document",  # these are documents, not questions
            output_dimension=DIM,
        )
        vectors = result.embeddings  # same order as `texts`

        conn.execute(
            UPDATE_EMBEDDING,
            [
                {
                    "embedding": str(v),
                    "model": MODEL,
                    "app_id": r["app_id"],
                    "review_id": r["review_id"],
                    "chunk_index": r["chunk_index"],
                }
                for r, v in zip(batch, vectors)
            ],
        )
        conn.commit()  # save this batch
        print(f"{start + len(batch)}/{len(rows)}")
