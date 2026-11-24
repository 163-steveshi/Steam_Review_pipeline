import voyageai
import os

from dotenv import load_dotenv

from sqlalchemy import create_engine, text

load_dotenv("./.env")
MODEL = "voyage-4"
DIM = 1024
BATCH_SIZE = 500

QUESTIONS = [
    "What do players think about rifle",
]
client = voyageai.Client(api_key=os.environ["VOYAGE_API_KEY"], max_retries=5)

vector_engine = create_engine(os.environ["DATABASE_URL_VECTOR"])
q_vecs = client.embed(
    QUESTIONS, model=MODEL, input_type="query", output_dimension=DIM
).embeddings


INSERT_QUERY = text("""
    INSERT INTO gold.eval_queries (question, embed_model, embedding)
    VALUES (
            :question, :embed_model, :embedding
    )
    ON CONFLICT (question) DO NOTHING;
""")
rows = [
    {
        "question": question,
        "embed_model": MODEL,
        "embedding": vec,
    }
    for question, vec in zip(QUESTIONS, q_vecs)
]
with vector_engine.connect() as conn:

    result = conn.execute(INSERT_QUERY, rows)
    conn.commit()
    print(f"Attempted {len(rows)} inserts")
    print(f"Successfully inserted: {result.rowcount}")
    count = conn.execute(text("""
        SELECT COUNT(*)
        FROM gold.eval_queries
    """)).scalar_one()

    print("Inserted:", result.rowcount)
    print("Total rows:", count)
    #     "What do players think about the co-op with friends?",
    #     "Is the game fun to play solo?",
    #     "What are the most common complaints?",
    #     "Are there bugs, crashes or performance problems?",
    #     "How do players feel about upgrading gear and buying equipment?",
    #     "What do players think about unlocking new islands?",
    #     "What do players say about the fish gambling mechanic?",
    #     "Do people enjoy collecting rare fish variants?",
    #     "What do players think about trick shots?",
    #     "Is the game scary",
    #     "Is it worth the price?($7.99 base)"
    # "What do players think about shotgun and rifle",
    # "How do players comments about goldfish",
    # "What do players think about cod or tuna",
    # "Do players say the game is too short or runs out of content",
    # "Are there multiplayer connection or lag issues?",
    # "Should I buy it with PEAK?",  # this game sell with boundle
    # "Does it support VR?",
    # "Is the game supporting Mac version?",
    # "How is the story and voice acting?",
