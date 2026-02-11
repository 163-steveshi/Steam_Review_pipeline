from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg://root:root@localhost:55432/steam_review"

engine = create_engine(DATABASE_URL)
# recommended use: engine.begin() auto-commits on success
# auto-rolls back on error

with engine.begin() as connection:
    result = connection.execute(text("SELECT * FROM silver.clean_latest_review;"))
    rows = result.fetchall()
    print(rows)

# close any idle connection
engine.dispose()
