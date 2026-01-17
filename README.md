# COMP4905
Honor Project Winter 2026


1.set up pgadmin4 and db
register server--> gerneral-> name your service: steam_review_db
register server--> connection:
  Host name/address: pgdatabase
  Port: 5432
  Maintenance database: steam_review
  Username: root
  password: root

conda install -c conda-forge sqlalchemy psycopg
  conda need lib: request, postgresql, sqlalchemy psycopg,  pydantic