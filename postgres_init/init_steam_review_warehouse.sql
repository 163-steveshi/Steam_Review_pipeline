CREATE ROLE steam_etl WITH LOGIN PASSWORD 'steam_etl';
CREATE DATABASE steam_review OWNER steam_etl;
GRANT ALL PRIVILEGES ON DATABASE steam_review TO steam_etl;