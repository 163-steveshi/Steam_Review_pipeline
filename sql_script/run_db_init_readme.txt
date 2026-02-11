psql \
  -h localhost \
  -p 55432 \
  -U root \
  -d steam_review \
  -f sql_script/db_init_api_based.sql