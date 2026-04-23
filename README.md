Initial Setup

1. Configure environment variables
#TODO: rewrite below description
Open the .env file and replace the example values with your actual configuration.
You can use them by default if you want.
Optional: If the KESTRA_POSTGRES_USER and KESTRA_POSTGRES_PASSWORD are changed to your custom value
You need to go to the /postgres_init/init_kestral.sql, change the information there: update the sql query value based on the changing value

2. Start the services
Go to the directory containing the docker-compose.yaml file and run:
docker compose up
3. Open pgAdmin
Open http://localhost:81 in your browser and log in to pgAdmin 4.
4. Register the PostgreSQL server in pgAdmin
In pgAdmin, register a new server using the database connection values defined in your .env file. The settings below are only an example.
General
•	Name: steam_review_db 
Connection
•	Host name/address: pgdatabase 
•	Port: 5432 
•	Maintenance database: steam_review 
•	Username: root 
•	Password: root 

5. Initialize the database tables
Run the SQL scripts in the sql_script folder to create the required tables for the pipeline.
Please replace the -p, -U, and -d values with the actual port, username, and database name defined in your .env file.
For the Steam Web API-based pipeline:
psql \
  -h localhost \
  -p 55432 \
  -U root \
  -d steam_review \
  -f sql_script/db_init_api_based.sql