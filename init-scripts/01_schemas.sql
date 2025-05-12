/*
===================================================
Initialization Script: Database Schemas
===================================================

Script Purpose:
    This script provisions three distinct schemas—bronze, silver, and gold—within the database identified by the POSTGRES_DB environment variable in your .env file.

Execution Context"
The script is executed automatically by Docker Compose on the initial startup of the bike_sales_warehouse service.
It runs only once; subsequent container restarts will not reapply the script if the data directory has already been initialized.

Important:
If you encounter errors during execution or need to reinitialize the database after making changes, perform the following steps:
    # Stop and remove containers, networks, and volumes
    docker compose down -v --remove-orphans

    # Restart the service
    docker compose up -d

This will recreate the volume and rerun the initialization script on the next startup.
*/

\connect bike_sales_data_warehouse;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
