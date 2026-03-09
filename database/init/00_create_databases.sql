-- 00_create_databases.sql

SELECT 'CREATE DATABASE dagster_db'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'dagster_db'
)\gexec

GRANT ALL PRIVILEGES ON DATABASE dagster_db TO dakota_user;
