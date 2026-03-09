-- 01_schemas.sql

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grant usage to application user
GRANT USAGE ON SCHEMA raw       TO dakota_user;
GRANT USAGE ON SCHEMA staging   TO dakota_user;
GRANT USAGE ON SCHEMA analytics TO dakota_user;

-- Grant create on schemas
GRANT CREATE ON SCHEMA raw       TO dakota_user;
GRANT CREATE ON SCHEMA staging   TO dakota_user;
GRANT CREATE ON SCHEMA analytics TO dakota_user;

-- Default privileges: any future tables in these schemas are accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA raw       GRANT ALL ON TABLES TO dakota_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging   GRANT ALL ON TABLES TO dakota_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO dakota_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw       GRANT ALL ON SEQUENCES TO dakota_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging   GRANT ALL ON SEQUENCES TO dakota_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON SEQUENCES TO dakota_user;
