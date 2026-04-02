-- PostgreSQL Read-Only User Init Script
-- This script creates a read-only user that can be used by non-indexing users.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'odc_read_only') THEN
        CREATE USER odc_read_only WITH PASSWORD 'odc_read_only_password';
    END IF;
END $$;

-- Standard database for ODC is opendatacube
GRANT CONNECT ON DATABASE opendatacube TO odc_read_only;

-- Grant usage and select on existing tables in public schema
GRANT USAGE ON SCHEMA public TO odc_read_only;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO odc_read_only;

-- Ensure future tables are also readable
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO odc_read_only;

-- ODC metadata type schema 'agdc'
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'agdc') THEN
        GRANT USAGE ON SCHEMA agdc TO odc_read_only;
        GRANT SELECT ON ALL TABLES IN SCHEMA agdc TO odc_read_only;
        ALTER DEFAULT PRIVILEGES IN SCHEMA agdc GRANT SELECT ON TABLES TO odc_read_only;
    END IF;
END $$;
