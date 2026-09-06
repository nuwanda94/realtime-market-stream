-- RBAC example for the market-stream Snowflake landing zone.
-- Placeholders are filled by `scripts/setup_snowflake.py`.
-- Review grants before applying; this is an example, not production policy.

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS {{ROLE}};
CREATE ROLE IF NOT EXISTS {{INGEST_ROLE}};
CREATE ROLE IF NOT EXISTS {{READER_ROLE}};

GRANT ROLE {{INGEST_ROLE}} TO ROLE {{ROLE}};
GRANT ROLE {{READER_ROLE}} TO ROLE {{ROLE}};

-- Attach the application user (optional; skip if using key-pair service users).
GRANT ROLE {{ROLE}} TO USER {{USER}};
