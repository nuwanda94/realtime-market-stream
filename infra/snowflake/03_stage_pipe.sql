-- Optional Snowpipe path (stage + pipe) for batch JSONL files.
-- Local-first dual-write uses INSERT batches instead of Snowpipe; keep this
-- as a reference if you later land files in a cloud stage.

USE ROLE {{INGEST_ROLE}};
USE WAREHOUSE {{WAREHOUSE}};
USE DATABASE {{DATABASE}};
USE SCHEMA {{SCHEMA}};

CREATE FILE FORMAT IF NOT EXISTS {{DATABASE}}.{{SCHEMA}}.MARKET_JSONL
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE
  COMMENT = 'JSONL rows matching LocalJsonlSnowflakeChannel capture files';

-- Internal stage. Swap to an external S3/GCS/Azure stage in production.
CREATE STAGE IF NOT EXISTS {{DATABASE}}.{{SCHEMA}}.{{STAGE}}
  FILE_FORMAT = {{DATABASE}}.{{SCHEMA}}.MARKET_JSONL
  COMMENT = 'Landing zone for captured snowflake JSONL batches';

CREATE PIPE IF NOT EXISTS {{DATABASE}}.{{SCHEMA}}.{{PIPE}}
  AUTO_INGEST = FALSE
  AS
  COPY INTO {{DATABASE}}.{{SCHEMA}}.{{TABLE_TICKS}} (EVENT_TYPE, SYMBOL, EVENT_DATE, INGESTED_AT, PAYLOAD)
  FROM (
    SELECT
      $1:event_type::VARCHAR,
      $1:symbol::VARCHAR,
      $1:event_date::VARCHAR,
      $1:ingested_at::VARCHAR,
      $1:payload
    FROM @{{DATABASE}}.{{SCHEMA}}.{{STAGE}}
  );

GRANT USAGE ON STAGE {{DATABASE}}.{{SCHEMA}}.{{STAGE}} TO ROLE {{INGEST_ROLE}};
GRANT OPERATE, MONITOR ON PIPE {{DATABASE}}.{{SCHEMA}}.{{PIPE}} TO ROLE {{INGEST_ROLE}};
GRANT USAGE ON STAGE {{DATABASE}}.{{SCHEMA}}.{{STAGE}} TO ROLE {{READER_ROLE}};
