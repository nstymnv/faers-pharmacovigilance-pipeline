-- Run manually via SnowSQL/Snowflake worksheet to verify .env credentials
-- resolve to the expected user/database/warehouse before running the pipeline.
SELECT CURRENT_USER(),
       CURRENT_DATABASE(),
       CURRENT_SCHEMA(),
       CURRENT_WAREHOUSE();