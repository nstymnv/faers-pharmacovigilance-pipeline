-- Run as the pipeline's working role (the one in SNOWFLAKE_ROLE). Everything
-- here is owned by that role except the external access integration, which
-- needs account-level CREATE INTEGRATION and so is the only statement run as
-- ACCOUNTADMIN, at the end of the script.
SET WORKING_ROLE = CURRENT_ROLE();

USE DATABASE FAERS_DB;
USE SCHEMA EXTRACTION;

-- Raw zone. FAERS XML is archived here byte-faithfully so the pipeline can be
-- re-parsed without re-downloading from FDA, which matters because FDA revises
-- quarterly files and eventually retires older ones.
--
-- Files are stored uncompressed: Snowpark Connect does not support compression
-- for XML reads.
CREATE STAGE IF NOT EXISTS FAERS_RAW
DIRECTORY = (ENABLE = TRUE);

-- Holds the stored procedure handler so its Python stays reviewable in git
-- instead of being embedded in a CREATE PROCEDURE body.
CREATE STAGE IF NOT EXISTS CODE;

-- The only outbound network access this account needs.
CREATE OR REPLACE NETWORK RULE FDA_FAERS_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('fis.fda.gov:443');

-- One row per attempted quarter ingest. Drives idempotency (a quarter already
-- SUCCEEDED is skipped) and tells the Airflow DAG what still needs loading.
CREATE TABLE IF NOT EXISTS INGESTION_LOG (
    QUARTER_KEY STRING NOT NULL,
    SOURCE_URL STRING,
    STATUS STRING NOT NULL,
    FILES_STAGED NUMBER,
    BYTES_STAGED NUMBER,
    STARTED_AT TIMESTAMP_LTZ,
    ENDED_AT TIMESTAMP_LTZ,
    ERROR_MESSAGE STRING
);

-- Integrations are account-level objects, so creating one needs ACCOUNTADMIN.
-- The working role only needs USAGE to attach it to the ingestion procedure.
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION FDA_FAERS_ACCESS
ALLOWED_NETWORK_RULES = (FAERS_DB.EXTRACTION.FDA_FAERS_RULE)
ENABLED = TRUE;

GRANT USAGE ON INTEGRATION FDA_FAERS_ACCESS TO ROLE IDENTIFIER($WORKING_ROLE);

USE ROLE IDENTIFIER($WORKING_ROLE);
