-- Identity and schemas for dbt in GitHub Actions (.github/workflows/ci.yml).
-- Run as the pipeline's working role, the one in SNOWFLAKE_ROLE, which owns
-- FAERS_WH, FAERS_DB and its schemas and so can grant on them.
--
-- CI builds the 2020q1 dev slice into CI_STAGING, CI_INTERMEDIATE and CI_MART
-- (generate_schema_name prefixes CI_ on the ci target), reading RAW. The role
-- can create tables in the CI_ schemas and read RAW, nothing else, so a leaked
-- key cannot touch the real STAGING/INTERMEDIATE/MART tables.
--
-- Before running, generate the key pair (keys/ is gitignored):
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out keys/faers_ci_key.p8 -nocrypt
--   openssl rsa -in keys/faers_ci_key.p8 -pubout -out keys/faers_ci_key.pub
-- and paste the body of the .pub file, without the BEGIN/END lines, into
-- RSA_PUBLIC_KEY below. The full text of the .p8 file becomes the GitHub secret
-- FAERS_CI_PRIVATE_KEY.
SET WORKING_ROLE = CURRENT_ROLE();

-- USERADMIN is the system role meant for creating users and roles, so no
-- statement here needs ACCOUNTADMIN.
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS FAERS_CI
COMMENT = 'dbt in GitHub Actions: builds the dev slice into the CI_ schemas';

-- TYPE = SERVICE: no password and no MFA, key-pair auth only, which is what a
-- CI runner needs. The default warehouse puts its queries under FAERS_RM.
CREATE USER IF NOT EXISTS FAERS_CI
TYPE = SERVICE
DEFAULT_ROLE = FAERS_CI
DEFAULT_WAREHOUSE = FAERS_WH
DEFAULT_NAMESPACE = FAERS_DB.CI_STAGING
RSA_PUBLIC_KEY = '<paste keys/faers_ci_key.pub body here>'
COMMENT = 'dbt in GitHub Actions (.github/workflows/ci.yml)';

-- IF NOT EXISTS means a re-run never replaces the key; rotate it with
-- ALTER USER FAERS_CI SET RSA_PUBLIC_KEY = '...'.

GRANT ROLE FAERS_CI TO USER FAERS_CI;

USE ROLE IDENTIFIER($WORKING_ROLE);

GRANT USAGE ON WAREHOUSE FAERS_WH TO ROLE FAERS_CI;
GRANT USAGE ON DATABASE FAERS_DB TO ROLE FAERS_CI;

-- The CI schemas are created here, owned by the working role, rather than by
-- dbt: the CI role gets no CREATE SCHEMA on the database, so it cannot create
-- schemas anywhere else. Tables inside them are created, and owned, by FAERS_CI.
CREATE SCHEMA IF NOT EXISTS FAERS_DB.CI_STAGING;
CREATE SCHEMA IF NOT EXISTS FAERS_DB.CI_INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS FAERS_DB.CI_MART;

GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA FAERS_DB.CI_STAGING TO ROLE FAERS_CI;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA FAERS_DB.CI_INTERMEDIATE TO ROLE FAERS_CI;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA FAERS_DB.CI_MART TO ROLE FAERS_CI;

-- The dbt sources. The Python loader recreates a RAW table when its layout
-- changes, which drops grants on it, so FUTURE grants cover tables created
-- later and ALL grants cover the ones that exist now.
GRANT USAGE ON SCHEMA FAERS_DB.RAW TO ROLE FAERS_CI;
GRANT SELECT ON ALL TABLES IN SCHEMA FAERS_DB.RAW TO ROLE FAERS_CI;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FAERS_DB.RAW TO ROLE FAERS_CI;
