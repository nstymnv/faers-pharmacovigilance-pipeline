-- Credit guardrail. Requires ACCOUNTADMIN.
--
-- The backfill parses ~30 GB of XML on warehouse compute, so the failure mode
-- worth protecting against is a runaway or accidentally repeated backfill rather
-- than day-to-day dbt runs. Raise CREDIT_QUOTA once a real per-quarter cost is
-- measured; see docs/next-steps-plan.md.
USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS FAERS_RM
WITH CREDIT_QUOTA = 50,
FREQUENCY = MONTHLY,
START_TIMESTAMP = IMMEDIATELY
TRIGGERS ON 75 PERCENT DO NOTIFY
         ON 90 PERCENT DO SUSPEND
         ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE FAERS_WH SET RESOURCE_MONITOR = FAERS_RM;
