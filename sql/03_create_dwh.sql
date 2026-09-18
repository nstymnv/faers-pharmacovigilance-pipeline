USE DATABASE FAERS_DB;

-- CREATE OR ALTER so re-running converges an existing warehouse onto these
-- settings instead of failing. AUTO_SUSPEND is deliberately short: XML parsing
-- runs on this warehouse, and idle time is the easiest credit leak to avoid.
CREATE OR ALTER WAREHOUSE FAERS_WH
WITH WAREHOUSE_SIZE = 'XSMALL',
AUTO_RESUME = TRUE,
AUTO_SUSPEND = 60,
INITIALLY_SUSPENDED = TRUE;
