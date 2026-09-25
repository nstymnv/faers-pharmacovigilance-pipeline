"""Deploy the FAERS ingestion stored procedure to Snowflake.

The handler is uploaded to a stage and referenced with IMPORTS rather than being
inlined in the CREATE PROCEDURE body, so the Python stays reviewable in git.

    python -m faers_ingestion.sprocs.deploy
"""

import logging
from pathlib import Path

import snowflake.connector

from faers_ingestion.config import CODE_STAGE, build_connection_parameters

HANDLER_FILE = Path(__file__).with_name("ingest_faers_quarter.py")

CREATE_PROCEDURE = f"""
create or replace procedure extraction.ingest_faers_quarter(
    year int, quarter int, force boolean default false
)
returns variant
language python
runtime_version = '3.11'
-- stream-inflate is pure Python from PyPI, for the quarters FDA packed with
-- Deflate64; the Anaconda channel the other packages come from does not have it.
artifact_repository = snowflake.snowpark.pypi_shared_repository
packages = ('snowflake-snowpark-python', 'requests', 'stream-inflate==0.0.43')
imports = ('@{CODE_STAGE}/{HANDLER_FILE.name}')
external_access_integrations = (fda_faers_access)
handler = 'ingest_faers_quarter.run'
"""

logger = logging.getLogger(__name__)


def deploy() -> None:
    with snowflake.connector.connect(**build_connection_parameters()) as conn:
        cursor = conn.cursor()

        cursor.execute(
            f"put file://{HANDLER_FILE} @{CODE_STAGE}"
            " auto_compress=false overwrite=true"
        )
        logger.info("uploaded %s to @%s", HANDLER_FILE.name, CODE_STAGE)

        cursor.execute(CREATE_PROCEDURE)
        logger.info("created procedure extraction.ingest_faers_quarter")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    deploy()
