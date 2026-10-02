"""Load the FDA deleted-cases list for one quarter into RAW.DELETED_CASES.

FAERS ships a plain-text list of retracted case numbers alongside each quarter's
XML. A retraction is not limited to earlier quarters — 153 of the 4,489 IDs in the
2020q1 list refer to reports published in 2020q1 itself — so the list is applied
across the whole warehouse rather than only to prior loads.

This is a few thousand short lines per quarter, so it goes through COPY INTO on a
normal connection instead of the Snowpark Connect session the XML needs.
"""

import logging

from faers_ingestion.config import RAW_SCHEMA, RAW_STAGE, quarter_key

DELETED_CASES_TABLE = f"{RAW_SCHEMA}.deleted_cases"

logger = logging.getLogger(__name__)

CREATE_TABLE = f"""
create table if not exists {DELETED_CASES_TABLE} (
    case_id string,
    source_quarter string,
    load_ts timestamp_ltz
)
"""


def load_deleted_cases(conn, year: int, quarter: int) -> int:
    """Replace one quarter's slice of the deleted-case list. Returns rows loaded."""
    key = quarter_key(year, quarter)
    stage_path = f"@{RAW_STAGE}/deleted/year={year}/quarter={quarter}/"
    cursor = conn.cursor()

    cursor.execute(CREATE_TABLE)
    cursor.execute(f"delete from {DELETED_CASES_TABLE} where source_quarter = %s", (key,))

    # force: the quarter's rows were just deleted, so COPY must re-read files it
    # has already seen rather than skipping them as loaded.
    cursor.execute(
        f"""
        copy into {DELETED_CASES_TABLE} (case_id, source_quarter, load_ts)
        from (select trim($1), %s, current_timestamp() from {stage_path})
        file_format = (type = csv field_delimiter = none skip_header = 0)
        force = true
        on_error = abort_statement
        """,
        (key,),
    )

    loaded = cursor.execute(
        f"select count(*) from {DELETED_CASES_TABLE} where source_quarter = %s", (key,)
    ).fetchone()[0]

    logger.info("%s: loaded %d deleted case ids for %s", DELETED_CASES_TABLE, loaded, key)

    return loaded
