"""Extract staged FAERS XML into the Snowflake raw tables.

Ingestion (download → stage) happens inside Snowflake via
extraction.ingest_faers_quarter; this step reads what that procedure staged.

    python -m faers_ingestion.main --quarters 2020q1
    python -m faers_ingestion.main --quarters 2020q1 2020q2 --ingest
"""

import argparse
import logging
import re
import time

import snowflake.connector

from faers_ingestion.config import build_connection_parameters, stage_xml_prefix
from faers_ingestion.extract.scanner import extract_data
from faers_ingestion.extract.spark import create_spark
from faers_ingestion.load.deleted_cases import load_deleted_cases
from faers_ingestion.load.loader import write_quarter
from faers_ingestion.load.parser import (
    extract_demographics,
    extract_drug,
    extract_reaction,
    extract_reports,
)

QUARTER_PATTERN = re.compile(r"^(20\d{2})q([1-4])$")

EXTRACTORS = {
    "REPORTS": extract_reports,
    "DEMOGRAPHICS": extract_demographics,
    "DRUG": extract_drug,
    "REACTION": extract_reaction,
}

logger = logging.getLogger(__name__)


def parse_quarter(value: str) -> tuple[int, int]:
    match = QUARTER_PATTERN.match(value.lower())
    if not match:
        raise argparse.ArgumentTypeError(f"expected a quarter like 2020q1, got {value!r}")

    return int(match.group(1)), int(match.group(2))


def ingest_quarter(year: int, quarter: int, force: bool) -> None:
    """Call the in-Snowflake procedure that downloads and stages the quarter."""
    with snowflake.connector.connect(**build_connection_parameters()) as conn:
        result = (
            conn.cursor()
            .execute(
                "call extraction.ingest_faers_quarter(%s, %s, %s)",
                (year, quarter, force),
            )
            .fetchone()[0]
        )

    logger.info("staged %sq%s: %s", year, quarter, result)


def load_quarter(spark, year: int, quarter: int) -> dict[str, int]:
    raw_data = extract_data(spark, stage_xml_prefix(year, quarter))
    quarter_key = f"{year}q{quarter}"

    counts = {}
    for table_name, extractor in EXTRACTORS.items():
        started = time.time()
        counts[table_name] = write_quarter(spark, extractor(raw_data), table_name, quarter_key)
        logger.info("%s took %.0fs", table_name, time.time() - started)

    raw_data.unpersist()

    # Loaded last, and deliberately so. Replacing this quarter's retracted-case
    # slice before the report tables would, on a failed XML load, leave new
    # retractions filtering an older set of reports. The two are not written in
    # one transaction, so the cheap, re-runnable side goes second.
    with snowflake.connector.connect(**build_connection_parameters()) as conn:
        counts["DELETED_CASES"] = load_deleted_cases(conn, year, quarter)

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quarters",
        nargs="+",
        required=True,
        type=parse_quarter,
        help="quarters to load, e.g. 2020q1 2020q2",
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="download and stage each quarter first (skips quarters already staged)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-stage a quarter even if it was already ingested",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    spark = create_spark()
    try:
        for year, quarter in args.quarters:
            if args.ingest:
                ingest_quarter(year, quarter, args.force)

            counts = load_quarter(spark, year, quarter)
            logger.info("%sq%s loaded: %s", year, quarter, counts)
    finally:
        # Explicit rather than left to process exit: under Airflow the worker
        # process outlives the task and would accumulate a session per run.
        spark.stop()


if __name__ == "__main__":
    main()
