"""Regenerate the frozen safetyreport schema from staged XML.

Run when FAERS adds fields, then review the diff before committing:

    python -m faers_ingestion.extract.schema_tools --year 2020 --quarter 1
"""

import argparse
import json
import logging

from faers_ingestion.config import stage_xml_prefix
from faers_ingestion.extract.scanner import SCHEMA_FILE
from faers_ingestion.extract.spark import create_spark

logger = logging.getLogger(__name__)


def regenerate(year: int, quarter: int) -> None:
    spark = create_spark()

    inferred = (
        spark.read.format("xml")
        .option("rowTag", "safetyreport")
        .load(stage_xml_prefix(year, quarter))
        .schema
    )

    SCHEMA_FILE.write_text(json.dumps(inferred.jsonValue(), indent=2) + "\n")
    logger.info("wrote %s with %d top-level fields", SCHEMA_FILE, len(inferred.fields))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--quarter", type=int, required=True, choices=[1, 2, 3, 4])
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    regenerate(args.year, args.quarter)


if __name__ == "__main__":
    main()
