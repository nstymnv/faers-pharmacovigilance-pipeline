import json
from functools import cache
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

SCHEMA_FILE = Path(__file__).with_name("safetyreport_schema.json")


@cache
def safetyreport_schema() -> StructType:
    """
    The frozen safetyreport schema.

    Inferring it instead costs a full pass over every XML file before any work
    starts — several minutes per quarter — and makes the result depend on file
    contents: where a quarter happens to have at most one <drugrecurrence> per
    drug, inference produces a struct rather than an array and the explode fails.

    Regenerate with extract.schema_tools when FAERS adds fields.
    """
    return StructType.fromJson(json.loads(SCHEMA_FILE.read_text()))


def extract_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Read the staged XML under `path` into one row per safetyreport.

    The result is cached because each raw table is parsed from it in turn; the
    caller unpersists it once all of them are written.
    """
    df = (
        spark.read.format("xml")
        .option("rowTag", "safetyreport")
        .schema(safetyreport_schema())
        .load(path)
    )
    df.cache()

    return df
