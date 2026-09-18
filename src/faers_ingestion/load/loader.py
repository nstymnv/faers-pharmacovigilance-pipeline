import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit

from faers_ingestion.config import RAW_SCHEMA

PARTITION_COLUMN = "source_quarter"

logger = logging.getLogger(__name__)


def _qualified(table_name: str) -> str:
    return f"{RAW_SCHEMA}.{table_name}"


def _is_quarter_partitioned(spark: SparkSession, table_name: str) -> bool:
    """Whether the existing table can hold a single quarter's rows.

    Tables written by the pre-Snowflake pipeline have neither the partition column
    nor the newer parser fields, so they are replaced wholesale on first load
    rather than appended to.
    """
    qualified = _qualified(table_name)
    if not spark.catalog.tableExists(qualified):
        return False

    columns = {field.name.lower() for field in spark.table(qualified).schema.fields}

    return PARTITION_COLUMN in columns


def write_quarter(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    quarter_key: str,
) -> int:
    """Replace one quarter's rows in a raw table.

    Reloading a single quarter used to mean rebuilding all four tables from every
    staged file. Tagging rows with their source quarter and replacing only that
    slice keeps a 20-quarter backfill restartable and makes a re-run of one
    quarter cost one quarter's compute.
    """
    tagged = df.withColumn(PARTITION_COLUMN, lit(quarter_key)).withColumn(
        "load_ts", current_timestamp()
    )
    # int(): Snowpark Connect returns a numpy integer from count().
    written = int(tagged.count())
    qualified = _qualified(table_name)

    if _is_quarter_partitioned(spark, table_name):
        spark.sql(f"delete from {qualified} where {PARTITION_COLUMN} = '{quarter_key}'")
        tagged.write.mode("append").saveAsTable(qualified)
        logger.info("%s: replaced %s with %d rows", qualified, quarter_key, written)
    else:
        tagged.write.mode("overwrite").saveAsTable(qualified)
        logger.info("%s: recreated with %d rows for %s", qualified, written, quarter_key)

    return written
