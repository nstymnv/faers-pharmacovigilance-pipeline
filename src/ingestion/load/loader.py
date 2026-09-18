from pyspark.sql import DataFrame


def write_to_db(df: DataFrame, table_name: str, sf_options: dict[str, str | None]) -> None:
    (
        df.write.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", table_name)
        .mode("overwrite")
        .save()
    )
