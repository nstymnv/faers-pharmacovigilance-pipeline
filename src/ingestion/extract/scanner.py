from pyspark.sql import DataFrame, SparkSession


def extract_data(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.format("xml").option("rowTag", "safetyreport").load(path)
    df.cache()

    return df
