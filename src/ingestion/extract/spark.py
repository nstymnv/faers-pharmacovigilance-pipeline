from pyspark.sql import SparkSession


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("FaersPipeline")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "com.databricks:spark-xml_2.12:0.18.0",
                    "net.snowflake:spark-snowflake_2.12:3.2.2-spark_3.5",
                    "net.snowflake:snowflake-jdbc:4.3.3",
                ]
            ),
        )
        .getOrCreate()
    )
