import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


spark = (
        SparkSession.builder
        .appName("FaersPipeline")
        .master("local[*]")
        .config(
        "spark.jars.packages",
        "com.databricks:spark-xml_2.12:0.17.0"
        )
        .getOrCreate()
        )


path = "/home/nastya/projects/faers-pharmacovigilance-pipeline/data/raw/XML/*.xml"

def extract_reports(path):
    df =  (
        spark.read.format("xml")
        .option("rowTag", "safetyreport")
        .load(path)
    )
    df = df.withColumn("drug", explode("patient.drug"))
    df = df.select("safetyreportid", "drug.*")
    df.printSchema()
    df.show(5, truncate=False)
    return df