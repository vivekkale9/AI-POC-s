import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load environment variables
load_dotenv()

def get_spark_session():
    return SparkSession.builder \
        .appName("MongoDB Spark Example") \
        .config("spark.jars.packages", os.getenv("SPARK_JARS_PACKAGES")) \
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST")) \
        .getOrCreate()

spark = get_spark_session()

