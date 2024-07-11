from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SparkMongoDBPOC") \
    .config("spark.mongodb.input.uri", "mongodb+srv://root:root@learningmongo.cr2lsf3.mongodb.net/?retryWrites=true&w=majority&appName=learningMongo.listings") \
    .getOrCreate()

document_id = "660b0653170840c1142613ad"

df = spark.read \
    .format("mongo") \
    .option("uri", "mongodb+srv://root:root@learningmongo.cr2lsf3.mongodb.net/?retryWrites=true&w=majority&appName=learningMongo.listings") \
    .load()

filtered_df = df.filter(col("_id") == document_id)

if filtered_df.count() == 1:
    updated_df = filtered_df.withColumn("description", col("description").cast("string"))

    updated_df.write \
        .format("mongo") \
        .option("replaceDocument", "true") \
        .save()  

    print(f"Description updated for document with _id: {document_id}")

else:
    print(f"No document found with _id: {document_id}")

spark.stop()
