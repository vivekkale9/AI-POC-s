from pyspark.sql.functions import col, collect_set, collect_list
from bson import ObjectId
import json
import os
from dotenv import load_dotenv

# Load environment variables (Databricks can also use secrets management)
load_dotenv()

# Access environment variables
mongo_uri = "mongodb+srv://root:root@learningmongo.cr2lsf3.mongodb.net"
database = "mRounds"

# Import Spark session
from sparkConfig import spark
def get_bo_for_product(product=None):
    try:
        masterbo_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority&appName=learningMongo") \
            .load()
        
        if product:
            # Filter by product and select BO
            result = masterbo_df.filter(col("product") == product) \
                .select("_id", "product", "BusinessObject")

            # Convert to list of dictionaries
            bo_list = [row.asDict() for row in result.collect()]

            if not bo_list:
                return {"message": f"No BOs found for product '{product}'"}

            return bo_list
        else:
            # Group by product and collect BusinessObjects
            result = masterbo_df.groupBy("product") \
                .agg(collect_list("BusinessObject").alias("BusinessObjects")) \
                .select("product", "BusinessObjects")

            # Convert to list of dictionaries
            product_bo_list = [row.asDict() for row in result.collect()]

            if not product_bo_list:
                return {"message": "No products or BOs found"}

            return product_bo_list

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {"error": "An internal error occurred"}
result = get_bo_for_product("mRounds")
print(result)
