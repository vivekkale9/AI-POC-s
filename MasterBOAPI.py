from flask import Blueprint, jsonify, request
from bson import ObjectId
import pyspark
from pyspark.sql.functions import col, collect_set, collect_list
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access environment variables
mongo_uri = os.getenv("MONGO_URI")
database = os.getenv("DATABASE")

# Import Spark session
from sparkConfig import spark

masterbo_blueprint = Blueprint('masterbo', __name__)


@masterbo_blueprint.route('/', methods=['GET'])
@masterbo_blueprint.route('/<product>', methods=['GET'])
def get_bo_for_product(product=None):
    

    try:
        masterbo_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority&appName=learningMongo") \
            .load()
        
        def format_bo(row):
            bo_dict = row.asDict()
            if '_id' in bo_dict and isinstance(bo_dict['_id'], pyspark.sql.types.Row):
                bo_dict['_id'] = bo_dict['_id'].oid
            return bo_dict
        
        
        if product:
            # Filter by product and select BO
            result = masterbo_df.filter(col("product") == product) \
                .select("_id", "product", "BusinessObject")

            # Convert to list of dictionaries
            bo_list = [format_bo(row) for row in result.collect()]

            if not bo_list:
                return jsonify({"message": f"No BOs found for product '{product}'"}), 404

            return jsonify(bo_list)
        else:
            # Select _id, product, and BusinessObject for all entries
            result = masterbo_df.select(
                "_id",
                "product",
                "BusinessObject"
            )

            # Convert to list of dictionaries
            all_bo_list = [format_bo(row) for row in result.collect()]

            if not all_bo_list:
                return jsonify({"message": "No products or BOs found"}), 404

            return jsonify(all_bo_list)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500


@masterbo_blueprint.route('/bo/<object_id>', methods=['GET'])
def get_bo_fields_by_id(object_id):
    filter_param = request.args.get('filter')
    
    if filter_param is None:
        return jsonify({"error": "Missing filter parameter"}), 400
    if filter_param != 'fields':
        return jsonify({"error": "Invalid filter parameter"}), 400

    try:
        # Validate ObjectId
        if not ObjectId.is_valid(object_id):
            return jsonify({"error": "Invalid ObjectId format"}), 400

        pipeline = [
            {"$match": {"_id": {"$oid": object_id}}},
            {"$project": {
                "BusinessObject": 1,
                "product": 1,
                "fields": 1
            }}
        ]

        # Read MasterBO collection with the specific pipeline
        result = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority&appName=learningMongo") \
            .option("pipeline", json.dumps(pipeline)) \
            .load()

        # Convert to list of dictionaries
        bo_data = result.collect()

        if not bo_data:
            return jsonify({"message": f"No Business Object found for ObjectId '{object_id}'"}), 404

        # Extract the fields array and other info from the first (and should be only) row
        bo_info = bo_data[0].asDict()
        
        response = {
            "BusinessObject": bo_info.get("BusinessObject"),
            "product": bo_info.get("product"),
            "fields": bo_info.get("fields", [])
        }

        return jsonify(response)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500



@masterbo_blueprint.route('/products', methods=['GET'])
def get_all_products():
    try:
        # Read MasterBO collection
        masterbo_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority&appName=learningMongo") \
            .load()

        # Get distinct products
        products = masterbo_df.select(collect_set("product").alias("products")).first()["products"]

        # Sort the products alphabetically
        sorted_products = sorted(products)

        return jsonify({"products": sorted_products})

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500
