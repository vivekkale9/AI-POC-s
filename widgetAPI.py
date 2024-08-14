from flask import Blueprint, jsonify, request
from datetime import datetime
from bson import ObjectId
from pyspark.sql.functions import col, struct, lit, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
import json
# import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access environment variables
mongo_uri = os.getenv("MONGO_URI")
database = os.getenv("DATABASE")

# Import Spark session
from sparkConfig import spark

widget_blueprint = Blueprint('widget', __name__)

@widget_blueprint.route('/create_widget', methods=['POST'])
def create_widget():
    try:
        # Get data from request body
        data = request.json

        # Validate required fields
        required_fields = ['sequence', 'roleId', 'legend', 'name', 'title', 'chartType']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Validate data types
        if not isinstance(data['sequence'], int):
            return jsonify({"error": "sequence must be an integer"}), 400
        if not ObjectId.is_valid(data['roleId']):
            return jsonify({"error": "Invalid roleId format"}), 400
        if not isinstance(data['legend'], bool) or not isinstance(data['name'], bool):
            return jsonify({"error": "legend and name must be boolean"}), 400
        if not isinstance(data['title'], str) or not isinstance(data['chartType'], str):
            return jsonify({"error": "title and chartType must be strings"}), 400

        # Create widget document
        widget = {
            "sequence": data['sequence'],
            "roleId": data['roleId'],
            "legend": data['legend'],
            "name": data['name'],
            "title": data['title'],
            "chartType": data['chartType'],
            "reportId": data['reportId'],
            "action": data['action'],
            "config": data['config'],
            "createdAt": datetime.utcnow().isoformat()
        }

        # Define schema for the widget
        schema = StructType([
            StructField("sequence", IntegerType(), True),
            StructField("roleId", StringType(), True),
            StructField("legend", BooleanType(), True),
            StructField("name", BooleanType(), True),
            StructField("title", StringType(), True),
            StructField("chartType", StringType(), True),
            StructField("reportId", StringType(), True),
            StructField("action", ArrayType(StructType([
                StructField("name", StringType(), True),
                StructField("filter", StringType(), True)
            ])), True),
            StructField("config", StructType([
                StructField("filter", ArrayType(StructType([
                    StructField("operation", StringType(), True),
                    StructField("value", StringType(), True)
                ])), True),
                StructField("groupBy", ArrayType(StringType()), True)
            ]), True),
            StructField("createdAt", StringType(), True)
        ])

        # Create a DataFrame with the widget
        widget_df = spark.createDataFrame([widget], schema)

        # Convert roleId to struct format that MongoDB expects for ObjectId
        widget_df = widget_df.withColumn("roleId", struct(lit(widget['roleId']).alias("oid")))
        widget_df = widget_df.withColumn("reportId", struct(lit(widget['reportId']).alias("oid")))


        # Write the DataFrame to MongoDB
        widget_df.write.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.Widget?retryWrites=true&w=majority") \
            .mode("append") \
            .save()

        # Read back the inserted document to get the _id
        inserted_widget = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.Widget?retryWrites=true&w=majority") \
            .option("pipeline", json.dumps([
                {"$match": {
                    "sequence": widget['sequence'],
                    "roleId": {"$oid": widget['roleId']},
                    "legend": widget['legend'],
                    "name": widget['name'],
                    "title": widget['title'],
                    "chartType": widget['chartType']
                }},
                {"$sort": {"_id": -1}},
                {"$limit": 1}
            ])) \
            .load()

        # Convert to dictionary and get the _id
        inserted_widget_dict = inserted_widget.first().asDict()
        inserted_widget_dict['_id'] = str(inserted_widget_dict['_id']['oid'])
        inserted_widget_dict['roleId'] = str(inserted_widget_dict['roleId']['oid'])
        inserted_widget_dict['reportId'] = str(inserted_widget_dict['reportId']['oid'])


        return jsonify({"message": "Widget created successfully", "widget": inserted_widget_dict}), 201

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500



@widget_blueprint.route('/update_widget/<widget_id>', methods=['PUT'])
def update_widget(widget_id):
    try:
        # Validate widget_id
        if not ObjectId.is_valid(widget_id):
            return jsonify({"error": "Invalid widget ID"}), 400

        # Get data from request body
        data = request.json

        # Validate that at least one field to update is provided
        updateable_fields = ['sequence', 'roleId', 'legend', 'name', 'title', 'chartType', 'action', 'config']
        if not any(field in data for field in updateable_fields):
            return jsonify({"error": "No valid fields to update provided"}), 400

        # Read the existing widget
        existing_widget = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.Widget?retryWrites=true&w=majority") \
            .option("pipeline", json.dumps([
                {"$match": {"_id": {"$oid": widget_id}}},
                {"$limit": 1}
            ])) \
            .load()

        if existing_widget.count() == 0:
            return jsonify({"error": "Widget not found"}), 404

        # Convert to dictionary
        widget_dict = existing_widget.first().asDict()

         # Update the fields
        for field in updateable_fields:
            if field in data:
                if field == 'roleId':
                    # Validate roleId format
                    if not ObjectId.is_valid(data['roleId']):
                        return jsonify({"error": "Invalid roleId format"}), 400
                    widget_dict[field] = {"oid": data[field]}
                elif field == 'sequence':
                    if not isinstance(data[field], int):
                        return jsonify({"error": "sequence must be an integer"}), 400
                    widget_dict[field] = data[field]
                elif field in ['legend', 'name']:
                    if not isinstance(data[field], bool):
                        return jsonify({"error": f"{field} must be boolean"}), 400
                    widget_dict[field] = data[field]
                elif field == 'action':
                    if not isinstance(data[field], list):
                        return jsonify({"error": "action must be a list"}), 400
                    for action in data[field]:
                        if not isinstance(action, dict) or 'name' not in action or 'filter' not in action:
                            return jsonify({"error": "Each action must be a dictionary with 'name' and 'filter' keys"}), 400
                    widget_dict[field] = data[field]
                elif field == 'config':
                    if not isinstance(data[field], dict) or 'filter' not in data[field] or 'groupBy' not in data[field]:
                        return jsonify({"error": "config must be a dictionary with 'filter' and 'groupBy' keys"}), 400
                    if not isinstance(data[field]['filter'], list) or not isinstance(data[field]['groupBy'], list):
                        return jsonify({"error": "config.filter and config.groupBy must be lists"}), 400
                    for filter_item in data[field]['filter']:
                        if not isinstance(filter_item, dict) or 'operation' not in filter_item or 'value' not in filter_item:
                            return jsonify({"error": "Each filter item must be a dictionary with 'operation' and 'value' keys"}), 400
                    widget_dict[field] = data[field]
                else:
                    widget_dict[field] = data[field]

        # Add last_updated timestamp
        widget_dict['last_updated'] = datetime.utcnow().isoformat()

        # Create updated DataFrame
        updated_df = spark.createDataFrame([widget_dict])

        # Ensure roleId is in the correct format for MongoDB
        if 'roleId' in widget_dict:
            updated_df = updated_df.withColumn("roleId", struct(lit(widget_dict['roleId']['oid']).alias("oid")))

        # Write the updated DataFrame back to MongoDB
        updated_df.write.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.Widget?retryWrites=true&w=majority") \
            .option("replaceDocument", "true") \
            .mode("append") \
            .save()

        # Convert ObjectId to string for JSON serialization
        widget_dict['_id'] = str(widget_dict['_id'])
        if 'roleId' in widget_dict:
            widget_dict['roleId'] = str(widget_dict['roleId']['oid'])

        return jsonify({"message": "Widget updated successfully", "widget": widget_dict}), 200

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500



@widget_blueprint.route('/get_widget/<widget_id>', methods=['GET'])
def get_widget(widget_id):
    try:
        # Validate widget_id
        if not ObjectId.is_valid(widget_id):
            return jsonify({"error": "Invalid widget ID"}), 400

        # Read the Widget collection
        widgets_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.Widget?retryWrites=true&w=majority") \
            .load()

        # Filter for the specific widget
        widget_df = widgets_df.filter(col("_id.oid") == widget_id)

        # Check if the widget exists
        if widget_df.count() == 0:
            return jsonify({"error": "Widget not found"}), 404

        # Retrieve the widget data
        widget = widget_df.first()

        if widget is None:
            return jsonify({"error": "Failed to retrieve widget data"}), 500

        # Convert to dictionary and prepare for JSON serialization
        widget_dict = widget.asDict()
        widget_dict['_id'] = str(widget_dict['_id']['oid'])

        return jsonify({"widget": widget_dict}), 200

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500



@widget_blueprint.route('/get_widgets_by_role', methods=['GET'])
def get_widgets_by_role():
    try:
        # Get roleId from query parameters
        role_id = request.args.get('roleId')

        if not role_id:
            return jsonify({"error": "roleId is required"}), 400

        # Validate roleId format
        if not ObjectId.is_valid(role_id):
            return jsonify({"error": "Invalid roleId format"}), 400

        # Create a PySpark DataFrame from MongoDB collection
        widgets_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.Widget?retryWrites=true&w=majority") \
            .load()

        # Filter the DataFrame by roleId
        filtered_widgets = widgets_df.filter(col("roleId.oid") == role_id)

        # Convert DataFrame to a list of dictionaries
        widgets_list = filtered_widgets.select(
            col("_id.oid").alias("_id"),
            col("roleId.oid").alias("roleId"),
            "sequence",
            "legend",
            "name",
            "title",
            "chartType",
            "reportId",
            "action",
            "config",
            "last_updated"
        ).toJSON().map(lambda j: json.loads(j)).collect()

        # Convert ObjectId to string for JSON serialization
        for widget in widgets_list:
            widget['_id'] = str(widget['_id'])
            widget['roleId'] = str(widget['roleId'])

        if not widgets_list:
            return jsonify({"message": f"No widgets found for roleId '{role_id}'"}), 404

        return jsonify({"widgets": widgets_list})

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500
 

# @widget_blueprint.route('/generate_widget_graph/<widget_id>', methods=['GET'])
# def generate_widget_graph(widget_id):
    try:
        # Validate widget_id
        if not ObjectId.is_valid(widget_id):
            return jsonify({"error": "Invalid widget ID"}), 400

        # Fetch the widget
        widget_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.Widget?retryWrites=true&w=majority") \
            .load() \
            .filter(col("_id.oid") == widget_id)

        if widget_df.count() == 0:
            return jsonify({"error": "Widget not found"}), 404

        widget = widget_df.first()
        report_id = widget['reportId']['oid']

        # Fetch report data
        report_data_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.reportData?retryWrites=true&w=majority") \
            .load() \
            .filter(col("_id.oid") == report_id)

        if report_data_df.count() == 0:
            return jsonify({"error": "No report data found for this widget"}), 404

        # Convert report data to JSON
        report_data_json = report_data_df.select(to_json(struct("*")).alias("json")).collect()[0]['json']
        report_data = json.loads(report_data_json)

        # Prepare data for LLM
        llm_input = {
            "widget_config": widget.asDict(),
            "report_data": report_data
        }

        # Call LLM API (using Groq in this example)
        GROQ_API_KEY = os.getenv("GROQ_API_KEY")
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        llm_response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers,
            json={
                "model": "mixtral-8x7b-32768",
                "messages": [
                    {"role": "system", "content": "You are an AI assistant that generates HTML code for graphs based on widget configurations and report data. Always format your response with 'DESCRIPTION:' followed by the description about the graph, then 'HTML:' followed by the HTML code."},
                    {"role": "user", "content": f"Generate HTML code for a graph based on this widget configuration and report data: {json.dumps(llm_input)}. Provide a brief description of the graph, then the HTML code to create it."}
                ]
            }
        )

        if llm_response.status_code != 200:
            return jsonify({"error": "Failed to generate graph from LLM"}), 500

        llm_output = llm_response.json()['choices'][0]['message']['content']

        # Split the output into description and HTML
        parts = llm_output.split('HTML:', 1)
        description = parts[0].replace('DESCRIPTION:', '').strip()
        html_code = parts[1].strip() if len(parts) > 1 else ""

        # Remove any markdown code block indicators
        html_code = html_code.replace("```html", "").replace("```", "").strip()

        return jsonify({
            "html_code": html_code,
            "description": description
        }), 200

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500