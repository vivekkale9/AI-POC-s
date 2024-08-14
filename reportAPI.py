from flask import Blueprint, jsonify, request, send_file
from io import BytesIO
import hashlib
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime
from bson import ObjectId
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, struct, lit, collect_list,lower, desc, asc, expr,to_json, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType
import json
import functools
from functools import reduce
import re
import os
from dotenv import load_dotenv

from enums import ReportStatus
from enum_1 import FilterType

# Load environment variables
load_dotenv()

mongo_uri = "mongodb+srv://raceuser2:CgDbFvKawgjroQvk@cwp-dev-cluster2.fzvgy.mongodb.net"
database = "devcwp"

# Access environment variables
# mongo_uri = os.getenv("MONGO_URI")
# database = os.getenv("DATABASE")

# Import Spark session
from sparkConfig import spark

report_blueprint = Blueprint('report', __name__)

@report_blueprint.route('/create_report', methods=['POST'])
def create_report():
    try:
        # Get data from request body
        data = request.json

        # Validate required fields
        required_fields = ['name','MasterBusinessObjectId', 'filter', 'groupBy', 'status']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Validate MasterBusinessObjectId format
        if not ObjectId.is_valid(data['MasterBusinessObjectId']):
            return jsonify({"error": "Invalid MasterBusinessObjectId format"}), 400
        
        # Validate roleId format
        if not ObjectId.is_valid(data['roleId']):
            return jsonify({"error": "Invalid roleId format"}), 400
        

        print("type of col('_id')",type(col("_id")))
        # Fetch the MasterBO entry
        master_bo_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority") \
            .load()
        
        print("MasterBO DataFrame Schema:")
        master_bo_df.printSchema()
        

        # Now filter the DataFrame
        master_bo = master_bo_df.filter(col("_id.oid") == data['MasterBusinessObjectId']).first()
        print("MasterBO entry:", master_bo)


        
        if not master_bo:
            return jsonify({"error": "MasterBusinessObject not found"}), 404
        
        # Extract fields from MasterBO
        fields = master_bo['fields']
        fields = [json.loads(field) for field in fields]
        
        # Ensure filter is from fields
        field_names = [field['name'] for field in fields]
        for filter_item in data["filter"]:
            if filter_item['key'] not in field_names:
                return jsonify({"error": f"Filter key '{filter_item['key']}' is not in the fields array"}), 400

        
        # Check if a report with the same name already exists
        existing_report = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .load()
        
        if existing_report.count() > 0:
            existing_report = existing_report.filter(col("name") == data['name'])
            if existing_report.count() > 0:
                return jsonify({"error": f"A report with the name '{data['name']}' already exists"}), 409

        # Create report document
        report = {
            "name": data['name'],
            "desc": data['desc'],
            "MasterBusinessObjectId": data['MasterBusinessObjectId'],
            "fields": fields,
            "filter": data['filter'],
            "modifiedBy": "",
            "groupBy": data['groupBy'],
            "status": data['status'],
            "createdAt": datetime.utcnow().isoformat(),
            "roleId": data['roleId']
        }

        # Define schema for the report
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("desc", StringType(), True),
            StructField("MasterBusinessObjectId", StringType(), True),
            StructField("fields", ArrayType(StructType([
                StructField("displayName", StringType(), True),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
                StructField("filterType", StringType(), True),
                StructField("order", IntegerType(), True),
                StructField("sticky", BooleanType(), True),
                StructField("visible", BooleanType(), True),
                StructField("isEmail", BooleanType(), True),
                StructField("multiValued", BooleanType(), True),
            ])), True),
            StructField("filter", ArrayType(StructType([
                StructField("key", StringType(), True),
                StructField("operation", StringType(), True),
                StructField("value", StringType(), True)
            ])), True),
            StructField("modifiedBy", StringType(), True),
            StructField("groupBy", ArrayType(StringType()), True),
            StructField("status", IntegerType(), True),
            StructField("createdAt", StringType(), True),
            StructField("roleId", StringType(), True)
        ])

        # Create a DataFrame with the report
        report_df = spark.createDataFrame([report], schema)

        # Convert MasterBusinessObjectId to struct format that MongoDB expects for ObjectId
        report_df = report_df.withColumn("MasterBusinessObjectId", struct(lit(report['MasterBusinessObjectId']).alias("oid")))

        report_df = report_df.withColumn("roleId", struct(lit(report['roleId']).alias("oid")))

        # Write the DataFrame to MongoDB
        report_df.write.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .mode("append") \
            .save()

        return jsonify({
            "message": "Report created successfully",
            "report": report
        }), 201

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500


from flask import jsonify
from pyspark.sql.functions import count

@report_blueprint.route('/total_reports', methods=['GET'])
def get_total_reports():
    try:
        # Read the ReportSchema collection
        report_schema_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .load()
        
        # Check if the DataFrame is empty
        if report_schema_df.rdd.isEmpty():
            return jsonify({"error": "ReportSchema collection is empty"}), 404
        
        # Count the total number of reports
        total_reports = report_schema_df.count()
        
        # Return the result
        return jsonify({
            "total_reports": total_reports
        }), 200

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500
    

@report_blueprint.route('/update_report/<report_id>', methods=['PUT'])
def update_report(report_id):
    try:
        # Get data from request body
        data = request.json

        # Validate that at least one field to update is provided
        updateable_fields = ['name', 'MasterBusinessObjectId', 'filter','fields', 'groupBy', 'status','desc']
        if not any(field in data for field in updateable_fields):
            return jsonify({"error": "No valid fields to update provided"}), 400

        # Validate report_id
        if not ObjectId.is_valid(report_id):
            return jsonify({"error": "Invalid report_id format"}), 400

        # Read the existing report
        existing_report = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .option("pipeline", json.dumps([
                {"$match": {"_id": {"$oid": report_id}}},
                {"$limit": 1}
            ])) \
            .load()

        if existing_report.count() == 0:
            return jsonify({"error": "Report not found"}), 404

        # Convert to dictionary
        report_dict = existing_report.first().asDict()

        field_schema = StructType([
            StructField("displayName", StringType(), True),
            StructField("name", StringType(), True),
            StructField("type", StringType(), True),
            StructField("filterType", StringType(), True),
            StructField("order", IntegerType(), True),
            StructField("sticky", BooleanType(), True),
            StructField("visible", BooleanType(), True),
            StructField("isEmail", BooleanType(), True),
            StructField("multiValued", BooleanType(), True),
        ])

        # Update the fields
        for field in updateable_fields:
            if field in data:
                if field == 'MasterBusinessObjectId':
                    # Validate MasterBusinessObjectId format
                    if not ObjectId.is_valid(data['MasterBusinessObjectId']):
                        return jsonify({"error": "Invalid MasterBusinessObjectId format"}), 400
                    report_dict[field] = {"oid": data[field]}
                elif field == 'fields':
                    if isinstance(data[field], dict):
                        # Update specific fields
                        for index, update_info in data[field].items():
                            try:
                                index = int(index)
                                if 0 <= index < len(report_dict[field]):
                                    # Convert the field to a dictionary if it's not already
                                    if isinstance(report_dict[field][index], pyspark.sql.types.Row):
                                        field_dict = report_dict[field][index].asDict()
                                    elif isinstance(report_dict[field][index], str):
                                        field_dict = json.loads(report_dict[field][index])
                                    else:
                                        field_dict = report_dict[field][index]
                                    
                                    field_dict.update(update_info)
                                    # Convert back to a Row object
                                    report_dict[field][index] = pyspark.sql.Row(**field_dict)
                            except ValueError:
                                print(f"Invalid index: {index}")

                

        # Add last_updated timestamp
        report_dict['last_updated'] = datetime.utcnow().isoformat()
        report_dict['modifiedBy'] = ""

        # Create updated DataFrame
        updated_df = spark.createDataFrame([report_dict])
        updated_df = updated_df.withColumn("fields", to_json(col("fields")))
        updated_df = updated_df.withColumn("fields", from_json(col("fields"), ArrayType(field_schema)))


        # Ensure MasterBusinessObjectId is in the correct format for MongoDB
        if 'MasterBusinessObjectId' in report_dict:
            updated_df = updated_df.withColumn("MasterBusinessObjectId", struct(lit(report_dict['MasterBusinessObjectId']['oid']).alias("oid")))

        # Write the updated DataFrame back to MongoDB
        updated_df.write.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .option("replaceDocument", "true") \
            .mode("append") \
            .save()
    

        # Convert ObjectId to string for JSON serialization
        report_dict['_id'] = str(report_dict['_id'])
        if 'MasterBusinessObjectId' in report_dict:
            report_dict['MasterBusinessObjectId'] = str(report_dict['MasterBusinessObjectId']['oid'])

        return jsonify({"message": "Report updated successfully", "report": report_dict}), 200

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500



@report_blueprint.route('/download_report/<report_id>', methods=['GET'])
def download_report(report_id):
    try:
        # Validate report_id format
        if not ObjectId.is_valid(report_id):
            return jsonify({"error": "Invalid report ID format"}), 400

        # Read the report_data collection
        report_data_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.report_data?retryWrites=true&w=majority") \
            .load()

        # Filter for the specific report
        report_data = report_data_df.filter(col("_id.oid") == report_id)

        if report_data.count() == 0:
            return jsonify({"error": "Report not found"}), 404

        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = report_data.toPandas()

        # Handle the 'data' column which is an object
        if 'data' in pandas_df.columns:
            pandas_df['data'] = pandas_df['data'].apply(lambda x: str(x) if x else '')

        # Create an Excel file
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            pandas_df.to_excel(writer, sheet_name='Report Data', index=False)

        output.seek(0)

        # Generate a filename
        filename = f"report_{report_id}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500



@report_blueprint.route('/get_data', methods=['GET'])
def getData():
    try:
        # Get parameters from the request
        report_id = request.args.get('reportId')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')

        # Get pagination parameters from the request
        start = int(request.args.get('start', 0))
        end = int(request.args.get('end', 10))


        # Ensure start is not negative and end is greater than start
        start = max(0, start)
        end = max(start + 1, end)

        # Get extra filters from the request body
        extra_filters = request.json.get('extraFilters', []) if request.is_json else []


        # Validate parameters
        if not report_id:
            return jsonify({"error": "Missing required parameters: reportId"}), 400

        # Convert string dates to datetime objects
        if start_date and end_date:
            try:
                start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except ValueError:
                return jsonify({"error": "Invalid date format. Use ISO format (YYYY-MM-DDTHH:MM:SS.mmmZ)"}), 400
        else:
            start_date = None
            end_date = None

        # Generate list of dates
        if start_date and end_date:
            date_list = pd.date_range(start=start_date, end=end_date).tolist()
        else:
            date_list = None

        # Function to check if data exists for a date in reportData and return it if it does
        def check_and_get_date_data(date=None):
            
            reportData_df = spark.read.format("mongo") \
                .option("uri", f"{mongo_uri}/{database}.reportData?retryWrites=true&w=majority") \
                .load()
            
            if reportData_df.rdd.isEmpty():
                return (date, None)
            
            if date:
                date_str = date.strftime("%Y-%m-%d")
                existing_data = reportData_df.filter((col("date") == date_str) & (col("reportId") == report_id)).collect()
            else:
                existing_data = reportData_df.filter(col("reportId") == report_id).collect()

            if existing_data:
                return (date, [row.asDict() for row in existing_data])
            return (date, None)

        # Check existing data in parallel
        if date_list:
            with ThreadPoolExecutor(max_workers=10) as executor:
                date_data = dict(executor.map(check_and_get_date_data, date_list))
        else:
            date_data = {None: check_and_get_date_data()[1]}

        # Fetch report schema
        report_schema_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .load()
        
        if report_schema_df.rdd.isEmpty():
           return jsonify({"error": "report_schema_df not found"}), 404
        
        report_schema = report_schema_df.filter(col("_id.oid") == report_id).first()

        if not report_schema:
            return jsonify({
                "reports": []
            }), 200

        # Fetch MasterBO information
        master_bo = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority") \
            .load() \
            .filter(col("_id.oid") == report_schema["MasterBusinessObjectId"].oid) \
            .first()
        
        field_types = {}
        for field in report_schema["fields"]:
            if isinstance(field, str):
                field_obj = json.loads(field)
                field_types[field_obj["name"]] = field_obj["type"]
            elif isinstance(field, dict):
                field_types[field["name"]] = field["type"]
            elif isinstance(field, pyspark.sql.types.Row):
                field_dict = field.asDict()
                field_types[field_dict["name"]] = field_dict["type"]
        

        if not master_bo:
            return jsonify({"error": "MasterBO not found"}), 404

        # Extract filter, groupBy, and table information
        filter_list = [{"key": filter_row["key"], "operation": filter_row["operation"], "value": filter_row["value"]} for filter_row in report_schema["filter"]]
        groupby_columns = report_schema["groupBy"]
        groupby_columns = groupby_columns if isinstance(groupby_columns, list) else [groupby_columns]
        table_name = master_bo["table"]
        report_fields = []
        for field in report_schema["fields"]:
            if isinstance(field, str):
                field_obj = json.loads(field)
                report_fields.append(field_obj["name"])
            elif isinstance(field, dict):
                report_fields.append(field["name"])
            elif isinstance(field, pyspark.sql.types.Row):
                field_dict = field.asDict()
                report_fields.append(field_dict["name"])


        plant_data_df = spark.read.format("mongo") \
                .option("uri", f"{mongo_uri}/{database}.{table_name}?retryWrites=true&w=majority") \
                .load()
        
        def apply_extra_filters(df):
            for filter_item in extra_filters:
                key = filter_item.get('key')
                operation = filter_item.get('operation')
                value = filter_item.get('value')

                column_type = field_types.get(key, "STRING")
                if column_type.upper() == 'MULTI':
                    if isinstance(value, list):
                        if operation == "=":
                            df = df.filter(col(f"data.{key}").isin(value))
                        elif operation == "!=":
                            df = df.filter(~col(f"data.{key}").isin(value))
                    else:
                        # Handle single value for multi-select field
                        if operation == "=":
                            df = df.filter(col(f"data.{key}") == value)
                        elif operation == "!=":
                            df = df.filter(col(f"data.{key}") != value)
                elif column_type.upper() == 'STRING':
                    if operation == "=":
                        df = df.filter(col(f"data.{key}") == value)
                    elif operation == "!=":
                        df = df.filter(col(key) != value)
                    elif operation.lower() == "contains":
                        df = df.filter(col(key).contains(value))
                    elif operation.lower() == "not contains":
                        df = df.filter(~col(key).contains(value))
                    elif operation.lower() == "startswith":
                        df = df.filter(col(key).startswith(value))
                elif column_type.upper() == 'INT':
                    if operation == "=":
                        df = df.filter(col(f"data.{key}") == value)
                    elif operation == "!=":
                        df = df.filter(col(key) != value)
                    elif operation == ">":
                        df = df.filter(col(key) > value)
                    elif operation == "<":
                        df = df.filter(col(key) < value)
                    elif operation == ">=":
                        df = df.filter(col(key) >= value)
                    elif operation == "<=":
                        df = df.filter(col(key) <= value)
                elif column_type.upper() == 'DATE':
                    date_field = to_date(col(key))
                    if operation == "=":
                        df = df.filter((date_field == value))
                    elif operation == ">=":
                        df = df.filter((date_field >= value))
                    elif operation == "<=":
                        df = df.filter((date_field <= value))
                    elif operation == ">":
                        df = df.filter((date_field > value))
                    elif operation == "<":
                        df = df.filter((date_field < value))
            return df
        
        def parse_field(field):
            if isinstance(field, str):
                try:
                    return json.loads(field)
                except json.JSONDecodeError:
                    return {}
            elif isinstance(field, dict):
                return field
            elif isinstance(field, pyspark.sql.types.Row):
                return field.asDict()
            else:
                return {}


        # Function to fetch, group, and save data for a single date
        def process_data_for_date(date=None):
            if date_data[date]:
                return date_data[date]  # Return existing data

            if date:
                date_str = date.strftime("%Y-%m-%d")
            filter_conditions = []
            for filter_item in filter_list:
                key = filter_item["key"]
                operation = filter_item["operation"]
                value = filter_item["value"]

                # Get the column type from field_types
                column_type = field_types.get(key, "STRING")
                if column_type.upper() == 'MULTI':
                    if isinstance(value, list):
                        if operation == "=":
                            filter_conditions.append(col(key).isin(value))
                        elif operation == "!=":
                            filter_conditions.append(~col(key).isin(value))
                    else:
                        # Handle single value for multi-select field
                        if operation == "=":
                            filter_conditions.append(col(key) == value)
                        elif operation == "!=":
                            filter_conditions.append(col(key) != value)
                elif column_type.upper() == 'STRING':
                    if operation == "contains":
                        filter_conditions.append(col(key).contains(value))
                    elif operation == "not contains":
                        filter_conditions.append(~col(key).contains(value))
                    if operation == "=":
                        filter_conditions.append(col(key) == value)
                    elif operation == "!=":
                        filter_conditions.append(col(key) != value)
                    elif operation == "starts with":
                        filter_conditions.append(col(key).startswith(value))

                elif column_type.upper() == 'INT':
                    if operation == "=":
                        filter_conditions.append(col(key) == value)
                    elif operation == "!=":
                        filter_conditions.append(col(key) != value)
                    elif operation == "<":
                        filter_conditions.append(col(key) < value)
                    elif operation == ">":
                        filter_conditions.append(col(key) > value)
                    elif operation == "<=":
                        filter_conditions.append(col(key) <= value)
                    elif operation == ">=":
                        filter_conditions.append(col(key) >= value)

                elif column_type.upper() == 'DATE':
                    # Check if the filter key is a valid date field
                    date_field = to_date(col(key))
                    if operation == "=":
                            filter_conditions.append((date_field == date.date()))
                    elif operation == ">=":
                        filter_conditions.append((date_field >= date.date()))
                    elif operation == "<=":
                        filter_conditions.append((date_field <= date.date()))
                    elif operation == ">":
                        filter_conditions.append((date_field > date.date()))
                    elif operation == "<":
                        filter_conditions.append((date_field < date.date()))
                    

                if filter_conditions:
                    filtered_df = plant_data_df.filter(reduce(lambda x, y: x & y, filter_conditions))
                else:
                    filtered_df = plant_data_df

            # If no data for this date, stop processing
            if filtered_df.count() == 0:
                return None
            
            # Get the list of columns in the filtered data
            filtered_columns = filtered_df.columns


            # Perform lookups for each prefix in lookupFields
            for lookup in master_bo["lookupFields"]:
                prefix = lookup["fieldPrefix"]
                key = lookup["key"]
                table = lookup["table"]

                # Check if any column in filtered_df starts with the prefix (case-insensitive)
                prefix_columns = [col for col in filtered_columns if col.lower().startswith(prefix)]

               
                if prefix_columns: 
                    # Load the lookup table
                    lookup_df = spark.read.format("mongo") \
                        .option("uri", f"{mongo_uri}/{database}.{table}?retryWrites=true&w=majority") \
                        .load()

                    # Get the fields to fetch from the current MasterBO entry
                    lookup_fields = []
                    for field in master_bo["fields"]:
                        parsed_field = parse_field(field)
                        if "lookupTable" in parsed_field:
                            if parsed_field["lookupTable"] == table:
                                lookup_fields.append(parsed_field["name"])
                    

                    existing_lookup_fields = [field for field in lookup_fields if field in lookup_df.columns]

                    lookup_df_renamed = lookup_df.withColumnRenamed("_id", "lookup_id")

                    filtered_df = filtered_df.join(
                        lookup_df_renamed.select("lookup_id", *existing_lookup_fields),
                        col(f"{key}.oid").cast("string") == col("lookup_id.oid").cast("string"),
                        "left_outer"
                    )

                    filtered_df = filtered_df.drop("lookup_id")

                    # Rename the lookup fields with the correct prefix
                    for field in existing_lookup_fields:
                        if field != key:
                            filtered_df = filtered_df.withColumnRenamed(field, f"{prefix}{field}")
                
                    

            # Create reportData entry
            if date:
                date_str = date.strftime("%Y-%m-%d")
            else:
                date_str = "all"


            reportData_entry = filtered_df.select(
                lit(date_str).alias("date"),
                lit(report_id).alias("reportId"),
                struct(filtered_df.columns).alias("data")
            )


            # Save to reportData collection
            reportData_entry.write.format("mongo") \
                .mode("append") \
                .option("uri", f"{mongo_uri}/{database}.reportData?retryWrites=true&w=majority") \
                .save()
            
            

            return reportData_entry.collect()

        # Process data for dates that don't have existing data
        if date_list:
            with ThreadPoolExecutor(max_workers=10) as executor:
                new_results = list(executor.map(process_data_for_date, [date for date, data in date_data.items() if data is None]))
        else:
            new_results = [process_data_for_date()] if date_data[None] is None else []

        # Combine existing data and new results
        all_results = []
        for data in date_data.values():
            if data:
                # Convert existing data to DataFrame
                existing_df = spark.createDataFrame(data)
                # Apply extra filters
                filtered_df = apply_extra_filters(existing_df)
                all_results.extend(filtered_df.collect())
        # Add new results to all_results
        for result in new_results:
            if result is not None:
                all_results.extend(result)



        if not all_results:
            return jsonify({"message": "No data found for the given date range"}), 404
        
        def json_serialize(obj):
            if isinstance(obj, ObjectId):
                return str(obj)
            elif isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, pyspark.sql.types.Row):
                return obj.asDict()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        def process_field(value):
            if isinstance(value, list):
                if len(value) == 1 and isinstance(value[0], (str, ObjectId)):
                    return str(value[0])
                return [str(item) if isinstance(item, ObjectId) else item for item in value]
            elif isinstance(value, ObjectId):
                return str(value)
            return value
        
        def standardize_response(data):

            if isinstance(data, pyspark.sql.types.Row):
                # This is the format when data is newly generated or from existing data
                row_data = data.data if hasattr(data, 'data') else data

                serializable_row_data = json.loads(json.dumps(row_data.asDict(), default=json_serialize))

                # Process each field
                processed_data = {field: process_field(serializable_row_data.get(field)) for field in report_fields if field in serializable_row_data}

                return {
                    **processed_data,
                    "date": data.date if hasattr(data, 'date') else None,
                    "reportId": data.reportId if hasattr(data, 'reportId') else None
                }
            elif isinstance(data, dict):
                # This is the format when data is already present as a dictionary
                row_data = data.get("data", {})

                serializable_row_data = json.loads(json.dumps(row_data, default=json_serialize))

                # Process each field
                processed_data = {field: process_field(serializable_row_data.get(field)) for field in report_fields if field in serializable_row_data}

                return {
                    **processed_data,
                    "date": data.get("date"),
                    "reportId": data.get("reportId")
                }
            else:
                # Unknown format, return as is
                return data
            

        standardized_results = [standardize_response(result) for result in all_results]
        total_results = len(standardized_results)

        # Apply pagination
        paginated_results = standardized_results[start:end]

        def enum_to_dict(enum_class):
            return {k: v.value for k, v in enum_class.__members__.items()}

        def format_report_schema(schema):
            schema_dict = {key: schema[key] for key in schema.asDict().keys()}
            
            formatted_report = {
                "businessObject": master_bo.BusinessObject if hasattr(master_bo, 'BusinessObject') else "",  # You might want to set this dynamically if it varies
                "createdAt": schema_dict.get("createdAt"),
                "desc": schema_dict.get("desc"),
                "fields": schema_dict.get("fields", []),
                "filter": schema_dict.get("filter", []),
                "groupBy": schema_dict.get("groupBy", []),
                "id": schema_dict["_id"].oid if "_id" in schema_dict else None,
                "last_updated": schema_dict.get("last_updated"),
                "masterBusinessObjectId": schema_dict["MasterBusinessObjectId"].oid if "MasterBusinessObjectId" in schema_dict else None,
                "modifiedBy": schema_dict.get("modifiedBy") or "",
                "name": schema_dict.get("name"),
                "product": master_bo.product if hasattr(master_bo, 'product') else "",  # You might want to set this dynamically if it varies
                "roleId": schema_dict["roleId"].oid if "roleId" in schema_dict else None,
                "status": schema_dict.get("status")
            }

            # Parse fields
            formatted_report["fields"] = [
                parse_field(field) for field in formatted_report["fields"]
            ]


            # Parse filter
            formatted_report["filter"] = [
                filter_item.asDict() if isinstance(filter_item, pyspark.sql.types.Row) else filter_item
                for filter_item in formatted_report["filter"]
            ]

            return formatted_report

        # Use the function to format the report schema
        formatted_report_schema = format_report_schema(report_schema)

        response = {
            "report": formatted_report_schema,
            "data": paginated_results,
            "pagination": {
                "total_count": total_results,
                "start": start,
                "end": min(end, total_results),
                "has_more": end < total_results
            }
        }
        filter_options = enum_to_dict(FilterType)

        # Add filter options to the response
        response["filter_options"] = filter_options
            
        return jsonify(response)
    
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500  
 

@report_blueprint.route('/check_report_name', methods=['GET'])
def check_report_name():
    try:
        name = request.args.get('name')
        if not name:
            return jsonify({"error": "Name parameter is required"}), 400

        # Read the ReportSchema collection
        existing_report = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .load()
        
        # Check if a report with the same name exists
        name_exists = existing_report.filter(lower(col("name")) == name.lower()).count() > 0

        return jsonify({"exists": name_exists})

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500
    

@report_blueprint.route('/copy_report/<report_id>', methods=['POST'])
def copy_report(report_id):
    try:
        # Validate report_id
        if not ObjectId.is_valid(report_id):
            return jsonify({"error": "Invalid report_id format"}), 400

        # Read the existing report
        existing_report = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .option("pipeline", json.dumps([
                {"$match": {"_id": {"$oid": report_id}}},
                {"$limit": 1}
            ])) \
            .load()

        if existing_report.count() == 0:
            return jsonify({"error": "Report not found"}), 404

        # Convert to dictionary
        report_dict = existing_report.first().asDict()

        # Generate new name for the copy
        original_name = report_dict['name']
        new_name = generate_copy_name(original_name)

        # Create a copy of the report with the new name
        copy_dict = report_dict.copy()
        copy_dict['name'] = new_name
        copy_dict['createdAt'] = datetime.utcnow().isoformat()
        del copy_dict['_id']  # Remove the original ID

        # Create updated DataFrame
        copy_df = spark.createDataFrame([copy_dict])

        # Ensure MasterBusinessObjectId is in the correct format for MongoDB
        if 'MasterBusinessObjectId' in copy_dict:
            copy_df = copy_df.withColumn("MasterBusinessObjectId", struct(lit(copy_dict['MasterBusinessObjectId']['oid']).alias("oid")))

        # Write the copied report to MongoDB
        copy_df.write.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .mode("append") \
            .save()

        return jsonify({"message": "Report copied successfully", "new_report_name": new_name}), 201

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500

def generate_copy_name(original_name):
    # Read all existing reports
    all_reports = spark.read.format("mongo") \
        .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
        .load()

    # Get all report names
    report_names = [row['name'] for row in all_reports.select('name').collect()]

    # Function to extract base name and copy numbers
    def extract_base_and_copies(name):
        parts = name.split(" copy(")
        base = parts[0]
        copies = [int(p[:-1]) for p in parts[1:]]
        return base, copies

    base, copies = extract_base_and_copies(original_name)

    if not copies:
        # This is a copy of an original report
        copy_number = 1
        while f"{base} copy({copy_number})" in report_names:
            copy_number += 1
        return f"{base} copy({copy_number})"
    else:
        # This is a copy of a copy
        last_copy_number = copies[-1]
        new_copy_number = 1
        while f"{original_name} copy({new_copy_number})" in report_names:
            new_copy_number += 1
        return f"{original_name} copy({new_copy_number})"


@report_blueprint.route('/get_reports_by_role', methods=['GET'])
def get_reports_by_role():
    try:
        # Get roleId from query parameters
        role_id = request.args.get('roleId')

        if not role_id:
            return jsonify({"error": "roleId is required"}), 400

        # Validate ObjectId
        if not ObjectId.is_valid(role_id):
            return jsonify({"error": "Invalid roleId format"}), 400


        pipeline = [
            {"$match": {"roleId": {"$oid": role_id}}},
            {"$project": {
                "_id": {"$toString": "$_id"},
                "MasterBusinessObjectId": 1,
                "fields": 1,
                "filter": 1,
                "groupBy": 1,
                "status": 1,
                "date": 1,
                "roleId": {"$toString": "$roleId"}
            }}
        ]

        # Read ReportSchema collection with the specific pipeline
        reports_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .option("pipeline", json.dumps(pipeline, default=str)) \
            .load()
        
        # Convert to list of dictionaries
        reports_list = reports_df.toJSON().map(lambda j: json.loads(j)).collect()

        if not reports_list:
            return jsonify({"message": f"No reports found for roleId '{role_id}'"}), 404

        return jsonify({"reports": reports_list})

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500
    

    
@report_blueprint.route('/search_reports', methods=['GET'])
def search_reports():
    try:
        # Get search parameters from the request
        name = request.args.get('name', '')
        description = request.args.get('description', '')
        created_by = request.args.get('createdBy', '')
        created_at_start = request.args.get('createdAtStart', '')
        created_at_end = request.args.get('createdAtEnd', '')

        # Read the ReportSchema collection
        report_schema_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .load()

        # Apply filters based on search parameters
        if name:
            report_schema_df = report_schema_df.filter(lower(col("name")).contains(name.lower()))
        if description:
            report_schema_df = report_schema_df.filter(lower(col("desc")).contains(description.lower()))
        if created_by:
            report_schema_df = report_schema_df.filter(col("createdBy") == created_by)
        if created_at_start:
            report_schema_df = report_schema_df.filter(col("createdAt") >= created_at_start)
        if created_at_end:
            report_schema_df = report_schema_df.filter(col("createdAt") <= created_at_end)

        # Select relevant columns and convert to list of dictionaries
        result = report_schema_df.select(
            col("_id").cast("string").alias("id"),
            "name",
            "desc",
            "createdAt",
            "last_updated",
            "fields",
            "filter",
            "groupBy",
            col("MasterBusinessObjectId").cast("string").alias("MasterBusinessObjectId"),
            col("roleId").cast("string").alias("roleId"),
            "status"
        ).limit(100).toJSON().map(lambda j: json.loads(j)).collect()

        if not result:
            return jsonify({"message": "No reports found matching the search criteria"}), 404

        return jsonify({"reports": result})

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500


@report_blueprint.route('/get_recent_reports', methods=['GET'])
def get_recent_reports():
    try:
        # Read the ReportSchema collection
        report_schema_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .load()

        # Filter reports that have been updated
        updated_reports = report_schema_df.filter(col("last_updated").isNotNull())

        # Sort by last_updated in descending order and limit to 10
        recent_reports = updated_reports.orderBy(desc("last_updated")).limit(10)

        # Select relevant columns and convert to list of dictionaries
        result = recent_reports.select(
            col("_id").cast("string").alias("id"),
            "name",
            "desc",
            "createdAt",
            "last_updated",
            "fields",
            "filter",
            "groupBy",
            col("MasterBusinessObjectId").cast("string").alias("MasterBusinessObjectId"),
            col("roleId").cast("string").alias("roleId"),
            "status"
        ).toJSON().map(lambda j: json.loads(j)).collect()

        if not result:
            return jsonify({"message": "No recently updated reports found"}), 404

        return jsonify({"recent_reports": result})

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500
    

@report_blueprint.route('/get_all_reports', methods=['GET'])
def get_all_reports():
    try:
        # Get pagination parameters from the request
        start = int(request.args.get('start', 0))
        end = int(request.args.get('end', 10))


        # Ensure start is not negative and end is greater than start
        start = max(0, start)
        end = max(start + 1, end)
        num_items = end - start

        # Get optional sorting parameter (only for description)
        sort_order = request.args.get('sort_order', 'asc')

        # Optional parameter to give recently update reports
        get_recent = request.args.get('get_recent', 'false').lower() == 'true'

        status = request.args.get('status', '').lower()

        products = request.args.get('product', '').split(',')

        # Get startDate and endDate parameters
        start_date_str = request.args.get('startDate')
        end_date_str = request.args.get('endDate')
        date_field = request.args.get('dateField')

        role_id = request.args.get('roleId')

        # Get optional filter parameters
        business_objects = request.args.get('business_object', '').split(',')

        # Get optional search parameter
        search_query = request.args.get('search')

        # Read the ReportSchema collection
        report_schema_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .load()
        
        # Exclude reports with status DRAFT (4)
        report_schema_df = report_schema_df.filter(col("status") != ReportStatus.DRAFT.value)
        print("report_schema_df sample:")
        report_schema_df.show(5, truncate=False)
        print(f"report_schema_df count: {report_schema_df.count()}")
        print("report_schema_df schema:")
        report_schema_df.printSchema()
        

        if get_recent:
            # If get_recent is True, we ignore other sorting and pagination
            report_schema_df = report_schema_df.orderBy(desc("last_updated")).limit(10)
            total_count = 10


        if role_id:
            try:
                # Validate the ObjectId
                role_object_id = ObjectId(role_id)
                report_schema_df = report_schema_df.filter(col("roleId.oid") == str(role_object_id))
            except Exception as e:
                return jsonify({"error": " Error occurred."}), 400
            
        # Apply last_updated filter (date range)
        if start_date_str and end_date_str:
            try:
                start_date = to_date(lit(start_date_str))
                end_date = to_date(lit(end_date_str))
                
                if date_field == 'last_updated':
                    report_schema_df = report_schema_df.filter(
                        (to_date(col("last_updated")) >= start_date) & (to_date(col("last_updated")) <= end_date)
                    )
                elif date_field == 'created_at':
                    report_schema_df = report_schema_df.filter(
                        (to_date(col("createdAt")) >= start_date) & (to_date(col("createdAt")) <= end_date)
                    )
                else:
                    return jsonify({"error": "Invalid dateField. Use 'last_updated' or 'created_at'"}), 400
            except ValueError:
                return jsonify({"error": "Invalid date format for startDate or endDate. Use YYYY-MM-DD format."}), 400



        # Apply status filter
        if status:
            if status == 'archived':
                report_schema_df = report_schema_df.filter(col("status") == ReportStatus.ARCHIVED.value)
            elif status == 'favorite':
                report_schema_df = report_schema_df.filter(col("status") == ReportStatus.FAVORITE.value)
            else:
                return jsonify({"error": "Invalid status. Use 'archived' or 'favorite'."}), 400


        # Apply business object filter if provided
        if business_objects and business_objects != ['']:
            master_bo_df = spark.read.format("mongo") \
                .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority") \
                .load()

            bo_conditions = functools.reduce(
                lambda a, b: a | b,
                [lower(master_bo_df["BusinessObject"]) == bo.lower() for bo in business_objects]
            )

            business_object_ids = master_bo_df.filter(bo_conditions).select("_id.oid").rdd.flatMap(lambda x: x).collect()

            report_schema_df = report_schema_df.filter(col("MasterBusinessObjectId.oid").isin(business_object_ids))


        # Apply search
        if search_query:
            search_condition = (
                expr(f"lower(name) like '%{search_query.lower()}%'") |
                expr(f"lower(desc) like '%{search_query.lower()}%'")
            )
            report_schema_df = report_schema_df.filter(search_condition)

        # Apply sorting to description
        if sort_order.lower() == 'desc':
            report_schema_df = report_schema_df.orderBy(desc("desc"))
        else:
            report_schema_df = report_schema_df.orderBy(asc("desc"))


        # Read the MasterBO collection
        master_bo_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority") \
            .load()

        # Join with report_schema_df
        joined_df = report_schema_df.join(
            master_bo_df,
            report_schema_df["MasterBusinessObjectId.oid"] == master_bo_df["_id.oid"],
            "left_outer"
        )

        # Apply product filter if provided
        if products and products != ['']:
            product_condition = functools.reduce(
                lambda a, b: a | b,
                [lower(master_bo_df["Product"]) == product.lower() for product in products]
            )
            joined_df = joined_df.filter(product_condition)

        # Get total count of reports after filtering and searching
        total_count = report_schema_df.count()


        # Apply pagination
        paginated_df = joined_df.offset(start).limit(num_items)
        print("paginated_df sample:")
        paginated_df.show(5, truncate=False)
        print(f"paginated_df count: {paginated_df.count()}")
        print("paginated_df schema:")
        paginated_df.printSchema()


        # Select relevant columns and convert to list of dictionaries
        reports = paginated_df.select(
            report_schema_df["_id.oid"].alias("id"),
            report_schema_df["name"],
            report_schema_df["desc"],
            report_schema_df["createdAt"],
            report_schema_df["last_updated"],
            report_schema_df["modifiedBy"],
            report_schema_df["status"],
            report_schema_df["MasterBusinessObjectId.oid"].alias("masterBusinessObjectId"),
            report_schema_df["roleId.oid"].alias("roleId"),
            report_schema_df["fields"],
            report_schema_df["filter"],
            report_schema_df["groupBy"],
            master_bo_df["BusinessObject"].alias("businessObject"),
            master_bo_df["Product"].alias("product")
        ).toJSON().map(lambda j: json.loads(j)).collect()

        print(f"Debug: reports: {reports}")


        if not reports:
            return jsonify({
                "reports": [],
                "pagination": {
                    "total_count": 0,
                    "start": start,
                    "end": min(end, total_count),
                    "has_more": end < total_count
                }
            }), 200

        return jsonify({
            "reports": reports,
            "pagination": {
                "total_count": total_count,
                "start": start,
                "end": min(end, total_count),
                "has_more": end < total_count
            }
        })

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500   
    
