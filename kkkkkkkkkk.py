@report_blueprint.route('/get_date', methods=['GET'])
def getData():
    try:
        # Get parameters from the request
        report_id = request.args.get('reportId')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')

        # Validate parameters
        if not all([report_id, start_date, end_date]):
            return jsonify({"error": "Missing required parameters: reportId, startDate, endDate"}), 400

        # Convert string dates to datetime objects
        try:
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        except ValueError:
            return jsonify({"error": "Invalid date format. Use ISO format (YYYY-MM-DDTHH:MM:SS.mmmZ)"}), 400

        # Generate list of dates
        date_list = pd.date_range(start=start_date, end=end_date).tolist()

        # Function to check if data exists for a date in reportData and return it if it does
        def check_and_get_date_data(date):
            date_str = date.strftime("%Y-%m-%d")
            reportData_df = spark.read.format("mongo") \
                .option("uri", f"{mongo_uri}/{database}.reportData?retryWrites=true&w=majority") \
                .load()
            
            if reportData_df.rdd.isEmpty():
                return (date, None)
            
            existing_data = reportData_df.filter((col("date") == date_str) & (col("reportId") == report_id)).collect()
            if existing_data:
                return (date, [row.asDict() for row in existing_data])
            return (date, None)

        # Check existing data in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            date_data = dict(executor.map(check_and_get_date_data, date_list))

        # Fetch report schema
        report_schema_df = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.ReportSchema?retryWrites=true&w=majority") \
            .load()
        
        if report_schema_df.rdd.isEmpty():
           return jsonify({"error": "report_schema_df not found"}), 404
        
        report_schema = report_schema_df.filter(col("_id.oid") == report_id).first()

        print(report_schema["MasterBusinessObjectId"])

        if not report_schema:
            return jsonify({"error": "Report not found"}), 404

        # Fetch MasterBO information
        master_bo = spark.read.format("mongo") \
            .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority") \
            .load() \
            .filter(col("_id.oid") == report_schema["MasterBusinessObjectId"].oid) \
            .first()

        if not master_bo:
            return jsonify({"error": "MasterBO not found"}), 404

        # Extract filter, groupBy, and table information
        filter_list = [json.loads(filter_str) for filter_str in report_schema["filter"]]
        print(filter_list)
        groupby_columns = report_schema["groupBy"]
        groupby_columns = groupby_columns if isinstance(groupby_columns, list) else [groupby_columns]
        print(groupby_columns)
        table_name = master_bo["table"]     
        print(table_name)


        plant_data_df = spark.read.format("mongo") \
                .option("uri", f"{mongo_uri}/{database}.{table_name}?retryWrites=true&w=majority") \
                .load()

        # Define schema for reportData
        reportData_schema = StructType([
            StructField("date", StringType(), False),
            StructField("reportId", StringType(), False),
            StructField("groupedData", ArrayType(StructType([
                StructField(groupby_columns[0], StringType(), True),
                StructField("data", ArrayType(StructType([
                    StructField(col, plant_data_df.schema[col].dataType, True) 
                    for col in plant_data_df.columns if col not in groupby_columns
                ])), True)
            ])), False)
        ])          


        # Function to fetch, group, and save data for a single date
        def process_data_for_date(date):
            if date_data[date]:
                return date_data[date]  # Return existing data

            filtered_df = plant_data_df.filter(to_date(col("createdAt")) == date.date())
            print("This is date filteresd",filtered_df)

            # If no data for this date, stop processing
            if filtered_df.count() == 0:
                return None

            # Apply filters
            for filter_item in filter_list:
                key = filter_item["key"]
                operation = filter_item["operation"]
                value = filter_item["value"]
                
                if operation == "<=":
                    filtered_df = filtered_df.filter(col(key) <= value)
                elif operation == ">=":
                    filtered_df = filtered_df.filter(col(key) >= value)
                elif operation == "=":
                    filtered_df = filtered_df.filter(col(key) == value)
                elif operation == "!=":  
                    filtered_df = filtered_df.filter(col(key) != value)
                elif operation == ">":
                    filtered_df = filtered_df.filter(col(key) > value)
                elif operation == "<":
                    filtered_df = filtered_df.filter(col(key) < value)
                elif operation == "contains":
                    filtered_df = filtered_df.filter(col(key).contains(value))
                elif operation == "not contains":
                    filtered_df = filtered_df.filter(~col(key).contains(value))
            
            # Get the list of columns in the filtered data
            filtered_columns = filtered_df.columns
            print(filtered_columns)

            def get_master_bo_fields(master_bo_id):
                related_master_bo_df = spark.read.format("mongo") \
                    .option("uri", f"{mongo_uri}/{database}.MasterBO?retryWrites=true&w=majority") \
                    .load() 
                
                print(related_master_bo_df)
                
                related_master_bo = related_master_bo_df.filter(col("_id.oid") == master_bo_id).first()
                
                if not related_master_bo:
                    print("hii")
                    raise ValueError(f"Related MasterBO not found for id: {master_bo_id}")
                print(related_master_bo["fields"])

                return related_master_bo["fields"]

            # # Perform lookups for each prefix in lookupFields
            for lookup in master_bo["lookupFields"]:
                prefix = lookup["fieldPrefix"]
                key = lookup["key"]
                table = lookup["table"]
                print("This is lookup table",table)
                related_master_bo_id = lookup["MasterBusinessObjectId"].oid
                print(related_master_bo_id)

                # Check if any column in filtered_df starts with the prefix (case-insensitive)
                prefix_columns = [col for col in filtered_columns if col.lower().startswith(prefix)]
                print("No prefix columns",prefix_columns)

                if prefix_columns: 
                    # Load the lookup table
                    lookup_df = spark.read.format("mongo") \
                        .option("uri", f"{mongo_uri}/{database}.{table}?retryWrites=true&w=majority") \
                        .load()

                    # Get the fields to fetch from the related MasterBO entry
                    
                    lookup_fields = get_master_bo_fields(related_master_bo_id)

                    print("Lookup fields:", lookup_fields)
                    print("Existing fields in lookup_df:", lookup_df.columns)


                    existing_lookup_fields = [field for field in lookup_fields if field in lookup_df.columns]
                    print("Fields to be selected:", existing_lookup_fields)

                    print("filtered_df schema before join:")
                    filtered_df.printSchema()

                    lookup_df_renamed = lookup_df.withColumnRenamed("_id", "lookup_id")

                    filtered_df = filtered_df.join(
                        lookup_df_renamed.select("lookup_id", *existing_lookup_fields),
                        col(key).cast("string") == col("lookup_id.oid").cast("string"),
                        "left_outer"
                    )

                    filtered_df = filtered_df.drop("lookup_id")

                    print("filtered_df schema after join:")
                    filtered_df.printSchema()

                    # Rename the lookup fields with the correct prefix
                    for field in existing_lookup_fields:
                        if field != key:
                            filtered_df = filtered_df.withColumnRenamed(field, f"{prefix}{field}")
                    
                    print("This is after the rename",filtered_df)

            # Perform groupby
            grouped_df = filtered_df.groupBy(*groupby_columns).agg(
                collect_list(
                    struct(*[filtered_df[c] for c in filtered_df.columns if c not in groupby_columns])
                ).alias("data")
            )

            print("this is groupby", grouped_df)

            # Convert to list of dictionaries
            result = grouped_df.collect()

            # Create reportData entry
            date_str = date.strftime("%Y-%m-%d")
            reportData_entry = {
                "date": date_str,
                "reportId": report_id,
                "groupedData": [
                    {
                        groupby_columns[0]: row[groupby_columns[0]],
                        "data": [item.asDict() for item in row["data"]]
                    } for row in result
                ]
            }

            # Verify the schema
            print("reportData_entry schema:")

            # Save to reportData collection
            spark.createDataFrame([reportData_entry]).write.format("mongo") \
                .mode("append") \
                .option("uri", f"{mongo_uri}/{database}.reportData?retryWrites=true&w=majority") \
                .save()

            return reportData_entry

        # Process data for dates that don't have existing data
        with ThreadPoolExecutor(max_workers=10) as executor:
            new_results = list(executor.map(process_data_for_date, [date for date, data in date_data.items() if data is None]))

        # Combine existing data and new results
        all_results = []
        for data in date_data.values():
            if data is not None:
                all_results.extend(data if isinstance(data, list) else [data])
        all_results.extend([result for result in new_results if result is not None])


        if not all_results:
            return jsonify({"message": "No data found for the given date range"}), 404

        # Apply limit to the final results
        limited_results = []
        for result in all_results:
            try:
                date = result['date']
                report_id = result['reportId']
                grouped_data = result['groupedData'] # Limit to 10 entries

                # Parse the groupedData string into a Python dictionary
                if isinstance(grouped_data, str):
                    grouped_data = json.loads(grouped_data)

                # Ensure the structure is as expected
                if 'data' in grouped_data:
                    # Limit the data array to 10 items
                    grouped_data['data'] = grouped_data['data'][:10]
                    
                    limited_results.append({
                        'date': date,
                        'reportId': report_id,
                        'groupedData': grouped_data
                    })
                else:
                    print(f"Unexpected groupedData structure for date {date}, reportId {report_id}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON for date {date}, reportId {report_id}")
            except Exception as e:
                print(f"Error processing result: {e}")
                print(f"Problematic result: {result}")
    
        # Add this debug print to see the final structure
        print("Final limited_results structure:")
        return jsonify(limited_results)
    
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"error": "An internal error occurred"}), 500