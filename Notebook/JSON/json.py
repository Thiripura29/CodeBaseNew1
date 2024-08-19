# Databricks notebook source
# Read the JSON file with multiline option enabled
read_df = spark\
    .read\
    .format("json")\
    .option("multiLine","true") \
    .load("s3://vidya-sankalp/datasets/json/json/Adalberto916_Orn563_b897a48b-cd8b-b118-7887-caa29321f6a3.json")

# read_df = spark\
#     .read\
#     .format("json")\
#     .load("s3://vidya-sankalp-datasets/corrupt_json/corrupt_json.json")

# Display the DataFrame
# display(read_df)

# COMMAND ----------

read_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col  # Importing the col function from pyspark.sql.functions module
from datetime import datetime  # Importing the datetime class from datetime module

def identify_json_corrupt_records(df):
    non_corrupt_df = None
    corrupted_record_df = None
    # Flag to indicate if corrupt records are found
    corrupt_record_found = False  # Initializing the flag as False

    # Flag to indicate if all records are corrupted
    does_all_records_are_corrupted = False  # Initializing the flag as False

    # Get the schema of the DataFrame
    dataframe_schema = df.schema.fields
    print(dataframe_schema)
    # Check if the DataFrame has only one column
    if len(dataframe_schema) == 1:
        does_all_records_are_corrupted = True  # If yes, set the flag as True to indicate that all records are corrupted
    else:
        # Check each field in the schema for '_corrupt_record'
        for field in dataframe_schema:
            if field.name == '_corrupt_record':
                # Print a message if corrupted records are found in JSON
                print("corrupted records are found in JSON, "
                    "it could be because of not enabling multiLine=True. So please try enabling it once again")
                corrupt_record_found = True  # Set the flag as True to indicate that corrupt records are found

    if does_all_records_are_corrupted or corrupt_record_found:

        # Cache the DataFrame for better performance
        df.cache()

        # Filter out the corrupt records into a new DataFrame
        corrupted_record_df = df.filter(col('_corrupt_record').isNotNull())

        # Filter out the non-corrupt records into a new DataFrame
        if not does_all_records_are_corrupted:
            non_corrupt_df = df.filter(col('_corrupt_record').isNull())
        
        return corrupted_record_df, non_corrupt_df
    else:
        return None, df       
    
corrupted_record_df, non_corrupted_records_df = identify_json_corrupt_records(read_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or replace temporary view using JSON format and specifying the path, multiline option, and inferSchema option
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view
# MAGIC USING json
# MAGIC OPTIONS (
# MAGIC   path 's3://vidya-sankalp-datasets/health-care/json', 
# MAGIC   multiLine 'true',
# MAGIC   inferSchema 'true'
# MAGIC );
# MAGIC
# MAGIC -- Select all columns from the temporary view
# MAGIC SELECT *
# MAGIC FROM temp_view;

# COMMAND ----------

# How to select columns from a column with a datatype of struct
# Display the selected attributes from the DataFrame
display(non_corrupted_records_df.select("attributes.age", "attributes.AGE_MONTHS", "attributes.C19_FULLY_VACCINATED", "attributes.C19_SCHEDULED_FIRST_SHOT"))

# Display the selected attributes from the DataFrame with renamed columns
display(non_corrupted_records_df.selectExpr("attributes.age", "attributes.age_months", "attributes.C19_FULLY_VACCINATED as covid19_FULLY_VACCINATED", "attributes.C19_SCHEDULED_FIRST_SHOT as covid19_scheduled_first_shot"))

# Display all attributes from the DataFrame
# display(non_corrupted_records_df.selectExpr("attributes.*"))

# COMMAND ----------

# Select columns "coverage", "symptoms", "record", and all columns from "attributes" in the DataFrame "read_df"
# and create a new DataFrame "trasnform_df1"
transform_df1 = non_corrupted_records_df.selectExpr("coverage","symptoms","record","attributes.*")

# COMMAND ----------

required_columns = ["first_name","middle_name","last_name","gender","age","age_months","marital_status","employment_condition.name as employment_type","birth_city","address","county","city","state","alcoholic","smoker",'disabled','household_size','uninsured',"C19_FULLY_VACCINATED","C19_SCHEDULED_FIRST_SHOT","diabetes_stage.name","diabetes","hypertension_severe",'immunizations',"bmi_percentile","coverage","symptoms","record"]

transform_df2 = transform_df1.selectExpr(required_columns)
# transform_df2.printSchema()
# display(transform_df2)

# COMMAND ----------

from pyspark.sql.functions import expr, when

# Generate meaningful case expressions based on columns
# Create a new column "alcoholic_status" that indicates if a person is alcoholic or non-alcoholic
transform_df3 = transform_df2.withColumn("alcoholic_status", when(transform_df2.alcoholic == 'true', "Alcoholic").otherwise("Non-Alcoholic"))
# Create a new column "smoker_status" that indicates if a person is a smoker or non-smoker
transform_df3 = transform_df3.withColumn("smoker_status", when(transform_df2.smoker == 'true' , "Smoker").otherwise("Non-Smoker"))
# Create a new column "employment_type_category" that categorizes the employment type as employed, unemployed, or other
transform_df3 = transform_df3.withColumn("employment_type_category",
                                         expr("CASE WHEN employment_type = 'Employed' THEN 'Employed' " +
                                              "WHEN employment_type = 'Unemployed' THEN 'Unemployed' " +
                                              "ELSE 'Other' END"))
# Create a new column "age_group" that categorizes the age into different age groups
transform_df3 = transform_df3.withColumn("age_group", expr("CASE WHEN age < 18 THEN 'Under 18' " +
                                                           "WHEN age >= 18 AND age < 30 THEN '18-29' " +
                                                           "WHEN age >= 30 AND age < 45 THEN '30-44' " +
                                                           "WHEN age >= 45 AND age < 60 THEN '45-59' " +
                                                           "ELSE '60 and above' END"))
# # Create a new column "household_size_category" that categorizes the household size as small, medium, or large
transform_df3 = transform_df3.withColumn("household_size_category",
                                         when(transform_df3.household_size < 3, "Small")
                                         .when((transform_df3.household_size >= 3) & ~(transform_df3.household_size > 5), "Medium")
                                         .otherwise("Large"))

# Create a new column "diabetes_category" that indicates if a person has diabetes or not
transform_df3 = transform_df3.withColumn("diabetes_category",
                                         when(transform_df2.diabetes == 'true', "Diabetic")
                                         .otherwise("Non-Diabetic"))
# # Create a new column "hypertension_severe_category" that indicates if a person has severe hypertension or not
transform_df3 = transform_df3.withColumn("hypertension_severe_category",
                                         when(transform_df2.hypertension_severe == 'true', "Severe Hypertension")
                                         .otherwise("Non-Severe Hypertension"))

# Select the required columns from transform_df3
required_columns_transformed = ["first_name", "middle_name", "last_name", "gender", "age", "age_group",
                                "marital_status","immunizations", "employment_type_category", "birth_city", "address", "county",
                                "city", "state", "alcoholic_status", "smoker_status", "disabled", "household_size",
                                "household_size_category", "uninsured", "C19_FULLY_VACCINATED",
                                "C19_SCHEDULED_FIRST_SHOT", "diabetes_category", "bmi_percentile","hypertension_severe_category",
                                "coverage", "symptoms", "record"]

transformed_df = transform_df3.selectExpr(required_columns_transformed)
# transformed_df.printSchema()
# display(transformed_df)

# COMMAND ----------

from pyspark.sql.functions import expr, when, from_unixtime

# Convert the C19_SCHEDULED_FIRST_SHOT column from milliseconds to seconds and cast it to a timestamp
transformed_df = transformed_df.withColumn("C19_SCHEDULED_FIRST_SHOT_seconds", transformed_df.C19_SCHEDULED_FIRST_SHOT / 1000)
transformed_df = transformed_df.withColumn("C19_SCHEDULED_FIRST_SHOT_timestamp", from_unixtime(transformed_df.C19_SCHEDULED_FIRST_SHOT_seconds).cast("timestamp"))

# Extract the date, day, month, year, hour, minute, and second from the C19_SCHEDULED_FIRST_SHOT_timestamp column
transformed_df = transformed_df.withColumn("date", transformed_df.C19_SCHEDULED_FIRST_SHOT_timestamp.cast("date"))
transformed_df = transformed_df.withColumn("day", expr("dayofmonth(C19_SCHEDULED_FIRST_SHOT_timestamp)"))
transformed_df = transformed_df.withColumn("month", expr("month(C19_SCHEDULED_FIRST_SHOT_timestamp)"))
transformed_df = transformed_df.withColumn("year", expr("year(C19_SCHEDULED_FIRST_SHOT_timestamp)"))
transformed_df = transformed_df.withColumn("hour", expr("hour(C19_SCHEDULED_FIRST_SHOT_timestamp)"))
transformed_df = transformed_df.withColumn("minute", expr("minute(C19_SCHEDULED_FIRST_SHOT_timestamp)"))
transformed_df = transformed_df.withColumn("second", expr("second(C19_SCHEDULED_FIRST_SHOT_timestamp)"))

# Calculate the day of the year, week of the year, and the last day of the month
transformed_df = transformed_df.withColumn("dayofyear", expr("dayofyear(C19_SCHEDULED_FIRST_SHOT_timestamp)"))
transformed_df = transformed_df.withColumn("weekofyear", expr("weekofyear(C19_SCHEDULED_FIRST_SHOT_timestamp)"))
transformed_df = transformed_df.withColumn("last_day", expr("last_day(C19_SCHEDULED_FIRST_SHOT_timestamp)"))

# Calculate the next day from the C19_SCHEDULED_FIRST_SHOT_timestamp column
transformed_df = transformed_df.withColumn("next_day", expr("date_add(C19_SCHEDULED_FIRST_SHOT_timestamp, 1)"))

# Calculate the date after adding/subtracting a specific number of days
transformed_df = transformed_df.withColumn("date_add", expr("date_add(C19_SCHEDULED_FIRST_SHOT_timestamp, 7)"))
transformed_df_with_date = transformed_df.withColumn("date_sub", expr("date_sub(C19_SCHEDULED_FIRST_SHOT_timestamp, 7)"))


# display(transformed_df_with_date)

# COMMAND ----------

transformed_df_with_date.printSchema()

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType
# Approach 1
# Function to extract the unique immunizations from a dictionary
def get_immunizations(immunizations):
    # Convert the immunizations column to a dictionary
    immunizations = immunizations.asDict()

    # Create an empty set to store unique immunizations
    immunizations_list = set()

    # Iterate through the dictionary and add each immunization to the set
    for k,v in immunizations.items():
        immunizations_list.add(k)

    # Convert the set to a list and return it
    return list(immunizations_list)

# Create a UDF (User Defined Function) to apply the get_immunizations function to the immunizations column
get_immunizations_udf = udf(get_immunizations, ArrayType(StringType()))

# Create a new column "immunizations_array" by applying the get_immunizations_udf UDF to the "immunizations" column
transformed_df = transformed_df_with_date.withColumn("immunizations_array", get_immunizations_udf(col('immunizations')))

# Display the transformed DataFrame
# display(transformed_df)

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType

# Approach 2
# Define a User Defined Function (UDF) to extract unique immunizations from a dictionary
@udf('array<string>')
def get_immunizations(immunizations):
    # Convert the immunizations column to a dictionary
    immunizations = immunizations.asDict()

    # Create an empty set to store unique immunizations
    immunizations_list = set()

    # Iterate through the dictionary and add each immunization to the set
    for k,v in immunizations.items():
        immunizations_list.add(k)

    # Convert the set to a list and return it
    return list(immunizations_list)

# Create a new column "immunizations_array" by applying the get_immunizations UDF to the "immunizations" column
transformed_df = transformed_df_with_date.withColumn("immunizations_array", get_immunizations(col('immunizations')))

# Display the transformed DataFrame
# display(transformed_df)

# COMMAND ----------

transformed_df.createOrReplaceTempView(name='transformed_df_with_date')

# COMMAND ----------

# Register the get_immunizations UDF with Spark
spark.udf.register('get_immunizations_udf', get_immunizations)
#spark.udf.register('get_immunizations_udf', get_immunizations, ArrayType(StringType()))

# COMMAND ----------

# SQL query to select all columns from transformed_df_with_date DataFrame,
# and get the size of the immunizations_keys_sql array, using array_size function
sql_code = """
SELECT *, array_size(immunizations_keys_sql) as immunizations_size
FROM (
    SELECT 
        *,
        get_immunizations_udf(immunizations) as immunizations_keys_sql
    FROM 
        transformed_df_with_date
)
"""
# Execute the SQL query and store the result in df DataFrame
df = spark.sql(sql_code)

# Display the DataFrame
display(df)

# COMMAND ----------

"""
array_distinct : Removes duplicate values from array
json_object_keys : Returns all the keys of the outermost JSON object as an array. It takes  STRING expression of a valid JSON array format and returns  ARRAY < STRING >
to_json : Converts a column containing a StructType, ArrayType or a MapType into a JSON string. Throws an exception, in the case of an unsupported type.
array_size = Returns the number of elements in array
"""

sql_code = """
SELECT *,array_size(immunizations_keys_sql) as immunizations_size
FROM(
SELECT 
    *, 
    array_distinct(json_object_keys(to_json(immunizations))) as immunizations_keys_sql 
FROM 
    transformed_df_with_date
)
"""
df = spark.sql(sql_code)

# Display the DataFrame
display(df)

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType,StructType,StringType,StructField,IntegerType

json_schema = ArrayType(ArrayType(IntegerType()))
# Define a User Defined Function (UDF) to extract unique immunizations values from a dictionary
@udf(json_schema)
def get_immunizations_values(immunizations):
    # Convert the immunizations column to a dictionary
    immunizations = immunizations.asDict()

    # Create an empty set to store unique immunizations
    immunizations_values_list = []

    # Iterate through the dictionary and add each immunization to the set
    for k,v in immunizations.items():
        immunizations_values_list.append([int(item) for item in v])
    print(immunizations_values_list)
    # Convert the set to a list and return it
    return immunizations_values_list

spark.udf.register('get_immunizations_values_udf', get_immunizations_values)

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StructType, StringType, StructField, IntegerType,MapType

json_schema = ArrayType(MapType(StringType(),ArrayType(IntegerType())))
# Define a User Defined Function (UDF) to extract unique immunizations values from a dictionary
@udf(json_schema)
def get_immunizations_as_json(immunizations):
    # Convert the immunizations column to a dictionary
    immunizations = immunizations.asDict()

    # Create an empty set to store unique immunizations
    immunizations_values_list = []

    # Iterate through the dictionary and add each immunization to the set
    for k,v in immunizations.items():
        immunizations_values_list.append({k:[int(item) for item in v]})
    print(immunizations_values_list)
    # Convert the set to a list and return it
    return immunizations_values_list

spark.udf.register('get_immunizations_as_json_udf', get_immunizations_as_json)

# COMMAND ----------

# SQL code to transform the DataFrame
sql_code = """
SELECT *, 
    -- Apply from_unixtime to each element in the flattened array of immunizations values
    transform(flatten_immunizations_values, x -> from_unixtime(x)) as immunizations_values_flatten,
    
    -- Apply from_unixtime to each element in each array within the map of immunizations
    transform(immunizations_converted_map, outer_item -> transform_values(outer_item, (k,v) -> transform(v, x -> from_unixtime(x)))) as immunizations_transformed_values
FROM(
    SELECT 
        *, 
        -- Use UDF to get flattened values from the struct
        get_immunizations_values_udf(immunizations) as immunizations_values,
        
        -- Flatten the array of immunizations values for easier transformation
        flatten(get_immunizations_values_udf(immunizations)) as flatten_immunizations_values,
        
        -- Convert the struct to a map for easier transformation
        get_immunizations_as_json_udf(immunizations) as immunizations_converted_map


    FROM 
        transformed_df_with_date
)
"""

df = spark.sql(sql_code)
# Display the DataFrame
display(df)
"""
Equivalent python code
from pyspark.sql import functions as F

# Step 1: Apply UDFs to get necessary columns
df = transformed_df_with_date.withColumn(
    "immunizations_values",
    F.expr("get_immunizations_values_udf(immunizations)")
).withColumn(
    "flatten_immunizations_values",
    F.expr("flatten(get_immunizations_values_udf(immunizations))")
).withColumn(
    "immunizations_converted_map",
    F.expr("get_immunizations_as_json_udf(immunizations)")
)

# Step 2: Transform the flattened immunization values
df = df.withColumn(
    "immunizations_values_flatten",
    F.expr("transform(flatten_immunizations_values, x -> from_unixtime(x))")
)

# Step 3: Transform the map of immunizations
df = df.withColumn(
    "immunizations_transformed_values",
    F.expr("transform(immunizations_converted_map, outer_item -> transform_values(outer_item, (k, v) -> transform(v, x -> from_unixtime(x))))")
)

# Show the result
df.show()

"""

# COMMAND ----------

# MAGIC %md
# MAGIC Some of the Important In-build Spark functions to Know

# COMMAND ----------

"""
Creating a new table for experimenting inbuilt spark functions
"""
from datetime import datetime
# Creating DataFrame using a list of rows
list_of_rows = [
  (1, "Prudhvi", "Akella", '[{"year":2011,"percentage":"90.1%"},{"year":2012,"percentage":"90.2%"}]','[{"2011":"90.1%"},{"2012":"90.1%"}]',["XYZ","XYZ2"],datetime(2012, 8, 1, 12, 0)),
  (2, "Ravi", "R", '[{"year":2011,"percentage":"90.1%"},{"year":2012,"percentage":"90.2%"}]','[{"2011":"90.1%"},{"2012":"90.1%"}]',["XYZ"],datetime(2011, 6, 2, 12, 0)),
  (3, "Sandeep", "T",'[{"year":2011,"percentage":"90.1%"},{"year":2012,"percentage":"90.2%"}]','[{"2011":"90.1%"},{"2012":"90.1%"}]',["XYZ","XYZ3"],datetime(2011, 7, 2, 12, 0)),
  (4, "Anil", "K", '[{"year":2011,"percentage":"90.1%"},{"year":2012,"percentage":"90.2%"}]','[{"2011":"90.1%"},{"2012":"90.1%"}]',["XYZ4","XYZ2"],datetime(2010, 6, 3, 12, 0))
]

# Approach 1: Create DataFrame using spark.createDataFrame() method
df = spark.createDataFrame(list_of_rows, schema="student_no int, first_name string, last_name string, yearly_percentage string,  yearly_percentage_map string,schools_list array<string>, joining_date timestamp")

# Approach 2: Creating DataFrame using RDD
rdd = spark.sparkContext.parallelize(list_of_rows)
df = spark.createDataFrame(rdd, schema="student_no int, first_name string, last_name string, yearly_percentage string,yearly_percentage_map string,schools_list array<string>, joining_date timestamp")

# Display the DataFrame
display(df)

# COMMAND ----------

df.createOrReplaceTempView('playground')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM playground

# COMMAND ----------

sql_code = """
-- from_json parses a JSON string into a specified data type.
-- Select all columns from the playground view
SELECT 
    *,
    -- Parse the JSON string in the 'yearly_percentage' column into an array of structs
    from_json(yearly_percentage, 'ARRAY<STRUCT<year: INT, percentage: STRING>>') as yearly_percentage_array,
    -- Parse the JSON string in the 'yearly_percentage_map' column into an array of maps
    from_json(yearly_percentage_map, 'ARRAY<MAP<STRING,STRING>>') as yearly_percentage_map_array 
FROM 
    playground
"""

df_with_converted_from_string_to_json = spark.sql(sql_code)

display(df_with_converted_from_string_to_json)

df_with_converted_from_string_to_json.createOrReplaceTempView("ready_for_explode")

# COMMAND ----------

sql_code = """
SELECT
    *,
    -- explode transforms an array into multiple rows.
    -- explode(yearly_percentage_array) as yearly_percentage_struct,
    -- posexplode transforms an array into multiple rows along with the position (index) of each element.
    -- posexplode(yearly_percentage_array) as (pos,yearly_percentage_struct),
    -- explode_outer transforms an array into multiple rows and produces null values if the array is null or empty.(preffered)
    -- explode_outer(yearly_percentage_array) as yearly_percentage_struct
    -- posexplode_outer transforms an array into multiple rows along with the position (index) of each element and produces null values if the array is null or empty. If the array is null or empty, it produces a row with null values for both the position and the element.
    -- posexplode_outer(yearly_percentage_array) as (pos,yearly_percentage_struct)
    -- inline explodes an array of structs into separate columns
    -- Each struct's fields become individual columns in the resulting rows
    inline(yearly_percentage_array)
FROM
ready_for_explode
"""

exploded_df = spark.sql(sql_code)


# COMMAND ----------

# create the view only when inline code is uncommented
exploded_df.createOrReplaceTempView('ready_for_aaray_functions')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ready_for_aaray_functions;

# COMMAND ----------

sql_code = """
    SELECT
        *,
        -- Returns array appended by elem. syntax: array_append(array, elem)
        array_append(schools_list, NULL) as array_append_col,
        -- Returns an expanded array where elem is inserted at the index position. syntax : array_insert(array, index, elem)
        array_insert(schools_list, size(schools_list) + 1, "abc_to_end") as array_insert_col,
        -- The function returns an array of the same type as the input argument where all NULL values have been removed.
        array_compact(array_append_col) as array_compact_col
        -- Removes duplicate values from array.
        array_distinct(schools_list) as array_distinct_col
    FROM 
        ready_for_aaray_functions
"""
df_with_array = spark.sql(sql_code)
display(df_with_array)
"""
Some of the other important bult-in array functions
https://docs.databricks.com/en/sql/language-manual/functions/array_except.html
https://docs.databricks.com/en/sql/language-manual/functions/array_intersect.html
https://docs.databricks.com/en/sql/language-manual/functions/array_prepend.html
https://docs.databricks.com/en/sql/language-manual/functions/array_sort.html
https://docs.databricks.com/en/sql/language-manual/functions/array_remove.html
https://docs.databricks.com/en/sql/language-manual/functions/array_union.html
"""

# COMMAND ----------

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)


simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)

"""
Union : Removes duplicates. Only distinct values are included in the final result set.
Union all : Includes all duplicates. Every row from each SELECT statement is included in the final result set

However in spark union all is depricated and union will act like union all and to remove duplicates we need to add distinct on top of union resultant dataframe to ensure the uniqueness.
"""

display(df.union(df2))
display(df.union(df2).distinct())
"""
unionByName: Similar to union, but matches columns by name rather than by position.
"""
display(df.unionByName(df2))
"""
Returns the common rows between two result sets, eliminating duplicates
"""
display(df.intersect(df2))
"""
It is used to return the rows from the first DataFrame that are not present in the second DataFrame, including duplicates. This is similar to the EXCEPT operation, but EXCEPT ALL does not remove duplicates from the result set.
"""
display(df.exceptAll(df2))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Below table temp_data has one column called raw contains JSON data that records temperature for every four hours in the day for the city of Chicago, you are asked to calculate the maximum temperature that was ever recorded for 12:00 PM hour across all the days.  Parse the JSON data and use the necessary array function to calculate the max temp.
# MAGIC
# MAGIC CREATE or REPLACE TABLE temp_data 
# MAGIC   AS SELECT ' 
# MAGIC   {
# MAGIC     "chicago": [
# MAGIC       {
# MAGIC         "date": "01-01-2021",
# MAGIC         "temp": [
# MAGIC           25,
# MAGIC           28,
# MAGIC           45,
# MAGIC           56,
# MAGIC           39,
# MAGIC           25
# MAGIC         ]
# MAGIC       },
# MAGIC       {
# MAGIC         "date": "01-02-2021",
# MAGIC         "temp": [
# MAGIC           25,
# MAGIC           28,
# MAGIC           49,
# MAGIC           54,
# MAGIC           38,
# MAGIC           25
# MAGIC         ]
# MAGIC       },
# MAGIC       {
# MAGIC         "date": "01-03-2021",
# MAGIC         "temp": [
# MAGIC           25,
# MAGIC           28,
# MAGIC           49,
# MAGIC           58,
# MAGIC           38,
# MAGIC           25
# MAGIC         ]
# MAGIC       }
# MAGIC     ]
# MAGIC   }' AS raw;
# MAGIC
# MAGIC   select array_max(from_json(raw:chicago[*].temp[3],'array<int>')) from temp_data;
