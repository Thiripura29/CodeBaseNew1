# Databricks notebook source
# MAGIC %md
# MAGIC <b>Notebook Highlights:</b>
# MAGIC
# MAGIC --> Different ways to read csv data from object store(s3), DBFS
# MAGIC
# MAGIC --> Datframe functions like col, select, where, withColumn, lit
# MAGIC
# MAGIC --> read_files sql function
# MAGIC
# MAGIC --> Different ways to create temporary views in databricks.
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Supported spark csv reader options</b>
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-csv.html
# MAGIC
# MAGIC <b>Work with malformed CSV records<b>
# MAGIC
# MAGIC When reading CSV files with a specified schema, it is possible that the data in the files does not match the schema. For example, a field containing name of the city will not parse as an integer. The consequences depend on the mode that the parser runs in:
# MAGIC
# MAGIC <b>PERMISSIVE (default)</b>: nulls are inserted for fields that could not be parsed correctly
# MAGIC
# MAGIC <b>DROPMALFORMED</b>: drops lines that contain fields that could not be parsed
# MAGIC
# MAGIC <b>FAILFAST</b>: aborts the reading if any malformed data is found
# MAGIC
# MAGIC <b>Note : CSV Reader will return DataFrame as an output</b>
# MAGIC
# MAGIC

# COMMAND ----------

df = spark \
     .read \
     .format('csv') \
     .option("header",'true') \
     .option("inferSchema",'true')\
     .load('/Volumes/databricks_catalog/s3_test/healthcare_1/csv files/csv/2024_05_08T04_08_53Z/allergies.csv')
"""
     The show() function in Spark DataFrame is used to display the contents of the DataFrame in a tabular format.
     Spark will display a certain number of rows from the DataFrame, usually the first 20 rows by default
     Here are some common arguments you can pass 
     numRows: Specifies the number of rows to display. By default, it displays the first 20 rows.
     truncate: Specifies whether to truncate the displayed data if it's too wide. Truncation means cutting off some characters to fit the data within the display width. By default, it's set to True
     vertical: Specifies whether to display the output in a vertical format. By default, it's set to False, meaning the data is displayed horizontally.
     Usage:
     n: An alias for numRows.
     df.show(n=100, truncate=False, vertical=True)  # Display without truncating and in vertical format
"""
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark DataFrame as a table of data with rows and columns, kind of like a spreadsheet. 
# MAGIC
# MAGIC It's a distributed collection of data organized into named columns, similar to a relational database table. 
# MAGIC
# MAGIC The key difference is that it can handle massive amounts of data across a cluster of computers, making it ideal for big data processing tasks. 
# MAGIC
# MAGIC So, you can perform various operations on this data, like filtering, aggregating, joining, and analyzing, using Spark's distributed computing power. 
# MAGIC
# MAGIC It's a powerful tool for handling large-scale data processing tasks efficiently.
# MAGIC
# MAGIC

# COMMAND ----------

options = {
    "header":"true",
    "inferSchema":"true"
}
df = spark \
     .read \
     .format('csv') \
     .options(**options) \
     .load('s3://hgs3-bucket/csv files/csv/2024_05_08T04_08_53Z/claims.csv')

"""
display() function is a powerful tool for visualizing and exploring data. It's particularly useful for working with Spark DataFrames in a Databricks notebook environment.
Here's what you can do with it.
Viewing DataFrames
Plotting Charts
Data Profiling
    powerful tool for understanding the structure and characteristics of your data. It provides summary statistics and insights about the columns in your DataFrame, helping you identify patterns, anomalies, and potential issues in your data. Here's what you can expect from the data profiling feature:

    Summary Statistics: Databricks automatically computes summary statistics for each column in your DataFrame, such as count, mean, standard deviation, minimum, maximum, and quantiles. This gives you a quick overview of the distribution and range of values in your data.

    Data Quality Metrics: In addition to summary statistics, Databricks calculates data quality metrics for each column, such as the percentage of missing values, distinct values, and the most frequent values. This helps you assess the completeness and uniqueness of your data.

    Histograms and Frequency Distributions: Databricks generates histograms and frequency distributions for numerical columns, allowing you to visualize the distribution of values and identify any outliers or unusual patterns.

    Top Values: For categorical columns, Databricks displays the top values and their frequencies, giving you insights into the most common categories and their prevalence in the data.

    Data Profiling Insights: Databricks provides additional insights and recommendations based on the data profiling results, highlighting potential issues or areas for further investigation. For example, it may flag columns with a high percentage of missing values or suggest data transformations to improve data quality.
Note: display is a databricks related function doesn't work in all the spark environments
"""
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC The <b>printSchema()</b> function is a method commonly used in Apache Spark to display the schema of a DataFrame. A schema in Spark defines the structure of the data, including the data types of each column.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

schema = StructType([
    StructField('ID', StringType(), True),
    StructField('CLAIMID', StringType(), True),
    StructField('CHARGEID', IntegerType(), True),
    StructField('PATIENTID', StringType(), True),
    StructField('TYPE', StringType(), True),
    StructField('AMOUNT', DoubleType(), True),
    StructField('METHOD', StringType(), True),
    StructField('FROMDATE', TimestampType(), True),
    StructField('TODATE', TimestampType(), True),
    StructField('PLACEOFSERVICE', StringType(), True),
    StructField('PROCEDURECODE', DoubleType(), True),
    StructField('MODIFIER1', StringType(), True),
    StructField('MODIFIER2', StringType(), True),
    StructField('DIAGNOSISREF1', IntegerType(), True),
    StructField('DIAGNOSISREF2', IntegerType(), True),
    StructField('DIAGNOSISREF3', IntegerType(), True),
    StructField('DIAGNOSISREF4', IntegerType(), True),
    StructField('UNITS', IntegerType(), True),
    StructField('DEPARTMENTID', IntegerType(), True),
    StructField('NOTES', StringType(), True),
    StructField('UNITAMOUNT', DoubleType(), True),
    StructField('TRANSFEROUTID', IntegerType(), True),
    StructField('TRANSFERTYPE', StringType(), True),
    StructField('PAYMENTS', DoubleType(), True),
    StructField('ADJUSTMENTS', IntegerType(), True),
    StructField('TRANSFERS', DoubleType(), True),
    StructField('OUTSTANDING', DoubleType(), True),
    StructField('APPOINTMENTID', StringType(), True),
    StructField('LINENOTE', StringType(), True),
    StructField('PATIENTINSURANCEID', StringType(), True),
    StructField('FEESCHEDULEID', IntegerType(), True),
    StructField('PROVIDERID', StringType(), True),
    StructField('SUPERVISINGPROVIDERID', StringType(), True)
])

df = spark \
     .read \
     .format('csv') \
     .option("header",'true') \
     .schema(schema) \
     .load('s3://vidya-sankalp-datasets/health-care/2024_05_08T04_08_53Z/claims/')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC When using the PERMISSIVE mode, you can enable the rescued data column to capture any data that wasn’t parsed because one or more fields in a record have one of the following issues:
# MAGIC
# MAGIC Absent from the provided schema.
# MAGIC
# MAGIC Does not match the data type of the provided schema.
# MAGIC
# MAGIC Has a case mismatch with the field names in the provided schema.
# MAGIC
# MAGIC The rescued data column is returned as a JSON document containing the columns that were rescued, and the source file path of the record. To remove the source file path from the rescued data column, you can set the SQL configuration spark.conf.set("spark.databricks.sql.rescuedDataColumn.filePath.enabled", "false"). You can enable the rescued data column by setting the option rescuedDataColumn to a column name when reading data, such as _rescued_data with spark.read.option("rescuedDataColumn", "_rescued_data").format("csv").load(<path>).
# MAGIC
# MAGIC The CSV parser supports three modes when parsing records: PERMISSIVE, DROPMALFORMED, and FAILFAST. When used together with rescuedDataColumn, data type mismatches do not cause records to be dropped in DROPMALFORMED mode or throw an error in FAILFAST mode. Only corrupt records—that is, incomplete or malformed CSV—are dropped or throw errors.
# MAGIC
# MAGIC When rescuedDataColumn is used in PERMISSIVE mode, the following rules apply to corrupt records:
# MAGIC
# MAGIC The first row of the file (either a header row or a data row) sets the expected row length.
# MAGIC
# MAGIC A row with a different number of columns is considered incomplete.
# MAGIC
# MAGIC Data type mismatches are not considered corrupt records.
# MAGIC
# MAGIC Only incomplete and malformed CSV records are considered corrupt and recorded to the _corrupt_record column or badRecordsPath.

# COMMAND ----------

display(spark.read.format('text').load('/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv'))

# COMMAND ----------

# MAGIC %md
# MAGIC How to identify good records and bad records while loading csv data into spark?
# MAGIC
# MAGIC The schema contains a special column _corrupt_record, which does not exist in the data. This column captures rows that did not parse correctly.
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/sql-ref-datatypes.html

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('carat', DoubleType(), True),
    StructField('cut', StringType(), True),
    StructField('color', StringType(), True),
    StructField('clarity', StringType(), True),
    StructField('depth', DoubleType(), True),
    StructField('table', DoubleType(), True),
    StructField('price', IntegerType(), True),
    StructField('x', DoubleType(), True),
    StructField('y', DoubleType(), True),
    StructField('z', DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# Adjust the reading of the CSV file to include the schema and capture corrupt records
diamonds_with_wrong_schema = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("s3://vidya-sankalp-datasets/health-care/malformed_data/diamonds.csv")

# Now, you can filter based on the `_corrupt_record` column
goodRecords_df = diamonds_with_wrong_schema.where("_corrupt_record IS NULL")
badRecords_df = diamonds_with_wrong_schema.where("_corrupt_record IS NOT NULL")
# Good Records will be propagated to the next layers
# Bads records will be moved the expection folders for further analysis.
display(badRecords_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>select()</b> function is used to select one or more columns from a DataFrame. It allows you to create a new DataFrame containing only the columns you specify.
# MAGIC
# MAGIC <b>col()</b> function is used to create a Column object representing a column in a DataFrame. This function is commonly used when performing column-based operations, such as selecting, filtering, or transforming data
# MAGIC
# MAGIC <b>alias()</b> function is used to rename a column in a DataFrame. It allows you to create a new DataFrame with the same data but with the column renamed.

# COMMAND ----------

from pyspark.sql.functions import col

ret_df = goodRecords_df.select('id','carat','cut','color')
display(ret_df)
# ------------------------------------------------------------------------
output_df = goodRecords_df.select(col('id'),col('carat'),col('cut'),col('color'))
display(output_df)
# ------------------------------------------------------------------------
output_df = goodRecords_df.select(col('id').alias("identifier"),col('carat'),col('cut'),col('color'))
display(output_df)
# ------------------------------------------------------------------------
columns_list = goodRecords_df.columns
ignore_columns = ['_corrupt_record']
goodRecords_df  = goodRecords_df.select([col for col in columns_list if col not in ignore_columns])
# Now you can propagate the good records to the next layers.
display(goodRecords_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>withColumn()</b> function is used to add, replace, or transform a column in a DataFrame. It allows you to create a new DataFrame with additional or modified columns based on existing ones.
# MAGIC
# MAGIC returns a new DataFrame with the specified modifications, leaving the original DataFrame unchanged.
# MAGIC
# MAGIC <b>withColumnRenamed()</b> function is used to rename a column in a DataFrame. It allows you to create a new DataFrame with the specified column renamed.
# MAGIC
# MAGIC <b>lit()</b> is a function used to create a literal value Column. It's often used when you want to add a constant value as a new column or perform transformations that involve literal values.
# MAGIC
# MAGIC In these examples:
# MAGIC
# MAGIC <b>lit(123)</b> creates a Column with a literal value of 123. This literal value can be any Python value (string, integer, float, boolean, etc.).
# MAGIC
# MAGIC <b>df["existingColumnName"] + lit(10)</b> adds a new column "transformedColumnName" to the DataFrame, where each value is the sum of the values in the existing column "existingColumnName" and 10.

# COMMAND ----------

from pyspark.sql.functions import lit
"""
  adding new_price column
"""
transformed_df = goodRecords_df.withColumn('new_price',col('price')/(col('x')*col('y')*col('z')))
display(transformed_df)
"""
  adding two new columns new_int_column,price_2_multiplier 
"""
transformed_df = transformed_df\
                  .withColumn('new_int_column',lit(123))\
                  .withColumn('price_2_multiplier',col('new_price') * lit(123))

display(transformed_df)

transformed_df = transformed_df.withColumnRenamed('price_2_multiplier','price_2_multiplier_renamed')

display(transformed_df)

# COMMAND ----------

diamonds_with_wrong_schema = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .schema(schema)\
    .option("rescuedDataColumn", "_rescued_data") \
    .load("s3://vidya-sankalp-datasets/health-care/malformed_data/diamonds.csv")

# Now, you can filter based on the `_corrupt_record` column
# goodRecords_df = diamonds_with_wrong_schema.where("_rescued_data IS  NULL")
# badRecords_df = diamonds_with_wrong_schema.where("_rescued_data IS NOT NULL")

display(diamonds_with_wrong_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Reads files under a provided location and returns the data in tabular form.
# MAGIC
# MAGIC Supports reading JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO, and ORC file formats. Can detect the file format automatically and infer a unified schema across all files.
# MAGIC
# MAGIC <b>Syntax</b>
# MAGIC
# MAGIC read_files(path [, option_key => option_value ] [...])
# MAGIC
# MAGIC <b>Arguments</b>
# MAGIC This function requires named parameter invocation for the option keys.
# MAGIC
# MAGIC path: A STRING with the URI of the location of the data. Supports reading from Azure Data Lake Storage Gen2 ('abfss://'), S3 (s3://) and Google Cloud Storage ('gs://'). Can contain globs. See File discovery for more details.
# MAGIC
# MAGIC option_key: The name of the option to configure. You need to use backticks (`) for options that contain dots (.).
# MAGIC
# MAGIC option_value: A constant expression to set the option to. Accepts literals and scalar functions.
# MAGIC
# MAGIC <b>Returns</b>
# MAGIC A table comprised of the data from files read under the given path.
# MAGIC
# MAGIC https://docs.databricks.com/en/sql/language-manual/functions/read_files.html

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files('s3://vidya-sankalp-datasets/health-care/2024_05_08T04_08_53Z/claims/');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files(
# MAGIC     's3://vidya-sankalp-datasets/health-care/2024_05_08T04_08_53Z/claims/',
# MAGIC     format => 'csv',
# MAGIC     schema => 'ID string, CLAIMID string, CHARGEID integer, PATIENTID string, TYPE string, AMOUNT double, METHOD string, FROMDATE timestamp, TODATE timestamp, PLACEOFSERVICE string, PROCEDURECODE double, MODIFIER1 string, MODIFIER2 string, DIAGNOSISREF1 integer, DIAGNOSISREF2 integer, DIAGNOSISREF3 integer, DIAGNOSISREF4 integer, UNITS integer, DEPARTMENTID integer, NOTES string, UNITAMOUNT double, TRANSFEROUTID integer, TRANSFERTYPE string, PAYMENTS double, ADJUSTMENTS integer, TRANSFERS double, OUTSTANDING double, APPOINTMENTID string, LINENOTE string, PATIENTINSURANCEID string, FEESCHEDULEID integer, PROVIDERID string, SUPERVISINGPROVIDERID string',
# MAGIC     header => true,
# MAGIC     mode => 'FAILFAST')

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-schema.html

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Database and Schema are interchangeable
# MAGIC CREATE SCHEMA IF NOT EXISTS vidya_sankalp

# COMMAND ----------

# MAGIC %md
# MAGIC Need to create a external location in unity catalog before creating a table using CREATE TABLE syntax
# MAGIC
# MAGIC Note: This is applicable only when workspace is enabled with unity catalog.
# MAGIC
# MAGIC https://docs.databricks.com/en/connect/unity-catalog/index.html#manage-external-locations
# MAGIC
# MAGIC https://docs.databricks.com/en/connect/unity-catalog/external-locations.html

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS claims_transactions(
# MAGIC   ID string, 
# MAGIC   CLAIMID string,
# MAGIC   CHARGEID integer, 
# MAGIC   PATIENTID string, 
# MAGIC   TYPE string, 
# MAGIC   AMOUNT double, 
# MAGIC   METHOD string, 
# MAGIC   FROMDATE timestamp, 
# MAGIC   TODATE timestamp, 
# MAGIC   PLACEOFSERVICE string, 
# MAGIC   PROCEDURECODE double, 
# MAGIC   MODIFIER1 string, 
# MAGIC   MODIFIER2 string, 
# MAGIC   DIAGNOSISREF1 integer, 
# MAGIC   DIAGNOSISREF2 integer, 
# MAGIC   DIAGNOSISREF3 integer, 
# MAGIC   DIAGNOSISREF4 integer, 
# MAGIC   UNITS integer, 
# MAGIC   DEPARTMENTID integer, 
# MAGIC   NOTES string, 
# MAGIC   UNITAMOUNT double, 
# MAGIC   TRANSFEROUTID integer, 
# MAGIC   TRANSFERTYPE string, 
# MAGIC   PAYMENTS double, 
# MAGIC   ADJUSTMENTS integer, 
# MAGIC   TRANSFERS double, 
# MAGIC   OUTSTANDING double, 
# MAGIC   APPOINTMENTID string, 
# MAGIC   LINENOTE string, 
# MAGIC   PATIENTINSURANCEID string, 
# MAGIC   FEESCHEDULEID integer, 
# MAGIC   PROVIDERID string, SUPERVISINGPROVIDERID string
# MAGIC )
# MAGIC USING CSV 
# MAGIC LOCATION 's3://vidya-sankalp-datasets/health-care/2024_05_08T04_08_53Z/claims/'
# MAGIC COMMENT 'this is a claims transactions table'
# MAGIC TBLPROPERTIES ('createdDate'='05/14/2024');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims_transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE claims_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED claims_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA vidya_sankalp

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED vidya_sankalp

# COMMAND ----------

# MAGIC %md
# MAGIC You can create or replace a temporary view (also known as a temporary table) to make DataFrame data accessible via SQL queries.
# MAGIC
# MAGIC In the below example:
# MAGIC
# MAGIC <b>transformed_df</b> is the DataFrame you want to create a temporary view from.
# MAGIC
# MAGIC <b>"diamonds_views"</b> is the name you want to assign to the temporary view.
# MAGIC If a temporary view with the same name already exists, it will be replaced with the new DataFrame.
# MAGIC
# MAGIC After creating the temporary view, you can query it using SQL syntax:
# MAGIC
# MAGIC

# COMMAND ----------

transformed_df.createOrReplaceTempView("diamonds_views")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM diamonds_views

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW IF NOT EXISTS tmp_diamonds_view
# MAGIC     (id COMMENT 'Unique identification number', carat, cut, color, clarity, depth, table, price, x, y, z)
# MAGIC     COMMENT 'tmporary view for diamonds table '
# MAGIC     AS SELECT id,carat, cut, color, clarity, depth, table, price, x, y, z from diamonds_views

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_diamonds_view
