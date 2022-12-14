# Databricks notebook source
# MAGIC %md
# MAGIC ## DS 2002: Capstone Project
# MAGIC 
# MAGIC 
# MAGIC ### Section I: Prerequisites
# MAGIC 
# MAGIC #### 1.0. Import Required Libraries

# COMMAND ----------

# needed to install pymongo again through databricks #
pip install pymongo

# COMMAND ----------

# importing all the libraries I will be using for this project #
import os
import json
import pymongo
import pyspark.pandas as pd  # This uses Koalas that is included in PySpark version 3.2 or newer.
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Instantiate Global Variables

# COMMAND ----------

## here I am making all the connections to my azure server, MongoDB atlas, and JSON data files and connecting my databases

# Azure SQL Server Connection Information #####################
jdbc_hostname = "ds2002-mysql-sb.mysql.database.azure.com" # copied over the server name from azure 
jdbc_port = 3306
src_database = "HR_management"

connection_properties = {
  "user" : "sbae",
  "password" : "Password123",
  "driver" : "org.mariadb.jdbc.Driver"
}

# MongoDB Atlas Connection Information ########################
atlas_cluster_name = "ds2002"
atlas_database_name = "HR_management_DW"
atlas_user_name = "sbae373"
atlas_password = "Password123"


# Data Files (JSON) Information ###############################

base_dir = "dbfs:/FileStore/ds2002-capstone"
database_dir = f"{base_dir}/{dst_database}"

data_dir = f"{base_dir}/source_data"
batch_dir = f"{data_dir}/batch"
stream_dir = f"{data_dir}/stream"

output_bronze = f"{database_dir}/fact_employees/bronze"
output_silver = f"{database_dir}/fact_employees/silver"
output_gold = f"{database_dir}/fact_employees/gold"

# Delete the Streaming Files ################################## 
dbutils.fs.rm(f"{database_dir}/fact_employees", True)

# Delete the Database Files ###################################
dbutils.fs.rm(database_dir, True)



# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Define Global Functions

# COMMAND ----------

# defining the global functions I will be using throughout this project

######################################################################################################################
# Use this Function to Fetch a DataFrame from the Azure SQL database server.
# ######################################################################################################################
def get_sql_dataframe(host_name, port, db_name, conn_props, sql_query):
    '''Create a JDBC URL to the Azure SQL Database'''
    jdbcUrl = f"jdbc:mysql://{host_name}:{port}/{db_name}"
    
    '''Invoke the spark.read.jdbc() function to query the database, and fill a Pandas DataFrame.'''
    dframe = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=conn_props)
    
    return dframe


# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the MongoDB Atlas database server Using PyMongo.
# ######################################################################################################################
def get_mongo_dataframe(user_id, pwd, cluster_name, db_name, collection, conditions, projection, sort):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.wxjbf.mongodb.net/{db_name}"  # changed my cluster characters here and removed the trailing characters "?retryWrites=true&w=majority" according to the pdf guide sent to us
    
    client = pymongo.MongoClient(mongo_uri)

    '''Query MongoDB, and fill a python list with documents to create a DataFrame'''
    db = client[db_name]
    if conditions and projection and sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection).sort(sort)))
    elif conditions and projection and not sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection)))
    else:
        dframe = pd.DataFrame(list(db[collection].find()))

    client.close()
    
    return dframe

# ######################################################################################################################
# Use this Function to Create New Collections by Uploading JSON file(s) to the MongoDB Atlas server.
# ######################################################################################################################
def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.wxjbf.mongodb.net/{db_name}"   # also changed here
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section II: Populate Dimensions by Ingesting Reference (Cold-path) Data 
# MAGIC #### 1.0. Fetch Reference Data From an Azure SQL Database
# MAGIC ##### 1.1. Create a New Databricks Metadata Database, and then Create a New Table that Sources its Data from a View in an Azure SQL database.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dropping the database if it exists to make sure I am not duplicating anything --
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS HR_management_DW CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating the database a new databricks metadata database -- 
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS HR_management_DW
# MAGIC COMMENT "Capstone Project Database"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/HR_management_DW"
# MAGIC 
# MAGIC WITH DBPROPERTIES (contains_pii = true, purpose = "DS-2002 Capstone Project");

# COMMAND ----------

# MAGIC %sql
# MAGIC -- connecting to my MySQL-Azure connection to fetch the reference data and creating a View --
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_jobs
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://ds2002-mysql-sb.mysql.database.azure.com:3306;database=HR_management",
# MAGIC   dbtable "min_salary.vDimJobs",
# MAGIC   user "sbae",
# MAGIC   password "Password123"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating a new table for dim_jobs that sources its data from a View in my azure SQL database --
# MAGIC 
# MAGIC USE DATABASE HR_management_DW;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS HR_management_DW.dim_jobs
# MAGIC COMMENT "Jobs Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/HR_management_DW/dim_jobs"
# MAGIC AS SELECT * FROM view_product

# COMMAND ----------

# MAGIC %sql
# MAGIC -- selecting everthing from my dim_jobs table --
# MAGIC 
# MAGIC SELECT * FROM HR_management_DW.dim_jobs LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- getting extensive formatted and unformatted information of dim_jobs from HR_management_DW --
# MAGIC 
# MAGIC DESCRIBE EXTENDED HR_management_DW.dim_jobs;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Create a New Table that Sources its Data from a Table in an Azure SQL database. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- connecting to my MySQL-Azure connection to fetch the reference data and creating another View --
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_date
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://ds2002-mysql-sb.mysql.database.azure.com:3306;database=HR_management",
# MAGIC   dbtable "dbo.DimDate",
# MAGIC   user "sbae",
# MAGIC   password "Password123"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating a new table for dim_date that sources its data from a View in my azure SQL database --
# MAGIC 
# MAGIC USE DATABASE HR_management_DW;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS HR_management_DW.dim_date
# MAGIC COMMENT "Date Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-mysql-sb.mysql.database.azure.com/dim_date"
# MAGIC AS SELECT * FROM view_date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- selecting everthing from my dim_date table --
# MAGIC 
# MAGIC SELECT * FROM HR_management_DW.dim_date LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- getting extensive formatted and unformatted information for dim_date from HR_management_DW --
# MAGIC 
# MAGIC DESCRIBE EXTENDED HR_management_DW.dim_date;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Fetch Reference Data from a MongoDB Atlas Database
# MAGIC ##### 2.1. View the Data Files on the Databricks File System

# COMMAND ----------

# viewing my data files that are on my databricks file system #
display(dbutils.fs.ls(batch_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.2. Create a New MongoDB Database, and Load JSON Data Into a New MongoDB Collection
# MAGIC **NOTE:** The following cell **can** be run more than once because the **set_mongo_collection()** function **is** idempotent.

# COMMAND ----------

# here I am creating a new MongoDB database and loading in my JSON data into this new MongoDB collection #

source_dir = '/dbfs/FileStore/ds2002-mysql-sb.mysql.database.azure.com/source_data/batch'
json_files = {"departments" : 'HR_management_departments.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, atlas_database_name, source_dir, json_files) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.3. Fetch Data from the New MongoDB Collection

# COMMAND ----------

# MAGIC %scala
# MAGIC // fetching the data from the new collection I made in the previous code chunk in MongoDB //
# MAGIC 
# MAGIC import com.mongodb.spark._
# MAGIC 
# MAGIC val df_departments = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "HR_management_DW").option("collection", "departments").load()
# MAGIC display(df_employees)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_departments.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.4. Use the Spark DataFrame to Create a New Table in the Databricks (HR_management_DW) Metadata Database

# COMMAND ----------

# MAGIC %scala
# MAGIC // using the spark dataframe to create a new table for dim_departments in my databricks metadata database for HR_management_DW //
# MAGIC 
# MAGIC df_departments.write.format("delta").mode("overwrite").saveAsTable("HR_management_DW.dim_departments")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- getting extensive formatted and unformatted information for dim_departments from HR_management_DW --
# MAGIC 
# MAGIC DESCRIBE EXTENDED HR_management_DW.dim_departments

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.5. Query the New Table in the Databricks Metadata Database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- querying this new dim_departments table in the databricks metadata database -- 
# MAGIC 
# MAGIC SELECT * FROM HR_management_DW.dim_departments LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Fetch Data from a File System
# MAGIC ##### 3.1. Use PySpark to Read From a CSV File

# COMMAND ----------

# using PySpark to read in my data 

locations_csv = f"{batch_dir}/HR_management_locations.csv"

df_locations = spark.read.format('csv').options(header='true', inferSchema='true').load(address_csv)
display(df_address)

# COMMAND ----------

df_locations.printSchema()

# COMMAND ----------

df_locations.write.format("delta").mode("overwrite").saveAsTable("HR_management_DW.dim_locations")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- getting extensive formatted and unformatted information of dim_locations from HR_management_DW --
# MAGIC 
# MAGIC DESCRIBE EXTENDED HR_management_DW.dim_locations;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- querying data of dim_locations from HR_management_DW --
# MAGIC SELECT * FROM HR_management_DW.dim_locations LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify Dimension Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- verifying to see that all my dimension tables were added to HR_management_DW 
# MAGIC -- it all shows up so I verified that everything has been crrectly inputted 
# MAGIC 
# MAGIC USE HR_management_DW;
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section III: Integrate Reference Data with Real-Time Data
# MAGIC #### 6.0. Use AutoLoader to Process Streaming (Hot Path) Data 
# MAGIC ##### 6.1. Bronze Table: Process 'Raw' JSON Data

# COMMAND ----------

# loading and processing my 'raw' JSON data #
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaHints", "job_key INT")
 .option("cloudFiles.schemaHints", "job_title STRING")
 .option("cloudFiles.schemaHints", "hire_date INT")
 .option("cloudFiles.schemaHints", "salary FLOAT") 
 .option("cloudFiles.schemaHints", "manager_key INT")
 .option("cloudFiles.schemaHints", "department_key INT")
 .option("cloudFiles.schemaHints", "first_name STRING")
 .option("cloudFiles.schemaHints", "last_name STRING")
 .option("cloudFiles.schemaHints", "email STRING")
 .option("cloudFiles.schemaLocation", output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load(stream_dir)
 .createOrReplaceTempView("employees_raw_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- adding metadata for traceability --
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW employees_bronze_tempview AS (
# MAGIC   SELECT *, current_timestamp() hire_date, input_file_name() source_file
# MAGIC   FROM output_employees_raw_tempview
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- querying to select all the data from this new view --
# MAGIC SELECT * FROM employees_bronze_tempview

# COMMAND ----------

(spark.table("employees_bronze_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_employees}/_checkpoint")
      .outputMode("append")
      .table("fact_employees_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.2. Silver Table: Include Reference Data

# COMMAND ----------

# including my reference data to create the employees_silver_tempview #
(spark.readStream
  .table("fact_employees_bronze")
  .createOrReplaceTempView("employees_silver_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- querying data from my new silver table view --
# MAGIC SELECT * FROM employees_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC -- getting extensive formatted and unformatted information for the employeess_silver_tempview --
# MAGIC DESCRIBE EXTENDED employees_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC -- integrating all the data and using it to create my fact_employees_silver_tempview --
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fact_employees_silver_tempview AS 
# MAGIC (
# MAGIC   SELECT e.employee_id,
# MAGIC   e.employee_id,
# MAGIC   j.job_id,
# MAGIC   d.department_id,
# MAGIC   l.location_id,
# MAGIC   e.hire_date,
# MAGIC   e.salary
# MAGIC 
# MAGIC   FROM HR_management.jobs j
# MAGIC   INNER JOIN HR_management.employees e
# MAGIC   ON j.job_id = e.job_id
# MAGIC   INNER JOIN HR_management.departments d
# MAGIC   ON e.department_id = d.department_id
# MAGIC   INNER JOIN HR_management.locations l
# MAGIC   ON d.location_id = l.location_id )

# COMMAND ----------

(spark.table("fact_employees_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_employees_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- querying to select all the data from my fact_employees_silver table --
# MAGIC SELECT * FROM fact_employees_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- getting extensive formatted and unformatted information for fact_employees_silver from HR_management_DW --
# MAGIC DESCRIBE EXTENDED HR_management_DW.fact_employees_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.4. Gold Table: Perform Aggregations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- in these aggregations I am looking to see what the average minimum and maximum salaries are for each job that is in my HR database 
# MAGIC -- I ordered this aggregation by the avg_min_salary in ascending order to make the viewing/reading experience better and easier to follow
# MAGIC 
# MAGIC SELECT dj.job_title AS job_title, 
# MAGIC     AVG(min_salary) AS avg_min_salary, 
# MAGIC     AVG(max_salary) AS avg_max_salary
# MAGIC FROM HR_management_DW2.event_fact ef
# MAGIC INNER JOIN HR_management_DW2.dim_employees de
# MAGIC ON ef.job_key = de.job_key
# MAGIC INNER JOIN HR_management_DW2.dim_jobs dj
# MAGIC ON de.job_key = dj.job_key
# MAGIC 
# MAGIC GROUP BY dj.job_title
# MAGIC ORDER BY avg_min_salary ASC
