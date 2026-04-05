# Databricks notebook source
# MAGIC %md
# MAGIC ####Requirement
# MAGIC 1. Load data from flight-time.json into a table
# MAGIC 2. Table structure is given below
# MAGIC
# MAGIC ```
# MAGIC     FL_DATE DATE, 
# MAGIC     OP_CARRIER STRING, 
# MAGIC     OP_CARRIER_FL_NUM STRING, 
# MAGIC     ORIGIN STRING, 
# MAGIC     ORIGIN_CITY_NAME STRING, 
# MAGIC     DEST STRING, 
# MAGIC     DEST_CITY_NAME STRING, 
# MAGIC     CRS_DEP_TIME LONG, 
# MAGIC     DEP_TIME LONG, 
# MAGIC     WHEELS_ON INT, 
# MAGIC     TAXI_IN INT, 
# MAGIC     CRS_ARR_TIME LONG, 
# MAGIC     ARR_TIME LONG, 
# MAGIC     CANCELLED INT, 
# MAGIC     DISTANCE INT
# MAGIC ```

# COMMAND ----------

flight_schema = 

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read data from the flight-time.json file

# COMMAND ----------


flight_time_raw_df = (spark.read.format("json")
        .option("mode" , "FailFast")
        .option("dateFormat" , "M/d/yyyy")
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/flight-time.json")
           )
#json connector will sort the column alphabetically by default rather than original format so we have to correct that.(so need to define the schema)
flight_time_raw_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Investigate the dataframe data and schema for problems

# COMMAND ----------

flight_time_raw_df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Define Dataframe schema before reading it

# COMMAND ----------

from pyspark.sql.types import StringType, LongType, IntegerType, DateType, StructType, StructField

flight_schema = StructType([
    StructField("FL_DATE", DateType()),
    StructField("OP_CARRIER", StringType()),
    StructField("OP_CARRIER_FL_NUM", StringType()),
    StructField("ORIGIN", StringType()),
    StructField("ORIGIN_CITY_NAME", StringType()),
    StructField("DEST", StringType()),
    StructField("DEST_CITY_NAME", StringType()),
    StructField("CRS_DEP_TIME", LongType()),
    StructField("DEP_TIME", LongType()),
    StructField("WHEELS_ON", IntegerType()),
    StructField("TAXI_IN", IntegerType()),
    StructField("CRS_ARR_TIME", LongType()),
    StructField("ARR_TIME", LongType()),
    StructField("CANCELLED", IntegerType()),
    StructField("DISTANCE", IntegerType())
]) 





# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Read datafile with schema-on-read

# COMMAND ----------


flight_time_raw_with_schema_df = (
    spark.read
        .format("json")
        .option("mode", "FAILFAST")
        .option("dateFormat", "M/d/yyyy")
        .schema(flight_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/flight-time.json")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Investigate the Dataframe data and schema for problems

# COMMAND ----------

flight_time_raw_with_schema_df.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Save the Dataframe to the table flight_time_raw

# COMMAND ----------

flight_time_raw_with_schema_df.write.mode("overwrite").saveAsTable("dev.spark_db.flight_time")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * From dev.spark_db.flight_time;
