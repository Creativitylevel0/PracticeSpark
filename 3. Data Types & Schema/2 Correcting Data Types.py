# Databricks notebook source
# MAGIC %md
# MAGIC ####Requirement
# MAGIC 1. Read raw data from flight_time_raw table
# MAGIC 2. Apply transformations to time values as hour to minute interval
# MAGIC
# MAGIC     1. CRS_DEP_TIME
# MAGIC     2. DEP_TIME
# MAGIC     3. WHEELS_ON
# MAGIC     4. CRS_ARR_TIME
# MAGIC     5. ARR_TIME
# MAGIC 3. Apply transformation to TAXI_IN to make it a minute interval

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * FROM dev.spark_db.flight_time

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Read data to create a dataframe

# COMMAND ----------

flight_time_raw_df =  spark.read.table("dev.spark_db.flight_time")


# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Develop logic to transform CRS_DEP_TIME to an interval

# COMMAND ----------

from pyspark.sql.functions import expr 

step_1_df =(
flight_time_raw_df.withColumns({
     "CRS_DEP_TIME_HH": expr("left(lpad(CRS_DEP_TIME,4,'0'), 2)"),
     "CRS_DEP_TIME_MM": expr("right(lpad(CRS_DEP_TIME,4,'0'), 2)")
 })
)

step_1_df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Develop a reusable function

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Apply function to dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Save results to the table 
