# Databricks notebook source
# MAGIC %md
# MAGIC ####Requirements
# MAGIC We have collected fire calls data files sf-fire-calls.csv
# MAGIC   1. Read the Data File
# MAGIC   2. Load it into the table for analysis
# MAGIC   3. Verify all records are loaded correctly
# MAGIC   4. The tab;e is predefined as below  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.spark_db.sf_fire_calls(
# MAGIC   CallNumber INT, UnitID STRING, IncidentNumber INT, CallType STRING, CallDate DATE,
# MAGIC   WatchDate DATE, CallFinalDisposition STRING, AvailableDtTm TIMESTAMP, Address STRING,
# MAGIC   City STRING, Zipcode STRING, Battalion STRING, StationArea STRING, Box STRING,
# MAGIC   OriginalPriority STRING, Priority STRING, FinalPriority STRING, ALSUnit BOOLEAN,
# MAGIC   CallTypeGroup STRING, NumAlarms INT, UnitType STRING, UnitSequenceInCallDispatch INT,
# MAGIC   FirePreventionDistrict STRING, SupervisorDistrict STRING, Neighborhood STRING,
# MAGIC   Location STRING, RowID STRING, Delay DOUBLE);
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()




# COMMAND ----------

# MAGIC %md 
# MAGIC ####Solution Approach

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Check the data file Structure
# MAGIC 2. Read the data file and create a dataframe

# COMMAND ----------

raw_fire_df = (
    spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/sf-fire-calls.csv")
)


# COMMAND ----------

# MAGIC %md
# MAGIC 3. Count the records(count)
# MAGIC 4. Check the dataframe for potential problems (display/show)
# MAGIC 5. Verify dataframe schema with the target table (printSchema)
# MAGIC 6. Transform the dataframe to match target table structure (withColumns, to_date, to_timestamp, cast)

# COMMAND ----------

raw_fire_df.count()

# COMMAND ----------

raw_fire_df.display()

# COMMAND ----------

raw_fire_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,expr

# Transform the dataframe to have the correct schema as table.
#fire_df = raw_fire_df.withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
# If Only one column schema is wrong you can use above code if not below

fire_df = raw_fire_df.withColumns({
    "AvailableDtTm": to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"),
    "Zipcode": expr("cast(Zipcode as string)"),
    "FinalPriority": expr("cast(FinalPriority as string)")
    })


# COMMAND ----------

# MAGIC %md
# MAGIC 7. Save the final dataframe into target table

# COMMAND ----------

fire_df.write.mode("overwrite").saveAsTable("dev.spark_db.sf_fire_calls")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dev.spark_db.sf_fire_calls;
