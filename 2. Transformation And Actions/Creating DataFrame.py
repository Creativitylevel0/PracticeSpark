# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. Create a Dataframe using a connector

# COMMAND ----------


#Spark have many types file format like csv,json,xml every connector have different options so search for it
file_df =(spark.read.format("csv") 
          .option("header", "true")
          .option("inferSchema","true")
          .load(path="/Volumes/dev/spark_db/datasets/spark_programming/data/sf-fire-calls.csv")
)
file_df.display()

# COMMAND ----------

#Search for option for the json connector in pyspark documentation(json format bydefault infer schema no need to add that option)
json_file_df = (spark.read.format("json")
             .load("/Volumes/dev/spark_db/datasets/spark_programming/data/diamonds.json")
)
json_file_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Create a Dataframe reading a Spark table

# COMMAND ----------

table_df = spark.table("dev.spark_db.sf_fire_calls")
table_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Create a Dataframe reading the result of a SQL query

# COMMAND ----------

sql_df = spark.sql("select * from dev.spark_db.sf_fire_calls limit 10")
sql_df.display()

# COMMAND ----------

table_name  = "dev.spark_db.sf_fire_calls"

sql_df2 = spark.sql(f"""select * 
                   from {table_name}
                   limit 5""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Create a dataframe from a Python list

# COMMAND ----------

#Mainly used for testing purpose

from datetime import datetime, date

data_list_schema = 'id long, name string, joining_date date, salary double, created_at timestamp'

data_list = [(1, "Prashant", date(2018, 1, 1), 924.0, datetime(2022, 1, 1, 9, 0)),
             (2, "Sushant", date(201, 2, 1), 1260.50, datetime(2022, 1, 2, 11, 0)),
             (3, "David", date(2022, 3, 1), 765.0, datetime(2022, 1, 3, 10, 0))]

list_df = spark.createDataFrame(data_list, data_list_schema) 
list_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Create a single column dataframe from a range

# COMMAND ----------


range_df = spark.range(1000,1012,2)
range_df.display()
