# Databricks notebook source
# MAGIC %md
# MAGIC ####1. How to create a spark session

# COMMAND ----------

from pyspark.sql import SparkSession

spark_session = SparkSession.builder.getOrCreate()


# COMMAND ----------

spark_session.version

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.What is pre-created Spark Session
# MAGIC

# COMMAND ----------

spark.version


# COMMAND ----------

# MAGIC %md
# MAGIC ####3. How to use SparkSession to read table data?

# COMMAND ----------

df = spark.table("dev.spark_db.diamonds")
#display(df)
df.show(10)
#df.display()
