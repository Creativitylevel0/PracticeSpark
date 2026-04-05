# Databricks notebook source
# MAGIC %md
# MAGIC ###Requirement:
# MAGIC ####1. Answer the below question using the dev.spark_db.sf_fire_calls
# MAGIC What are top 3 zip codes that accounted for most calls?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM dev.spark_db.sf_fire_calls;
# MAGIC -- SELECT zipcode, Count(*) 
# MAGIC -- FROM dev.spark_db.sf_fire_calls
# MAGIC -- GROUP BY zipcode ORDER BY Count(*) DESC LIMIT 3;
# MAGIC -- Answer using a SQL query
# MAGIC select CallType, Zipcode, count(*) as count
# MAGIC from dev.spark_db.sf_fire_calls
# MAGIC where CallType is not null
# MAGIC group by CallType, Zipcode
# MAGIC order by count desc
# MAGIC limit 3;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Answer the above question using the Spark DataFrame API

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 Create a DataFrame reading data from the table
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

fire_df = spark.read.table("dev.spark_db.sf_fire_calls")

fire_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 Apply Transformations

# COMMAND ----------

#Apply Transformations
df_1 = fire_df.select("CallType", "Zipcode") 
df_2 = df_1.where("CallType is not null")
df_3 = df_2.groupBy("CallType","Zipcode").count()
df_4 = df_3.orderBy("count", ascending=False)
df_5 = df_4.limit(3)


# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 Apply an Action
# MAGIC
# MAGIC 1. Optimize the query
# MAGIC 2. Execute the optimized plan 
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/images/query-optimization.jpg" alt="Query Opimization" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

df_5.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Spark Dataframe API concepts
# MAGIC 1. Spark creates optimized query plan
# MAGIC 2. Dataframes are immutable -- So create new one when we need new transformation using old df. 
# MAGIC Note: Intermediate df will not take any memory only the last df will take memory.
# MAGIC 3. Every Transformation returns a dataframe
# MAGIC 2. Dataframe offers composable API

# COMMAND ----------

# MAGIC %md
# MAGIC 1. How to see the optimized query plan
# MAGIC     1. Use explain method
# MAGIC     2. Check the query runtime profile

# COMMAND ----------

#how to see optimized query plan
df_5.explain(mode ="extended") #mode = "simple" or "codegen" #explain() - Auxiliary function to display the optimized query plan as a string

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Dataframes are immutable\
# MAGIC You can see or use any intermediate dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Every Transformation returns a dataframe
# MAGIC 4. Dataframe offers composable API\
# MAGIC   Example of composable API

# COMMAND ----------

result_df = (
    fire_df
    .select("CallType", "Zipcode")
    .where("CallType is not null")
    .groupBy("CallType","Zipcode")
    .count()
    .orderBy("count", ascending=False)
    .limit(3)
) # no need for intermediate variables(df) until we need it.



# COMMAND ----------

result_df.show()
result_df.explain(mode ="extended")
