# Databricks notebook source
# MAGIC %md ###### Read Tip Data from s3

# COMMAND ----------

tipfilepath = "/mnt/preprocess_tip/*.parquet"
bizfilepath = "/mnt/preprocess_business/*.parquet"
tipDF = spark.read.parquet(tipfilepath)
bizDF = spark.read.parquet(bizfilepath)

# COMMAND ----------

# MAGIC %md 1. Replace junk values with Unknown
# MAGIC 2. create dataframe only with business_id and Reviews

# COMMAND ----------

from pyspark.sql.functions import when, column as col
tipDF = tipDF.withColumnRenamed("text","Reviews")
tipDF = tipDF.withColumn("Reviews",when(col("Reviews") == '...','Unknown').otherwise(col("Reviews"))).fillna("Unknown")
tipDF = tipDF.select("business_id","Reviews")
bizDF = bizDF.select("business_id","name")
reviewsDF =(
  tipDF.alias("a").join(bizDF.alias("b").hint("broadcast"),"business_id")
  .select("a.business_id","b.name","a.Reviews")
)


# COMMAND ----------

# MAGIC %md ###### Write Business reviews to s3

# COMMAND ----------

filepath = "/mnt/cleansed_tip"
reviewsDF.coalesce(4).write.parquet(path=filepath,mode="overwrite")
