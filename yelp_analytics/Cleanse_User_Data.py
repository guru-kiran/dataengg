# Databricks notebook source
# MAGIC %md ###### Read User Data from s3

# COMMAND ----------

filepath = "/mnt/preprocess_user/*.parquet"
userDF = spark.read.parquet(filepath)

# COMMAND ----------

# MAGIC %md 1. Convert friends column to array type
# MAGIC 2. Convert yelping_since column to unix_datetime format

# COMMAND ----------

from pyspark.sql.functions import column as col, split, unix_timestamp, from_unixtime
from pyspark.sql.types import ArrayType, StringType, TimestampType
userDF = (userDF
          .withColumn("friends", split(col("friends"),",\s*").cast(ArrayType(StringType())))
          .withColumn("yelping_since",from_unixtime(unix_timestamp("yelping_since")))
         )

# COMMAND ----------

# MAGIC %md ###### write cleansed user data to s3

# COMMAND ----------

filepath = "/mnt/cleansed_user"
userDF.write.parquet(path=filepath,mode="overwrite")
