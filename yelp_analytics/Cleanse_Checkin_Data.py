# Databricks notebook source
# MAGIC %md ##### Read checkin data from s3

# COMMAND ----------

filepath="/mnt/preprocess_checkin/*.parquet"
checkinDF = spark.read.parquet(filepath)

# COMMAND ----------

# MAGIC %md ##### convert string delimited date column to array of timestamp values

# COMMAND ----------

from pyspark.sql.functions import split, column as col
from pyspark.sql.types import ArrayType, TimestampType
checkinDF =( checkinDF
            .withColumn("date", split(col("date"),",\s*").cast(ArrayType(TimestampType())))
           )
checkinMDF = checkinDF.fillna("Unknown")


# COMMAND ----------

# MAGIC %md ##### write the cleansed checkin data to s3

# COMMAND ----------

filepath = "/mnt/cleansed_checkin"
checkinMDF.write.parquet(path=filepath,mode="append")
