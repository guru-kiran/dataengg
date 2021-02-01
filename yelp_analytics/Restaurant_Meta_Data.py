# Databricks notebook source
# MAGIC %md ###### Read business data from s3

# COMMAND ----------

filepath = "/mnt/cleansed_business/*.parquet"
bizDF = spark.read.parquet(filepath)

# COMMAND ----------

# MAGIC %md ###### Filter for restaurants data only

# COMMAND ----------

from pyspark.sql.functions import array_contains, column as col
resDF= bizDF.filter(array_contains(col("categories"),"Restaurants"))


# COMMAND ----------

# MAGIC %md ###### write restaurant metadata to hive table

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "yelphivetables/restaurant"
(
  resDF.write
  .option("path","s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME)).bucketBy(3,"business_id")
  .saveAsTable(name="restaurant_metadata",mode="overwrite")
)
