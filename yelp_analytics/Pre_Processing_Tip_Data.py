# Databricks notebook source
# MAGIC %md ## Yelp Tip Raw Data Analysis

# COMMAND ----------

# MAGIC %md 1. Read the yelp checkin data and get an idea about the attributes of the business. 
# MAGIC 2. Check for the number of partitions created and analyse the data skewness.

# COMMAND ----------

# Read yelp business data
filePath = "/mnt/yelptip"
badFilePath = "/mnt/bad_data_tip"
tipDF = spark.read.option("badRecordsPath",badFilePath).json(path=filePath) # create dataframe

# COMMAND ----------

# MAGIC %md ##### Check For Duplicates in Yelp Tip

# COMMAND ----------

# Total No of Rows
tot_Cnt =tipDF.count()
print("Total No of Rows: ", tot_Cnt)
unq_Cnt =tipDF.drop_duplicates().count()
print("Unique No of Rows: ", unq_Cnt)

# COMMAND ----------

# MAGIC %md ##### Remove Duplicate Records

# COMMAND ----------

tipDF = tipDF.drop_duplicates()

# COMMAND ----------

# MAGIC %md ##### Check for Data Skewness

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
# get no of partitions
implictPart = tipDF.rdd.getNumPartitions()
print("Implict no of partitions:", implictPart)

#get each partition size
partitions =tipDF.withColumn("Partition_id", spark_partition_id()).groupBy("Partition_id").count().orderBy("Partition_id")
display(partitions)

# COMMAND ----------

# MAGIC %md ##### Convert Json to Parquet

# COMMAND ----------

# MAGIC %md Since json is storage heavy and we are converting the raw data to parquet

# COMMAND ----------

outPath = "/mnt/preprocess_tip"
tipDF.write.parquet(path=outPath, mode="overwrite",compression="snappy")
