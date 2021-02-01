# Databricks notebook source
# MAGIC %md ## Yelp User Raw Data Analysis

# COMMAND ----------

# MAGIC %md 1. Read the yelp checkin data and get an idea about the attributes of the business. 
# MAGIC 2. Check for the number of partitions created and analyse the data skewness.

# COMMAND ----------

# Read yelp User data
filePath = "/mnt/yelpuser"
badFilePath = "/mnt/bad_data_user"
userDF = spark.read.option("badRecordsPath",badFilePath).json(path=filePath) # create dataframe

# COMMAND ----------

# MAGIC %md ##### Check For Duplicates in Yelp Tip

# COMMAND ----------

# Total No of Rows
tot_Cnt =userDF.count()
print("Total No of Rows: ", tot_Cnt)
unq_Cnt =userDF.drop_duplicates().count()
print("Unique No of Rows: ", unq_Cnt)

# COMMAND ----------

# MAGIC %md ##### Check for Data Skewness

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
# get no of partitions
implictPart = userDF.rdd.getNumPartitions()
print("Implict no of partitions:", implictPart)
#get each partition size
partitions =userDF.withColumn("Partition_id", spark_partition_id()).groupBy("Partition_id").count().orderBy("Partition_id")
#partitions =userDF.withColumn("Partition_id", spark_partition_id())
#distPartitions = partitions.select("partition_id").distinct()
display(partitions)

# COMMAND ----------

# MAGIC %md Here we could see data are skweed resulting in the smaller size of partitions.

# COMMAND ----------

# MAGIC %md ##### Repartitioning the data to avoid data skewness

# COMMAND ----------

rDF = userDF.repartition(18)
repartitions =rDF.withColumn("Partition_id",spark_partition_id()).groupBy("Partition_id").count().orderBy("Partition_id")
display(repartitions)

# COMMAND ----------

# MAGIC %md ##### Convert Json to Parquet

# COMMAND ----------

# MAGIC %md Since json is storage heavy and we are converting the raw data to parquet

# COMMAND ----------

outPath = "/mnt/preprocess_user"
rDF.write.parquet(path=outPath, mode="overwrite",compression="snappy")
