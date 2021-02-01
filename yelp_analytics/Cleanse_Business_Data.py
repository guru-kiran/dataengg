# Databricks notebook source
# MAGIC %md ###### Read business data from s3 and create dataframe

# COMMAND ----------

filepath ="/mnt/preprocess_business/*.parquet"
bizDF = spark.read.parquet(filepath)


# COMMAND ----------

# MAGIC %md ###### Transform some of the columns in dataframe

# COMMAND ----------

# MAGIC %md 1. flatten BusinessParking in attribute column
# MAGIC 2.  convert string delimited categories column to array type

# COMMAND ----------

from pyspark.sql.functions import column as col, regexp_replace, get_json_object, split, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import unix_timestamp, from_unixtime

# extract BusinessParking as a seperate column
bizDF = (bizDF
         .withColumn("BusinessParking",(col("attributes.BusinessParking")))
         .withColumn("Ambience",(col("attributes.Ambience")))
         .withColumn("DietaryRestrictions",(col("attributes.DietaryRestrictions")))
         .withColumn("GoodForMeal",(col("attributes.GoodForMeal")))
        )

# standardise the malformed json structure of BusinessParking column
bizDF = (bizDF
         .withColumn("BusinessParking",regexp_replace("BusinessParking",'\'','"'))
         .withColumn("BusinessParking",regexp_replace("BusinessParking",'False','"False"'))
         .withColumn("BusinessParking",regexp_replace("BusinessParking",'True','"True"'))
         .withColumn("categories", split(col("categories"),",\s*").cast(ArrayType(StringType())))
         .withColumn("Ambience",regexp_replace("Ambience",'\'','"'))
         .withColumn("Ambience",regexp_replace("Ambience",'False','"False"'))
         .withColumn("Ambience",regexp_replace("Ambience",'True','"True"'))
         .withColumn("DietaryRestrictions",regexp_replace("DietaryRestrictions",'\'','"'))
         .withColumn("DietaryRestrictions",regexp_replace("DietaryRestrictions",'False','"False"'))
         .withColumn("DietaryRestrictions",regexp_replace("DietaryRestrictions",'True','"True"'))
         .withColumn("GoodForMeal",regexp_replace("GoodForMeal",'\'','"'))
         .withColumn("GoodForMeal",regexp_replace("GoodForMeal",'False','"False"'))
         .withColumn("GoodForMeal",regexp_replace("GoodForMeal",'True','"True"'))
         .withColumn("unix_timestamp", unix_timestamp(col("current_timestamp")))
        )
display(bizDF)




# COMMAND ----------

# MAGIC %md ###### Extract and flatten some fields

# COMMAND ----------

attrDF = bizDF.select(
   col("business_id")
  ,col("name").alias("BusinessName")
  ,col("categories")
  ,col("address")
  ,col("city")
  ,col("state")
  ,col("postal_code")
  ,col("is_open")
  ,col("review_count")
  ,col("stars").alias("Rating")
  ,col("attributes.AcceptsInsurance")
  ,col("attributes.AgesAllowed")
  ,regexp_replace(regexp_replace(col("attributes.Alcohol"),'u\'',''),'\'','').alias("Alcohol")
  ,get_json_object("Ambience","$.touristy").alias("HasTouristyAmbience")
  ,get_json_object("Ambience","$.hipster").alias("HasHipsterAmbience")
  ,get_json_object("Ambience","$.romantic").alias("HasRomanticAmbience")
  ,get_json_object("Ambience","$.intimate").alias("HasIntimateAmbience")
  ,get_json_object("Ambience","$.trendy").alias("HasTrendyAmbience")
  ,get_json_object("Ambience","$.upscale").alias("HasUpscaleAmbience")
  ,get_json_object("Ambience","$.classy").alias("HasClassyAmbience")
  ,get_json_object("Ambience","$.casual").alias("HasCasualAmbience")
  ,get_json_object("DietaryRestrictions","$.dairy-free").alias("IsDairyFree")
  ,get_json_object("DietaryRestrictions","$.gluten-free").alias("IsGlutenFree")
  ,get_json_object("DietaryRestrictions","$.vegan").alias("IsVegan")
  ,get_json_object("DietaryRestrictions","$.kosher").alias("IsKosher")
  ,get_json_object("DietaryRestrictions","$.halal").alias("IsHalal")
  ,get_json_object("DietaryRestrictions","$.soy-free").alias("IsSoyFree")
  ,get_json_object("DietaryRestrictions","$.vegetarian").alias("IsVeg")
  ,get_json_object("GoodForMeal","$.dessert").alias("ServesDessert")
  ,get_json_object("GoodForMeal","$.latenight").alias("ServesLateNight")
  ,get_json_object("GoodForMeal","$.lunch").alias("ServesLunch")
  ,get_json_object("GoodForMeal","$.dinner").alias("ServesDinner")
  ,get_json_object("GoodForMeal","$.brunch").alias("ServesBrunch")
  ,get_json_object("GoodForMeal","$.breakfast").alias("ServesBreakFast")
  ,col("attributes.BYOB")
  ,col("attributes.BYOBCorkage")
  ,col("attributes.BikeParking")
  ,col("attributes.BusinessAcceptsBitcoin")
  ,col("attributes.BusinessAcceptsCreditCards")
  ,get_json_object("BusinessParking","$.garage").alias("HasGarage")
  ,get_json_object("BusinessParking","$.street").alias("HasStreetParking")
  ,get_json_object("BusinessParking","$.validated").alias("HasParkingValidated")
  ,get_json_object("BusinessParking","$.validated").alias("HasParkinglot")
  ,get_json_object("BusinessParking","$.validated").alias("HasValetParking")
  ,col("attributes.ByAppointmentOnly")
  ,col("attributes.Caters")
  ,col("attributes.CoatCheck")
  ,col("attributes.Corkage")
  ,col("attributes.DogsAllowed")
  ,col("attributes.DriveThru")
  ,col("attributes.GoodForDancing")
  ,col("attributes.GoodForKids")
  ,col("attributes.HairSpecializesIn")
  ,regexp_replace(regexp_replace(col("attributes.HappyHour"),'u\'',''),'\'','').alias("HappyHour")
  ,regexp_replace(regexp_replace(col("attributes.HasTV"),'u\'',''),'\'','').alias("HasTV")
  ,regexp_replace(regexp_replace(col("attributes.NoiseLevel"),'u\'',''),'\'','').alias("NoiseLevel")
  ,regexp_replace(regexp_replace(col("attributes.Open24Hours"),'u\'',''),'\'','').alias("Open24Hours")
  ,regexp_replace(regexp_replace(col("attributes.OutdoorSeating"),'u\'',''),'\'','').alias("OutdoorSeating")
  ,regexp_replace(regexp_replace(col("attributes.RestaurantsAttire"),'u\'',''),'\'','').alias("RestaurantsAttire")
  ,regexp_replace(regexp_replace(
    col("attributes.RestaurantsCounterService"),'u\'',''),'\'','').alias("RestaurantsCounterService")
  ,regexp_replace(regexp_replace(
    col("attributes.RestaurantsDelivery"),'u\'',''),'\'','').alias("RestaurantsDelivery")
  ,regexp_replace(regexp_replace(
    col("attributes.RestaurantsGoodForGroups"),'u\'',''),'\'','').alias("RestaurantsGoodForGroups")
  ,regexp_replace(regexp_replace(
    col("attributes.RestaurantsPriceRange2"),'u\'',''),'\'','').alias("RestaurantsPriceRange2")
  ,regexp_replace(regexp_replace(
    col("attributes.RestaurantsReservations"),'u\'',''),'\'','').alias("RestaurantsReservations")
  ,regexp_replace(regexp_replace(
    col("attributes.RestaurantsTableService"),'u\'',''),'\'','').alias("RestaurantsTableService")
  ,regexp_replace(regexp_replace(
    col("attributes.RestaurantsTakeOut"),'u\'',''),'\'','').alias("RestaurantsTakeOut")
  ,regexp_replace(regexp_replace(
    col("attributes.Smoking"),'u\'',''),'\'','').alias("Smoking")
  ,regexp_replace(regexp_replace(
    col("attributes.WheelchairAccessible"),'u\'',''),'\'','').alias("WheelchairAccessible")
  ,regexp_replace(regexp_replace(
    col("attributes.WiFi"),'u\'',''),'\'','').alias("WiFi")
  ,from_unixtime(col("unix_timestamp")).alias("created_at")
  
)


# COMMAND ----------

# MAGIC %md ###### handle data sparsity and add audit columns

# COMMAND ----------

bizMasterDF = attrDF.fillna("Unknown")

# COMMAND ----------

# MAGIC %md ##### write business data to cleansed zone

# COMMAND ----------

filepath = "/mnt/cleansed_business"
bizMasterDF.coalesce(2).write.parquet(path=filepath,mode="overwrite")
