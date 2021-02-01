# Databricks notebook source
# MAGIC %md #### Mount Yelp Business Data

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "datalandingzone/business"
MOUNT_NAME = "yelpbusiness"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### List All Business Data Files

# COMMAND ----------

dbutils.fs.ls("/mnt/yelpbusiness")

# COMMAND ----------

# MAGIC %md #### Mount Yelp CheckIn Data

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "datalandingzone/checkin"
MOUNT_NAME = "yelpcheckin"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### List All CheckIn Data Files

# COMMAND ----------

dbutils.fs.ls("/mnt/yelpcheckin")

# COMMAND ----------

# MAGIC %md #### Mount Yelp Tip Data

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "datalandingzone/tip"
MOUNT_NAME = "yelptip"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### List All Tip Data Files

# COMMAND ----------

dbutils.fs.ls("/mnt/yelptip")

# COMMAND ----------

# MAGIC %md #### Mount Yelp User Data

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "datalandingzone/user"
MOUNT_NAME = "yelpuser"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### List All User Data Files

# COMMAND ----------

dbutils.fs.ls("/mnt/yelpuser")

# COMMAND ----------

filepath="/mnt/yelpuser"
df = spark.read.json(path=filepath)
display(df)

# COMMAND ----------

# MAGIC %md #### Mount Business Bad Data Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "datalandingzone/bad_data/business"
MOUNT_NAME = "bad_data_business"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount Checkin Bad Data Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "datalandingzone/bad_data/checkin"
MOUNT_NAME = "bad_data_checkin"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount Tip Bad Data Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "datalandingzone/bad_data/tip"
MOUNT_NAME = "bad_data_tip"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount User Bad Data Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "datalandingzone/bad_data/user"
MOUNT_NAME = "bad_data_user"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount Business Data Pre Processing Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "preprocesszone/business"
MOUNT_NAME = "preprocess_business"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount Checkin Data Pre Processing Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "preprocesszone/checkin"
MOUNT_NAME = "preprocess_checkin"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount Tip Data Pre Processing Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "preprocesszone/tip"
MOUNT_NAME = "preprocess_tip"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount User Data Pre Processing Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "preprocesszone/user"
MOUNT_NAME = "preprocess_user"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount Business Data Cleansed Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "cleansedzone/business"
MOUNT_NAME = "cleansed_business"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount Checkin Data Cleansed Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "cleansedzone/checkin"
MOUNT_NAME = "cleansed_checkin"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount Tip Data Cleansed Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "cleansedzone/tip"
MOUNT_NAME = "cleansed_tip"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount User Data Cleansed Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "cleansedzone/user"
MOUNT_NAME = "cleansed_user"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md #### Mount Restaurant Data Folder

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "cleansedzone/restaurant"
MOUNT_NAME = "restaurant"
dbutils.fs.mount("s3n://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
