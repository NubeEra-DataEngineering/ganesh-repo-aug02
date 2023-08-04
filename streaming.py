# Databricks notebook source
# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls("/tmp/")

# COMMAND ----------

dbutils.fs.put("/tmp/abc.txt","Welcome to Databricks File System",True)

# COMMAND ----------

dbutils.fs.help("mkdirs")

# COMMAND ----------

dbutils.fs.ls("s3://bucketname/folder/file.ext")

spark.read.format("parquet").load("s3://bucketname/folder/parquet")

spark.sql("SELECT * FROM parquet.`s3://bucketname/folder/file.parquet`")

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks secrets list-scopes
# MAGIC # databricks secrets create-scope --scope mydata

# COMMAND ----------

strS3ObjectPath="s3://bkt-04aug-mujahed/workstation_env.yml"
dbutils.fs.ls(strS3ObjectPath)

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

# MAGIC %sh
# MAGIC git status

# COMMAND ----------

dbutils.fs.ls("s3://dasfsdcf3dw3edw3dfds/workstation_env.yml")

# COMMAND ----------

spark.sql("SELECT * from parquet.`s3://bkt-04aug-mujahed/userdata1.parquet`")

# COMMAND ----------

# COMMAND ----------

# Databricks notebook source
access_key = 
secret_key = 

#Mount bucket on databricks
encoded_secret_key = secret_key.replace("/", "%2F")

aws_bucket_name = "bkt-nithin-04aug"

mount_name = "nithinawss3"

dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)

display(dbutils.fs.ls("/mnt/%s" % mount_name))

mount_name = "nithinawss3"

file_name="iris.csv"

df = spark.read.format("csv").load("/mnt/%s/%s" % (mount_name , file_name))

df.show()

# COMMAND ----------

strMountPoint="/mnt/%s/%s" % (mount_name , file_name)

dbutils.fs.put(strMountPoint+"/abc.txt","Welcome to Text File",True)

# COMMAND ----------

strMountPointParquetFile="/mnt/%s/abc.parquet" % (mount_name)
df.write.parquet(strMountPointParquetFile)

# COMMAND ----------

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Dead Data DataSet

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/samples/population-vs-price/data_geo.csv")

# COMMAND ----------

data = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/databricks-datasets/samples/population-vs-price/data_geo.csv")

# COMMAND ----------

data.cache()  # Cache data for faster reuse

data = data.dropna()   # Remove Missing values

# COMMAND ----------

# MAGIC %python
# MAGIC data.take(5)
# MAGIC display(data)

# COMMAND ----------

data.createOrReplaceTempView("data_geo")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_geo

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming DataSet

# COMMAND ----------

file_path = "/databricks-datasets/structured-streaming/events"
checkpoint_path= "/tmp/ss-tutorial/_checkpoint"

raw_df =(spark.readStream.format("cloudFiles").option("cloudFiles.format","json").option("cloudFiles.schemaLocation",checkpoint_path).load(file_path))

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp
transformed_df = (
    raw_df.select("*",
                  col("_metadata.file_path").alias("source_file"),
                  current_timestamp().alias("processing_time")
                  )
    )

# COMMAND ----------

target_path = "/tmp/ss-tutorial/"
checkpoint_path = "/tmp/ss-tutorial/_checkpoint"

transformed_df.writeStream.trigger(availableNow=True).option("checkpointLocation",checkpoint_path).option("path",target_path).start()

# COMMAND ----------
