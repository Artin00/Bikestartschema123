# Databricks notebook source
#Creating the files for the schema/tables within the Silver folder
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Silver/payment")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Silver/trip")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Silver/rider")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Silver/station")

# COMMAND ----------

#Creating the Silver schemas and adding the data from the bronze folder to the silver folder with the changed data types
from pyspark.sql.types import StructType, IntegerType, DateType, DecimalType, VarcharType, TimestampType, BooleanType, FloatType, StructField, StringType
from pyspark.sql.functions import col

#Payment
payments_bronze = spark.read.format("delta").load("dbfs:/tmp/Artin/Bronze/payment")
pbs = payments_bronze.withColumn("payment_id", col("payment_id").cast(IntegerType())) \
      .withColumn("date", col("date").cast(DateType())) \
      .withColumn("amount", col("amount").cast(DecimalType())) \
      .withColumn("rider_id", col("rider_id").cast(IntegerType()))
pbs.printSchema()

payment_silver = pbs.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Silver/payment")
payment_time = spark.read.load("dbfs:/tmp/Artin/Silver/payment", format = "delta")
payment_time.printSchema()

#Trip
trip_bronze = spark.read.load("dbfs:/tmp/Artin/Bronze/trip", format = "delta")
tbs = trip_bronze.withColumn("trip_id", col("trip_id").cast(VarcharType(255))) \
                 .withColumn("ridable_type", col("ridable_type").cast(StringType())) \
                 .withColumn("started_at", col("started_at").cast(TimestampType())) \
                 .withColumn("ended_at", col("ended_at").cast(TimestampType())) \
                 .withColumn("started_station_id", col("started_station_id").cast(IntegerType())) \
                 .withColumn("ended_station_id", col("ended_station_id").cast(IntegerType())) \
                 .withColumn("rider_id", col("rider_id").cast(IntegerType()))
tbs.printSchema()

trip_silver = tbs.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Silver/trip")
trip_time = spark.read.load("dbfs:/tmp/Artin/Silver/trip", format = "delta")
trip_time.printSchema()

#Rider
rider_bronze = spark.read.load("dbfs:/tmp/Artin/Bronze/rider", format = "delta")
rbs = rider_bronze.withColumn("rider_id", col("rider_id").cast(IntegerType())) \
                  .withColumn("first", col("first").cast(VarcharType(255))) \
                  .withColumn("last", col("last").cast(VarcharType(255))) \
                  .withColumn("address", col("address").cast(VarcharType(255))) \
                  .withColumn("birthday", col("birthday").cast(DateType())) \
                  .withColumn("account_start_date", col("account_start_date").cast(DateType())) \
                  .withColumn("account_end_date", col("account_end_date").cast(DateType())) \
                  .withColumn("is_member", col("is_member").cast(BooleanType()))
rbs.printSchema()

rider_silver = rbs.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Silver/rider")
rider_time = spark.read.load("dbfs:/tmp/Artin/Silver/rider", format = "delta")
rider_time.printSchema()

#Station
station_bronze = spark.read.load("dbfs:/tmp/Artin/Bronze/station", format = "delta")
sbs = station_bronze.withColumn("station_id", col("station_id").cast(VarcharType(255))) \
      .withColumn("name", col("name").cast(VarcharType(255))) \
      .withColumn("longitude", col("longitude").cast(FloatType())) \
      .withColumn("latitude", col("latitude").cast(FloatType()))

sbs.printSchema()

station_silver = sbs.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Silver/station")
station_time = spark.read.load("dbfs:/tmp/Artin/Silver/station", format = "delta")
station_time.printSchema()


# COMMAND ----------

#Checking to see if it the code has worked
dfas = spark.read.load("dbfs:/tmp/Artin/Silver/payment", format = "delta")
dfbs = spark.read.load("dbfs:/tmp/Artin/Silver/trip", format = "delta")
dfcs = spark.read.load("dbfs:/tmp/Artin/Silver/rider", format = "delta")
dfds = spark.read.load("dbfs:/tmp/Artin/Silver/station", format = "delta")

display(dfas.limit(1))
display(dfbs.limit(1))
display(dfcs.limit(1))
display(dfds.limit(1))
