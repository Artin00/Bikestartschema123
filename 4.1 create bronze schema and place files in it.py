# Databricks notebook source
#Creating the files for the schema/tables within the Bronze folder
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Bronze/payment")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Bronze/trip")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Bronze/rider")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Bronze/station")

# COMMAND ----------

#Creating the schema for the Bronze folder of each csv files
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 
df1 = payment_Schema = StructType([ \
                            StructField("payment_id", StringType(), True), \
                            StructField("date", StringType(), True), \
                            StructField("amount",  StringType(), True), \
                            StructField("rider_id", StringType(), True)])

df2 = trip_schema = StructType ([ \
                             StructField("trip_id", StringType(),True), \
                             StructField("rideable_type", StringType(),True), \
                             StructField("started_at", StringType(),True), \
                             StructField("ended_at", StringType(), True), \
                            StructField("started_station_id", StringType(), True), \
                            StructField("ended_station_id", StringType(), True), \
                           StructField("rider_id", StringType(), True)])

df3 = rider_schema = StructType([ \
                          StructField("rider_id", StringType(), True), \
                          StructField("first", StringType(), True), \
                          StructField("last", StringType(), True), \
                          StructField("address", StringType(), True), \
                          StructField("birthday", StringType(), True), \
                          StructField("account_start_date", StringType(), True), \
                          StructField("account_end_date", StringType(), True), \
                          StructField("is_member", StringType(), True)])

df4 = station_schema = StructType([ \
                            StructField("station_id", StringType(), True), \
                            StructField("name", StringType(), True), \
                            StructField("longitude", StringType(), True), \
                            StructField("latitude", StringType(), True)])

#Exporting the files within the specific tables with the schemas created before
payments_df = spark.read.csv("dbfs:/tmp/Artin/landing_2/payments.csv", schema = df1)
payments_df.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Bronze/payment")
 


trips_df = spark.read.csv("dbfs:/tmp/Artin/landing_2/trips.csv", schema = df2)
trips_df.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Bronze/trip")
 


riders_df = spark.read.csv("dbfs:/tmp/Artin/landing_2/riders.csv", schema = df3)
riders_df.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Bronze/rider")
 

stations_df = spark.read.csv("dbfs:/tmp/Artin/landing_2/stations.csv", schema = df4)
stations_df.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Bronze/station")

# COMMAND ----------

#Check to see if the Schemas have imported correctly
dfa = spark.read.load("dbfs:/tmp/Artin/Bronze/payment", format = "delta")
dfb = spark.read.load("dbfs:/tmp/Artin/Bronze/trip", format = "delta")
dfc = spark.read.load("dbfs:/tmp/Artin/Bronze/rider", format = "delta")
dfd = spark.read.load("dbfs:/tmp/Artin/Bronze/station", format = "delta")

display(dfa.limit(1))
display(dfb.limit(1))
display(dfc.limit(1))
display(dfd.limit(1))
