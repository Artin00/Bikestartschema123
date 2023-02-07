# Databricks notebook source
dbutils.fs.rm("dbfs:/tmp/Artin/Gold", True)


# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.bike")

# COMMAND ----------

#Create the folders for each of the tables

#Fact table
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/fact.trip")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/fact.payment")

#Dim tables
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.station")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.date")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.time")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.bike")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.rider")

# COMMAND ----------

dfbike = df231.drop("trip_id","started_at","ended_at","started_station_id","ended_station_id", "rider_id")
dfbikes = dfbike.select(dfbike.bike_id, dfbike.ridable_type)
dfbikes.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Gold/dim.bike")
display(dfbikes.limit(10))

# COMMAND ----------

#Creating Bike Table
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit, window

df23 = spark.read.load("dbfs:/tmp/Artin/Silver/trip", format = "delta")
display(df23)
window = Window.orderBy("rideable_type")
df231 = df23.select(row_number().over(Window.orderBy(lit(1))).alias("bike_id"), "rideable_type")
display(df231)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit, window

df23 = spark.read.load("dbfs:/tmp/Artin/Silver/trip", format = "delta")
display(df23)
df231 = df23.select("rideable_type").distinct()
display(df231)
df2313 = df231.select(row_number().over(Window.orderBy(lit(1))).alias("bike_id"), "rideable_type")
display(df2313)
df2313.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Gold/dim.bike")

# COMMAND ----------

#Creating Date table as well the Time table
from pyspark.sql.window import Window
from pyspark.sql.functions import split, col, substring, row_number, lit, window, to_timestamp
from pyspark.sql.types import StructType, IntegerType, DateType, DecimalType, VarcharType, TimestampType, BooleanType, FloatType, StructField, StringType

df01 = spark.read.load("dbfs:/tmp/Artin/Silver/trip", format ="delta")
df02 = spark.read.load("dbfs:/tmp/Artin/Silver/payment", format = "delta")
display(df01)
display(df02)
dfp = df02.select(col("date"))
dfs = df01.select(col("started_at"),col("ended_at"))
display(dfs)
display(dfp)  
dfsg = dfs.withColumn("date1", split(dfs["started_at"], " ").getItem(0)) \
          .withColumn("time1", split(dfs["started_at"]," ").getItem(1)) \
          .withColumn("date2", split(dfs["ended_at"], " ").getItem(0)) \
          .withColumn("time2", split(dfs["ended_at"], " ").getItem(1)) \
          .drop("started_at","ended_at")
display(dfsg.limit(10))

dfpg = dfp.select(col("date").cast(StringType()))



dfdate = dfsg.select("date1").union(dfsg.select("date2")).distinct().drop("time1","time2")
display(dfdate)
dfdates =dfdate.select(col("date1").cast(StringType()))

dftotaldates = dfdates.union(dfpg).distinct()

dftime = dfsg.select("time1").union(dfsg.select("time2")).distinct().drop("date1","date2")
display(dftime)

dftimenearlydone = dftime.select("time1", to_timestamp(col("time1"), "HH:mm:ss").cast(TimestampType())) \
                         .select(row_number().over(Window.orderBy(lit(1))).alias("time_id"),"time1")
ditimedone = dftimenearlydone.withColumnRenamed("time1","time")

datetimedone = ditimedone
display(datetimedone)

datetimedone.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Gold/dim.time")

display(dftotaldates)

dfdatesnearlydone = dftotaldates.select(col("date1").cast(DateType())) \
                                .select(row_number().over(Window.orderBy(lit(1))).alias("date_id"),"date1")
dfdatesdone = dfdatesnearlydone.withColumnRenamed("date1","date")
display(dfdatesdone)

dfdatesdone.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Gold/dim.date")


# COMMAND ----------

#Add Station to the allocated folder
df = spark.read.load("dbfs:/tmp/Artin/Silver/station", format = "delta")
display(df)
df1 = df.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Gold/dim.station")

# COMMAND ----------

#Creating a column for the trip fact table
from pyspark.sql.functions import datediff, col, current_date
from pyspark.sql.types import StringType, IntegerType
dfrider = spark.read.load("dbfs:/tmp/Artin/Silver/rider", format = "delta")
dfage = dfrider.select(col("birthday"), current_date().alias("current_date"), datediff(current_date(), col("birthday")).alias("datediff"))
dfage.show()

dfagedone = dfage.select(col("datediff")/365.25).alias("age")
display(dfagedone.limit(10))

dfagedonefr = dfagedone.withColumnRenamed("(datediff / 365.25)","age")
dfagedonefr1 = dfagedonefr.select(col("age").cast(IntegerType()))
display(dfagedonefr1.limit(10))

# COMMAND ----------

#Export gold rider file from silver 
rider = spark.read.load("dbfs:/tmp/Artin/Silver/rider", format = "delta")
riders = rider.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Gold/dim.rider")
display(rider.limit(10))

# COMMAND ----------

#Creating the Payment fact table
pay = spark.read.load("dbfs:/tmp/Artin/Silver/payment", format = "delta")
date_dim = spark.read.load("dbfs:/tmp/Artin/Gold/dim.date", format = "delta")
display(pay.limit(10))
display(date_dim)


from pyspark.sql import functions as F
paym = pay.join(date_dim.select("date_id","date"), on="date", how="left")
display(paym)

payme = paym.drop("date")
display(payme)

paymen = payme.select("payment_id", "rider_id", "amount", "date_id")
display(paymen)

paymen.write.format("delta").mode("overwrite").save("dbfs:/tmp/Artin/Gold/face.payment")
