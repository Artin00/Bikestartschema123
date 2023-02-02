# Databricks notebook source
#Gold schema use 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

trips_schema = StructType ([ \
                             StructField("trip_id", StringType(),True), \
                             StructField("rider_id", StringType(),True), \
                             StructField("started_at_date_id", StringType(),True), \
                             StructField("ended_at_date_id", StringType(), True), \
                             StructField("started_at_time_id", StringType(), True), \
                             StructField("ended_at_time_id", StringType(), True), \
                             StructField("bike_id", StringType(), True), \
                           StructField("trip_duration", StringType(), True), \
                           StructField("rider_age", StringType(), True), \
                           StructField("started_station_id", StringType(), True), \
                           StructField("ended_station_id", StringType(), True)])

payments_schema = StructType([ \
                            StructField("payment_id", StringType(), True), \
                            StructField("rider_id", StringType(), True), \
                            StructField("date_id",  StringType(), True), \
                            StructField("amount", StringType(), True)])

date_schema = StructType([ \
                         StructField("date_id", StringType(), True), \
                         StructField("date", StringType(), True)])

time_schema = StructType([ \
                         StructField("time_id", StringType(), True), \
                         StructField("time", StringType(), True)])

stations_schema = StructType([ \
                             StructField("station_id", StringType(), True), \
                             StructField("name", StringType(), True), \
                             StructField("latitude", StringType(), True), \
                             StructField("longitide", StringType(), True)])

bike_schema = StructType([ \
                         StructField("biker_id", StringType(), True), \
                         StructField("ridable_type", StringType(), True)])

rider_schema = StructType([ \
                          StructField("rider_id", StringType(), True), \
                          StructField("first", StringType(), True), \
                          StructField("last", StringType(), True), \
                          StructField("address", StringType(), True), \
                          StructField("birthday", StringType(), True), \
                          StructField("account_start_date", StringType(), True), \
                          StructField("account_end_date", StringType(), True), \
                          StructField("is_member", StringType(), True)])


