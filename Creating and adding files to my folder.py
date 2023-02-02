# Databricks notebook source
#Looking at the different file pathways in dbfs
dbutils.fs.ls('/')

# COMMAND ----------

#Creating a new file path following the guide, produce a landing folder to store the zip files
dbutils.fs.mkdirs('dbfs:/tmp/Artin/landing') 

# COMMAND ----------

#Moving the files by changing their file path
dbutils.fs.mv('dbfs:/FileStore/tables/payments.zip','dbfs:/tmp/Artin/landing/payments.zip')
dbutils.fs.mv('dbfs:/FileStore/tables/trips.zip','dbfs:/tmp/Artin/landing/trips.zip')
dbutils.fs.mv('dbfs:/FileStore/tables/riders.zip','dbfs:/tmp/Artin/landing/riders.zip')
dbutils.fs.mv('dbfs:/FileStore/tables/stations.zip', 'dbfs:/tmp/Artin/landing/stations.zip')

# COMMAND ----------

#Creating the files ready for the next steps
dbutils.fs.mkdirs('dbfs:/tmp/Artin/Bronze')
dbutils.fs.mkdirs('dbfs:/tmp/Artin/Silver')
dbutils.fs.mkdirs('dbfs:/tmp/Artin/Gold')
