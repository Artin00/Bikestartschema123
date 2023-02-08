# Databricks notebook source
#Looking at the different file pathways in dbfs
dbutils.fs.ls('/')

# COMMAND ----------

# MAGIC %md
# MAGIC Upload zip data into "/FileStore/tables/*"

# COMMAND ----------

#Moving the files by changing their file path
dbutils.fs.mv('dbfs:/FileStore/tables/payments.zip','dbfs:/tmp/Artin/landing/payments.zip')
dbutils.fs.mv('dbfs:/FileStore/tables/trips.zip','dbfs:/tmp/Artin/landing/trips.zip')
dbutils.fs.mv('dbfs:/FileStore/tables/riders.zip','dbfs:/tmp/Artin/landing/riders.zip')
dbutils.fs.mv('dbfs:/FileStore/tables/stations.zip', 'dbfs:/tmp/Artin/landing/stations.zip')
