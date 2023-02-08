# Databricks notebook source
#Creating the databases

dbutils.fs.mkdirs("dbfs:/tmp/Artin/landing")

dbutils.fs.mkdirs("dbfs:/tmp/Artin/landing_2")

#Bronze folder
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Bronze/payment")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Bronze/rider")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Bronze/station")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Bronze/trip")

#Silver folder
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Silver/payment")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Silver/rider")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Silver/station")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Silver/trip")

#Gold folder
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.station")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.date")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.bike")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.rider")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/dim.time")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/fact.payment")
dbutils.fs.mkdirs("dbfs:/tmp/Artin/Gold/fact.trip")
