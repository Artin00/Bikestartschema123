# Databricks notebook source
from zipfile import ZipFile

with ZipFile('/dbfs/tmp/Artin/landing/payments.zip','r') as payments:
    
    payments.extract("payments.csv", path='/dbfs/tmp/Artin/landing_2')
payments.close()

with ZipFile('/dbfs/tmp/Artin/landing/riders.zip','r') as riders:
    
    riders.extract("riders.csv", path='/dbfs/tmp/Artin/landing_2')
riders.close()

with ZipFile('/dbfs/tmp/Artin/landing/trips.zip','r') as trips:
    
    trips.extract("trips.csv", path='/dbfs/tmp/Artin/landing_2')
trips.close()

with ZipFile('/dbfs/tmp/Artin/landing/stations.zip','r') as stations:
    
    stations.extract("stations.csv", path='/dbfs/tmp/Artin/landing_2')
stations.close()
