# Databricks notebook source
from zipfile import ZipFile

!wget "https://github.com/Artin00/Bikestartschema123/raw/main/payments.zip" -P "/dbfs/tmp/Artin/github"
!wget "https://github.com/Artin00/Bikestartschema123/raw/main/trips.zip" -P "/dbfs/tmp/Artin/github"
!wget "https://github.com/Artin00/Bikestartschema123/raw/main/stations.zip" -P "/dbfs/tmp/Artin/github"
!wget "https://github.com/Artin00/Bikestartschema123/raw/main/riders.zip" -P "/dbfs/tmp/Artin/github"

with ZipFile('/dbfs/tmp/Artin/github/payments.zip','r') as payments:
    
    payments.extract("payments.csv", path='/dbfs/tmp/Artin/landing_2')
payments.close()

with ZipFile('/dbfs/tmp/Artin/github/riders.zip','r') as riders:
    
    riders.extract("riders.csv", path='/dbfs/tmp/Artin/landing_2')
riders.close()

with ZipFile('/dbfs/tmp/Artin/github/trips.zip','r') as trips:
    
    trips.extract("trips.csv", path='/dbfs/tmp/Artin/landing_2')
trips.close()

with ZipFile('/dbfs/tmp/Artin/github/stations.zip','r') as stations:
    
    stations.extract("stations.csv", path='/dbfs/tmp/Artin/landing_2')
stations.close()
