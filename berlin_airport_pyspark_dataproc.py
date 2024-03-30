#!/usr/bin/env python
# coding: utf-8

# In[3]:


from datetime import datetime
from datetime import timedelta
today = datetime.today().strftime('%Y-%m-%d')
yesterday = datetime.today() - timedelta(days=1)


# In[8]:


import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--input_departures', required=True)
parser.add_argument('--input_arrivals', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_departures = args.input_departures
input_arrivals = args.input_arrivals
output = args.output



spark = SparkSession.builder     .appName('test')     .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc_bucket_name')


# In[ ]:


from pyspark.sql import types


# In[ ]:


arrivals_schema = types.StructType([
    types.StructField('flight_id', types.StringType(), True), 
types.StructField('flight_identification_number', types.StringType(), True), 
types.StructField('flight_status', types.StringType(), True), 
types.StructField('flight_status_color', types.StringType(), True), 
types.StructField('flight_aircraft_model', types.StringType(), True), 
types.StructField('flight_airline_name', types.StringType(), True), 
types.StructField('airport_origin_name', types.StringType(), True), 
types.StructField('airport_origin_country', types.StringType(), True), 
types.StructField('airport_origin_city', types.StringType(), True), 
types.StructField('airport_origin_timezone', types.StringType(), True), 
types.StructField('airport_destination_name', types.StringType(), True), 
types.StructField('airport_destination_timezone', types.StringType(), True), 
types.StructField('airport_destination_country', types.StringType(), True), 
types.StructField('airport_destination_city', types.StringType(), True), 
types.StructField('flight_time_scheduled_departure', types.TimestampType(), True), 
types.StructField('flight_time_scheduled_arrival', types.TimestampType(), True), 
types.StructField('flight_time_real_departure', types.TimestampType(), True), 
types.StructField('flight_time_real_arrival', types.TimestampType(), True), 
types.StructField('flight_time_duration_minutes', types.LongType(), True)])


# In[ ]:


departures_schema = types.StructType([
    types.StructField('flight_id', types.StringType(), True), 
types.StructField('flight_identification_number', types.StringType(), True), 
types.StructField('flight_status', types.StringType(), True), 
types.StructField('flight_status_color', types.StringType(), True), 
types.StructField('flight_aircraft_model', types.StringType(), True), 
types.StructField('flight_airline_name', types.StringType(), True), 
types.StructField('airport_origin_name', types.StringType(), True), 
types.StructField('airport_origin_country', types.StringType(), True), 
types.StructField('airport_origin_city', types.StringType(), True), 
types.StructField('airport_origin_timezone', types.StringType(), True), 
types.StructField('airport_destination_name', types.StringType(), True), 
types.StructField('airport_destination_timezone', types.StringType(), True), 
types.StructField('airport_destination_country', types.StringType(), True), 
types.StructField('airport_destination_city', types.StringType(), True), 
types.StructField('flight_time_scheduled_departure', types.TimestampType(), True), 
types.StructField('flight_time_scheduled_arrival', types.TimestampType(), True), 
types.StructField('flight_time_real_departure', types.TimestampType(), True), 
types.StructField('flight_time_real_arrival', types.TimestampType(), True), 
types.StructField('flight_time_duration_minutes', types.LongType(), True)])


# In[ ]:


df_arrivals_spark = spark.read.schema(arrivals_schema).options(delimiter=',',header='true',).csv(input_arrivals)


# In[ ]:


df_departures_spark = spark.read.schema(departures_schema).options(delimiter=',',header='true',).csv(input_departures)



# In[ ]:


df_union = df_departures_spark.union(df_arrivals_spark)


# In[ ]:


df_union.write.format('bigquery')     .option('table', output)     .mode("overwrite")     .save()
    



