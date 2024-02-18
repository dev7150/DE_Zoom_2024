#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-28654135259-txkaul8j')

df_green = spark.read.parquet(input_green)

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet(input_yellow)


df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


common_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]



df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.createOrReplaceTempView('trips_data')


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

df_result.show()
# df_result.write.format('bigquery').option('table','trips_data_all.output').save()


df_result.write.format('bigquery') \
  .option('table', output) \
  .save()


# python spark_sql-Copy1.py \
#     --input_green=data/pq/green/2020/*/ \
#     --input_yellow=data/pq/yellow/2020/*/ \
#     --output=data/report-2020

# URL="spark://de-zoomcamp.australia-southeast2-a.c.dtc-de-410504.internal:7077"

# spark-submit \
#     --master="${URL}" \
#     spark_sql-Copy1.py \
#         --input_green=data/pq/green/2021/*/ \
#         --input_yellow=data/pq/yellow/2021/*/ \
#         --output=data/report-2021

# dtc-de-410504.trips_data_all.reports_2020

# gcloud dataproc jobs submit pyspark \
#     --cluster=de-zoomcamp-cluster \
#     --region=us-central1 \
#     gs://spark-dev-71/code/spark_sql.py \
#     -- \
#         --input_green=gs://spark-dev-71/pq/green/2020/*/ \
#         --input_yellow=gs://spark-dev-71/pq/yellow/2020/*/ \
#         --output=gs://spark-dev-71/report-2020



# gcloud dataproc jobs submit pyspark \
#     --cluster=de-zoomcamp-cluster \
#     --region=us-central1 \
#     --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
#     gs://spark-dev-71/code/spark_sql.py \
#     -- \
#         --input_green=gs://spark-dev-71/pq/green/2020/*/ \
#         --input_yellow=gs://spark-dev-71/pq/yellow/2020/*/ \
#         --output=trips_data_all.reports-2020