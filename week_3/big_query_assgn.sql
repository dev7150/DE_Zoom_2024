CREATE OR REPLACE EXTERNAL TABLE
 `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://baltten/nyc_green_taxi_data_2022/fa6b24a3b7da48169b2dd0cccbdabe06-0.parquet'];