CREATE OR REPLACE EXTERNAL TABLE
 `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://baltten/nyc_green_taxi_data_2022/fa6b24a3b7da48169b2dd0cccbdabe06-0.parquet'];


CREATE OR REPLACE TABLE nytaxi.green_tripdata_2022_non_partitoned AS
SELECT * FROM `ny_taxi.external_green_tripdata_2022`

CREATE OR REPLACE TABLE ny_taxi.green_tripdata_2022_non_partitoned_updated AS
SELECT
  *,
 cast(TIMESTAMP_MICROS( CAST(lpep_pickup_datetime / 1000 AS INT64)) as timestamp) AS lpep_pickup_datetime_updated
FROM
  ny_taxi.green_tripdata_2022_non_partitoned;



CREATE OR REPLACE TABLE dtc-de-410504.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime_updated)
CLUSTER BY PUlocationID AS
SELECT * FROM  ny_taxi.green_tripdata_2022_non_partitoned_updated


SELECT distinct PULocationID FROM `dtc-de-410504.ny_taxi.green_tripdata_partitoned_clustered` 
WHERE TIMESTAMP_TRUNC(lpep_pickup_datetime_updated, DAY) BETWEEN TIMESTAMP("2022-06-01") AND TIMESTAMP("2022-06-30")


SELECT distinct PULocationID FROM `dtc-de-410504.ny_taxi.green_tripdata_2022_non_partitoned_updated` 
WHERE TIMESTAMP_TRUNC(lpep_pickup_datetime_updated, DAY) BETWEEN TIMESTAMP("2022-06-01") AND TIMESTAMP("2022-06-30")