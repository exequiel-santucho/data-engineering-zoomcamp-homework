-- MAKE SURE YOU REPLACE zeta-serenity-412422 WITH THE NAME OF YOUR DATASET! 
-- When you run the query, only run 5 of the ALTER TABLE statements at one time (by highlighting only 5). 
-- Otherwise BigQuery will say too many alterations to the table are being made.

CREATE TABLE zeta-serenity-412422.module4_ny_data_all.green_tripdata AS
SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019;

CREATE TABLE zeta-serenity-412422.module4_ny_data_all.yellow_tripdata AS
SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019;

insert into zeta-serenity-412422.module4_ny_data_all.green_tripdata
SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020;

insert into  zeta-serenity-412422.module4_ny_data_all.yellow_tripdata 
SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020; 

  -- Fixes yellow table schema
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.yellow_tripdata
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.yellow_tripdata
  RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.yellow_tripdata
  RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.yellow_tripdata
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.yellow_tripdata
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.yellow_tripdata
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.yellow_tripdata
  RENAME COLUMN dropoff_location_id TO DOLocationID;

  -- Fixes green table schema
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.green_tripdata
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.green_tripdata
  RENAME COLUMN pickup_datetime TO lpep_pickup_datetime;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.green_tripdata
  RENAME COLUMN dropoff_datetime TO lpep_dropoff_datetime;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.green_tripdata
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.green_tripdata
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.green_tripdata
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE zeta-serenity-412422.module4_ny_data_all.green_tripdata
  RENAME COLUMN dropoff_location_id TO DOLocationID;