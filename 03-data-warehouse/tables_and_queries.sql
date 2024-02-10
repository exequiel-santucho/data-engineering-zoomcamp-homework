-- Creating EXTERNAL TABLE referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `zeta-serenity-412422.ny_taxi_data.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://module3-ny-green-taxi-data/green_tripdata_2022-*.parquet']
);

-- Check green trip data
SELECT * FROM `zeta-serenity-412422.ny_taxi_data.external_green_tripdata` LIMIT 10;

-- Creating TABLE referring to local above created table
CREATE OR REPLACE TABLE zeta-serenity-412422.ny_taxi_data.green_tripdata AS
SELECT * FROM zeta-serenity-412422.ny_taxi_data.external_green_tripdata
;

--------------
-- Question 1: What is count of records for the 2022 Green Taxi Data??
SELECT count(1) AS counts
FROM `zeta-serenity-412422.ny_taxi_data.external_green_tripdata`;
-- Answer: 840402

--------------
-- Question 2: Write a query to count the distinct number of PULocationID for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT COUNT(DISTINCT PULocationID) AS count_pulocationsid_distinct
FROM zeta-serenity-412422.ny_taxi_data.green_tripdata
;

SELECT COUNT(DISTINCT PULocationID) AS count_pulocationsid_distinct
FROM zeta-serenity-412422.ny_taxi_data.external_green_tripdata
;

-- Answer: In "JOB INFORMATION" we can see that the estimated amount of data that will be read is:
-- for Table 6.41 MB (10 MB billed) and for External Table 0 B (0 B billed)

--------------
-- Question 3: How many records have a fare_amount of 0?
SELECT COUNT(*) AS zero_values
FROM zeta-serenity-412422.ny_taxi_data.external_green_tripdata
WHERE fare_amount=0;

-- Answer: 1622

--------------
-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

-- Answer: for given options the best strategy is "Partition by lpep_pickup_datetime Cluster on PUlocationID",
-- assuming that the most of queries are based upon dates and use it as a filter, and then ordered by PUlocationID.
CREATE OR REPLACE TABLE zeta-serenity-412422.ny_taxi_data.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM zeta-serenity-412422.ny_taxi_data.external_green_tripdata;

-- (Bytes processed 114.11 MB (115 MB Bytes billed)

--------------
-- Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

SELECT DISTINCT PULocationID as distinct_pulocationid
FROM zeta-serenity-412422.ny_taxi_data.green_tripdata
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
;
-- (Bytes processed 12.82 MB (13 MB Bytes billed)

SELECT DISTINCT PULocationID as distinct_pulocationid
FROM zeta-serenity-412422.ny_taxi_data.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
;
-- (Bytes processed 1.12 MB (10 MB Bytes billed)
-- Answer: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

--------------
-- Question 6: Where is the data stored in the External Table you created?

--Answer: Checking the docs at https://cloud.google.com/bigquery/docs/external-data-sources: External tables are similar to standard BigQuery tables, in that these tables store their metadata and schema in BigQuery storage. However, their data resides in an external source.
-- So the answer is: GCP Bucket

--------------
-- Question 7: It is best practice in Big Query to always cluster your data:

-- Answer: True

--------------
--Question 8 (No points): Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
SELECT count(*) FROM zeta-serenity-412422.ny_taxi_data.green_tripdata;

--Answer: This query will process 0 B when run. I think this is that way because storage info (count(*) = number of rows) was calculated when created the table.